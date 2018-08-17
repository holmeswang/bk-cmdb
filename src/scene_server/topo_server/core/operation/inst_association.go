/*
 * Tencent is pleased to support the open source community by making 蓝鲸 available.
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package operation

import (
	"context"
	"strconv"
	"strings"

	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/condition"
	frtypes "configcenter/src/common/mapstr"
	metatype "configcenter/src/common/metadata"
	gparams "configcenter/src/common/paraparse"
	"configcenter/src/scene_server/topo_server/core/inst"
	"configcenter/src/scene_server/topo_server/core/model"
	"configcenter/src/scene_server/topo_server/core/types"
)

func (c *commonInst) setInstAsst(params types.ContextParams, obj model.Object, inst inst.Inst) error {

	currInstID, err := inst.GetInstID()
	if nil != err {
		return err
	}
	attrs, err := obj.GetAttributes()
	if nil != err {
		return err
	}

	assts, err := c.asst.SearchObjectAssociation(params, obj.GetID())
	if nil != err {
		return err
	}

	data := inst.GetValues()
	for _, attr := range attrs {
		if !attr.IsAssociationType() {
			continue
		}

		// extract the assocaition insts ids
		asstVal, exists := data.Get(attr.GetID())
		if !exists {
			continue
		}

		asstInstIDS := []int64{}
		switch targetAssts := asstVal.(type) {
		default:
		case string:
			tmpIDS := strings.Split(targetAssts, common.InstAsstIDSplit)
			for _, asstID := range tmpIDS {
				if 0 == len(strings.TrimSpace(asstID)) {
					continue
				}

				id, err := strconv.ParseInt(asstID, 10, 64)
				if nil != err {
					blog.Errorf("[operation-inst] failed to parse  the value(%s), error info is %s", asstID, err.Error())
					return err
				}
				asstInstIDS = append(asstInstIDS, id)
			}

		case []metatype.InstNameAsst:
			for _, item := range targetAssts {
				asstInstIDS = append(asstInstIDS, item.InstID)
			}
		}

		// update the inst association
		for _, asst := range assts {
			if asst.ObjectAttID != attr.GetID() {
				continue
			}

			// delete the inst asst
			innerCond := condition.CreateCondition()
			innerCond.Field(common.BKObjIDField).Eq(obj.GetID())
			innerCond.Field(common.BKAsstObjIDField).Eq(asst.AsstObjID)
			innerCond.Field(common.BKInstIDField).Eq(currInstID)
			if err = c.asst.DeleteInstAssociation(params, innerCond); nil != err {
				blog.Errorf("[operation-inst] failed to delete inst association,by the condition(%#v), error info is %s", innerCond.ToMapStr(), err.Error())
				return err
			}

			// create a new new asst
			validInstIDS := []string{}
			for _, asstInstID := range asstInstIDS {

				// check the inst id
				if err := c.isValidInstID(params, metatype.Object{ObjectID: asst.AsstObjID}, asstInstID); nil != err {
					blog.Warnf("[operation-int] the association object(%s)' instid(%d) is invalid", asst.AsstObjID, asstInstID)
					continue
				}
				validInstIDS = append(validInstIDS, strconv.Itoa(int(asstInstID)))
				// create a new inst in inst asst table
				if err = c.asst.CreateCommonInstAssociation(params, &metatype.InstAsst{InstID: currInstID, ObjectID: obj.GetID(), AsstInstID: asstInstID, AsstObjectID: asst.AsstObjID}); nil != err {
					blog.Errorf("[operation-inst] failed to create inst association, error info is %s", err.Error())
					return err
				}
			}

			// update asst value
			inst.SetValue(attr.GetID(), strings.Join(validInstIDS, common.InstAsstIDSplit))
			break
		} // end foreach assts

	}

	return nil
}

func (c *commonInst) convertInstIDIntoStruct(params types.ContextParams, asstObj metatype.Association, instIDS []string, needAsstDetail bool) ([]metatype.InstNameAsst, error) {

	obj, err := c.obj.FindSingleObject(params, asstObj.AsstObjID)
	if nil != err {
		return nil, err
	}

	ids := []int64{}
	for _, id := range instIDS {
		if 0 == len(strings.TrimSpace(id)) {
			continue
		}
		idbit, err := strconv.ParseInt(id, 10, 64)
		if nil != err {
			return nil, err
		}

		ids = append(ids, idbit)
	}

	cond := condition.CreateCondition()
	cond.Field(obj.GetInstIDFieldName()).In(ids)

	query := &metatype.QueryInput{}
	query.Condition = cond.ToMapStr()
	query.Limit = common.BKNoLimit
	rsp, err := c.clientSet.ObjectController().Instance().SearchObjects(context.Background(), obj.GetObjectType(), params.Header, query)

	if nil != err {
		blog.Errorf("[operation-inst] failed to request object controller, error info is %s", err.Error())
		return nil, params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
	}

	if common.CCSuccess != rsp.Code {
		blog.Errorf("[operation-inst] faild to delete the object(%s) inst by the condition(%#v), error info is %s", obj.GetID(), cond, rsp.ErrMsg)
		return nil, params.Err.Error(rsp.Code)
	}

	instAsstNames := []metatype.InstNameAsst{}
	for _, instInfo := range rsp.Data.Info {
		instName, err := instInfo.String(obj.GetInstNameFieldName())
		if nil != err {
			return nil, err
		}
		instID, err := instInfo.Int64(obj.GetInstIDFieldName())
		if nil != err {
			return nil, err
		}

		if needAsstDetail {
			instAsstNames = append(instAsstNames, metatype.InstNameAsst{
				ObjID:      obj.GetID(),
				ObjectName: obj.GetName(),
				ObjIcon:    obj.GetIcon(),
				InstID:     instID,
				InstName:   instName,
				InstInfo:   instInfo,
			})
			continue
		}

		instAsstNames = append(instAsstNames, metatype.InstNameAsst{
			ObjID:      obj.GetID(),
			ObjectName: obj.GetName(),
			ObjIcon:    obj.GetIcon(),
			InstID:     instID,
			InstName:   instName,
		})

	}

	return instAsstNames, nil
}

func (c *commonInst) searchAssociationInst(params types.ContextParams, objID string, query *metatype.QueryInput) ([]int64, error) {

	obj, err := c.obj.FindSingleObject(params, objID)
	if nil != err {
		return nil, err
	}

	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		return nil, err
	}
	//fmt.Println("search cond:", searchCond, obj.GetID(), insts)

	instIDS := make([]int64, 0)
	for _, inst := range insts {
		id, err := inst.GetInstID()
		if nil != err {
			return nil, err
		}
		instIDS = append(instIDS, id)
	}

	return instIDS, nil
}

func (c *commonInst) FindInstChildTopo(params types.ContextParams, obj model.Object, instID int64, query *metatype.QueryInput) (count int, results []interface{}, err error) {
	results = []interface{}{}
	if nil == query {
		query = &metatype.QueryInput{}
		cond := condition.CreateCondition()
		cond.Field(obj.GetInstIDFieldName()).Eq(instID)
		cond.Field(common.BKOwnerIDField).Eq(params.SupplierAccount)
		query.Condition = cond.ToMapStr()
	}

	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		return 0, nil, err
	}
	//fmt.Println("cond:", obj.GetID(), query, len(insts))
	tmpResults := map[string]*commonInstTopo{}
	for _, inst := range insts {

		childs, err := inst.GetChildObjectWithInsts()
		if nil != err {
			return 0, nil, err
		}

		for _, child := range childs {

			commonInst, exists := tmpResults[child.Object.GetID()]
			if !exists {
				commonInst = &commonInstTopo{}
				commonInst.ObjectName = child.Object.GetName()
				commonInst.ObjIcon = child.Object.GetIcon()
				commonInst.ObjID = child.Object.GetID()
				commonInst.Children = []metatype.InstNameAsst{}
				tmpResults[child.Object.GetID()] = commonInst
			}

			commonInst.Count = commonInst.Count + len(child.Insts)

			for _, childInst := range child.Insts {

				instAsst := metatype.InstNameAsst{}
				id, err := childInst.GetInstID()
				if nil != err {
					return 0, nil, err
				}

				name, err := childInst.GetInstName()
				if nil != err {
					return 0, nil, err
				}

				instAsst.ID = strconv.Itoa(int(id))
				instAsst.InstID = id
				instAsst.InstName = name
				instAsst.ObjectName = child.Object.GetName()
				instAsst.ObjIcon = child.Object.GetIcon()
				instAsst.ObjID = child.Object.GetID()

				tmpResults[child.Object.GetID()].Children = append(tmpResults[child.Object.GetID()].Children, instAsst)
			}
		}
	}

	for _, subResult := range tmpResults {
		results = append(results, subResult)
	}

	return len(results), results, nil
}

func (c *commonInst) FindInstParentTopo(params types.ContextParams, obj model.Object, instID int64, query *metatype.QueryInput) (count int, results []interface{}, err error) {

	results = []interface{}{}
	if nil == query {
		query = &metatype.QueryInput{}
		cond := condition.CreateCondition()
		cond.Field(obj.GetInstIDFieldName()).Eq(instID)
		cond.Field(common.BKOwnerIDField).Eq(params.SupplierAccount)
		query.Condition = cond.ToMapStr()
	}

	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		return 0, nil, err
	}

	tmpResults := map[string]*commonInstTopo{}
	for _, inst := range insts {

		parents, err := inst.GetParentObjectWithInsts()
		if nil != err {
			return 0, nil, err
		}

		for _, parent := range parents {

			commonInst, exists := tmpResults[parent.Object.GetID()]
			if !exists {
				commonInst = &commonInstTopo{}
				commonInst.ObjectName = parent.Object.GetName()
				commonInst.ObjIcon = parent.Object.GetIcon()
				commonInst.ObjID = parent.Object.GetID()
				commonInst.Children = []metatype.InstNameAsst{}
				tmpResults[parent.Object.GetID()] = commonInst
			}

			commonInst.Count = commonInst.Count + len(parent.Insts)

			for _, parentInst := range parent.Insts {
				instAsst := metatype.InstNameAsst{}
				id, err := parentInst.GetInstID()
				if nil != err {
					return 0, nil, err
				}

				name, err := parentInst.GetInstName()
				if nil != err {
					return 0, nil, err
				}
				instAsst.ID = strconv.Itoa(int(id))
				instAsst.InstID = id
				instAsst.InstName = name
				instAsst.ObjectName = parent.Object.GetName()
				instAsst.ObjIcon = parent.Object.GetIcon()
				instAsst.ObjID = parent.Object.GetID()

				tmpResults[parent.Object.GetID()].Children = append(tmpResults[parent.Object.GetID()].Children, instAsst)
			}
		}
	}

	for _, subResult := range tmpResults {
		results = append(results, subResult)
	}

	return len(results), results, nil
}

func (c *commonInst) FindInstTopo(params types.ContextParams, obj model.Object, instID int64, query *metatype.QueryInput) (count int, results []commonInstTopoV2, err error) {

	if nil == query {
		query = &metatype.QueryInput{}
		cond := condition.CreateCondition()
		cond.Field(obj.GetInstIDFieldName()).Eq(instID)
		cond.Field(common.BKOwnerIDField).Eq(params.SupplierAccount)
		query.Condition = cond.ToMapStr()
	}

	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		blog.Errorf("[operation-inst] failed to find the inst, error info is %s", err.Error())
		return 0, nil, err
	}

	for _, inst := range insts {

		//fmt.Println("the insts:", inst.GetValues(), query)
		id, err := inst.GetInstID()
		if nil != err {
			blog.Errorf("[operation-inst] failed to find the inst, error info is %s", err.Error())
			return 0, nil, err
		}

		name, err := inst.GetInstName()
		if nil != err {
			blog.Errorf("[operation-inst] failed to find the inst, error info is %s", err.Error())
			return 0, nil, err
		}

		commonInst := commonInstTopo{Children: []metatype.InstNameAsst{}}
		commonInst.ObjectName = inst.GetObject().GetName()
		commonInst.ObjID = inst.GetObject().GetID()
		commonInst.ObjIcon = inst.GetObject().GetIcon()
		commonInst.InstID = id
		commonInst.InstName = name

		_, parentInsts, err := c.FindInstParentTopo(params, inst.GetObject(), id, nil)
		if nil != err {
			blog.Errorf("[operation-inst] failed to find the inst, error info is %s", err.Error())
			return 0, nil, err
		}

		_, childInsts, err := c.FindInstChildTopo(params, inst.GetObject(), id, nil)
		if nil != err {
			blog.Errorf("[operation-inst] failed to find the inst, error info is %s", err.Error())
			return 0, nil, err
		}

		results = append(results, commonInstTopoV2{
			Prev: parentInsts,
			Next: childInsts,
			Curr: commonInst,
		})

	}

	return len(results), results, nil
}

func (c *commonInst) FindInstByAssociationInst(params types.ContextParams, obj model.Object, data frtypes.MapStr) (cont int, results []inst.Inst, err error) {

	asstParamCond := &AssociationParams{}

	if err := data.MarshalJSONInto(asstParamCond); nil != err {
		blog.Errorf("[operation-inst] find inst by association inst , error info is %s", err.Error())
		return 0, nil, params.Err.Errorf(common.CCErrTopoInstSelectFailed, err.Error())
	}

	instCond := map[string]interface{}{}
	instCond[common.BKOwnerIDField] = params.SupplierAccount
	if obj.IsCommon() {
		instCond[common.BKObjIDField] = obj.GetID()
	}
	targetInstIDS := []int64{}

	for keyObjID, objs := range asstParamCond.Condition {
		// Extract the ID of the instance according to the associated object.
		cond := map[string]interface{}{}
		if common.GetObjByType(keyObjID) == common.BKINnerObjIDObject {
			cond[common.BKObjIDField] = keyObjID
			cond[common.BKOwnerIDField] = params.SupplierAccount
		}

		for _, objCondition := range objs {

			if objCondition.Operator != common.BKDBEQ {

				if obj.GetID() == keyObjID {
					// deal self condition
					instCond[objCondition.Field] = map[string]interface{}{
						objCondition.Operator: objCondition.Value,
					}
					continue
				}

				// deal association condition
				cond[objCondition.Field] = map[string]interface{}{
					objCondition.Operator: objCondition.Value,
				}

				continue
			}

			if obj.GetID() == keyObjID {
				// deal self condition
				switch t := objCondition.Value.(type) {
				case string:
					instCond[objCondition.Field] = map[string]interface{}{
						common.BKDBEQ: gparams.SpeceialCharChange(t),
					}
				default:
					instCond[objCondition.Field] = objCondition.Value
				}

				continue
			}

			// deal association condition
			cond[objCondition.Field] = objCondition.Value

		} // end foreach objs

		if obj.GetID() == keyObjID {
			// no need to search the association objects
			continue
		}

		innerCond := new(metatype.QueryInput)
		if fields, ok := asstParamCond.Fields[keyObjID]; ok {
			innerCond.Fields = strings.Join(fields, ",")
		}
		innerCond.Condition = cond

		asstInstIDS, err := c.searchAssociationInst(params, keyObjID, innerCond)
		if nil != err {
			blog.Errorf("[operation-inst]failed to search the association inst, error info is %s", err.Error())
			return 0, nil, err
		}
		blog.V(4).Infof("[FindInstByAssociationInst] search association insts, keyObjID %s, condition: %v, results: %v", keyObjID, innerCond, asstInstIDS)

		query := &metatype.QueryInput{}
		query.Condition = map[string]interface{}{
			"bk_asst_inst_id": map[string]interface{}{
				common.BKDBIN: asstInstIDS,
			},
			"bk_asst_obj_id": keyObjID,
			"bk_obj_id":      obj.GetID(),
		}

		asstInst, err := c.asst.SearchInstAssociation(params, query)
		if nil != err {
			blog.Errorf("[operation-inst] failed to search the association inst, error info is %s", err.Error())
			return 0, nil, err
		}

		for _, asst := range asstInst {
			targetInstIDS = append(targetInstIDS, asst.InstID)
		}
		blog.V(4).Infof("[FindInstByAssociationInst] search association, objectID=%s, keyObjID=%s, condition: %v, results: %v", obj.GetID(), keyObjID, query, targetInstIDS)

	} // end foreach conditions

	if 0 != len(targetInstIDS) {
		instCond[obj.GetInstIDFieldName()] = map[string]interface{}{
			common.BKDBIN: targetInstIDS,
		}
	} else if 0 != len(asstParamCond.Condition) {
		if _, ok := asstParamCond.Condition[obj.GetID()]; !ok {
			instCond[obj.GetInstIDFieldName()] = map[string]interface{}{
				common.BKDBIN: targetInstIDS,
			}
		}
	}

	query := &metatype.QueryInput{}
	query.Condition = instCond
	if fields, ok := asstParamCond.Fields[obj.GetID()]; ok {
		query.Fields = strings.Join(fields, ",")
	}
	query.Limit = asstParamCond.Page.Limit
	query.Sort = asstParamCond.Page.Sort
	query.Start = asstParamCond.Page.Start
	blog.V(4).Infof("[FindInstByAssociationInst] search inst condition: %v", instCond)
	return c.FindInst(params, obj, query, false)
}
