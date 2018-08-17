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
	"strings"

	"configcenter/src/apimachinery"
	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/condition"
	frtypes "configcenter/src/common/mapstr"
	metatype "configcenter/src/common/metadata"
	"configcenter/src/scene_server/topo_server/core/inst"
	"configcenter/src/scene_server/topo_server/core/model"
	"configcenter/src/scene_server/topo_server/core/types"
)

// InstOperationInterface inst operation methods
type InstOperationInterface interface {
	CreateInst(params types.ContextParams, obj model.Object, data frtypes.MapStr) (inst.Inst, error)
	CreateInstBatch(params types.ContextParams, obj model.Object, batchInfo *InstBatchInfo) (*BatchResult, error)
	DeleteInst(params types.ContextParams, obj model.Object, cond condition.Condition) error
	DeleteInstByInstID(params types.ContextParams, obj model.Object, instID []int64) error
	FindInst(params types.ContextParams, obj model.Object, cond *metatype.QueryInput, needAsstDetail bool) (count int, results []inst.Inst, err error)
	FindInstByAssociationInst(params types.ContextParams, obj model.Object, data frtypes.MapStr) (cont int, results []inst.Inst, err error)
	FindInstChildTopo(params types.ContextParams, obj model.Object, instID int64, query *metatype.QueryInput) (count int, results []interface{}, err error)
	FindInstParentTopo(params types.ContextParams, obj model.Object, instID int64, query *metatype.QueryInput) (count int, results []interface{}, err error)
	FindInstTopo(params types.ContextParams, obj model.Object, instID int64, query *metatype.QueryInput) (count int, results []commonInstTopoV2, err error)
	UpdateInst(params types.ContextParams, data frtypes.MapStr, obj model.Object, cond condition.Condition, instID int64) error

	SetProxy(modelFactory model.Factory, instFactory inst.Factory, asst AssociationOperationInterface, obj ObjectOperationInterface)
}

// NewInstOperation create a new inst operation instance
func NewInstOperation(client apimachinery.ClientSetInterface) InstOperationInterface {
	return &commonInst{
		clientSet: client,
	}
}

type FieldName string
type AssociationObjectID string
type RowIndex int
type InputKey string
type InstID int64

type asstObjectAttribute struct {
	obj   model.Object
	attrs []model.Attribute
}

type BatchResult struct {
	Errors       []string `json:"error"`
	Success      []string `json:"success"`
	UpdateErrors []string `json:"update_error"`
}

type commonInst struct {
	clientSet    apimachinery.ClientSetInterface
	modelFactory model.Factory
	instFactory  inst.Factory
	asst         AssociationOperationInterface
	obj          ObjectOperationInterface
}

func (c *commonInst) SetProxy(modelFactory model.Factory, instFactory inst.Factory, asst AssociationOperationInterface, obj ObjectOperationInterface) {
	c.modelFactory = modelFactory
	c.instFactory = instFactory
	c.asst = asst
	c.obj = obj
}

func (c *commonInst) innerHasHost(params types.ContextParams, moduleIDS []int64) (bool, error) {
	cond := map[string][]int64{
		common.BKModuleIDField: moduleIDS,
	}

	rsp, err := c.clientSet.HostController().Module().GetModulesHostConfig(context.Background(), params.Header, cond)
	if nil != err {
		blog.Errorf("[operation-module] failed to request the object controller, error info is %s", err.Error())
		return false, params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
	}

	if !rsp.Result {
		blog.Errorf("[operation-module]  failed to search the host module configures, error info is %s", err.Error())
		return false, params.Err.New(rsp.Code, rsp.ErrMsg)
	}

	return 0 != len(rsp.Data), nil
}
func (c *commonInst) hasHost(params types.ContextParams, targetInst inst.Inst) ([]deletedInst, bool, error) {

	id, err := targetInst.GetInstID()
	if nil != err {
		return nil, false, err
	}

	targetObj := targetInst.GetObject()
	if !targetObj.IsCommon() {
		if targetObj.GetObjectType() == common.BKInnerObjIDModule {
			exists, err := c.innerHasHost(params, []int64{id})
			if nil != err {
				return nil, false, err
			}

			if exists {

				return nil, true, nil
			}
		}
	}

	instIDS := []deletedInst{}
	instIDS = append(instIDS, deletedInst{instID: id, obj: targetObj})
	childInsts, err := targetInst.GetMainlineChildInst()
	if nil != err {
		return nil, false, err
	}

	for _, childInst := range childInsts {

		ids, exists, err := c.hasHost(params, childInst)
		if nil != err {
			return nil, false, err
		}
		if exists {
			return instIDS, true, nil
		}
		instIDS = append(instIDS, ids...)
	}

	return instIDS, false, nil
}
func (c *commonInst) isValidInstID(params types.ContextParams, obj metatype.Object, instID int64) error {

	cond := condition.CreateCondition()
	cond.Field(obj.GetInstIDFieldName()).Eq(instID)
	if obj.IsCommon() {
		cond.Field(common.BKObjIDField).Eq(obj.ObjectID)
	}

	query := &metatype.QueryInput{}
	query.Condition = cond.ToMapStr()
	query.Limit = common.BKNoLimit

	rsp, err := c.clientSet.ObjectController().Instance().SearchObjects(context.Background(), obj.GetObjectType(), params.Header, query)
	if nil != err {
		blog.Errorf("[operation-inst] failed to request object controller, error info is %s", err.Error())
		return params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
	}

	if common.CCSuccess != rsp.Code {
		blog.Errorf("[operation-inst] faild to delete the object(%s) inst by the condition(%#v), error info is %s", obj.ObjectID, cond, rsp.ErrMsg)
		return params.Err.New(rsp.Code, rsp.ErrMsg)
	}

	if rsp.Data.Count > 0 {
		return nil
	}

	return params.Err.Error(common.CCErrTopoInstSelectFailed)
}

func (c *commonInst) CreateInst(params types.ContextParams, obj model.Object, data frtypes.MapStr) (inst.Inst, error) {

	// create new insts
	blog.V(3).Infof("the data inst:%#v", data)
	item := c.instFactory.CreateInst(params, obj)
	item.SetValues(data)

	if err := NewSupplementary().Validator(c).ValidatorCreate(params, obj, item.ToMapStr()); nil != err {
		blog.Errorf("[operation-inst] valid is bad, the data is (%#v)  error info is %s", item.ToMapStr(), err.Error())
		return nil, err
	}

	if err := item.Create(); nil != err {
		blog.Errorf("[operation-inst] failed to save the object(%s) inst data (%#v), error info is %s", obj.GetID(), data, err.Error())
		return nil, err
	}

	NewSupplementary().Audit(params, c.clientSet, item.GetObject(), c).CommitCreateLog(nil, nil, item)

	if err := c.setInstAsst(params, obj, item); nil != err {
		blog.Errorf("[operation-inst] failed to set the inst asst, error info is %s", err.Error())
		return nil, err
	}

	return item, nil
}

func (c *commonInst) DeleteInstByInstID(params types.ContextParams, obj model.Object, instID []int64) error {

	cond := condition.CreateCondition()
	cond.Field(common.BKOwnerIDField).Eq(params.SupplierAccount)
	cond.Field(obj.GetInstIDFieldName()).In(instID)
	if obj.IsCommon() {
		cond.Field(common.BKObjIDField).Eq(obj.GetID())
	}

	query := &metatype.QueryInput{}
	query.Condition = cond.ToMapStr()

	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		return err
	}

	deleteIDS := []deletedInst{}
	for _, inst := range insts {
		ids, exists, err := c.hasHost(params, inst)
		if nil != err {
			return params.Err.Error(common.CCErrTopoHasHostCheckFailed)
		}

		if exists {
			return params.Err.Error(common.CCErrTopoHasHostCheckFailed)
		}

		deleteIDS = append(deleteIDS, ids...)
	}

	for _, delInst := range deleteIDS {
		preAudit := NewSupplementary().Audit(params, c.clientSet, obj, c).CreateSnapshot(delInst.instID, condition.CreateCondition().ToMapStr())

		innerCond := condition.CreateCondition()
		innerCond.Field(common.BKAsstObjIDField).Eq(obj.GetID())
		innerCond.Field(common.BKOwnerIDField).Eq(params.SupplierAccount)
		innerCond.Field(common.BKAsstInstIDField).Eq(delInst.instID)
		err := c.asst.CheckBeAssociation(params, obj, innerCond)
		if nil != err {
			return err
		}

		if err := c.asst.DeleteInstAssociation(params, innerCond); nil != err {
			blog.Errorf("[operation-inst] failed to set the inst asst, error info is %s", err.Error())
			return err
		}

		innerCond = condition.CreateCondition()
		innerCond.Field(common.BKObjIDField).Eq(obj.GetID())
		innerCond.Field(common.BKOwnerIDField).Eq(params.SupplierAccount)
		innerCond.Field(common.BKInstIDField).Eq(delInst)
		if err := c.asst.DeleteInstAssociation(params, innerCond); nil != err {
			blog.Errorf("[operation-inst] failed to set the inst asst, error info is %s", err.Error())
			return err
		}

		// clear association
		rsp, err := c.clientSet.ObjectController().Instance().DelObject(context.Background(), obj.GetObjectType(), params.Header, cond.ToMapStr())

		if nil != err {
			blog.Errorf("[operation-inst] failed to request object controller, error info is %s", err.Error())
			return params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
		}

		if common.CCSuccess != rsp.Code {
			blog.Errorf("[operation-inst] faild to delete the object(%s) inst by the condition(%#v), error info is %s", obj.GetID(), cond.ToMapStr(), rsp.ErrMsg)
			return params.Err.Error(rsp.Code)
		}

		NewSupplementary().Audit(params, c.clientSet, obj, c).CommitDeleteLog(preAudit, nil, nil)

	}
	return nil
}

func (c *commonInst) DeleteInst(params types.ContextParams, obj model.Object, cond condition.Condition) error {

	// clear inst associations
	query := &metatype.QueryInput{}
	query.Limit = common.BKNoLimit
	query.Condition = cond.ToMapStr()

	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		blog.Errorf("[operation-inst] failed to search insts by the condition(%#v), error info is %s", cond.ToMapStr(), err.Error())
		return err
	}
	for _, inst := range insts {
		targetInstID, err := inst.GetInstID()
		if nil != err {
			return err
		}
		err = c.DeleteInstByInstID(params, obj, []int64{targetInstID})
		if nil != err {
			return err
		}

	}

	return nil
}

func (c *commonInst) FindOriginInst(params types.ContextParams, obj model.Object, cond *metatype.QueryInput) (*metatype.InstResult, error) {

	switch obj.GetID() {
	case common.BKInnerObjIDHost:
		rsp, err := c.clientSet.HostController().Host().GetHosts(context.Background(), params.Header, cond)
		if nil != err {
			blog.Errorf("[operation-inst] failed to request object controller, error info is %s", err.Error())
			return nil, params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
		}

		if common.CCSuccess != rsp.Code {

			blog.Errorf("[operation-inst] faild to delete the object(%s) inst by the condition(%#v), error info is %s", obj.GetID(), cond, rsp.ErrMsg)
			return nil, params.Err.New(rsp.Code, rsp.ErrMsg)
		}

		return &metatype.InstResult{Count: rsp.Data.Count, Info: frtypes.NewArrayFromInterface(rsp.Data.Info)}, nil

	default:

		rsp, err := c.clientSet.ObjectController().Instance().SearchObjects(context.Background(), obj.GetObjectType(), params.Header, cond)

		if nil != err {
			blog.Errorf("[operation-inst] failed to request object controller, error info is %s", err.Error())
			return nil, params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
		}

		if common.CCSuccess != rsp.Code {

			blog.Errorf("[operation-inst] faild to delete the object(%s) inst by the condition(%#v), error info is %s", obj.GetID(), cond, rsp.ErrMsg)
			return nil, params.Err.New(rsp.Code, rsp.ErrMsg)
		}
		return &rsp.Data, nil
	}

}
func (c *commonInst) FindInst(params types.ContextParams, obj model.Object, cond *metatype.QueryInput, needAsstDetail bool) (count int, results []inst.Inst, err error) {

	rsp, err := c.FindOriginInst(params, obj, cond)
	if nil != err {
		blog.Errorf("[operation-inst] failed to find origin inst , error info is %s", err.Error())
		return 0, nil, err
	}

	asstObjAttrs, err := c.asst.SearchObjectAssociation(params, obj.GetID())
	if nil != err {
		blog.Errorf("[operation-inst] failed to search object associations, error info is %s", err.Error())
		return 0, nil, err
	}

	for idx, instInfo := range rsp.Info {

		for _, attrAsst := range asstObjAttrs {
			if attrAsst.ObjectAttID == common.BKChildStr || attrAsst.ObjectAttID == common.BKInstParentStr {
				continue
			}

			if !instInfo.Exists(attrAsst.ObjectAttID) { // the inst data is old, but the attribute is new.
				continue
			}

			asstFieldValue, err := instInfo.String(attrAsst.ObjectAttID)
			if nil != err {
				blog.Errorf("[operation-inst] failed to get the inst'attr(%s) value int the data(%#v), error info is %s", attrAsst.ObjectAttID, instInfo, err.Error())
				return 0, nil, err
			}
			instVals, err := c.convertInstIDIntoStruct(params, attrAsst, strings.Split(asstFieldValue, ","), needAsstDetail)
			if nil != err {
				blog.Errorf("[operation-inst] failed to convert association asst(%#v) origin value(%#v) value(%s), error info is %s", attrAsst, instInfo, asstFieldValue, err.Error())
				return 0, nil, err
			}
			rsp.Info[idx].Set(attrAsst.ObjectAttID, instVals)

		}
	}
	return rsp.Count, inst.CreateInst(params, c.clientSet, obj, rsp.Info), nil
}

func (c *commonInst) UpdateInst(params types.ContextParams, data frtypes.MapStr, obj model.Object, cond condition.Condition, instID int64) error {

	if err := NewSupplementary().Validator(c).ValidatorUpdate(params, obj, data, instID, cond); nil != err {
		return err
	}

	// update association
	query := &metatype.QueryInput{}
	query.Condition = cond.ToMapStr()
	query.Limit = common.BKNoLimit
	if 0 < instID {
		innerCond := condition.CreateCondition()
		innerCond.Field(obj.GetInstIDFieldName()).Eq(instID)
		query.Condition = innerCond.ToMapStr()
	}
	_, insts, err := c.FindInst(params, obj, query, false)
	if nil != err {
		blog.Errorf("[operation-inst] failed to search insts by the condition(%#v), error info is %s", cond.ToMapStr(), err.Error())
		return err
	}
	for _, inst := range insts {

		data.ForEach(func(key string, val interface{}) {
			inst.SetValue(key, val)
		})

		if err := c.setInstAsst(params, obj, inst); nil != err {
			blog.Errorf("[operation-inst] failed to set the inst asst, error info is %s", err.Error())
			return err
		}
	}

	// update insts
	inputParams := frtypes.New()
	inputParams.Set("data", data)
	inputParams.Set("condition", cond.ToMapStr())
	preAuditLog := NewSupplementary().Audit(params, c.clientSet, obj, c).CreateSnapshot(-1, cond.ToMapStr())
	rsp, err := c.clientSet.ObjectController().Instance().UpdateObject(context.Background(), obj.GetObjectType(), params.Header, inputParams)

	if nil != err {
		blog.Errorf("[operation-inst] failed to request object controller, error info is %s", err.Error())
		return params.Err.Error(common.CCErrCommHTTPDoRequestFailed)
	}

	if common.CCSuccess != rsp.Code {
		blog.Errorf("[operation-inst] faild to set the object(%s) inst by the condition(%#v), error info is %s", obj.GetID(), cond.ToMapStr(), rsp.ErrMsg)
		return params.Err.New(rsp.Code, rsp.ErrMsg)
	}
	currAuditLog := NewSupplementary().Audit(params, c.clientSet, obj, c).CreateSnapshot(-1, cond.ToMapStr())
	NewSupplementary().Audit(params, c.clientSet, obj, c).CommitUpdateLog(preAuditLog, currAuditLog, nil)
	return nil
}
