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
	"fmt"
	"strconv"

	"github.com/rs/xid"

	"configcenter/src/common/blog"
	"configcenter/src/common/condition"
	frtypes "configcenter/src/common/mapstr"
	"configcenter/src/common/metadata"
	"configcenter/src/scene_server/topo_server/core/model"
	"configcenter/src/scene_server/topo_server/core/types"
)

func setErrors(result frtypes.MapStr, objID, operationKey, errStr string) frtypes.MapStr {

	subResult := frtypes.New()
	if result.Exists(objID) {
		tmp, _ := result.MapStr(objID)
		if nil != tmp {
			subResult = tmp
		}
	}

	if failed, ok := subResult[operationKey]; ok {
		failedArr := failed.([]string)
		failedArr = append(failedArr, errStr)
		subResult.Set(operationKey, failedArr)
	} else {
		subResult.Set(operationKey, []string{
			errStr,
		})
	}
	result[objID] = subResult

	return result
}

func (o *object) getGroup(params types.ContextParams, objID, propertyGroupName string) (model.Group, error) {

	// find group
	grpCond := condition.CreateCondition()
	grpCond.Field(metadata.GroupFieldObjectID).Eq(objID)
	grpCond.Field(metadata.GroupFieldGroupName).Eq(propertyGroupName)
	grps, err := o.grp.FindObjectGroup(params, grpCond)
	if nil != err {
		return nil, err
	}

	if 0 != len(grps) {
		return grps[0], nil // should be only one group
	}

	newGrp := o.modelFactory.CreateGroup(params)
	newGrp.SetName(propertyGroupName)
	newGrp.SetID(xid.New().String())
	newGrp.SetSupplierAccount(params.SupplierAccount)
	newGrp.SetObjectID(objID)
	err = newGrp.Save(nil)

	return newGrp, err
}

func (o *object) CreateObjectBatch(params types.ContextParams, data frtypes.MapStr) (frtypes.MapStr, error) {

	inputData := map[string]ImportObjectData{}
	if err := data.MarshalJSONInto(&inputData); nil != err {
		return nil, err
	}

	result := frtypes.New()
	for objID, inputData := range inputData {

		if err := o.IsValidObject(params, objID); nil != err {
			result = setErrors(result, objID, "errors", fmt.Sprintf("the object(%s) is invalid", objID))
			continue
		}

		// update the object's attribute
		for idx, attr := range inputData.Attr {

			metaAttr := metadata.Attribute{}
			targetAttr, err := metaAttr.Parse(attr)
			targetAttr.OwnerID = params.SupplierAccount
			targetAttr.ObjectID = objID
			if nil != err {
				blog.Error("not found the  objid: %s", objID)
				result = setErrors(result, objID, "errors", err.Error())
				continue
			}

			if 0 == len(targetAttr.PropertyGroupName) {
				targetAttr.PropertyGroup = "Default"
			}

			// find group
			grp, err := o.getGroup(params, objID, targetAttr.PropertyGroupName)
			if nil != err {
				result = setErrors(result, objID, "errors", params.Lang.Languagef("import_row_int_error_str", idx, err))
				continue
			}
			targetAttr.PropertyGroup = grp.GetID()

			// create or update the attribute
			attrID, err := attr.String(metadata.AttributeFieldPropertyID)
			if nil != err {
				result = setErrors(result, objID, "insert_failed", params.Lang.Languagef("import_row_int_error_str", idx, err.Error()))
				continue
			}
			attrCond := condition.CreateCondition()
			attrCond.Field(metadata.AttributeFieldSupplierAccount).Eq(params.SupplierAccount)
			attrCond.Field(metadata.AttributeFieldObjectID).Eq(objID)
			attrCond.Field(metadata.AttributeFieldPropertyID).Eq(attrID)
			attrs, err := o.attr.FindObjectAttribute(params, attrCond)
			if nil != err {
				result = setErrors(result, objID, "insert_failed", params.Lang.Languagef("import_row_int_error_str", idx, err.Error()))
				continue
			}

			if 0 == len(attrs) {

				newAttr := o.modelFactory.CreateAttribute(params)
				if err = newAttr.Save(targetAttr.ToMapStr()); nil != err {
					result = setErrors(result, objID, "insert_failed", params.Lang.Languagef("import_row_int_error_str", idx, err.Error()))
					continue
				}

			}

			for _, newAttr := range attrs {

				if err := newAttr.Update(targetAttr.ToMapStr()); nil != err {
					result = setErrors(result, objID, "update_failed", params.Lang.Languagef("import_row_int_error_str", idx, err.Error()))
					continue
				}

			}

			result = setErrors(result, objID, "success", strconv.FormatInt(idx, 10))
		}

	}

	return result, nil
}
func (o *object) FindObjectBatch(params types.ContextParams, data frtypes.MapStr) (frtypes.MapStr, error) {

	cond := &ExportObjectCondition{}
	if err := data.MarshalJSONInto(cond); nil != err {
		return nil, err
	}

	result := frtypes.New()

	for _, objID := range cond.ObjIDS {
		obj, err := o.FindSingleObject(params, objID)
		if nil != err {
			return nil, err
		}

		attrs, err := obj.GetAttributes()
		if nil != err {
			return nil, err
		}

		result.Set(objID, frtypes.MapStr{
			"attr": attrs,
		})
	}

	return result, nil
}
