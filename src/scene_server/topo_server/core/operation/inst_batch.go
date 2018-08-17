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
	"configcenter/src/common"
	"configcenter/src/common/blog"
	"configcenter/src/common/errors"
	"configcenter/src/scene_server/topo_server/core/model"
	"configcenter/src/scene_server/topo_server/core/types"
)

func (c *commonInst) CreateInstBatch(params types.ContextParams, obj model.Object, batchInfo *InstBatchInfo) (*BatchResult, error) {

	var rowErr map[int64]error
	results := &BatchResult{}
	if common.InputTypeExcel != batchInfo.InputType || nil == batchInfo.BatchInfo {
		return results, nil
	}

	assObjectInt := NewAsstObjectInst(params.Header, params.Engin, params.SupplierAccount, nil)
	assObjectInt.SetMapFields(obj.GetID())
	err := assObjectInt.GetObjAsstObjectPrimaryKey()
	if nil != err {
		blog.Error("failed to read the object att, error is %s ", err.Error())
		return nil, params.Err.Errorf(common.CCErrCommSearchPropertyFailed, err.Error())
		//return fmt.Errorf("get host assocate object  property failure, error:%s", err.Error())
	}
	rowErr, err = assObjectInt.InitInstFromData(*batchInfo.BatchInfo)
	if nil != err {
		blog.Error("failed to read the object att, error is %s ", err.Error())
		return nil, params.Err.Error(common.CCErrTopoInstSelectFailed)
		//return fmt.Errorf("get host assocate object instance data failure, error:%s", err.Error()), nil, nil, nil
	}

	for errIdx, err := range rowErr {
		results.Errors = append(results.Errors, params.Lang.Languagef("import_row_int_error_str", errIdx, err.Error()))
	}

	for colIdx, colInput := range *batchInfo.BatchInfo {
		delete(colInput, "import_from")

		if err := assObjectInt.SetObjAsstPropertyVal(colInput); nil != err {
			results.Errors = append(results.Errors, params.Lang.Languagef("import_row_int_error_str", colIdx, err.Error()))
			continue
		}

		//fmt.Println("input:", colInput)

		item := c.instFactory.CreateInst(params, obj)
		item.SetValues(colInput)

		if item.GetValues().Exists(obj.GetInstIDFieldName()) {
			// check update
			targetInstID, err := item.GetInstID()
			if nil != err {
				blog.Errorf("[operation-inst] failed to get inst id, error info is %s", err.Error())
				results.Errors = append(results.Errors, params.Lang.Languagef("import_row_int_error_str", colIdx, err.Error()))
				continue
			}
			if err = NewSupplementary().Validator(c).ValidatorUpdate(params, obj, item.ToMapStr(), targetInstID, nil); nil != err {
				blog.Errorf("[operation-inst] failed to valid, error info is %s", err.Error())
				results.Errors = append(results.Errors, params.Lang.Languagef("import_row_int_error_str", colIdx, err.Error()))
				continue
			}

		} else {
			// check create
			if err = NewSupplementary().Validator(c).ValidatorCreate(params, obj, item.ToMapStr()); nil != err {
				switch tmpErr := err.(type) {
				case errors.CCErrorCoder:
					if tmpErr.GetCode() != common.CCErrCommDuplicateItem {
						blog.Errorf("[operation-inst] failed to valid, input value(%#v) the instname is %s, error info is %s", item.GetValues(), obj.GetInstNameFieldName(), err.Error())
						results.Errors = append(results.Errors, params.Lang.Languagef("import_row_int_error_str", colIdx, err.Error()))
						continue
					}
				}

			}
		}

		// set data
		err = item.Save(colInput)
		if nil != err {
			blog.Errorf("[operation-inst] failed to save the object(%s) inst data (%#v), error info is %s", obj.GetID(), colInput, err.Error())
			results.Errors = append(results.Errors, params.Lang.Languagef("import_row_int_error_str", colIdx, err.Error()))
			continue
		}
		NewSupplementary().Audit(params, c.clientSet, item.GetObject(), c).CommitCreateLog(nil, nil, item)
		if err := c.setInstAsst(params, obj, item); nil != err {
			blog.Errorf("[operation-inst] failed to set the inst asst, error info is %s", err.Error())
			return nil, err
		}
	} // end foreach batchinfo

	return results, nil
}
