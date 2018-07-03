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

package transaction

// ID transaction id definition
type ID string

// Transaction the transaction interface methods
type Transaction interface {
	ID() ID
	GetStatus() Status
	Commit() error
	Rollback() error
}

// New create a new transaction instance
func New(transDefinition Definition) Transaction {
	return &transaction{}
}

type transaction struct {
}

func (t *transaction) ID() ID {
	return ""
}

func (t *transaction) GetStatus() Status {
	// TODO: 需要返回事务的状态
	return nil
}

func (t *transaction) Commit() error {
	// TODO: 实现事务提交逻辑
	return nil
}

func (t *transaction) Rollback() error {
	// TODO:实现事务回滚逻辑
	return nil
}
