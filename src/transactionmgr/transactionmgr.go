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

package transactionmgr

import (
	"configcenter/src/transactionmgr/transaction"
)

// TransactionMgr transaction manager
type TransactionMgr interface {
	GetTransaction() transaction.Transaction
	Suspend(tran transaction.Transaction) error
	Resume(tran transaction.Transaction) error
}

// New create a new transaction manager
func New() TransactionMgr {
	return &transactionmgr{}
}

type transactionmgr struct {
	stat *statistics
}

func (t *transactionmgr) GetTransaction() transaction.Transaction {
	return nil
}

func (t *transactionmgr) Suspend(tran transaction.Transaction) error {
	return nil
}

func (t *transactionmgr) Resume(tran transaction.Transaction) error {
	return nil
}
