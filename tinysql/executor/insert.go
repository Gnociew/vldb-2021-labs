// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// 构建 InsertExec 执行器，用于处理 INSERT 语句。

package executor

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InsertExec represents an insert executor.
// 插入执行器
type InsertExec struct {
	*InsertValues // 嵌入的 InsertValues 结构体，包含插入操作所需的所有信息

	Priority mysql.PriorityEnum // 插入操作的优先级
}

// 执行插入操作
func (e *InsertExec) exec(ctx context.Context, rows [][]types.Datum) error {
	// 获取当前会话的变量
	sessVars := e.ctx.GetSessionVars()
	// 方法返回前清理缓冲区
	defer sessVars.CleanBuffers()
	// 获取当前会话的事务
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	// 初始化缓冲区存储 BufStore
	sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(txn, kv.TempTxnMemBufCap)
	// 更新语句上下文中的记录行数
	sessVars.StmtCtx.AddRecordRows(uint64(len(rows)))

	// 遍历每一行数据，调用 addRecord 方法将数据插入到表中
	for _, row := range rows {
		logutil.BgLogger().Debug("row", zap.Int("col", len(row)))
		var err error
		// 在面向对象编程中，组合（Composition）是一种设计模式，其中一个类包含另一个类的实例，从而实现类之间的关系。
		// 在这种关系中，包含的类被称为“组合类”，而被包含的类被称为“被组合类”。
		// 在 InsertExec 结构体中，InsertValues 结构体被组合到 InsertExec 结构体中。
		// 这意味着 InsertExec 结构体包含一个 InsertValues 结构体的实例，并可以直接访问和使用 InsertValues 结构体的方法和属性。

		// Hint: step II.4
		// YOUR CODE HERE (lab4)
		// 每行数据都会使用被组合的 InsertValues.addRecord 进行写入
		_, err = e.addRecord(ctx, row)
		if err != nil {
			return err
		}
	}
	return nil
}

// Next implements the Executor Next interface.
// 执行插入操作
func (e *InsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	// 重置 chunk
	req.Reset()
	var err error
	// 如果有子执行器（插入操作中嵌入了 Select 语句）
	if len(e.children) > 0 && e.children[0] != nil {
		// Hint: step II.3.2
		// YOUR CODE HERE (lab4)
		// 根据 Select 的 Insert 会使用 insertRowsFromSelect 函数进行处理
		// vldb-2021-labs/tinysql/executor/insert_common.go
		err = insertRowsFromSelect(ctx, e)
		return err
	}
	// Hint: step II.3.1
	// YOUR CODE HERE (lab4)
	// 普通的 Insert 会使用 insertRows 函数进行处理
	// vldb-2021-labs/tinysql/executor/insert_common.go
	err = insertRows(ctx, e)
	return err
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	e.ctx.GetSessionVars().CurrInsertValues = chunk.Row{}
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
// 打开插入执行器
func (e *InsertExec) Open(ctx context.Context) error {
	// 有的 Insert 是根据 Select 的结果写入的，这种情况下 Insert 中嵌入了一条 Select 语句，
	// InsertExec 中也嵌入了一个 SelectionExec，
	// 在 Open 的时候也需要通过 SelectionExec.Open 初始化 SelectionExec
	if e.SelectExec != nil {
		var err error
		// Hint: step II.2
		// YOUR CODE HERE (lab4)
		// 通过 SelectionExec.Open 初始化 SelectionExec
		err = e.SelectExec.Open(ctx)
		return err
	}

	// 如果所有赋值不是常量，调用 initEvalBuffer 方法初始化评估缓冲区
	if !e.allAssignmentsAreConstant {
		e.initEvalBuffer()
	}
	return nil
}
