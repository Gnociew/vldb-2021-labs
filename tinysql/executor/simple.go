// Copyright 2016 PingCAP, Inc.
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

// 文件定义了一个简单的 SQL 语句执行器 SimpleExec，
// 用于执行一些简单的 SQL 语句，如 USE、BEGIN、COMMIT 和 ROLLBACK。

package executor

import (
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`,`BeginStmt`, `CommitStmt` and `RollbackStmt`.
// 表示简单语句执行器
type SimpleExec struct {
	baseExecutor

	Statement ast.StmtNode
	done      bool
	is        infoschema.InfoSchema
}

// Next implements the Executor Next interface.
// 实现了 Executor 接口的 Next 方法，用于执行简单的 SQL 语句。
func (e *SimpleExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}

	// 根据语句类型调用相应的执行方法
	switch x := e.Statement.(type) {
	case *ast.UseStmt:
		err = e.executeUse(x)
	case *ast.BeginStmt:
		err = e.executeBegin(ctx, x)
	case *ast.CommitStmt:
		e.executeCommit(x)
	case *ast.RollbackStmt:
		err = e.executeRollback(x)
	}
	e.done = true
	return err
}

// 执行 USE 语句，切换当前数据库
func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := model.NewCIStr(s.DBName)

	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	e.ctx.GetSessionVars().CurrentDB = dbname.O
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := e.ctx.GetSessionVars()
	terror.Log(sessionVars.SetSystemVar(variable.CharsetDatabase, dbinfo.Charset))
	terror.Log(sessionVars.SetSystemVar(variable.CollationDatabase, dbinfo.Collate))
	return nil
}

// 执行 BEGIN 语句，开启一个事务
func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	// create a transaction inside another is equal to commit and begin
	if txnCtx.History != nil {
		var err error
		// Hint: step I.5.1
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 通过 session/session.go 中的 session.NewTxn 函数（被定义在 sessionctx.Context 接口中）来创建一个新的事务
		// 如果此时这个 session 中有尚未提交的事务，NewTxn 会先提交事务后开启一个新事务。
		err = e.ctx.NewTxn(ctx)
		if err != nil {
			return err
		}
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	// Call ctx.Txn(true) to active pending txn.
	var err error
	// Hint: step I.5.1
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// YOUR CODE HERE (lab4)
	// 在开启新事务后，会通过 session.Txn 函数（也被定义在 sessionctx.Context 接口中）等待这个事务获取到 startTS。
	// 此外，begin 时会将环境变量中的 mysql.ServerStatusInTrans 设置为 true。
	_, err = e.ctx.Txn(true)
	return err
}

// 执行 COMMIT 语句，提交事务
func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	// Hint: step I.5.2
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 将 5.1 中的 mysql.ServerStatusInTrans 变量设置为 false
	// 触发 finishStmt 被调用，进而调用 session.CommitTxn 提交事务
	sessVars := e.ctx.GetSessionVars()
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
}

// 执行 ROLLBACK 语句，回滚事务
func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetSessionVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	var (
		txn kv.Transaction
		err error
	)

	// Hint: step I.5.3
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 在 executeRollback 函数内部就对事物进行 Rollback
	// 通过 session.Txn 函数来获取当前事务，但是不会等待事务激活
	txn, err = e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if txn.Valid() {
		sessVars.TxnCtx.ClearDelta()
		// Hint: step I.5.3
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 对事务进行 Rollback
		// 调用这个事务的 Rollback 方法进行清理
		err = txn.Rollback()
		return err
	}
	return nil
}
