// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

// 定义了与 TiDB 会话管理相关的功能和逻辑。
// 主要用于管理和维护 TiDB 会话的状态、执行 SQL 语句、事务处理等。

package session

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// 管理多个 domain.Domain 实例。
type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

// 获取或创建一个 domain.Domain 实例
func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// If this is the only domain instance, and the caller doesn't provide store.
	// 如果只有一个 domain 实例且未提供 store，直接返回该实例
	if len(dm.domains) == 1 && store == nil {
		for _, r := range dm.domains {
			return r, nil
		}
	}

	key := store.UUID()
	d = dm.domains[key]
	if d != nil {
		return
	}

	ddlLease := time.Duration(atomic.LoadInt64(&schemaLease))
	statisticLease := time.Duration(atomic.LoadInt64(&statsLease))
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		logutil.BgLogger().Info("new domain",
			zap.String("store", store.UUID()),
			zap.Stringer("ddl lease", ddlLease),
			zap.Stringer("stats lease", statisticLease))
		factory := createSessionFunc(store)
		sysFactory := createSessionWithDomainFunc(store)
		d = domain.NewDomain(store, ddlLease, statisticLease, factory)
		err1 = d.Init(ddlLease, sysFactory)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			logutil.BgLogger().Error("[ddl] init domain failed",
				zap.Error(err1))
		}
		return true, err1
	})
	if err != nil {
		return nil, err
	}
	dm.domains[key] = d

	return
}

// 删除指定 store 对应的 domain 实例
func (dm *domainMap) Delete(store kv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

// 全局变量
var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	// store.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = int64(1 * time.Second)

	// statsLease is the time for reload stats table.
	statsLease = int64(3 * time.Second)
)

// 设置指定 store 的引导状态
func setStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	storeBootstrapped[storeUUID] = true
}

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
// 设置模式租约时间
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
// 设置统计租约时间
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// DisableStats4Test disables the stats for tests.
// 在测试中禁用统计信息
func DisableStats4Test() {
	SetStatsLease(-1)
}

// Parse parses a query string to raw ast.StmtNode.
// 解析 SQL 查询字符串为抽象语法树（AST）
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	logutil.BgLogger().Debug("compiling", zap.String("source", src))
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, warns, err := p.Parse(src, charset, collation)
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
	}
	if err != nil {
		logutil.BgLogger().Warn("compiling",
			zap.String("source", src),
			zap.Error(err))
		return nil, err
	}
	return stmts, nil
}

// Compile is safe for concurrent use by multiple goroutines.
// 将抽象语法树节点（AST）编译为物理执行计划
func Compile(ctx context.Context, sctx sessionctx.Context, stmtNode ast.StmtNode) (sqlexec.Statement, error) {
	compiler := executor.Compiler{Ctx: sctx}
	stmt, err := compiler.Compile(ctx, stmtNode)
	return stmt, err
}

// 完成 SQL 语句的执行
func finishStmt(ctx context.Context, sctx sessionctx.Context, se *session, sessVars *variable.SessionVars,
	meetsErr error, sql sqlexec.Statement) error {
	if meetsErr != nil {
		if !sessVars.InTxn() {
			logutil.BgLogger().Info("rollbackTxn for ddl/autocommit failed")
			se.RollbackTxn(ctx)
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		var err error
		// Hint: step I.5.2.1
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 调用 session.CommitTxn 提交事务
		err = se.commitTxn(ctx)
		if err != nil {
			if _, ok := sql.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
				err = errors.Annotatef(err, "previous statement: %s", se.GetSessionVars().PrevStmt)
			}
			return err
		}
		return nil
	}

	return checkStmtLimit(ctx, sctx, se)
}

// 检查事务中的语句数量限制
func checkStmtLimit(ctx context.Context, sctx sessionctx.Context, se *session) error {
	// If the user insert, insert, insert ... but never commit, TiDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	history := GetHistory(sctx)
	if history.Count() > 5000 {
		se.RollbackTxn(ctx)
		return errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
			history.Count(), sctx.GetSessionVars().IsAutocommit())
	}
	return err
}

// RunStmt exposes runStmt, only for test usage.
// 暴露 runStmt 函数，仅供测试使用
func RunStmt(ctx context.Context, sctx sessionctx.Context, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	return runStmt(ctx, sctx, s)
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
// 执行 SQL 语句，并提交或回滚当前事务
func runStmt(ctx context.Context, sctx sessionctx.Context, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {

	// 获取会话和会话变量
	se := sctx.(*session)
	sessVars := se.GetSessionVars()

	defer func() {
		// If it is not a select statement, we record its slow log here,
		// then it could include the transaction commit time.
		// 如果结果集为空，记录慢日志
		if rs == nil {
			sessVars.PrevStmt = executor.FormatSQL(s.OriginText())
		}
	}()

	// 检查事务是否已终止
	err = se.checkTxnAborted(s)
	if err != nil {
		return nil, err
	}

	// Hint: step I.3.3
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 调用执行器的 Exec 函数，执行 SQL 语句
	rs, err = s.Exec(ctx)
	sessVars.TxnCtx.StatementCount++
	// 处理非只读语句的提交或回滚
	if !s.IsReadOnly() {
		// Handle the stmt commit/rollback.
		if txn, err1 := sctx.Txn(false); err1 == nil {
			if txn.Valid() {
				if err != nil {
					sctx.StmtRollback()
				} else {
					// Hint: step I.3.4
					// YOUR CODE HERE (lab4)
					// panic("YOUR CODE HERE")
					// 在执行完 Exec 函数后，如果没有出现错误，
					// 则调用 session.StmtCommit 方法将这一条语句 Commit 到整个事务所属的 membuffer 当中去
					// vldb-2021-labs/tinysql/session/txn.go
					err = sctx.StmtCommit()
					if err != nil {
						return nil, err
					}
				}
			}
		} else {
			logutil.BgLogger().Error("get txn failed", zap.Error(err1))
		}
	}
	// 完成 SQL 语句的执行
	err = finishStmt(ctx, sctx, se, sessVars, err, s)

	// 处理挂起的事务状态
	if se.txn.pending() {
		// After run statement finish, txn state is still pending means the
		// statement never need a Txn(), such as:
		//
		// set @@tidb_general_log = 1
		// set @@autocommit = 0
		// select 1
		//
		// Reset txn state to invalid to dispose the pending start ts.
		se.txn.changeToInvalid()
	}

	return rs, err
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
func GetHistory(ctx sessionctx.Context) *StmtHistory {
	hist, ok := ctx.GetSessionVars().TxnCtx.History.(*StmtHistory)
	if ok {
		return hist
	}
	hist = new(StmtHistory)
	ctx.GetSessionVars().TxnCtx.History = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, sctx sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk()
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// ResultSetToStringSlice changes the RecordSet to [][]string.
func ResultSetToStringSlice(ctx context.Context, s Session, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := GetRows4Test(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

// Session errors.
var (
	ErrForUpdateCantRetry = terror.ClassSession.New(mysql.ErrForUpdateCantRetry, mysql.MySQLErrName[mysql.ErrForUpdateCantRetry])
)

func init() {
	sessionMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrForUpdateCantRetry: mysql.ErrForUpdateCantRetry,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSession] = sessionMySQLErrCodes
}
