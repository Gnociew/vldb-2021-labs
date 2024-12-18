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

// 该文件定义了与执行 SQL 语句相关的适配器和辅助函数

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

// recordSet wraps an executor, implements sqlexec.RecordSet interface
// 包装执行器（Executor）
type recordSet struct {
	fields     []*ast.ResultField // 字段列表
	executor   Executor           // 执行器
	stmt       *ExecStmt          // 执行语句
	lastErr    error              // 最后的错误
	txnStartTS uint64             // 事务开始时间戳
}

// 返回字段列表
func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = colNames2ResultFields(a.executor.Schema(), a.stmt.OutputNames, a.stmt.Ctx.GetSessionVars().CurrentDB)
	}
	return a.fields
}

// 将列名转换为结果字段列表
func colNames2ResultFields(schema *expression.Schema, names []*types.FieldName, defaultDB string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schema.Len())
	defaultDBCIStr := model.NewCIStr(defaultDB)
	for i := 0; i < schema.Len(); i++ {
		dbName := names[i].DBName
		if dbName.L == "" && names[i].TblName.L != "" {
			dbName = defaultDBCIStr
		}
		origColName := names[i].OrigColName
		if origColName.L == "" {
			origColName = names[i].ColName
		}
		rf := &ast.ResultField{
			Column:       &model.ColumnInfo{Name: origColName, FieldType: *schema.Columns[i].RetType},
			ColumnAsName: names[i].ColName,
			Table:        &model.TableInfo{Name: names[i].OrigTblName},
			TableAsName:  names[i].TblName,
			DBName:       dbName,
		}
		// This is for compatibility.
		// See issue https://github.com/pingcap/tidb/issues/10513 .
		if len(rf.ColumnAsName.O) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.O = rf.ColumnAsName.O[:mysql.MaxAliasIdentifierLen]
		}
		// Usually the length of O equals the length of L.
		// Add this len judgement to avoid panic.
		if len(rf.ColumnAsName.L) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.L = rf.ColumnAsName.L[:mysql.MaxAliasIdentifierLen]
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we update last query found rows in session variable as current found rows.
// The reason we need update is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply update last query found rows by the number of row in chunk.
// 使用 recordSet 的执行器获取下一个可用的 chunk
func (a *recordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.stmt.Text), zap.Stack("stack"))
	}()

	err = Next(ctx, a.executor, req)
	if err != nil {
		a.lastErr = err
		return err
	}
	numRows := req.NumRows()
	if numRows == 0 {
		if a.stmt != nil {
			a.stmt.Ctx.GetSessionVars().LastFoundRows = a.stmt.Ctx.GetSessionVars().StmtCtx.FoundRows()
		}
		return nil
	}
	if a.stmt != nil {
		a.stmt.Ctx.GetSessionVars().StmtCtx.AddFoundRows(uint64(numRows))
	}
	return nil
}

// NewChunk create a chunk base on top-level executor's newFirstChunk().
// 基于顶层执行器的 newFirstChunk 方法创建一个新的 chunk
func (a *recordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(a.executor)
}

// 关闭 recordSet 的执行器
func (a *recordSet) Close() error {
	err := a.executor.Close()
	sessVars := a.stmt.Ctx.GetSessionVars()
	sessVars.PrevStmt = FormatSQL(a.stmt.OriginText())
	return err
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
// 实现了 sqlexec.Statement 接口，用于构建物理执行计划。
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	// 模式信息
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	// 物理执行计划
	Plan plannercore.Plan
	// Text represents the origin query text.
	// 原始查询文本
	Text string

	// 语法树节点
	StmtNode ast.StmtNode

	// 会话上下文
	Ctx sessionctx.Context

	// LowerPriority represents whether to lower the execution priority of a query.
	// 是否降低查询的执行优先级
	LowerPriority bool

	// OutputNames will be set if using cached plan
	// 输出的列名
	OutputNames []*types.FieldName
}

// OriginText returns original statement as a string.
// 返回原始语句的字符串表示
func (a *ExecStmt) OriginText() string {
	return a.Text
}

// IsReadOnly returns true if a statement is read only.
// 返回语句是否为只读
func (a *ExecStmt) IsReadOnly() bool {
	return ast.IsReadOnly(a.StmtNode)
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
// 从物理执行计划构建执行器
func (a *ExecStmt) Exec(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	// 结束后捕获可能的 panic 并记录日志
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(string); !ok {
			panic(r)
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.Text), zap.Stack("stack"))
	}()

	// 获取会话上下文
	sctx := a.Ctx

	// 构建执行器
	///
	var e Executor
	// Hint: step I.4.1
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 调用 ExecStmt.buildExecutor，通过物理执行计划，构建执行器
	e, err = a.buildExecutor()
	if err != nil {
		return nil, err
	}

	// 打开执行器
	///
	// Hint: step I.4.2
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 调用顶层的 Executor.Open 方法后，会传递到其中的子 Executor 当中，
	// 这一操作会递归地将所有的 Executor 都初始化。
	err = e.Open(ctx)
	if err != nil {
		terror.Call(e.Close)
		return nil, err
	}

	// 处理不返回结果的执行器
	// 执行器不返回结果的原因通常是因为执行的 SQL 语句本身不需要返回结果集。
	// 这些语句包括 DML 语句（如 INSERT、UPDATE、DELETE）、DDL 语句（如 CREATE TABLE、DROP TABLE、ALTER TABLE）、事务控制语句（如 BEGIN、COMMIT、ROLLBACK）以及其他控制语句（如 SET、USE）。
	// 这些语句执行后通常只返回受影响的行数或执行状态，而不返回具体的数据结果集。
	if handled, result, err := a.handleNoDelay(ctx, e); handled {
		return result, err
	}

	// 获取事务开始时间戳
	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}

	// 返回结果集
	return &recordSet{
		executor:   e, // 如果执行器会返回结果，那么执行器会被层层返回到 tinysql/server/conn.go 的 clientConn.handleQuery 中
		stmt:       a,
		txnStartTS: txnStartTS,
	}, nil
}

// HandleNoDelay exposes handleNoDelay, only for test usage
func (a *ExecStmt) HandleNoDelay(ctx context.Context, e Executor) (bool, sqlexec.RecordSet, error) {
	return a.handleNoDelay(ctx, e)
}

// handleNoDelay execute the given Executor if it does not return results to the client.
// The first return value stands for if it handle the executor.
// 执行不返回结果的执行器
func (a *ExecStmt) handleNoDelay(ctx context.Context, e Executor) (bool, sqlexec.RecordSet, error) {
	toCheck := e

	// If the executor doesn't return any result to the client, we execute it without delay.
	// 检查执行器是否会返回结果
	if toCheck.Schema().Len() == 0 {
		// Hint: step I.4.3
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 如果这个 Executor 不会返回结果
		// 在 handleNoDelayExecutor 内部立即执行
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	}

	// 返回未处理的执行器
	return false, nil, nil
}

// 通过 Next 函数递归执行执行器
func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	var err error
	defer func() {
		terror.Log(e.Close()) // 关闭执行器，记录可能的错误
	}()

	// Hint: step I.4.3.1
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 通过 Next 函数递归执行 Executor
	// 使用 newFirstChunk 函数来生成存储结果的 Chunk
	err = Next(ctx, e, newFirstChunk(e))
	if err != nil {
		return nil, err
	}
	return nil, err
}

// BuildExecutor exposes buildExecutor, only for test usage
func (a *ExecStmt) BuildExecutor() (Executor, error) {
	return a.buildExecutor()
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
// 从计划构建执行器
func (a *ExecStmt) buildExecutor() (Executor, error) {
	ctx := a.Ctx

	b := newExecutorBuilder(ctx, a.InfoSchema)
	e := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

// FormatSQL is used to format the original SQL, e.g. truncating long SQL, appending prepared arguments.
// 格式化原始 SQL，例如截断长 SQL，附加准备好的参数。
func FormatSQL(sql string) stringutil.StringerFunc {
	return func() string {
		length := len(sql)
		if uint64(length) > logutil.DefaultQueryLogMaxLen {
			sql = fmt.Sprintf("%.*q(len:%d)", logutil.DefaultQueryLogMaxLen, sql, length)
		}
		return QueryReplacer.Replace(sql)
	}
}
