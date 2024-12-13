package commands

// TODO delete the commands package.

import (
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Command is an abstraction which covers the process from receiving a request from gRPC to returning a response.
// 接口，定义了一个事务命令的抽象
type Command interface {
	// 返回命令的上下文信息，通常包含区域信息和请求的元数据
	Context() *kvrpcpb.Context

	// 返回命令的开始时间戳，用于标识事务的唯一性
	StartTs() uint64

	// WillWrite returns a list of all keys that might be written by this command. Return nil if the command is readonly.
	// 返回一个二维字节数组，表示命令可能会写入的所有键。如果命令是只读的，则返回 nil。
	WillWrite() [][]byte

	// Read executes a readonly part of the command. Only called if WillWrite returns nil. If the command needs to write
	// to the DB it should return a non-nil set of keys that the command will write.
	// 执行命令的只读部分。只有当 WillWrite 返回 nil 时才会调用此方法。
	// 如果命令需要写入数据库，则返回一个非空的键集合。
	Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error)

	// PrepareWrites is for building writes in an mvcc transaction. Commands can also make non-transactional
	// reads and writes using txn. Returning without modifying txn means that no transaction will be executed.
	// 用于在 MVCC 事务中构建写操作。命令也可以使用 txn 进行非事务性的读写操作。如果返回而不修改 txn，则不会执行任何事务
	PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error)
}

// Run runs a transactional command.
// 运行一个事务命令。
// 它的作用是协调命令的执行过程，包括读取、写入和事务管理。
func RunCommand(cmd Command, storage storage.Storage, latches *latches.Latches) (interface{}, error) {
	ctxt := cmd.Context() // 获取命令的上下文信息
	var resp interface{}  // 初始化响应变量

	// 获取命令可能会写入的所有键
	keysToWrite := cmd.WillWrite()
	// 只读命令
	if keysToWrite == nil {
		// The command is readonly or requires access to the DB to determine the keys it will write.
		// 从存储中获取一个读取器（Reader），用于读取数据库内容
		reader, err := storage.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		// 创建一个只读事务（RoTxn），使用读取器和命令的开始时间戳
		txn := mvcc.RoTxn{Reader: reader, StartTS: cmd.StartTs()}
		// 调用命令的 Read 方法执行只读操作
		resp, keysToWrite, err = cmd.Read(&txn)
		reader.Close()
		if err != nil {
			return nil, err
		}
	}
	// 写命令
	if keysToWrite != nil {
		// The command will write to the DB.

		// 等待所有需要写入的键的锁，以确保并发安全
		latches.WaitForLatches(keysToWrite)
		defer latches.ReleaseLatches(keysToWrite) // 函数返回时释放

		// 从存储中获取一个读取器（Reader），用于读取数据库内容
		reader, err := storage.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		defer reader.Close() // 在函数返回时关闭

		// Build an mvcc transaction.
		// 创建一个新的 MVCC 事务（MvccTxn），使用读取器和命令的开始时间戳
		txn := mvcc.NewTxn(reader, cmd.StartTs())
		// 调用命令的 PrepareWrites 方法构建写操作
		resp, err = cmd.PrepareWrites(&txn)
		if err != nil {
			return nil, err
		}

		// 验证事务，确保没有冲突
		latches.Validate(&txn, keysToWrite)

		// Building the transaction succeeded without conflict, write all writes to backing storage.
		// 将所有写操作写入存储
		err = storage.Write(ctxt, txn.Writes())
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// 辅助类型
// 提供一些默认的实现，减少具体命令类型的重复代码。具体的命令类型可以嵌入这些辅助类型，并根据需要覆盖相应的方法

// CommandBase provides some default function implementations for the Command interface.
type CommandBase struct {
	context *kvrpcpb.Context
	startTs uint64
}

func (base CommandBase) Context() *kvrpcpb.Context {
	return base.context
}

func (base CommandBase) StartTs() uint64 {
	return base.startTs
}

func (base CommandBase) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	return nil, nil, nil
}

// ReadOnly is a helper type for commands which will never write anything to the database. It provides some default
// function implementations.
type ReadOnly struct{}

func (ro ReadOnly) WillWrite() [][]byte {
	return nil
}

func (ro ReadOnly) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	return nil, nil
}
