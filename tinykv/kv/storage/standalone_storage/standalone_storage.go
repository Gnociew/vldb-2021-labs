// Storage 接口的一个单节点版本 StandAloneStorage

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.

package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// 结构体 StandAloneStorage
type StandAloneStorage struct {
	db *badger.DB // badge.DB 实例, 用于存储数据
}

// StandAloneStorage 的构造函数
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneStorage{
		db: db,
	}
}

// Start 方法
// 但在单节点存储中不需要与调度器客户端交互，因此直接返回 nil
// (s *StandAloneStorage) 表示该方法属于 StandAloneStorage 结构体
func (s *StandAloneStorage) Start(_ scheduler_client.Client) error {
	return nil // nil 可以用来表示 error 接口的零值，表示没有错误发生
}

// Stop 方法
func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

// Reader 方法
// ctx 表示一个上下文对象，用于存储一些请求的元数据
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// // YOUR CODE HERE (lab1).
	// panic("not implemented yet")
	// return nil, nil

	// 创建一个新的 Badger 事务
	txn := s.db.NewTransaction(false) // false: 只读事务
	// 创建并返回一个 BadgerReader 实例
	reader := NewBadgerReader(txn)
	return reader, nil
}

// Write 方法
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// YOUR CODE HERE (lab1).
	// Try to check the definition of `storage.Modify` and txn interface of `badger`.
	// As the column family is not supported by `badger`, a wrapper is used to simulate it.
	// panic("not implemented yet")
	// return nil

	// 创建一个新的 Badger 写事务
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	// 遍历 batch 中的每个修改操作
	for _, mod := range batch {
		switch mod.Data.(type) {
		case storage.Put:
			// 处理 Put 操作
			put := mod.Data.(storage.Put)
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			// 处理 Delete 操作
			del := mod.Data.(storage.Delete)
			err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
			if err != nil {
				return err
			}
		}
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// Client 方法
// 在单节点存储中，不需要与调度器客户端交互，因此直接返回 nil
func (s *StandAloneStorage) Client() scheduler_client.Client {
	return nil
}

// 实现 StorageReader 接口
// 在 Go 语言中，接口的实现是隐式的，不需要显式地声明类型实现了某个接口。只要一个类型实现了接口中定义的所有方法，该类型就被认为实现了该接口
type BadgerReader struct {
	txn *badger.Txn // 数据库事务，用于读取数据
}

func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

// 获取指定列族和键的值
func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	// 使用 engine_util.GetCFFromTxn 函数从事务中获取值
	val, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// 迭代指定列族的数据
func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	// 返回一个 engine_util.DBIterator 类型的数据库迭代器，用于遍历列族中的数据
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerReader) Close() {
	b.txn.Discard() // 释放事务
}
