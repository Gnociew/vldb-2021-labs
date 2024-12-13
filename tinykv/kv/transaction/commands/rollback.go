// 处理事务的回滚操作。
// 事务的回滚是为了撤销未提交的事务，确保数据库的一致性。

package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Rollback struct {
	CommandBase
	request *kvrpcpb.BatchRollbackRequest
}

func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// 准备回滚操作，将未提交的变更撤销
func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// 创建响应对象
	response := new(kvrpcpb.BatchRollbackResponse)

	// 遍历回滚请求中的所有键
	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return response, nil
}

// 回滚单个键的变更
func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	// 获取指定键的锁
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	log.Info("rollbackKey",
		zap.Uint64("startTS", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	// panic("rollbackKey is not implemented yet")

	// 如果锁不存在或锁的时间戳不匹配
	if lock == nil || lock.Ts != txn.StartTS {

		// There is no lock, check the write status.
		// 查找当前事务的写入记录，返回写入记录和提交时间戳
		existingWrite, ts, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		// Try to insert a rollback record if there's no correspond records, use `mvcc.WriteKindRollback` to represent
		// the type. Also the command could be stale that the record is already rolled back or committed.
		// If there is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		// if the key has already been rolled back, so nothing to do.
		// If the key has already been committed. This should not happen since the client should never send both
		// commit and rollback requests.
		// There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		// 尝试插入回滚记录，如果没有对应的记录，使用 `mvcc.WriteKindRollback` 表示类型。
		// 此外，该命令可能是过时的，记录已经回滚或提交。
		// 如果没有写入记录，可能是预写丢失了。无论如何，我们插入一个回滚写入。
		// 如果键已经被回滚，则无需做任何事情。
		// 如果键已经被提交。这不应该发生，因为客户端不应该同时发送提交和回滚请求。

		// 如果没有写入记录，可能是预写丢失了。无论如何，我们插入一个回滚写入。
		if existingWrite == nil {
			// YOUR CODE HERE (lab2).
			// 插入回滚记录
			write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, &write)
			return nil, nil
		} else {
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// The key has already been rolled back, so nothing to do.
				// 键已经被回滚，因此无需做任何事情。
				return nil, nil
			}

			// The key has already been committed. This should not happen since the client should never send both
			// commit and rollback requests.
			// 键已经被提交。这不应该发生，因为客户端不应该同时发送提交和回滚请求。
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
	}

	// 如果锁的类型是 Put，删除键的值
	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}

	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	txn.DeleteLock(key)

	return nil, nil
}

func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}
