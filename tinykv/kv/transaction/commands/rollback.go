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

func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.BatchRollbackResponse)

	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return response, nil
}

func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	// lock, err := txn.GetLock(key)
	// if err != nil {
	// 	return nil, err
	// }
	// log.Info("rollbackKey",
	// 	zap.Uint64("startTS", txn.StartTS),
	// 	zap.String("key", hex.EncodeToString(key)))

	// panic("rollbackKey is not implemented yet")
	// if lock == nil || lock.Ts != txn.StartTS {
	// 	// There is no lock, check the write status.
	// 	existingWrite, ts, err := txn.CurrentWrite(key)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	// Try to insert a rollback record if there's no correspond records, use `mvcc.WriteKindRollback` to represent
	// 	// the type. Also the command could be stale that the record is already rolled back or committed.
	// 	// If there is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
	// 	// if the key has already been rolled back, so nothing to do.
	// 	// If the key has already been committed. This should not happen since the client should never send both
	// 	// commit and rollback requests.
	// 	// There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
	// 	if existingWrite == nil {
	// 		// YOUR CODE HERE (lab2).

	// 		return nil, nil
	// 	} else {
	// 		if existingWrite.Kind == mvcc.WriteKindRollback {
	// 			// The key has already been rolled back, so nothing to do.
	// 			return nil, nil
	// 		}

	// 		// The key has already been committed. This should not happen since the client should never send both
	// 		// commit and rollback requests.
	// 		err := new(kvrpcpb.KeyError)
	// 		err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
	// 		respValue := reflect.ValueOf(response)
	// 		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
	// 		return response, nil
	// 	}
	// }

	// if lock.Kind == mvcc.WriteKindPut {
	// 	txn.DeleteValue(key)
	// }

	// write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	// txn.PutWrite(key, txn.StartTS, &write)
	// txn.DeleteLock(key)

	// return nil, nil

	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	log.Info("rollbackKey",
		zap.Uint64("startTS", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	if lock == nil || lock.Ts != txn.StartTS {
		// There is no lock, check the write status.
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
		// 如果没有相应的记录，则尝试插入一个回滚记录，使用 mvcc.WriteKindRollback 来表示类型。此外，命令可能已经过期，记录已经回滚或已经提交。
		// 如果没有写入操作，可能是预写操作丢失了，我们仍然插入一个回滚写入操作。
		// 如果键已经被回滚，那么不需要做任何操作。
		// 如果键已经被提交，这是不应该发生的，因为客户端不应该同时发送提交和回滚请求。
		// 如果没有写入操作，可能是预写操作丢失了，我们仍然插入一个回滚写入操作。
		if existingWrite == nil {
			// YOUR CODE HERE (lab2).
			write := mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			}
			txn.PutWrite(key, txn.StartTS, &write)
			return nil, nil
		} else {
			// 回滚
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// The key has already been rolled back, so nothing to do.
				return nil, nil
			}

			// 提交
			// The key has already been committed. This should not happen since the client should never send both
			// commit and rollback requests.
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
	}

	// 正常的回滚操作

	// 删除value
	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}
	// 写入回滚记录
	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	// 清理锁
	txn.DeleteLock(key)

	return nil, nil
}

func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}
