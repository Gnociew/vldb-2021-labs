// 文件定义了 Commit 类型及其相关方法，用于处理事务的提交操作。
// 事务的提交是两阶段提交（2PC）的第二阶段，确保所有预写的变更最终被提交到数据库中。

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

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// 准备提交操作，将预写的变更提交到数据库中
func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion

	// YOUR CODE HERE (lab2).
	// Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
	// report unexpected error.
	// 检查 commitTs 是否有效，commitTs 必须大于事务的 startTs。如果不是，则报告意外错误。

	// panic("PrepareWrites is not implemented for commit command")

	response := new(kvrpcpb.CommitResponse)
	if commitTs <= txn.StartTS {
		response.Error = &kvrpcpb.KeyError{
			Retryable: fmt.Sprintf("invalid commitTs %v for startTs %v", commitTs, txn.StartTS)}
		return response, nil
	}

	// Commit each key.
	// 遍历请求中的所有键，逐个提交
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

// 提交单个键的变更
func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	// 获取键上的锁
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	// / 如果没有对应的锁用于此事务。
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))

	// 如果锁不存在或锁的时间戳不匹配
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab2).
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.
		// 键被不同的事务锁定，或者键上没有锁。需要检查此键的提交/回滚记录，如果没有找到任何记录，则报告锁未找到错误。
		// 此外，提交请求可能是过时的，因为它已经提交或回滚。

		// 锁不存在
		// 1.目标键上的锁已被清理（可能是由其他事务清理的垃圾锁，或是因为事务回滚导致的锁释放）。
		// 2.提交请求可能是过时的，即事务在此前已经完成了提交或回滚。
		// 锁的时间戳不匹配
		// 1.当前事务尝试提交的锁时间戳与键上的锁时间戳不同，说明锁属于其他事务。
		// 2.这通常是因为事务并发导致的冲突，或者是事务请求重试时访问了其他事务的锁

		// 检查提交记录
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {

			// 如果找到当前事务的提交记录
			if write.Kind != mvcc.WriteKindRollback && write.StartTS == txn.StartTS {
				log.Warn("stale commit request", zap.Uint64("start_ts", txn.StartTS),
					zap.String("key", hex.EncodeToString(key)),
					zap.Uint64("commit_ts", commitTs))
				return nil, nil // 当前事务已经提交，返回成功，视为幂等操作

				// 如果找到当前事物的回滚记录
			} else if write.Kind == mvcc.WriteKindRollback && write.StartTS == txn.StartTS {
				respValue := reflect.ValueOf(response)
				// 表明原因
				keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("The transaction with stast_ts %v has been rolled back ", txn.StartTS)}
				reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
				return response, nil

				// 提交记录属于其他事务，返回写入冲突错误
			} else {
				keyError := &kvrpcpb.KeyError{
					Retryable: fmt.Sprintf("WriteConflict detected on key %s with conflict", string(key)),
				}
				log.Warn("write conflict detected", zap.Uint64("start_ts", txn.StartTS),
					zap.String("key", hex.EncodeToString(key)),
					zap.Uint64("conflict_ts", write.StartTS))
				return keyError, nil
			}
		}

		// 没有找到提交或回滚记录，返回锁未找到错误
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	// Commit a Write object to the DB
	// 将 Write 对象提交到数据库
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	// 解锁键
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
