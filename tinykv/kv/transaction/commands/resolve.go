// 处理事务中的锁解析操作。
// 锁解析是为了处理事务中的锁，确保事务的提交或回滚操作能够顺利进行。

package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ResolveLock struct {
	CommandBase
	request  *kvrpcpb.ResolveLockRequest
	keyLocks []mvcc.KlPair
}

func NewResolveLock(request *kvrpcpb.ResolveLockRequest) ResolveLock {
	return ResolveLock{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// 准备写操作，将锁解析结果写入数据库
func (rl *ResolveLock) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// A map from start timestamps to commit timestamps which tells us whether a transaction (identified by start ts)
	// has been committed (and if so, then its commit ts) or rolled back (in which case the commit ts is 0).

	// 这是一个从 事务的开始时间戳（start timestamps）到 提交时间戳（commit timestamps）的映射，用于表示每个事务的状态：
	// 如果事务已提交：
	//		该映射记录事务的开始时间戳（start ts）与其对应的提交时间戳（commit ts）。
	// 如果事务已回滚：
	//		该映射将事务的提交时间戳设置为 0，表示事务已被回滚。

	// 获取提交版本
	commitTs := rl.request.CommitVersion
	response := new(kvrpcpb.ResolveLockResponse)

	log.Info("There keys to resolve",
		zap.Uint64("lockTS", txn.StartTS),
		zap.Int("number", len(rl.keyLocks)),
		zap.Uint64("commit_ts", commitTs))
	// panic("ResolveLock is not implemented yet")
	for _, kl := range rl.keyLocks {
		// YOUR CODE HERE (lab2).
		// Try to commit the key if the transaction is committed already, or try to rollback the key if it's not.
		// The `commitKey` and `rollbackKey` functions could be useful.
		// 尝试提交键，如果事务已经提交；或者尝试回滚键，如果事务未提交。
		// `commitKey` 和 `rollbackKey` 函数可能会有用。
		log.Debug("resolve key", zap.String("key", hex.EncodeToString(kl.Key)))
		if commitTs > 0 {
			// 提交键
			_, err := commitKey(kl.Key, commitTs, txn, response)
			if err != nil {
				return nil, err
			}
		} else {
			// 回滚键
			_, err := rollbackKey(kl.Key, txn, response)
			if err != nil {
				return nil, err
			}
		}
	}

	return response, nil
}

func (rl *ResolveLock) WillWrite() [][]byte {
	return nil
}

func (rl *ResolveLock) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	// Find all locks where the lock's transaction (start ts) is in txnStatus.
	txn.StartTS = rl.request.StartVersion
	keyLocks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	rl.keyLocks = keyLocks
	keys := [][]byte{}
	for _, kl := range keyLocks {
		keys = append(keys, kl.Key)
	}
	return nil, keys, nil
}
