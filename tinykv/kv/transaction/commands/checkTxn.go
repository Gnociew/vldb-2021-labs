// 检查事务的状态。
// 这个文件的主要作用是处理事务状态检查请求，确定事务是否已经提交、回滚或仍然处于进行中。

package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.LockTs,
		},
		request: request,
	}
}

// 准备写操作，将事务状态检查结果写入数据库
func (c *CheckTxnStatus) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	key := c.request.PrimaryKey
	response := new(kvrpcpb.CheckTxnStatusResponse)

	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// 如果存在锁并且指定的键在给定的事务开始时间戳被锁定
	if lock != nil && lock.Ts == txn.StartTS {
		// 锁已过期（锁的创建时间加上生存时间小于当前时间）
		// 如果锁已过期，系统需要将锁回滚，并标记事务为失败
		if physical(lock.Ts)+lock.Ttl < physical(c.request.CurrentTs) {

			// YOUR CODE HERE (lab2).
			// Lock has expired, try to rollback it. `mvcc.WriteKindRollback` could be used to
			// represent the type. Try using the interfaces provided by `mvcc.MvccTxn`.
			// 锁已过期，尝试回滚它。可以使用 `mvcc.WriteKindRollback` 表示类型。
			// 尝试使用 `mvcc.MvccTxn` 提供的接口。

			log.Info("checkTxnStatus rollback the primary lock as it's expired",
				zap.Uint64("lock.TS", lock.Ts),
				zap.Uint64("lock.Ttl", lock.Ttl),
				zap.Uint64("physical(lock.TS)", physical(lock.Ts)),
				zap.Uint64("txn.StartTS", txn.StartTS),
				zap.Uint64("currentTS", c.request.CurrentTs),
				zap.Uint64("physical(currentTS)", physical(c.request.CurrentTs)))

			// 插入回滚记录
			write := mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindRollback} // 创建一个回滚记录
			txn.PutWrite(key, lock.Ts, &write)                                  // 将回滚记录写入事务
			txn.DeleteLock(key)
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
			response.Action = kvrpcpb.Action_TTLExpireRollback // 设置响应的操作类型，表明锁已过期，进行了回滚操作
			// var Action_name = map[int32]string{
			// 	0: "NoAction",
			// 	1: "TTLExpireRollback",
			// 	2: "LockNotExistRollback",
			// }
		} else {
			// 锁未过期
			// Lock has not expired, leave it alone.
			response.Action = kvrpcpb.Action_NoAction
			// 设置响应的锁生存时间为锁的生存时间，告知调用方锁的剩余有效期
			response.LockTtl = lock.Ttl
		}

		return response, nil

	}

	// 如果 lock == nil，表示目标键没有锁。
	// 可能是由于事务预写失败、锁被清理、或者客户端发起了无效请求。
	// 需要检查写入记录（通过 txn.CurrentWrite(key)）来判断是否已经存在提交记录或回滚记录。
	// 如果不存在任何记录，插入回滚记录，确保一致性。
	// 如果 lock != nil 且 lock.Ts != txn.StartTS，说明目标键上的锁属于其他事务，当前事务无权操作该锁。
	// 需要检查写入记录，判断当前事务的状态。根据记录类型（提交或回滚）执行相应的操作，确保事务一致性。

	// 检查写入记录
	existingWrite, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	// panic("CheckTxnStatus is not implemented yet")

	// 不存在任何记录，插入回滚记录，确保一致性
	if existingWrite == nil {

		// YOUR CODE HERE (lab2).
		// The lock never existed, it's still needed to put a rollback record on it so that
		// the stale transaction commands such as prewrite on the key will fail.
		// Note try to set correct `response.Action`,
		// the action types could be found in kvrpcpb.Action_xxx.
		// 锁从未存在，仍然需要在其上放置回滚记录，以便使诸如键上的预写等过时的事务命令失败。
		// 尝试设置正确的 `response.Action`，操作类型可以在 `kvrpcpb.Action_xxx` 中找到。

		write := mvcc.Write{StartTS: c.request.LockTs, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, c.request.LockTs, &write)
		// 插入一条回滚记录，标记该键的操作失败
		// 即使锁不存在，插入回滚记录可以明确标记该键的状态，避免其他事务误读
		response.Action = kvrpcpb.Action_LockNotExistRollback
		return response, nil
	}

	if existingWrite.Kind == mvcc.WriteKindRollback {
		// The key has already been rolled back, so nothing to do.
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}

	// The key has already been committed.
	// 已提交
	response.CommitVersion = commitTs
	response.Action = kvrpcpb.Action_NoAction
	return response, nil
}

// 从时间戳中提取物理时间部分
func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func (c *CheckTxnStatus) WillWrite() [][]byte {
	return [][]byte{c.request.PrimaryKey}
}
