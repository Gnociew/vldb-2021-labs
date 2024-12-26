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

// 处理分布式事务中的锁解析逻辑

package tikv

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ResolvedCacheSize is max number of cached txn status.
// 缓存的事务状态的最大数量
const ResolvedCacheSize = 2048

// bigTxnThreshold : transaction involves keys exceed this threshold can be treated as `big transaction`.
// 涉及键数超过此阈值的事务可以被视为“大事务”
const bigTxnThreshold = 16

// LockResolver resolves locks and also caches resolved txn status.
// When transaction execution meets unexpected error, there may be some not-committed neither not-rollback keys left.
// LockResolver deal with such keys when encountered.
// 负责解析锁并缓存已解析的事务状态
type LockResolver struct {
	store Storage // 存储接口

	// 包含一个读写锁和两个缓存结构
	mu struct {
		sync.RWMutex
		resolved       map[uint64]TxnStatus // 缓存已解析的事务状态（FIFO，事务ID -> 事务状态）
		recentResolved *list.List           // 最近解析的事务状态列表
	}
}

func newLockResolver(store Storage) *LockResolver {
	r := &LockResolver{
		store: store,
	}
	r.mu.resolved = make(map[uint64]TxnStatus)
	r.mu.recentResolved = list.New()
	return r
}

// NewLockResolver is exported for other pkg to use, suppress unused warning.
var _ = NewLockResolver

// NewLockResolver creates a LockResolver.
// It is exported for other pkg to use. For instance, binlog service needs
// to determine a transaction's commit state.
// 创建一个新的 LockResolver 实例，并导出供其他包使用
func NewLockResolver(etcdAddrs []string) (*LockResolver, error) {
	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))

	spkv, err := NewEtcdSafePointKV(etcdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(), false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s.lockResolver, nil
}

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
// 事务的最终状态，可以是锁定、提交或回滚
type TxnStatus struct {
	ttl      uint64         // 事务的生存时间
	commitTS uint64         // 事务的提交时间戳
	action   kvrpcpb.Action // 事务的操作类型
}

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { return s.ttl == 0 && s.commitTS > 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { return s.commitTS }

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.

// 锁的默认生存时间
var defaultLockTTL uint64 = 3000

// TODO: Consider if it's appropriate.
// 锁的最大生存时间
var maxLockTTL uint64 = 120000

// ttl = ttlFactor * sqrt(writeSizeInMiB)
// 计算锁的生存时间
var ttlFactor = 6000

// Lock represents a lock from tikv server.
// 事务的最终状态，可以是锁定、提交或回滚
type Lock struct {
	Key      []byte
	Primary  []byte
	TxnID    uint64
	TTL      uint64
	TxnSize  uint64
	LockType kvrpcpb.Op
}

// 返回锁的字符串表示形式
func (l *Lock) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("key: ")
	prettyWriteKey(buf, l.Key)
	buf.WriteString(", primary: ")
	prettyWriteKey(buf, l.Primary)
	return fmt.Sprintf("%s, txnStartTS: %d, ttl: %d, type: %s", buf.String(), l.TxnID, l.TTL, l.LockType)
}

// NewLock creates a new *Lock.
// 创建一个新的 Lock 实例
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:     l.GetKey(),
		Primary: l.GetPrimaryLock(),
		TxnID:   l.GetLockVersion(),
		TTL:     l.GetLockTtl(),
	}
}

// 创建一个新的 Lock 实例
func (lr *LockResolver) saveResolved(txnID uint64, status TxnStatus) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		return
	}
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > ResolvedCacheSize {
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

// 获取已解析的事务状态
func (lr *LockResolver) getResolved(txnID uint64) (TxnStatus, bool) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

// ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
//  1. Use the `lockTTL` to pick up all expired locks. Only locks that are too
//     old are considered orphan locks and will be handled later. If all locks
//     are expired then all locks will be resolved so the returned `ok` will be
//     true, otherwise caller should sleep a while before retry.
//  2. For each lock, query the primary key to get txn(which left the lock)'s
//     commit status.
//  3. Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
//     the same transaction.

// 解析一组锁，并返回事务过期时间、推送的事务 ID 列表以及可能的错误
// /
// 通常在以下情况下被调用：
// 1.事务冲突：当一个事务在提交时发现有未决的锁（即其他事务持有的锁）阻止其提交时，会调用 ResolveLocks 来尝试解析这些锁。
// 2.锁超时：当检测到某些锁已经超出了预期的生存时间（TTL）时，系统会调用 ResolveLocks 来检查这些锁的状态并进行处理。
// 3.事务恢复：在系统崩溃或重启后，恢复过程中可能会调用 ResolveLocks 来清理未决的锁，以确保数据一致性
// /
// 解析锁（Resolve Lock）是指处理分布式事务中的锁，以确定锁的最终状态并采取相应的操作。
func (lr *LockResolver) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, []uint64 /*pushed*/, error) {
	var msBeforeTxnExpired txnExpireTime // 事务过期时间

	// 如果没有锁，则返回事务过期时间
	if len(locks) == 0 {
		return msBeforeTxnExpired.value(), nil, nil
	}

	var pushFail bool
	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[RegionVerID]struct{}) // 已解析的事务
	pushed := make([]uint64, 0, len(locks))                // 推送的事务 ID 列表

	// 遍历锁列表，获取每个锁的事务状态
	// 如果获取事务状态时发生错误，更新事务过期时间并返回错误
	// 锁的事务状态是指与特定锁相关的事务的当前状态，锁的事务状态用于确定锁是否已经被提交、回滚，或者仍然处于进行中。
	for _, l := range locks {
		status, err := lr.getTxnStatusFromLock(bo, l, callerStartTS)
		if err != nil {
			msBeforeTxnExpired.update(0)
			err = errors.Trace(err)
			return msBeforeTxnExpired.value(), nil, err
		}

		// 如果事务状态的 TTL 为 0，表示事务已提交或回滚，解析锁
		if status.ttl == 0 {
			cleanRegions, exists := cleanTxns[l.TxnID]
			if !exists {
				cleanRegions = make(map[RegionVerID]struct{})
				cleanTxns[l.TxnID] = cleanRegions
			}

			err = lr.resolveLock(bo, l, status, cleanRegions)
			// 如果发生错误，更新事务过期时间并返回错误
			if err != nil {
				msBeforeTxnExpired.update(0)
				err = errors.Trace(err)
				return msBeforeTxnExpired.value(), nil, err
			}
		} else {
			// 否则，更新事务过期时间
			// 追踪当前锁关联事务的剩余 TTL（时间到期前的存活时间），并基于此决定后续处理逻辑。
			msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, status.ttl)
			msBeforeTxnExpired.update(msBeforeLockExpired)
			// In the write conflict scenes, callerStartTS is set to 0 to avoid unnecessary push minCommitTS operation.
			if callerStartTS > 0 {
				pushFail = true
				continue
			}
		}
	}
	if pushFail {
		// If any of the lock fails to push minCommitTS, don't return the pushed array.
		// 如果 pushFail 为真，不返回推送的事务 ID 列表
		pushed = nil
	}

	return msBeforeTxnExpired.value(), pushed, nil
}

// 跟踪事务的过期时间
type txnExpireTime struct {
	initialized bool
	txnExpire   int64
}

// 更新事务的过期时间
func (t *txnExpireTime) update(lockExpire int64) {
	if lockExpire <= 0 {
		lockExpire = 0
	}
	if !t.initialized {
		t.txnExpire = lockExpire
		t.initialized = true
		return
	}
	if lockExpire < t.txnExpire {
		t.txnExpire = lockExpire
	}
}

// 获取事务的过期时间
func (t *txnExpireTime) value() int64 {
	if !t.initialized {
		return 0
	}
	return t.txnExpire
}

// GetTxnStatus queries tikv-server for a txn's status (commit/rollback).
// If the primary key is still locked, it will launch a Rollback to abort it.
// To avoid unnecessarily aborting too many txns, it is wiser to wait a few
// seconds before calling it after Prewrite.
// 查询 TiKV 服务器以获取事务的状态（提交或回滚）。如果主键仍然被锁定，它将启动回滚以中止事务。
func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error) {
	var status TxnStatus
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	currentTS, err := lr.store.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return status, err
	}
	return lr.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, true)
}

// getTxnStatusFromLock gets transaction status from given lock
// 获取给定锁的事务状态
func (lr *LockResolver) getTxnStatusFromLock(bo *Backoffer, l *Lock, callerStartTS uint64) (TxnStatus, error) {
	var currentTS uint64
	var err error
	var status TxnStatus
	currentTS, err = lr.store.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return TxnStatus{}, err
	}

	rollbackIfNotExist := false
	for {
		status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, currentTS, rollbackIfNotExist)
		if err == nil {
			return status, nil
		}
		return TxnStatus{}, err
	}
}

// getTxnStatus sends the CheckTxnStatus request to the TiKV server.
// When rollbackIfNotExist is false, the caller should be careful with the txnNotFoundErr error.
func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool) (TxnStatus, error) {

	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}

	// CheckTxnStatus may meet the following cases:
	// 1. LOCK
	// 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
	// 1.2 Lock TTL -- active transaction holding the lock.
	// 2. NO LOCK
	// 2.1 Txn Committed
	// 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
	// 2.3 No lock -- concurrence prewrite.

	var status TxnStatus
	var req *tikvrpc.Request
	// build the request
	// YOUR CODE HERE (lab3).
	// 构建 CheckTxnStatus 请求
	req = tikvrpc.NewRequest(
		tikvrpc.CmdCheckTxnStatus,
		&kvrpcpb.CheckTxnStatusRequest{
			PrimaryKey: primary,
			CurrentTs:  currentTS,
			LockTs:     txnID,
		})

	for {
		// 获取锁的 Region 信息
		loc, err := lr.store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return status, errors.Trace(err)
		}

		// 发送请求并获取响应
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return status, errors.Trace(err)
		}

		// 检查是否有 Region 错误
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return status, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String())) // 重试
			if err != nil {
				return status, errors.Trace(err)
			}
			continue
		}

		// 检查响应是否为空
		if resp.Resp == nil {
			return status, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		logutil.BgLogger().Debug("cmdResp", zap.Bool("nil", cmdResp == nil))

		// Assign status with response
		// YOUR CODE HERE (lab3).
		// 解析响应体，将事务状态赋值给 status 变量
		status.ttl = cmdResp.LockTtl
		status.commitTS = cmdResp.CommitVersion
		status.action = cmdResp.Action

		return status, nil
	}

}

// resolveLock resolve the lock for the given transaction status which is checked from primary key.
// If status is committed, the secondary should also be committed.
// If status is not committed and the
func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status TxnStatus, cleanRegions map[RegionVerID]struct{}) error {
	// 判断事务是否为大事务，如果是，则需要清理整个区域
	cleanWholeRegion := l.TxnSize >= bigTxnThreshold

	for {
		// 获取锁所在的区域位置
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[loc.Region]; ok { // 如果该区域已经被清理过，则直接返回
			return nil
		}

		// YOUR CODE HERE (lab3).
		// 构建 ResolveLock 请求
		var req *tikvrpc.Request
		req = tikvrpc.NewRequest(
			tikvrpc.CmdResolveLock,
			&kvrpcpb.ResolveLockRequest{
				StartVersion:  l.TxnID,
				CommitVersion: status.commitTS,
			})

		// 发送请求并获取响应
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}

		// 检查是否有 Region 错误
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}

		// 检查响应是否为空
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}

		// 解析响应体，检查是否有键错误
		cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected resolve err: %s, lock: %v", keyErr, l)
			logutil.BgLogger().Error("resolveLock error", zap.Error(err))
			return err
		}
		if cleanWholeRegion {
			cleanRegions[loc.Region] = struct{}{}
		}
		return nil
	}
}
