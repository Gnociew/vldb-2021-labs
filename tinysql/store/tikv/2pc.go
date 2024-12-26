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

package tikv

import (
	"bytes"
	"context"
	"math"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// 两阶段提交操作的接口
type twoPhaseCommitAction interface {
	// 处理单个批次的提交操作
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchKeys) error
	// 返回操作的字符串表示
	String() string
}

type actionPrewrite struct{} // 预写操作
type actionCommit struct{}   // 提交操作
type actionCleanup struct{}  // 清理操作

// 接口实现
var (
	_ twoPhaseCommitAction = actionPrewrite{}
	_ twoPhaseCommitAction = actionCommit{}
	_ twoPhaseCommitAction = actionCleanup{}
)

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionCommit) String() string {
	return "commit"
}

func (actionCleanup) String() string {
	return "cleanup"
}

// twoPhaseCommitter executes a two-phase commit protocol.
// 包含执行两阶段提交所需的各种信息和状态
type twoPhaseCommitter struct {
	store     *TinykvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*mutationEx // 变更操作的映射，键是字符串，值是 mutationEx 结构体
	lockTTL   uint64
	commitTS  uint64
	connID    uint64         // 日志记录的连接 ID
	cleanWg   sync.WaitGroup // 等待清理操作完成的同步等待组
	txnSize   int

	primaryKey []byte

	mu struct {
		sync.RWMutex
		// undeterminedErr 字段用于保存提交主键时遇到的 RPC 错误。
		// 在分布式事务的两阶段提交（2PC）过程中，提交主键是一个关键步骤。
		// 如果在提交主键时遇到错误，可能会导致事务的状态不确定，即事务可能已经部分提交，但由于错误的发生，无法确定事务的最终状态。
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
}

// batchExecutor is txn controller providing rate control like utils
// 事务控制器，提供速率控制等功能
type batchExecutor struct {
	rateLim           int                  // concurrent worker numbers
	rateLimiter       *rateLimit           // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter   // here maybe more different type committer in the future
	action            twoPhaseCommitAction // the work action type
	backoffer         *Backoffer           // Backoffer
	tokenWaitDuration time.Duration        // get token wait time
}

// 对 pb.Mutation 的扩展，表示一个变更操作
type mutationEx struct {
	pb.Mutation
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		connID:        connID,
		regionTxnSize: map[uint64]int{},
	}, nil
}

// The txn mutations buffered in `txn.us` before commit.
// Your task is to convert buffer to KV mutations in order to execute as a transaction.
// This function runs before commit execution
// txn.us 中缓冲的事务变更操作。
// 你的任务是将缓冲区转换为 KV 变更操作，以便作为事务执行。
// 这个函数在提交执行之前运行
func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var (
		keys    [][]byte // 所有参与事务的键
		size    int      // 事务的大小
		putCnt  int
		delCnt  int
		lockCnt int
	)
	mutations := make(map[string]*mutationEx) // 变更操作的映射，tinykv/proto/proto/kvrpcpb.proto
	txn := c.txn                              // 当前事务

	// tinysql/kv/buffer_store.go
	// 遍历 BufferStore 中所有缓冲的键值对，并对每个键值对执行给定的函数 f

	// 定义了一个匿名函数
	// 大括号 {} 的内容是回调函数的具体实现
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		// In membuffer, there are 2 kinds of mutations
		//   put: there is a new value in membuffer
		//   delete: there is a nil value in membuffer
		// You need to build the mutations from membuffer here
		// 在 membuffer 中，有两种变更操作
		//   put: membuffer 中有一个新值
		//   delete: membuffer 中的值为 nil
		// 你需要在这里从 membuffer 构建变更操作
		// MemBuffer 是一个内存缓冲区，用于暂存事务中的键值对操作。
		// 在分布式事务处理中，变更操作（如插入、删除等）首先会被暂存在 MemBuffer 中，直到事务提交时才会被实际应用到存储中。

		// put 操作
		if len(v) > 0 {
			// `len(v) > 0` means it's a put operation.
			// YOUR CODE HERE (lab3).
			// panic("YOUR CODE HERE")
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:    pb.Op_Put,
					Key:   k,
					Value: v,
				},
			}
			putCnt++
		} else {
			// `len(v) == 0` means it's a delete operation.
			// YOUR CODE HERE (lab3).
			// panic("YOUR CODE HERE")

			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Del,
					Key: k,
				},
			}
			delCnt++
		}

		// Update the keys array and statistic information
		// // 更新 keys 数组和统计信息
		// YOUR CODE HERE (lab3).
		// panic("YOUR CODE HERE")

		keys = append(keys, k)
		size += len(k) + len(v)
		return nil
	})
	if err != nil {
		// 退出，不会继续执行后续代码
		return errors.Trace(err)
	}

	// In prewrite phase, there will be a lock for every key
	// If the key is already locked but is not updated, you need to write a lock mutation for it to prevent lost update
	// Don't forget to update the keys array and statistic information
	// 在预写阶段，每个键都会有一个锁
	// 如果键已经被锁定但未更新，你需要为其写入一个锁变更操作以防止丢失更新
	// 不要忘记更新 keys 数组和统计信息

	// txn.lockKeys 事物中需要加锁的键的集合
	for _, lockKey := range txn.lockKeys {
		// YOUR CODE HERE (lab3).
		_, ok := mutations[string(lockKey)]
		if !ok {
			// panic("YOUR CODE HERE")
			mutations[string(lockKey)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Lock,
					Key: lockKey,
				},
			}
			lockCnt++
			keys = append(keys, lockKey)
			size += len(lockKey)
		}
	}

	// fmt.Printf("keys: %v\n", keys)

	if len(keys) == 0 {
		return nil
	}
	// 将累计的大小存储到事务的 txnSize 属性中
	c.txnSize = size

	if size > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(size)
	}

	// 记录大事务的日志
	const logEntryCount = 10000     // 日志条目计数的阈值
	const logSize = 4 * 1024 * 1024 // 日志大小的阈值，4MB
	if len(keys) > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(keys[0])
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", len(keys)),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	// 检查事务开始时间戳的有效性
	// 如果事务的开始时间戳等于 math.MaxUint64，则认为这是一个无效的时间戳
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	c.keys = keys
	c.mutations = mutations
	c.lockTTL = txnLockTTL(txn.startTime, size)
	return nil
}

// 返回事务的主键
// 如果 primaryKey 为空，则返回 keys 数组中的第一个键
func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.keys[0]
	}
	return c.primaryKey
}

// 计算事务的锁 TTL（生存时间）
// 根据事务的读取时间增加锁 TTL。通过比较当前时间戳和 startTS + lockTTL 来决定是否清理锁。
// 如果事务读取时间较长，增加其 TTL 有助于防止在预写后立即被中止
const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > maxLockTTL {
			lockTTL = maxLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

// doActionOnKeys groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
// There are three kind of actions which implement the twoPhaseCommitAction interface.
// actionPrewrite prewrites a transaction
// actionCommit commits a transaction
// actionCleanup rollbacks a transaction
// This function split the keys by region and parallel execute the batches in a transaction using given action

// 将键分组为主批次和次批次，并对它们执行指定的操作
// 使用 GroupKeysByRegion 方法将键按 Region 分组。
// 确保包含主键的组优先处理。
// 如果操作是提交或清理，首先处理主批次。
// 如果操作是提交，次批次在后台 goroutine 中提交以减少延迟。
// 否则，直接处理次批次
func (c *twoPhaseCommitter) doActionOnKeys(bo *Backoffer, action twoPhaseCommitAction, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	var batches []batchKeys
	var sizeFunc = c.keySize
	if _, ok := action.(actionPrewrite); ok {
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if len(bo.errors) == 0 {
			for region, keys := range groups {
				c.regionTxnSize[region.id] = len(keys)
			}
		}
		sizeFunc = c.keyValueSize
	}
	// Make sure the group that contains primary key goes first.
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	_, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	if firstIsPrimary && (actionIsCommit || actionIsCleanup) {
		// primary should be committed/cleanup first
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if actionIsCommit {
		// Commit secondary batches in background goroutine to reduce latency.
		// The backoffer instance is created outside of the goroutine to avoid
		// potential data race in unit test since `CommitMaxBackoff` will be updated
		// by test suites.
		secondaryBo := NewBackoffer(context.Background(), CommitMaxBackoff).WithVars(c.txn.vars)
		go func() {
			e := c.doActionOnBatches(secondaryBo, action, batches)
			if e != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e))
			}
		}()
	} else {
		err = c.doActionOnBatches(bo, action, batches)
	}
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
// 并行地对批次执行指定的操作
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchKeys) error {
	if len(batches) == 0 {
		return nil
	}

	if len(batches) == 1 {
		e := action.handleSingleBatch(c, bo, batches[0])
		if e != nil {
			logutil.BgLogger().Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
		}
		return errors.Trace(e)
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > 32 {
		rateLim = 32
	}
	batchExecutor := newBatchExecutor(rateLim, c, action, bo)
	err := batchExecutor.process(batches)
	return errors.Trace(err)
}

// 计算键和值的总大小
func (c *twoPhaseCommitter) keyValueSize(key []byte) int {
	size := len(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		size += len(mutation.Value)
	}
	return size
}

// 计算键的大小
func (c *twoPhaseCommitter) keySize(key []byte) int {
	return len(key)
}

// You need to build the prewrite request in this function
// All keys in a batch are in the same region
// 你需要在这个函数中构建预写请求
// 一个批次中的所有键都位于同一个 Region
// 构建预写请求指在分布式事务的两阶段提交协议（2PC）的预写阶段中，
// 生成一个 RPC 请求，将事务中的变更操作打包为一个请求结构（PrewriteRequest），并发送到对应的存储节点
func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchKeys) *tikvrpc.Request {
	var req *pb.PrewriteRequest
	// Build the prewrite request from the input batch,
	// should use `twoPhaseCommitter.primary` to ensure that the primary key is not empty.
	// YOUR CODE HERE (lab3).
	var mutations []*pb.Mutation
	// 遍历批次中的所有键
	for _, key := range batch.keys {
		// 为每个键创建一个 pb.Mutation 对象，设置操作类型为 Op_Put，并设置键和值
		mutation := &pb.Mutation{
			Op:    pb.Op_Put,
			Key:   key,
			Value: c.mutations[string(key)].Value,
		}
		mutations = append(mutations, mutation)
	}

	req = &pb.PrewriteRequest{ // tinykv/proto/pkg/kvrpcpb/kvrpcpb.pb.go
		Mutations:    mutations,
		PrimaryLock:  c.primary(),
		StartVersion: c.startTS,
		LockTtl:      c.lockTTL,
	}
	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, req, pb.Context{})
}

// handleSingleBatch prewrites a batch of keys
// 处理单个批次的预写操作
func (actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// 构建预写请求
	// 每个批次中的所有键值对属于同一个 Region，TiKV 的 RPC 接口要求请求必须针对单个 Region
	req := c.buildPrewriteRequest(batch)

	// 发送请求并处理响应
	// 为什么使用循环：如果请求遇到可恢复的错误（例如 Region 信息过期或锁冲突），需要重试
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}

		// 处理 Region 错误：如 Leader 转移、Region 分裂等
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			// The region info is read from region cache,
			// so the cache miss cases should be considered
			// You need to handle region errors here
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String())) // 退避（等待一段时间再重试）
			if err != nil {
				return errors.Trace(err)
			}
			// re-split keys and prewrite again.
			// // 重新分割键并再次预写
			err = c.prewriteKeys(bo, batch.keys)
			return errors.Trace(err)
		}

		// 检查预写响应
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		// 响应中没有键错误（通常是由于锁冲突导致的），预写成功
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			return nil
		}
		// 处理键错误
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Extract lock from key error
			// // 从键错误中提取锁的信息（如哪个事务持有锁、锁的键值范围等）
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			logutil.BgLogger().Debug("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			locks = append(locks, lock)
		}

		// While prewriting, if there are some overlapped locks left by other transactions,
		// TiKV will return key errors. The statuses of these transactions are unclear.
		// ResolveLocks will check the transactions' statuses by locks and resolve them.
		// Set callerStartTS to 0 so as not to update minCommitTS.
		// 解决锁冲突
		//ResolveLocks 的工作：
		// 1.检查持有锁的事务状态（提交或回滚）。
		// 2.如果事务已提交或回滚，清理锁。
		// 3.如果锁未超时，等待锁的持有者完成操作。
		msBeforeExpired, _, err := c.store.lockResolver.ResolveLocks(bo, 0, locks)
		if err != nil {
			return errors.Trace(err)
		}
		// msBeforeExpired 为锁的剩余生存时间
		// 如果锁需要等待，调用 BackoffWithMaxSleep 暂停一段时间后重试
		// 退避函数本身不会自动执行重试逻辑，它的职责是提供一个等待机制和错误上下文，控制重试行为
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// 设置 undeterminedErr 字段的值
func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

// 获取 undeterminedErr 字段的值
func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

// 处理单个批次的提交操作
func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// follow actionPrewrite.handleSingleBatch, build the commit request
	// 构建提交请求
	var resp *tikvrpc.Response
	var err error
	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)

	// build and send the commit request
	// YOUR CODE HERE (lab3).

	// tinykv/proto/pkg/kvrpcpb/kvrpcpb.pb.go
	req := &pb.CommitRequest{
		StartVersion:  c.startTS,
		Keys:          batch.keys,
		CommitVersion: c.commitTS,
	}
	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdCommit, req, pb.Context{})
	resp, err = sender.SendReq(bo, tikvReq, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Debug("actionCommit handleSingleBatch", zap.Bool("nil response", resp == nil))

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	// 如果本批次的第一个键是事务的主键（primary()），且请求发送时发生了 RPC 错误：
	// 将错误标记为未决（Undetermined Error）：未决错误意味着 TiDB 无法确定该事务是否已经成功提交。
	// 原因：
	// 主键是事务的控制键，其状态决定事务的最终结果。
	// 如果 TiDB 无法确认主键是否已提交，则不能直接返回成功或失败，只能让上层应用处理。
	// TiDB/TiKV 采用了一种约定：主键总是放在键集合的第一个位置。
	isPrimary := bytes.Equal(batch.keys[0], c.primary())
	if isPrimary && sender.rpcError != nil {
		c.setUndeterminedErr(errors.Trace(sender.rpcError))
	}

	failpoint.Inject("mockFailAfterPK", func() {
		if !isPrimary {
			err = errors.New("commit secondary keys error")
		}
	})
	if err != nil {
		return errors.Trace(err)
	}

	// handle the response and error refer to actionPrewrite.handleSingleBatch
	// YOUR CODE HERE (lab3).
	// panic("YOUR CODE HERE")
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		// The region info is read from region cache,
		// so the cache miss cases should be considered
		// You need to handle region errors here
		// 可能出现因缓存过期而导致对应的存储节点返回 Region Error，此时需要分割 batch 后重试
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// 重新commit
		err = c.commitKeys(bo, batch.keys)
		return errors.Trace(err)
	}

	// 确保提交请求的响应有效
	if resp.Resp == nil {
		if isPrimary {
			c.setUndeterminedErr(errors.Trace(ErrBodyMissing))
		}
		return errors.Trace(ErrBodyMissing)
	}
	// 提取响应中的错误信息
	// KeyError 表示某个键的提交操作失败
	commitResp := resp.Resp.(*pb.CommitResponse)
	keyErr := commitResp.GetError()
	if keyErr != nil {
		// 使用读锁
		c.mu.RLock()
		defer c.mu.RUnlock()
		err := extractKeyErr(keyErr)
		// 主键已提交，但提交次键失败
		if c.mu.committed {
			logutil.BgLogger().Error("2PC failed commit key after primary key committed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		// 主键提交失败
		logutil.BgLogger().Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}

	// 提交响应中没有错误信息
	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

// 清理事务未完成的状态，例如清除未提交的锁和释放资源
func (actionCleanup) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	// // follow actionPrewrite.handleSingleBatch, build the rollback request

	// // build and send the rollback request
	// // YOUR CODE HERE (lab3).
	// panic("YOUR CODE HERE")
	var resp *tikvrpc.Response
	var err error
	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)

	req := &pb.BatchRollbackRequest{
		StartVersion: c.startTS,
		Keys:         batch.keys,
	}
	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, req, pb.Context{})
	resp, err = sender.SendReq(bo, tikvReq, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Debug("actionCleanup handleSingleBatch", zap.Bool("nil response", resp == nil))

	// // handle the response and error refer to actionPrewrite.handleSingleBatch
	// // YOUR CODE HERE (lab3).
	// panic("YOUR CODE HERE")
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		// The region info is read from region cache,
		// so the cache miss cases should be considered
		// You need to handle region errors here
		// 可能出现因缓存过期而导致对应的存储节点返回 Region Error，此时需要分割 batch 后重试
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// 重新rollback
		err = c.cleanupKeys(bo, batch.keys)
		return errors.Trace(err)
	}

	// 确保提交请求的响应有效
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cleanupResp := resp.Resp.(*pb.BatchRollbackResponse)
	keyErr := cleanupResp.GetError()
	// 提取响应中的错误信息
	if keyErr != nil {
		if keyErr.GetLocked() == nil {
			// 如果锁已经不存在，认为清理成功
			logutil.BgLogger().Debug("Lock not exist, cleanup considered successful",
				zap.Uint64("txnStartTS", c.startTS))
			return nil
		}
		c.mu.RLock()
		defer c.mu.RUnlock()
		err = errors.Errorf("conn %d 2PC cleanup failed: %s", c.connID, keyErr)
		logutil.BgLogger().Debug("2PC failed cleanup key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	return nil
}

func (c *twoPhaseCommitter) prewriteKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionPrewrite{}, keys)
}

func (c *twoPhaseCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCommit{}, keys)
}

func (c *twoPhaseCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCleanup{}, keys)
}

// execute executes the two-phase commit protocol.
// Prewrite phase:
//  1. Split keys by region -> batchKeys
//  2. Prewrite all batches with transaction's start timestamp
//
// Commit phase:
//  1. Get the latest timestamp as commit ts
//  2. Check if the transaction can be committed(schema change during execution will fail the transaction)
//  3. Commit the primary key
//  4. Commit the secondary keys
//
// Cleanup phase:
//
//	When the transaction is unavailable to successfully committed,
//	transaction will fail and cleanup phase would start.
//	Cleanup phase will rollback a transaction.
//	1. Cleanup primary key
//	2. Cleanup secondary keys

// 预写阶段：
//  1. 按区域拆分键 -> batchKeys
//  2. 使用事务的开始时间戳预写所有批次
//
// 提交阶段：
//  1. 获取最新的时间戳作为提交时间戳
//  2. 检查事务是否可以提交（执行期间的模式更改将导致事务失败）
//  3. 提交主键
//  4. 提交次键
//
// 清理阶段：
//
//	当事务无法成功提交时，事务将失败并开始清理阶段。
//	清理阶段将回滚事务。
//	1. 清理主键
//	2. 清理次键

// 协调事务的提交和回滚操作。
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {

	// 定义了一个匿名函数，确保无论事务的执行结果如何，都会在函数退出时执行
	// 清理阶段
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()

		// 如果事务未提交且不在未决状态，则触发回滚清理操作
		if !committed && !undetermined {

			// 在创建协程前调用，表示有一个新的协程即将开始。
			// 增加 WaitGroup 的计数器，防止 Wait() 提前结束
			c.cleanWg.Add(1)

			// 通过 go func() 启动一个新的协程来异步执行事务的清理任务
			go func() {
				cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
				cleanupBo := NewBackoffer(cleanupKeysCtx, cleanupMaxBackoff).WithVars(c.txn.vars)
				logutil.BgLogger().Debug("cleanupBo", zap.Bool("nil", cleanupBo == nil))
				// cleanup phase
				// YOUR CODE HERE (lab3).
				// panic("YOUR CODE HERE")
				// 回滚
				err := c.cleanupKeys(cleanupBo, c.keys)
				if err != nil {
					logutil.Logger(ctx).Info("2PC cleanup failed",
						zap.Error(err),
						zap.Uint64("txnStartTS", c.startTS))
				} else {
					logutil.Logger(ctx).Info("2pc clean up done",
						zap.Uint64("txtStartTs", c.startTS))
				}

				// 协程完成任务时，调用 Done()，将计数器减 1。
				// 当计数器归零时，表示所有任务已完成，阻塞在 Wait() 的其他协程或线程可以继续
				c.cleanWg.Done()
			}()
		}
		c.txn.commitTS = c.commitTS
	}()

	// prewrite phase
	// 预写阶段
	prewriteBo := NewBackoffer(ctx, PrewriteMaxBackoff).WithVars(c.txn.vars)
	logutil.BgLogger().Debug("prewriteBo", zap.Bool("nil", prewriteBo == nil))
	// YOUR CODE HERE (lab3).
	// panic("YOUR CODE HERE")
	err = c.prewriteKeys(prewriteBo, c.keys)
	if err != nil {
		logutil.Logger(ctx).Warn("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txtStartTs", c.startTS))
		return errors.Trace(err)
	}

	// commit phase
	// 提交阶段

	// 获取全局提交版本号
	// 调用 getTimestampWithRetry 从 TiKV 集群的时间服务中获取事务的全局提交版本号
	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff).WithVars(c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// 验证提交版本号的有效性
	// 提交版本号必须大于事务开始时间戳
	if commitTS <= c.startTS {
		err = errors.Errorf("conn %d Invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return errors.Trace(err)
	}
	// 设置事务的提交时间戳
	c.commitTS = commitTS
	if err = c.checkSchemaValid(); err != nil {
		return errors.Trace(err)
	}

	// 检查事务是否超时
	if c.store.oracle.IsExpired(c.startTS, kv.MaxTxnTimeUse) {
		err = errors.Errorf("conn %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	// 创建 Backoffer 对象 commitBo：用于处理提交阶段的重试和容错逻辑
	commitBo := NewBackoffer(ctx, CommitMaxBackoff).WithVars(c.txn.vars)
	logutil.BgLogger().Debug("commitBo", zap.Bool("nil", commitBo == nil))

	// Commit the transaction with `commitBo`.
	// If there is an error returned by commit operation, you should check if there is an undetermined error before return it.
	// Undetermined error should be returned if exists, and the database connection will be closed.
	// 使用 `commitBo` 提交事务。
	// 如果提交操作返回错误，你应该在返回错误之前检查是否存在未确定的错误。
	// 如果存在未确定的错误，则应返回该错误，并关闭数据库连接。
	// YOUR CODE HERE (lab3).
	// panic("YOUR CODE HERE")

	err = c.commitKeys(commitBo, c.keys)
	if err != nil {
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.Trace(terror.ErrResultUndetermined)
		}
		if !c.mu.committed {
			logutil.Logger(ctx).Error("2pc failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

type schemaLeaseChecker interface {
	Check(txnTS uint64) error
}

// checkSchemaValid checks if there are schema changes during the transaction execution(from startTS to commitTS).
// Schema change in a transaction is not allowed.
func (c *twoPhaseCommitter) checkSchemaValid() error {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if ok {
		err := checker.Check(c.commitTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

// batchKeys is a batch of keys in the same region.
type batchKeys struct {
	region RegionVerID
	keys   [][]byte
}

// appendBatchBySize appends keys to []batchKeys. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchBySize(b []batchKeys, region RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}

// newBatchExecutor create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchExecutor(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitAction, backoffer *Backoffer) *batchExecutor {
	return &batchExecutor{rateLimit, nil, committer,
		action, backoffer, time.Duration(1 * time.Millisecond)}
}

// initUtils do initialize batchExecutor related policies like rateLimit util
func (batchExe *batchExecutor) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchExe.rateLimiter = newRateLimit(batchExe.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchExe *batchExecutor) startWorker(exitCh chan struct{}, ch chan error, batches []batchKeys) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchExe.rateLimiter.getToken(exitCh); !exit {
			batchExe.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchExe.rateLimiter.putToken()
				var singleBatchBackoffer *Backoffer
				if _, ok := batchExe.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchExe.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchExe.backoffer.Fork()
					defer singleBatchCancel()
				}
				ch <- batchExe.action.handleSingleBatch(batchExe.committer, singleBatchBackoffer, batch)
			}()
		} else {
			logutil.Logger(batchExe.backoffer.ctx).Info("break startWorker",
				zap.Stringer("action", batchExe.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchExe *batchExecutor) process(batches []batchKeys) error {
	var err error
	err = batchExe.initUtils()
	if err != nil {
		logutil.Logger(batchExe.backoffer.ctx).Error("batchExecutor initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	backoffer := batchExe.backoffer
	var cancel context.CancelFunc
	if _, ok := batchExe.action.(actionPrewrite); ok {
		backoffer, cancel = batchExe.backoffer.Fork()
		defer cancel()
	}
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchExe.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(backoffer.ctx).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", batchExe.committer.connID),
				zap.Stringer("action type", batchExe.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchExe.committer.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(backoffer.ctx).Debug("2PC doActionOnBatches to cancel other actions",
					zap.Uint64("conn", batchExe.committer.connID),
					zap.Stringer("action type", batchExe.action),
					zap.Uint64("txnStartTS", batchExe.committer.startTS))
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	close(exitCh)

	return err
}
