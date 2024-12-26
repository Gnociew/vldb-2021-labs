// PeerStorage 是分布式键值存储系统中一个关键组件，负责持久化 Raft 节点的状态和日志信息。
// 它实现了 raft.Storage 接口，为 Raft 协议提供持久化的存储支持。
// 此外，它管理节点的快照、日志条目，以及与节点生命周期相关的元数据。

package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// 应用快照操作的结果，包含了应用快照前后的 Region 信息
type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

// 编译时检查，确保 PeerStorage 类型实现了 raft.Storage 接口中的所有方法
var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	peerID uint64 // 当前节点的唯一标识
	// current region information of the peer
	region *metapb.Region // 节点所属的区域信息
	// current raft state of the peer
	raftState rspb.RaftLocalState // 存储 Raft 协议的核心状态，例如 LastIndex、LastTerm
	// current snapshot state
	snapState snap.SnapState // 当前的快照状态（生成中、应用中等）
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task // 用于任务调度的通道
	// gennerate snapshot tried count
	// 跟踪当前节点尝试生成快照的次数。
	// 它的存在目的是为了防止无限次尝试生成快照而导致资源浪费或死循环
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines // 用于存储 Raft 和 Kv 数据的引擎
	// Tag used for logging
	Tag string // 节点的标识符，用于日志记录
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
// 创建一个新的 PeerStorage 实例
// 它从存储引擎中获取持久化的 Raft 状态，并返回一个 PeerStorage 对象
// 持久化的 Raft 状态包括 Raft 日志、硬状态、应用状态和快照
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, peerID uint64, tag string) (*PeerStorage, error) {
	log.Debug(fmt.Sprintf("%s creating storage for %s", tag, region.String()))

	// 初始化 Raft 状态，包括加载持久化的 Raft 日志和硬状态
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	// 初始化应用状态，包括加载持久化的应用状态
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	// 检查 Raft 日志的最后一个索引是否小于应用状态的已应用索引；如果是，触发 panic，记录错误信息
	// Raft 协议保证所有已提交的日志条目都被应用到状态机中，并且所有节点的日志条目在索引和任期上是一致的。
	// 如果 Raft 日志的最后一个索引小于应用状态的已应用索引，说明有些日志条目已经被应用到状态机中，但这些日志条目在当前的 Raft 日志中不存在。这违反了一致性保证
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	// 创建并返回 PeerStorage 实例
	return &PeerStorage{
		Engines:     engines,
		peerID:      peerID,
		region:      region,
		Tag:         tag,
		raftState:   *raftState,
		regionSched: regionSched,
	}, nil
}

// 获取 peerstorage 的初始状态，，包括 Raft 的硬状态和配置状态
func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	// 获取 Raft 状态
	raftState := ps.raftState
	// 检查硬状态是否为空
	// 如果硬状态为空，使用 y.AssertTruef 断言 PeerStorage 未初始化，并记录错误信息
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}

	// 如果硬状态不为空，返回硬状态和从 Region 信息中提取的配置状态
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

// 获取指定范围内的 Raft 日志条目
func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	// 检查范围是否有效
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}

	// 初始化变量
	buf := make([]eraftpb.Entry, 0, high-low) // 创建一个空的日志条目切片 buf，容量为 high-low
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)           // 创建一个只读事务 txn
	defer txn.Discard()                                    // 在函数结束时丢弃
	startKey := meta.RaftLogKey(ps.region.Id, low)         // 起始键
	endKey := meta.RaftLogKey(ps.region.Id, high)          // 结束键
	iter := txn.NewIterator(badger.DefaultIteratorOptions) // 创建一个迭代器
	defer iter.Close()                                     // 在函数结束时关闭迭代器

	// 遍历 Raft 日志条目
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		// 如果当前键大于等于结束键，跳出循环
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value() // 获取值
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry // 反序列化值
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// 当前读取的日志条目的索引与预期的索引不匹配，说明存在日志条目缺失，跳出循环
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	// 检查条目数量是否正确
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

// 获取指定索引的日志条目的任期（Term）
func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	// 如果索引等于截断索引，返回截断索引的任期
	// 当索引等于截断索引时，表示请求的日志条目已经被截断。在这种情况下，无法从日志中直接获取该条目的任期，因此需要返回截断索引的任期
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	// 检查索引范围是否有效
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	// 如果截断索引的任期等于最后一个日志条目的任期，或者索引等于最后一个日志条目的索引，返回最后一个日志条目的任期。
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}

	var entry eraftpb.Entry // 获取日志条目
	// 调用 engine_util.GetMeta 函数从 Raft 存储引擎中获取指定索引的日志条目
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// 获取日志条目的最后一个索引
func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

// 获取日志条目的第一个有效索引，由于日志条目可能被截断，因此第一个有效日志条目的索引是截断索引加一
func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

// 获取当前 PeerStorage 的快照
func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot

	// 如果当前快照状态为生成中，等待快照生成完成
	if ps.snapState.StateType == snap.SnapState_Generating {
		// 使用 select 语句从 Receiver 通道中接收快照数据。如果接收到快照数据，将其赋值给 snapshot 变量
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		// 如果快照元数据不为空，重置 snapTriedCnt 计数器，并验证快照
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warn(fmt.Sprintf("failed to try generating snapshot, regionID: %d, peerID: %d, times: %d", ps.region.GetId(), ps.peerID, ps.snapTriedCnt))
		}
	}

	// 如果快照尝试次数超过 5 次，返回错误并重置 snapTriedCnt 计数器
	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	// 请求生成快照
	log.Info(fmt.Sprintf("requesting snapshot, regionID: %d, peerID: %d", ps.region.GetId(), ps.peerID))
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1) // 创建一个新的通道 ch，用于接收生成的快照
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	// 向 regionSched 通道发送一个 RegionTaskGen 任务，请求生成快照
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

// 检查 peerStorage 是否已初始化
func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

// 获取 PeerStorage 的 Region 信息
func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

// 检查索引范围是否有效
func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() { // 检查 low 是否小于等于截断索引
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 { // 检查 high 是否大于最后一个日志条目的索引加一
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

// 获取截断索引
func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState().TruncatedState.Index
}

// 获取截断索引的任期
func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState().TruncatedState.Term
}

// 获取已应用的日志条目的索引
func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState().AppliedIndex
}

// 获取应用状态
// 应用状态（Apply State）是指状态机已经应用的日志条目的状态信息。
// 应用状态用于跟踪哪些日志条目已经被应用到状态机中，以确保状态机的一致性和正确性
// 应用状态通常包括以下几个关键字段：
// 已应用索引（AppliedIndex）：
//
//	表示状态机已经应用的最新日志条目的索引。
//	这个索引用于确保状态机的状态与日志条目保持一致。
//
// 截断状态（TruncatedState）：
//
//	包含截断索引和截断任期，用于表示日志条目被截断的状态。
//	截断索引（TruncatedIndex）：表示日志条目被截断的索引。
//	截断任期（TruncatedTerm）：表示截断索引对应的日志条目的任期。
func (ps *PeerStorage) applyState() *rspb.RaftApplyState {
	state, _ := meta.GetApplyState(ps.Engines.Kv, ps.region.GetId())
	return state
}

// 验证快照的有效性
func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {

	// 检查快照索引是否过时
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Info(fmt.Sprintf("snapshot is stale, generate again, regionID: %d, peerID: %d, snapIndex: %d, truncatedIndex: %d", ps.region.GetId(), ps.peerID, idx, ps.truncatedIndex()))
		return false
	}

	// 反序列化快照数据
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil { // 如果反序列化失败
		log.Error(fmt.Sprintf("failed to decode snapshot, it may be corrupted, regionID: %d, peerID: %d, err: %v", ps.region.GetId(), ps.peerID, err))
		return false
	}

	// 检查快照 epoch 是否过时
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Info(fmt.Sprintf("snapshot epoch is stale, regionID: %d, peerID: %d, snapEpoch: %s, latestEpoch: %s", ps.region.GetId(), ps.peerID, snapEpoch, latestEpoch))
		return false
	}
	return true
}

// Append the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
// 将给定的日志条目追加到 Raft 日志中，并更新 Raft 状态
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	log.Debug(fmt.Sprintf("%s append %d entries", ps.Tag, len(entries)))

	prevLastIndex := ps.raftState.LastIndex

	// 如果没有新条目要追加，直接返回
	if len(entries) == 0 {
		return nil
	}

	// 确定待追加日志的最后一个条目的索引和任期
	lastEntry := entries[len(entries)-1]
	lastIndex := lastEntry.Index
	lastTerm := lastEntry.Term

	// panic("not implemented yet")
	// YOUR CODE HERE (lab1).
	// 追加日志条目
	for _, entry := range entries {
		// Hint1: in the raft write batch, the log key could be generated by `meta.RaftLogKey`.
		//       Also the `LastIndex` and `LastTerm` raft states should be updated after the `Append`.
		//       Use the input `raftWB` to save the append results, do check if the input `entries` are empty.
		//       Note the raft logs are stored as the `meta` type key-value pairs, so the `RaftLogKey` and `SetMeta`
		//       functions could be useful.
		// 提示1：在 Raft 写批次中，日志键可以通过 `meta.RaftLogKey` 生成。
		//       同时，`LastIndex` 和 `LastTerm` Raft 状态应该在追加日志条目后更新。
		//       使用输入的 `raftWB` 保存追加结果，检查输入的 `entries` 是否为空。
		//       注意，Raft 日志存储为 `meta` 类型的键值对，因此 `RaftLogKey` 和 `SetMeta` 函数可能会有用。

		// 使用 meta.RaftLogKey 生成日志条目的存储键
		// tinykv/kv/raftstore/meta/keys.go
		key := meta.RaftLogKey(ps.region.GetId(), entry.GetIndex()) // entry 相关：tinykv/proto/pkg/eraftpb/eraftpb.pb.go
		// 使用 WriteBatch 的 SetMeta 方法保存日志条目
		// tinykv/kv/util/engine_util/write_batch.go
		raftWB.SetMeta(key, &entry)
		log.Debug(fmt.Sprintf("entry=%v", entry))
	}

	// 删除与新日志冲突的旧日志条目（索引大于新日志的最后索引）
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		// Hint2: As the to be append logs may conflict with the old ones, try to delete the left
		//       old ones whose entry indexes are greater than the last to be append entry.
		//       Delete these previously appended log entries which will never be committed.
		// 提示2：由于要追加的日志条目可能与旧的日志条目冲突，尝试删除索引大于最后一个要追加的日志条目的旧日志条目。
		//       删除这些之前追加的但永远不会被提交的日志条目。
		key := meta.RaftLogKey(ps.region.GetId(), i)
		raftWB.DeleteMeta(key)
		log.Debug(fmt.Sprintf("Deleted old entry at index=%d", i))

	}

	// 更新 Raft 状态
	ps.raftState.LastIndex = lastIndex
	ps.raftState.LastTerm = lastTerm
	return nil

}

// 清除 PeerStorage 中的元数据
func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
// 删除不在 newRegion 范围内的数据
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	// 清除旧的起始键到新的起始键之间的数据
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	// 清除新的结束键到旧的结束键之间的数据
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// 清除与指定 Region 相关的元数据和日志条目，用于在 Region 被销毁或重置时释放资源
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()

	// 删除 Region 状态和应用状态
	kvWB.DeleteMeta(meta.RegionStateKey(regionID)) // RegionStateKey：表示该 Region 的状态（例如正常、被销毁等）
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))  // 	ApplyStateKey：表示应用状态（例如已应用日志的索引）

	// 确定第一个需要删除的日志条目的索引
	firstIndex := lastIndex + 1 // 假定没有需要删除的日志
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	// 删除日志条目
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	// 删除 Raft 状态
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))

	log.Info(fmt.Sprintf(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	))
	return nil
}

// Apply the peer with given snapshot.
// 应用给定的快照到 PeerStorage
// 它会清除旧的数据，更新 Raft 状态，并将新的快照数据应用到存储中
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Info(fmt.Sprintf("%v begin to apply snapshot", ps.Tag))

	// 反序列化快照数据
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// 检查 region id 是否匹配
	if snapData.Region.Id != ps.region.Id {
		return nil, fmt.Errorf("mismatch region id %v != %v", snapData.Region.Id, ps.region.Id)
	}

	// 清除旧数据
	if ps.isInitialized() {
		// we can only delete the old data when the peer is initialized.
		if err := ps.clearMeta(kvWB, raftWB); err != nil {
			return nil, err
		}
		ps.clearExtraData(snapData.Region)
	}

	// 更新 Raft 状态
	ps.raftState.LastIndex = snapshot.Metadata.Index
	ps.raftState.LastTerm = snapshot.Metadata.Term

	// 创建并返回 ApplySnapResult
	// 表示应用快照操作的结果。它包含了应用快照前后的 Region 信息
	applyRes := &ApplySnapResult{
		PrevRegion: ps.region,
		Region:     snapData.Region,
	}
	ps.region = snapData.Region

	// 更新应用状态
	applyState := &rspb.RaftApplyState{
		AppliedIndex: snapshot.Metadata.Index,
		// The snapshot only contains log which index > applied index, so
		// here the truncate state's (index, term) is in snapshot metadata.
		TruncatedState: &rspb.RaftTruncatedState{
			Index: snapshot.Metadata.Index,
			Term:  snapshot.Metadata.Term,
		},
	}
	kvWB.SetMeta(meta.ApplyStateKey(ps.region.GetId()), applyState)
	meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)

	// 设置快照状态
	ch := make(chan bool)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Applying,
	}

	// 发送 region 任务
	// RegionTaskApply 是一个任务结构体，表示一个应用快照的任务
	// 快照的实际应用任务并不是在当前线程或当前函数中直接处理完的，而是通过 region 任务处理器 调度到其他线程或协程中执行的。
	// why？1.异步执行提高并发能力 2.解耦逻辑 3.任务优先级管理 4.线程安全
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: snapshot.Metadata,
		StartKey: snapData.Region.GetStartKey(),
		EndKey:   snapData.Region.GetEndKey(),
	}
	// wait until apply finish
	// 等待任务完成
	<-ch

	log.Debug(fmt.Sprintf("%v apply snapshot for region %v with state %v ok", ps.Tag, snapData.Region, applyState))
	return applyRes, nil
}

// SaveReadyState processes the ready generated by the raft instance, the main task is to send raft messages
// to other peers, and persist the entries. The raft log entries will be saved to the raft kv, and the
// snapshot will be applied to the snapshot apply task will be handled by the region worker.
//
// Save memory states to disk.
//
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.

// SaveReadyState 处理由 Raft 实例生成的 ready 对象，主要任务是发送 Raft 消息给其他 peers，并持久化日志条目。
// Raft 日志条目将保存到 Raft 存储中，快照将应用到快照应用任务中，由 region worker 处理。
// 将内存状态保存到磁盘。
// 不要在此函数中修改 ready 对象，这是为了确保稍后正确推进 ready 对象。

// 持久化 Raft 的 Ready 状态，包括快照、日志条目和硬状态，同时与其他模块配合完成数据同步
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// 初始化写批次对象
	kvWB, raftWB := new(engine_util.WriteBatch), new(engine_util.WriteBatch)

	// 保存当前 Raft 状态，用于比较是否发生变化
	prevRaftState := ps.raftState
	var applyRes *ApplySnapResult = nil
	var err error

	// // 如果 Ready 包含快照，则应用快照
	if !raft.IsEmptySnap(&ready.Snapshot) {
		applyRes, err = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, err
		}
	}
	// panic("not implemented yet")
	// YOUR CODE HERE (lab1).
	// Hint: the outputs of the raft ready are: snapshot, entries, states, try to process
	//       them correctly. Note the snapshot apply may need the kv engine while others will
	//       always use the raft engine.
	// 提示：Raft ready 的输出包括：快照、日志条目、状态，尝试正确处理它们。
	//       注意，应用快照可能需要使用 KV 引擎，而其他操作将始终使用 Raft 引擎。

	// 如果 Ready 包含新的日志条目，将其持久化
	if len(ready.Entries) != 0 {
		// Hint1: Process entries if it's not empty.
		// 如果日志条目不为空，处理日志条目。
		err := ps.Append(ready.Entries, raftWB)
		if err != nil {
			return nil, fmt.Errorf("failed to append entries: %w", err)
		}
	}

	// Last index is 0 means the peer is created from raft message
	// and has not applied snapshot yet, so skip persistent hard state.
	// 最后一个索引为 0 表示该 peer 是从 Raft 消息中创建的，并且尚未应用快照，因此跳过持久化硬状态。
	if ps.raftState.LastIndex > 0 {
		// Hint2: Handle the hard state if it is NOT empty.
		// 提示2：如果硬状态不为空，处理硬状态。
		if !raft.IsEmptyHardState(ready.HardState) {
			ps.raftState.HardState = &ready.HardState
		}
	}

	// // 比较当前 Raft 状态与之前的状态，如果发生变化，持久化更新后的状态
	if !proto.Equal(&prevRaftState, &ps.raftState) {
		raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), &ps.raftState)
	}

	// // 提交写入到存储引擎
	kvWB.MustWriteToDB(ps.Engines.Kv)
	raftWB.MustWriteToDB(ps.Engines.Raft)
	return applyRes, nil
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
