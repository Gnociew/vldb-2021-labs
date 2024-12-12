package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// 回调函数被封装在 message.Callback 类型中，并在请求处理完成后调用，以通知调用者请求的结果
// tinykv/kv/raftstore/message/callback.go

// 通知回调函数当前请求已经过时
func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

// 通知回调函数指定的 Region 已经被移除
func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
// 创建新的 raft 节点 （peer）
// ！！！创建时不是生成，而是实例化。createPeer 的职责是基于现有元信息实例化 peer，而不是随意生成一个完全新的节点。
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	metaPeer := util.FindPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Info(fmt.Sprintf("region %v create peer with ID %d", region, metaPeer.Id))
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
//  用于为指定节点创建一个 peer，主要用于通过快照恢复或初始化的场景
// 在创建这个复制的 peer 时，我们只知道 region_id 和 peer_id，Region 的详细信息将在应用快照后获取

// 成员变更（Membership Change）是 Raft 协议中的一个重要概念，它允许在运行时动态地添加或移除集群中的节点（peer）。
// 成员变更通常用于扩展集群、替换故障节点或进行负载均衡。
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// We will remove tombstone key when apply snapshot
	log.Info(fmt.Sprintf("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId()))
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// Peer is the basic component of region. Region is a raft group and its leader processes the read/write
// requests for a specific key range. A region usually is consists of several peers and one of which will
// be elected to be the leader.
// A peer has some meta information like the peer meta and region meta, also it has a inner raft instance
// whose type is `raft.RawNode`. All the peers will share the same storage engine, the raft kv and db kv,
// the difference is that they are responsible for different key ranges.
type peer struct {
	// Mark the peer as stopped, set when peer is destroyed
	stopped bool

	// The ticker of the peer, used to trigger
	// * raft tick
	// * raft log gc
	// * region heartbeat
	// * split check
	ticker *ticker

	// Record the meta information of the peer
	Meta     *metapb.Peer
	regionId uint64

	// Instance of the Raft module
	// Raft 模块的实例
	RaftGroup *raft.RawNode

	// The peer storage for the Raft module
	// PeerStorage 是 Raft 模块中的一个关键组件，负责管理和存储 Raft 日志、快照以及其他与 Raft 状态相关的数据。
	// 它在 Raft 协议中扮演着持久化存储的角色，确保 Raft 日志和状态在节点重启或故障恢复后能够正确恢复。
	peerStorage *PeerStorage

	// Record the callback of the proposals
	applyProposals []*proposal

	// Cache the peers information from other stores
	// when sending raft messages to other peers, it's used to get the store id of target peer
	// 缓存，用于存储其他存储节点的 peer 信息，以便在需要时快速访问。
	peerCache map[uint64]*metapb.Peer

	// Record the instants of peers being added into the configuration.
	// Remove them after they are not pending any more.
	PeersStartPendingTime map[uint64]time.Time

	// An inaccurate difference in region size since last reset.
	// split checker is triggered when it exceeds the threshold, it makes split checker not scan the data very often
	// 当超过阈值时触发分裂检查器，这使得分裂检查器不会频繁扫描数据
	SizeDiffHint uint64
	// Approximate size of the region.
	// It's updated everytime the split checker scan the data
	// 每次分裂检查器扫描数据时更新
	ApproximateSize *uint64

	Tag string

	// Index of last scheduled committed raft log.
	LastApplyingIdx uint64

	// Index of last scheduled compacted raft log.
	LastCompactedIdx uint64
}

// 创建一个新的 peer
func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {

	// 检查 peer 的 ID 是否有效
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id") // 格式化并返回一个 error 类型的值
	}
	// 生成标签
	// fmt.Sprintf 函数用于格式化字符串并返回一个字符串类型的值。它不会输出到标准输出，而是返回格式化后的字符串
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

	// 创建 peerStorage
	ps, err := NewPeerStorage(engines, region, regionSched, meta.GetId(), tag)
	if err != nil {
		return nil, err
	}

	// 获取当前已应用的索引
	appliedIndex := ps.AppliedIndex()

	// 创建 raft 配置
	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}

	// 创建 raft 实例
	// 每个 peer 都有一个独立的 Raft 实例 (raft.RawNode)，用于管理该 peer 的具体行为，包括日志复制、选举领导者、处理 Raft 消息等。
	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	// 初始化 peer 结构体
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		LastApplyingIdx:       appliedIndex,
		ticker:                newTicker(region.GetId(), cfg),
	}

	// If this region has only one peer and I am the one, campaign directly.
	// 如果 Region 只有一个 peer 并且是当前节点，直接发起竞选
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

// 将一个 peer 信息插入到缓存中
func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

// 从缓存中移除指定的 peer信息
func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

// 从 peerCache 中获取指定 ID 的 peer 信息。
// 如果缓存中没有该 peer 信息，则从 peerStorage 中获取，并插入缓存
func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

// 获取下一个日志条目的索引
func (p *peer) nextProposalIndex() uint64 {
	// 当前 Raft 日志的最后一个索引 + 1
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

// / Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
// 尝试销毁自身
func (p *peer) MaybeDestroy() bool {
	// 如果 peer 已经停止，则记录日志并跳过销毁操作
	if p.stopped {
		log.Info(fmt.Sprintf("%v is being destroyed, skip", p.Tag))
		return false
	}
	return true
}

// / Does the real destroy worker.Task which includes:
// / 1. Set the region to tombstone;
// / 2. Clear data;
// / 3. Notify all pending requests.
// 销毁一个 peer
// 将其从系统中移除，并清理与之相关的资源、元数据和存储内容
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now() // 记录开始时间
	region := p.Region()
	log.Info(fmt.Sprintf("%v begin to destroy", p.Tag))

	// Set Tombstone state explicitly
	// 设置墓碑状态
	// 管理底层存储写入操作的写批量（WriteBatch）对象。它们的主要作用是将多个写操作批量提交到存储引擎，从而提高效率和保证原子性
	// 用来将键值存储（KV 数据库）和 Raft 日志存储（Raft 数据库）中与 peer 相关的数据清除的
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil { // 清除 peerStorage 元数据
		return err
	}
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
	// write kv rocksdb first in case of restart happen between two write
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	// 如果 peerStorage 已初始化且不保留数据，则清除数据
	if p.peerStorage.isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		p.peerStorage.ClearData()
	}

	// 遍历所有挂起的提案，通知 Region 已被移除
	for _, proposal := range p.applyProposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.applyProposals = nil

	log.Info(fmt.Sprintf("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start)))
	return nil
}

// peer 是否已初始化
// 初始化状态表示 peerStorage 已经具备了完整的区域数据和日志，并且可以参与 Raft 协议的正常运行
func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

// 获取 peer 的 storeID
func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

// 获取peer 所属的 region 信息
func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

// / Set the region of a peer.
// / This will update the region of the peer, caller must ensure the region
// / has been preserved in a durable device.
func (p *peer) SetRegion(region *metapb.Region) {
	p.peerStorage.SetRegion(region)
}

func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

// / Returns `true` if the raft group has replicated a snapshot but not committed it yet.
func (p *peer) HasPendingSnapshot() bool {
	return p.RaftGroup.GetSnap() != nil
}

func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debug(fmt.Sprintf("%v send message err: %v", p.Tag, err))
		}
	}
}

// / Collects all pending peers and update `peers_start_pending_time`.
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.peerStorage.truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.getPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debug(fmt.Sprintf("%v peer %v start pending at %v", p.Tag, id, now))
				}
			}
		}
	}
	return pendingPeers
}

func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

// / Returns `true` if any new peer catches up with the leader in replicating logs.
// / And updates `PeersStartPendingTime` if needed.
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.peerStorage.truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debug(fmt.Sprintf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed))
				return true
			}
		}
	}
	return false
}

func (p *peer) ReadyToHandlePendingSnap() bool {
	// If apply worker is still working, written apply state may be overwritten
	// by apply worker. So we have to wait here.
	// Please note that committed_index can't be used here. When applying a snapshot,
	// a stale heartbeat can make the leader think follower has already applied
	// the snapshot, and send remaining log entries, which may increase committed_index.
	return p.LastApplyingIdx == p.peerStorage.AppliedIndex()
}

func (p *peer) TakeApplyProposals() *MsgApplyProposal {
	if len(p.applyProposals) == 0 {
		return nil
	}
	props := p.applyProposals
	p.applyProposals = nil
	return &MsgApplyProposal{
		Id:       p.PeerId(),
		RegionId: p.regionId,
		Props:    props,
	}
}

func (p *peer) HandleRaftReady(msgs []message.Msg, pdScheduler chan<- worker.Task, trans Transport) (*ApplySnapResult, []message.Msg) {
	// if p.stopped {
	// 	return nil, msgs
	// }

	// if p.HasPendingSnapshot() && !p.ReadyToHandlePendingSnap() {
	// 	log.Debug(fmt.Sprintf("%v [apply_id: %v, last_applying_idx: %v] is not ready to apply snapshot.", p.Tag, p.peerStorage.AppliedIndex(), p.LastApplyingIdx))
	// 	return nil, msgs
	// }

	// // YOUR CODE HERE (lab1). There are some missing code pars marked with `Hint` above, try to finish them.
	// // Hint1: check if there's ready to be processed, if no return directly.
	// panic("not implemented yet")

	// // Start to handle the raft ready.
	// log.Debug(fmt.Sprintf("%v handle raft ready", p.Tag))

	// ready := p.RaftGroup.Ready()
	// // TODO: workaround for:
	// //   in kvproto/eraftpb, we use *SnapshotMetadata
	// //   but in etcd, they use SnapshotMetadata
	// if ready.Snapshot.GetMetadata() == nil {
	// 	ready.Snapshot.Metadata = &eraftpb.SnapshotMetadata{}
	// }

	// // The leader can write to disk and replicate to the followers concurrently
	// // For more details, check raft thesis 10.2.1.
	// if p.IsLeader() {
	// 	p.Send(trans, ready.Messages)
	// 	ready.Messages = ready.Messages[:0]
	// }
	// ss := ready.SoftState
	// if ss != nil && ss.RaftState == raft.StateLeader {
	// 	p.HeartbeatScheduler(pdScheduler)
	// }

	// applySnapResult, err := p.peerStorage.SaveReadyState(&ready)
	// if err != nil {
	// 	panic(fmt.Sprintf("failed to handle raft ready, error: %v", err))
	// }
	// if !p.IsLeader() {
	// 	p.Send(trans, ready.Messages)
	// }

	// if applySnapResult != nil {
	// 	/// Register self to applyMsgs so that the peer is then usable.
	// 	msgs = append(msgs, message.NewPeerMsg(message.MsgTypeApplyRefresh, p.regionId, &MsgApplyRefresh{
	// 		id:     p.PeerId(),
	// 		term:   p.Term(),
	// 		region: p.Region(),
	// 	}))

	// 	// Snapshot's metadata has been applied.
	// 	p.LastApplyingIdx = p.peerStorage.truncatedIndex()
	// } else {
	// 	committedEntries := ready.CommittedEntries
	// 	ready.CommittedEntries = nil
	// 	l := len(committedEntries)
	// 	if l > 0 {
	// 		p.LastApplyingIdx = committedEntries[l-1].Index
	// 		msgs = append(msgs, message.Msg{Type: message.MsgTypeApplyCommitted, Data: &MsgApplyCommitted{
	// 			regionId: p.regionId,
	// 			term:     p.Term(),
	// 			entries:  committedEntries,
	// 		}, RegionID: p.regionId})
	// 	}
	// }

	// // YOUR CODE HERE (lab1). There are some missing code pars marked with `Hint` above, try to finish them.
	// // Hint2: Try to advance the states in the raft group of this peer after processing the raft ready.
	// //        Check about the `Advance` method in for the raft group.
	// panic("not implemented yet")

	// return applySnapResult, msgs

	if p.stopped {
		return nil, msgs
	}

	if p.HasPendingSnapshot() && !p.ReadyToHandlePendingSnap() {
		log.Debug(fmt.Sprintf("%v [apply_id: %v, last_applying_idx: %v] is not ready to apply snapshot.", p.Tag, p.peerStorage.AppliedIndex(), p.LastApplyingIdx))
		return nil, msgs
	}

	// YOUR CODE HERE (lab1). There are some missing code pars marked with `Hint` above, try to finish them.
	// Hint1: check if there's ready to be processed, if no return directly.
	if !p.RaftGroup.HasReady() {
		log.Debug(fmt.Sprintf("%v no raft ready", p.Tag))
		return nil, msgs
	}

	// Start to handle the raft ready.
	log.Debug(fmt.Sprintf("%v handle raft ready", p.Tag))

	ready := p.RaftGroup.Ready()
	// TODO: workaround for:
	//   in kvproto/eraftpb, we use *SnapshotMetadata
	//   but in etcd, they use SnapshotMetadata
	if ready.Snapshot.GetMetadata() == nil {
		ready.Snapshot.Metadata = &eraftpb.SnapshotMetadata{}
	}

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if p.IsLeader() {
		p.Send(trans, ready.Messages)
		ready.Messages = ready.Messages[:0]
	}
	ss := ready.SoftState
	if ss != nil && ss.RaftState == raft.StateLeader {
		p.HeartbeatScheduler(pdScheduler)
	}

	applySnapResult, err := p.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic(fmt.Sprintf("failed to handle raft ready, error: %v", err))
	}
	if !p.IsLeader() {
		p.Send(trans, ready.Messages)
	}

	if applySnapResult != nil {
		/// Register self to applyMsgs so that the peer is then usable.
		msgs = append(msgs, message.NewPeerMsg(message.MsgTypeApplyRefresh, p.regionId, &MsgApplyRefresh{
			id:     p.PeerId(),
			term:   p.Term(),
			region: p.Region(),
		}))

		// Snapshot's metadata has been applied.
		p.LastApplyingIdx = p.peerStorage.truncatedIndex()
	} else {
		committedEntries := ready.CommittedEntries
		ready.CommittedEntries = nil
		l := len(committedEntries)
		if l > 0 {
			p.LastApplyingIdx = committedEntries[l-1].Index
			msgs = append(msgs, message.Msg{Type: message.MsgTypeApplyCommitted, Data: &MsgApplyCommitted{
				regionId: p.regionId,
				term:     p.Term(),
				entries:  committedEntries,
			}, RegionID: p.regionId})
		}
	}

	// YOUR CODE HERE (lab1). There are some missing code pars marked with `Hint` above, try to finish them.
	// Hint2: Try to advance the states in the raft group of this peer after processing the raft ready.
	//        Check about the `Advance` method in for the raft group.
	// 在处理完ready后尝试推进该peer所在raft group的状态
	p.RaftGroup.Advance(ready)

	return applySnapResult, msgs
}

func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	p.RaftGroup.Campaign()
	return true
}

func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(p.Region(), clonedRegion)
	if err != nil {
		return
	}
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}

func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
	toPeer := p.getPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debug(fmt.Sprintf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer))

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}

// Propose a request.
//
// Return true means the request has been proposed successfully.
func (p *peer) Propose(kv *badger.DB, cfg *config.Config, cb *message.Callback, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse) bool {
	if p.stopped {
		return false
	}

	isConfChange := false

	policy, err := p.inspect(req)
	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}
	var idx uint64
	switch policy {
	case RequestPolicy_ProposeNormal:
		idx, err = p.ProposeNormal(cfg, req)
	case RequestPolicy_ProposeTransferLeader:
		return p.ProposeTransferLeader(cfg, req, cb)
	case RequestPolicy_ProposeConfChange:
		isConfChange = true
		idx, err = p.ProposeConfChange(cfg, req)
	}

	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}

	p.PostPropose(idx, p.Term(), isConfChange, cb)
	return true
}

func (p *peer) PostPropose(index, term uint64, isConfChange bool, cb *message.Callback) {
	proposal := &proposal{
		isConfChange: isConfChange,
		index:        index,
		term:         term,
		cb:           cb,
	}
	p.applyProposals = append(p.applyProposals, proposal)
}

// / Count the number of the healthy nodes.
// / A node is healthy when
// / 1. it's the leader of the Raft group, which has the latest logs
// / 2. it's a follower, and it does not lag behind the leader a lot.
// /    If a snapshot is involved between it and the Raft leader, it's not healthy since
// /    it cannot works as a node in the quorum to receive replicating logs from leader.
func (p *peer) countHealthyNode(progress map[uint64]raft.Progress) int {
	healthy := 0
	for _, pr := range progress {
		if pr.Match >= p.peerStorage.truncatedIndex() {
			healthy += 1
		}
	}
	return healthy
}

// / Validate the `ConfChange` request and check whether it's safe to
// / propose the specified conf change request.
// / It's safe iff at least the quorum of the Raft group is still healthy
// / right after that conf change is applied.
// / Define the total number of nodes in current Raft cluster to be `total`.
// / To ensure the above safety, if the cmd is
// / 1. A `AddNode` request
// /    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
// / 2. A `RemoveNode` request
// /    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
// /    need to be up to date for now. If 'allow_remove_leader' is false then
// /    the peer to be removed should not be the leader.
func (p *peer) checkConfChange(cfg *config.Config, cmd *raft_cmdpb.RaftCmdRequest) error {
	changePeer := GetChangePeerCmd(cmd)
	changeType := changePeer.GetChangeType()
	peer := changePeer.GetPeer()

	progress := p.RaftGroup.GetProgress()
	total := len(progress)
	if total <= 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		progress[peer.Id] = raft.Progress{}
	case eraftpb.ConfChangeType_RemoveNode:
		if _, ok := progress[peer.Id]; ok {
			delete(progress, peer.Id)
		} else {
			// It's always safe to remove a not existing node.
			return nil
		}
	}

	healthy := p.countHealthyNode(progress)
	quorumAfterChange := Quorum(len(progress))
	if healthy >= quorumAfterChange {
		return nil
	}

	log.Info(fmt.Sprintf("%v rejects unsafe conf chagne request %v, total %v, healthy %v quorum after change %v",
		p.Tag, changePeer, total, healthy, quorumAfterChange))

	return fmt.Errorf("unsafe to perform conf change %v, total %v, healthy %v, quorum after chagne %v",
		changePeer, total, healthy, quorumAfterChange)
}

func Quorum(total int) int {
	return total/2 + 1
}

func (p *peer) transferLeader(peer *metapb.Peer) {
	log.Info(fmt.Sprintf("%v transfer leader to %v", p.Tag, peer))

	p.RaftGroup.TransferLeader(peer.GetId())
}

func (p *peer) ProposeNormal(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	proposeIndex := p.nextProposalIndex()
	err = p.RaftGroup.Propose(data)
	if err != nil {
		return 0, err
	}
	if proposeIndex == p.nextProposalIndex() {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		return 0, &util.ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

// Return true if the transfer leader request is accepted.
func (p *peer) ProposeTransferLeader(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
	transferLeader := getTransferLeaderCmd(req)
	peer := transferLeader.Peer

	p.transferLeader(peer)
	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	cb.Done(makeTransferLeaderResponse())

	return true
}

// Fails in such cases:
// 1. A pending conf change has not been applied yet;
// 2. Removing the leader is not allowed in the configuration;
// 3. The conf change makes the raft group not healthy;
// 4. The conf change is dropped by raft group internally.
func (p *peer) ProposeConfChange(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	if p.RaftGroup.Raft.PendingConfIndex > p.peerStorage.AppliedIndex() {
		log.Info(fmt.Sprintf("%v there is a pending conf change, try later", p.Tag))
		return 0, fmt.Errorf("%v there is a pending conf change, try later", p.Tag)
	}

	if err := p.checkConfChange(cfg, req); err != nil {
		return 0, err
	}

	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	changePeer := GetChangePeerCmd(req)
	var cc eraftpb.ConfChange
	cc.ChangeType = changePeer.ChangeType
	cc.NodeId = changePeer.Peer.Id
	cc.Context = data

	log.Info(fmt.Sprintf("%v propose conf change %v peer %v", p.Tag, cc.ChangeType, cc.NodeId))

	proposeIndex := p.nextProposalIndex()
	if err = p.RaftGroup.ProposeConfChange(cc); err != nil {
		return 0, err
	}
	if p.nextProposalIndex() == proposeIndex {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		return 0, &util.ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

type RequestPolicy int

const (
	RequestPolicy_ProposeNormal RequestPolicy = 0 + iota
	RequestPolicy_ProposeTransferLeader
	RequestPolicy_ProposeConfChange
	RequestPolicy_Invalid
)

func (p *peer) inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
	if req.AdminRequest != nil {
		if GetChangePeerCmd(req) != nil {
			return RequestPolicy_ProposeConfChange, nil
		}
		if getTransferLeaderCmd(req) != nil {
			return RequestPolicy_ProposeTransferLeader, nil
		}
	}

	hasRead, hasWrite := false, false
	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
			hasRead = true
		case raft_cmdpb.CmdType_Delete, raft_cmdpb.CmdType_Put:
			hasWrite = true
		case raft_cmdpb.CmdType_Invalid:
			return RequestPolicy_Invalid, fmt.Errorf("invalid cmd type %v, message maybe corrupted", r.CmdType)
		}

		if hasRead && hasWrite {
			return RequestPolicy_Invalid, fmt.Errorf("read and write can't be mixed in one request.")
		}
	}
	return RequestPolicy_ProposeNormal, nil
}

func getTransferLeaderCmd(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.TransferLeaderRequest {
	if req.AdminRequest == nil {
		return nil
	}
	return req.AdminRequest.TransferLeader
}

func makeTransferLeaderResponse() *raft_cmdpb.RaftCmdResponse {
	adminResp := &raft_cmdpb.AdminResponse{}
	adminResp.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
	adminResp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	resp.AdminResponse = adminResp
	return resp
}

func GetChangePeerCmd(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.ChangePeerRequest {
	if msg.AdminRequest == nil || msg.AdminRequest.ChangePeer == nil {
		return nil
	}
	return msg.AdminRequest.ChangePeer
}
