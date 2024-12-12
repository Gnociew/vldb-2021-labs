package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// 自定义类型，表示不同类型的定时器
type PeerTick int

const (
	PeerTickRaft               PeerTick = 0 // Raft 定时器
	PeerTickRaftLogGC          PeerTick = 1 // Raft 日志垃圾回收定时器
	PeerTickSplitRegionCheck   PeerTick = 2 // 分裂区域检查定时器
	PeerTickSchedulerHeartbeat PeerTick = 3 // 调度器心跳定时器
)

// 用于处理 Raft 消息
type peerMsgHandler struct {
	// *peer 表示该结构体中包含一个指向 peer 结构体的指针。
	// 这种方式称为嵌入，它允许 peerMsgHandler 结构体直接访问 peer 结构体中的字段和方法，就像它们是 peerMsgHandler 结构体的一部分一样
	*peer                      // peer 结构体，表示一个 Raft 节点
	applyCh chan []message.Msg // 用于接收应用消息的通道
	ctx     *GlobalContext     // 全局上下文 raftstone.go 中定义
}

func newPeerMsgHandler(peer *peer, applyCh chan []message.Msg, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer:    peer,
		applyCh: applyCh,
		ctx:     ctx,
	}
}

// 处理消息
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	// 根据消息类型进行处理
	switch msg.Type {

	// 处理 Raft 消息：其他 Peer 发送过来 Raft 消息，包括心跳、日志、投票消息等
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Error(fmt.Sprintf("%s handle raft message error %v", d.Tag, err))
		}

	// 处理 Raft 命令：上层提出的 proposal，其中包含了需要通过 Raft 同步的操作，以及操作成功之后需要调用的 callback 函数
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback) // 提交 Raft 命令

	// 处理定时器消息
	case message.MsgTypeTick:
		d.onTick()

	// 处理应用消息：ApplyFsm 在将日志应用到状态机之后发送给 PeerFsm 的消息，用于在进行操作之后更新某些内存状态
	case message.MsgTypeApplyRes:
		res := msg.Data.(*MsgApplyRes)
		d.onApplyResult(res)

	//处理分裂区域消息
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Info(fmt.Sprintf("%s on split with %v", d.Tag, split.SplitKey)) // 记录分裂区域日志
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)

	// 处理区域近似大小消息
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))

	// 处理垃圾回收快照消息
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)

	// 处理启动消息
	case message.MsgTypeStart:
		d.startTicker()
	}
}

// 处理定时器消息
func (d *peerMsgHandler) onTick() {
	// stopped 是 peer 的字段，在 peer.go 中定义
	if d.stopped {
		return
	}
	d.ticker.tickClock()

	// 触发 Raft 基础定时任务：例如发送心跳消息、检查选举超时等
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	// 触发 Raft 日志垃圾回收任务：定期清理不再需要的日志条目，释放存储空间
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	// 触发调度器心跳任务：定期向调度器发送心跳消息，报告节点状态
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	// 触发分裂区域检查任务：定期检查是否需要分裂区域，以保持数据均衡
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	// 将 regionId 发送到 tickDriverSender 通道，，以便 tickDriver 可以处理定时任务
	// 在分布式系统中，定时任务通常需要在特定的时间间隔内执行。为了协调这些任务，系统可能会使用一个 tickDriver 来管理和调度定时任务。
	d.ctx.tickDriverSender <- d.regionId
}

// 启动定时器并设置定时任务
func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId // 通知 tickDriver 需要处理该区域的定时任务
	// 调度定时任务，为每个定时任务设置下一个运行时间
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

// 处理快照垃圾回收任务
// 遍历传入的快照列表，根据快照的状态和元数据决定是否删除快照文件
func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex() // 当前节点的压缩索引
	compactedTerm := d.peerStorage.truncatedTerm() // 当前节点的压缩任期

	// 遍历快照列表
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey

		// 如果快照正在发送，则尝试获取快照
		// 在快照发送过程中，确保快照文件的有效性非常重要。如果快照文件无效或已被删除，可能会导致数据同步失败或节点恢复失败。因此，在处理快照垃圾回收任务时，需要检查正在发送的快照文件是否仍然有效
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			// 如果获取快照失败，则记录错误日志并继续下一个快照
			if err != nil {
				log.Error(fmt.Sprintf("%s failed to load snapshot for %s %v", d.Tag, key, err))
				continue
			}
			// 如果快照的任期小于压缩任期，或者快照的索引小于压缩索引，表示快照已被压缩，删除快照文件
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Info(fmt.Sprintf("%s snap file %s has been compacted, delete", d.Tag, key))
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				// 如果快照的元数据不为空，则获取快照的修改时间
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				// 如果快照文件的修改时间超过 4 小时，则删除快照文件
				if time.Since(modTime) > 4*time.Hour {
					log.Info(fmt.Sprintf("%s snap file %s has been expired, delete", d.Tag, key))
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
			// 处理应用过的快照
			// 如果快照的任期小于等于压缩任期，并且快照的索引小于等于压缩索引，表示快照已被应用，删除快照文件
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Info(fmt.Sprintf("%s snap file %s has been applied, delete", d.Tag, key))
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Error(fmt.Sprintf("%s failed to load snapshot for %s %v", d.Tag, key, err))
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

// 处理Raft 节点的准备状态，执行必要的操作并发送消息
// 准备状态是 Raft 节点在处理完一轮日志复制和状态更新后，准备好进行下一轮操作的状态。在这个状态下，节点会处理已经达成一致的日志条目、应用状态机、发送消息等操作
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	// 初始化一个空的消息切片，用于存储将要发送的消息
	msgs := make([]message.Msg, 0)

	// 处理应用提案（将客户端请求提交到 Raft 集群，并通过 Raft 协议达成一致后，将这些请求应用到状态机中的过程）
	// 应用提案的流程：
	// 客户端请求：客户端向 Raft 集群中的某个节点发送请求。
	// 提交提案：接收请求的节点将请求封装成提案，并提交到 Raft 日志中。
	// 日志复制：Raft 协议将提案复制到集群中的其他节点，确保所有节点的日志一致。
	// 达成一致：当大多数节点（通常是集群中的多数派）都接收到并记录了提案，提案被认为达成一致。
	// 应用提案：达成一致的提案被应用到状态机中，执行相应的操作。
	// 响应客户端：节点将操作结果返回给客户端

	// 如果存在应用提案，创建一个 MsgTypeApplyProposal 类型的消息，并将其添加到消息切片中
	if p := d.TakeApplyProposals(); p != nil {
		msg := message.Msg{Type: message.MsgTypeApplyProposal, Data: p, RegionID: p.RegionId}
		msgs = append(msgs, msg)
	}

	// 处理 Raft Ready 消息，返回应用快照结果和更新后的消息切片
	// 调用 peer 结构体的 HandleRaftReady 方法，传入消息切片、调度任务发送器和传输器。返回应用快照结果和更新后的消息切片
	applySnapResult, msgs := d.peer.HandleRaftReady(msgs, d.ctx.schedulerTaskSender, d.ctx.trans)

	// 处理应用快照结果
	if applySnapResult != nil {
		prevRegion := applySnapResult.PrevRegion // 应用快照前的 region
		region := applySnapResult.Region         // 应用快照后的 region

		log.Info(fmt.Sprintf("%s snapshot for region %s is applied", d.Tag, region)) // 记录应用快照日志
		meta := d.ctx.storeMeta                                                      // 获取存储元数据
		meta.Lock()                                                                  // 加锁
		defer meta.Unlock()                                                          // 确保在函数执行完毕或提前返回时，释放对 meta 的锁
		initialized := len(prevRegion.Peers) > 0
		// 如果之前的区域已初始化，记录区域变化的日志信息，并删除之前的区域
		// Region 初始化是指一个 Region 是否已经被创建并包含有效的副本节点信息
		if initialized {
			log.Info(fmt.Sprintf("%s region changed from %s -> %s after applying snapshot", d.Tag, prevRegion, region))
			meta.regionRanges.Delete(&regionItem{region: prevRegion})
		}
		// 将当前区域插入或替换到区域范围中，如果存在旧区域，抛出异常
		if oldRegion := meta.regionRanges.ReplaceOrInsert(&regionItem{region: region}); oldRegion != nil {
			panic(fmt.Sprintf("%s unexpected old region %+v, region %+v", d.Tag, oldRegion, region))
		}
		meta.regions[region.Id] = region // 更新存储元数据中的 region 信息
	}
	d.applyCh <- msgs // 将消息切片发送到应用通道
}

// 处理 Raft 基础定时任务
func (d *peerMsgHandler) onRaftBaseTick() {
	// When having pending snapshot, if election timeout is met, it can't pass
	// the pending conf change check because first index has been updated to
	// a value that is larger than last index.
	// 检查是否有待处理的快照
	if d.HasPendingSnapshot() {
		// need to check if snapshot is applied.
		d.ticker.schedule(PeerTickRaft) // 重新调度
		return
	}
	// TODO: make Tick returns bool to indicate if there is ready.
	d.RaftGroup.Tick() // 推进 Raft 状态机的时间（定期调用 Tick 方法来推进 Raft 状态机的内部计时器），处理定时任务
	d.ticker.schedule(PeerTickRaft)
}

// 处理应用结果
// 当日志条目被应用到状态机后，Raft 节点会收到应用结果，这个方法负责处理这些结果并执行相应的操作
func (d *peerMsgHandler) onApplyResult(res *MsgApplyRes) {

	log.Debug(fmt.Sprintf("%s async apply finished %v", d.Tag, res)) // 记录异步应用完成日志
	// handle executing committed log results
	// 处理已提交的日志结果
	for _, result := range res.execResults {
		switch x := result.(type) {
		case *execResultChangePeer: // 处理更改节点
			d.onReadyChangePeer(x)
		case *execResultCompactLog: // 处理压缩日志
			d.onReadyCompactLog(x.firstIndex, x.truncatedIndex)
		case *execResultSplitRegion: // 处理分裂区域
			d.onReadySplitRegion(x.derived, x.regions)
		}
	}
	res.execResults = nil // 清空执行结果
	if d.stopped {
		return
	}

	// 更新大小差异提示
	// d.SizeDiffHint ：自上次检查以来数据大小的累计变化量
	// res.sizeDiffHint ：本次应用操作中的数据大小变化量
	diff := d.SizeDiffHint + res.sizeDiffHint
	if diff > 0 {
		d.SizeDiffHint = diff
	} else {
		d.SizeDiffHint = 0
	}
}

// 处理接收到的 Raft 消息
func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	// 记录 Raft 消息日志
	log.Debug(fmt.Sprintf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId()))

	if !d.validateRaftMessage(msg) { // 验证 Raft 消息的有效性：用于验证基本有效性，检查消息的基本结构和字段
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() { // 处理墓碑消息
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) { // 检查消息：进一步检查有效性，通常包括更复杂的逻辑和条件。它可能涉及到对消息内容的更深入检查，以确保消息在当前上下文中是有效的
		return nil
	}
	// 检查快照（不是所有 msg 都携带快照）
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil { // key 不为 nil 表示快照文件不再使用
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())      // 将发送消息的 peer 插入到缓存中
	err = d.RaftGroup.Step(*msg.GetMessage()) // 调用 step 方法处理消息
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) { // 检查是否有新的 peer 追赶
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender) // 发送心跳消息
	}
	return nil
}

// 验证 Raft 消息的基本有效性
// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId() // 获取 region ID
	from := msg.GetFromPeer()     // 发送者
	to := msg.GetToPeer()         // 接收者
	// 记录 Raft 消息日志
	log.Debug(fmt.Sprintf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId()))
	// 检查接收者的 store ID 是否匹配（保证消息发送给正确的节点）
	if to.GetStoreId() != d.storeID() {
		log.Warn(fmt.Sprintf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID()))
		return false
	}
	// 检查消息是否包含 region epoch（用于跟踪和管理 Region 的版本信息，确保在集群中进行配置变更（如添加或删除节点）和数据变更（如分裂或合并 Region）时，所有节点的一致性）
	// 包含两个主要字段：ConfVer 和 Version，分别表示配置版本和数据版本
	if msg.RegionEpoch == nil {
		log.Error(fmt.Sprintf("[region %d] missing epoch in raft message, ignore it", regionID))
		return false
	}
	return true
}

// 进一步检查消息的有效性
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()            // 发送者的 region epoch
	isVoteMsg := util.IsVoteMessage(msg.Message) // 是否为投票消息，如果消息是，则需要发送墓碑消息来通知发送者移除自己。这是为了确保系统的一致性和稳定性，避免过期的投票消息干扰当前的选举过程或导致不一致的状态
	fromStoreID := msg.FromPeer.GetStoreId()     // 发送者的 store ID

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.

	region := d.Region() // 当前节点的区域信息
	// 检查消息的 RegionEpoch 是否过期（落后于当前节点的 RegionEpoch 版本信息），并且发送者不在当前区域中
	// 发送消息的节点不再属于当前节点所在的 Region 通常发生在集群进行配置变更（如添加或删除节点）或数据变更（如分裂或合并 Region）之后
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg) // 处理过期消息
		return true
	}

	target := msg.GetToPeer() // 消息的目标节点
	// 目标节点的 ID 小于当前节点的 ID，表示消息可能过期，可以被丢弃
	if target.Id < d.PeerId() {
		log.Info(fmt.Sprintf("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId()))
		return true
		// 目标节点的 ID 大于当前节点的 ID，调用 MaybeDestroy 方法检查当前节点是否应该被销毁
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Info(fmt.Sprintf("%s is stale as received a larger peer %s, destroying", d.Tag, target))
			d.destroyPeer()
			// 销毁当前节点后将消息发送到存储路由器
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

// 处理过期消息
func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	// 墓碑消息：用于通知节点移除自己，通常在节点被移除或不再属于某个 Region 时发送
	// 不需要发送墓碑消息
	if !needGC {
		log.Info(fmt.Sprintf("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch))
		return
	}

	// 构建并发送墓碑消息
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Error(fmt.Sprintf("[region %d] send message failed %v", regionID, err))
	}
}

// 处理墓碑消息（垃圾回收消息）
func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	// 检查消息的 RegionEpoch 是否过期
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	// 检查消息的目标节点是否匹配
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Info(fmt.Sprintf("%s receive stale gc msg, ignore", d.Tag))
		return
	}
	log.Info(fmt.Sprintf("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer))
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// 检查 Raft 消息是否包含快照，并验证快照的有效性
// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}

	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot) // 从快照中获取快照键
	snapData := new(rspb.RaftSnapshotData)                // 创建 Raft 快照数据
	err := snapData.Unmarshal(snapshot.Data)              // 反序列化快照数据
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region // 快照中的 Region 信息
	peerID := msg.ToPeer.Id       // 接收者的 ID
	// 检查快照中是否包含接收者节点
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	// 如果快照中不包含接收者节点，则记录日志并返回
	if !contains {
		log.Info(fmt.Sprintf("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID))
		return &key, nil
	}

	// 检查元数据一致性
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()

	// 从存储元数据中获取指定区域 ID 对应的区域信息并与当前节点的区域信息进行比较
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() { // 在 Raft 协议中，节点初始化通常包括加载初始状态、加入集群、同步数据等
			log.Info(fmt.Sprintf("%s stale delegate detected, skip", d.Tag))
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	// 检查区域是否重叠（节点中的现有快照 VS Raft消息中的快照）
	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Info(fmt.Sprintf("%s region overlapped %s %s", d.Tag, existRegion, snapRegion))
		return &key, nil
	}

	// check if snapshot file exists.
	// 检查快照文件是否存在
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// 销毁当前节点
func (d *peerMsgHandler) destroyPeer() {
	log.Info(fmt.Sprintf("%s starts destroy", d.Tag))
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized() // 判断当前节点是否已初始化（是否正在应用快照）

	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}

	// 关闭当前节点的路由并更新状态
	d.ctx.router.close(regionID)
	d.stopped = true

	// 删除元数据中的区域范围：一个 Region 所覆盖的键空间范围，[Start Key, End Key）。通过区域范围，可以确定某个键值对数据属于哪个 Region
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	// 删除区域信息
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

// 处理节点配置变更
// 在使用 Raft 协议的系统中，节点配置变更（如添加或移除节点）是通过一致性协议来进行的
// 每个节点都会接收到配置变更的消息，并根据消息内容进行相应的操作。在某些情况下，当前节点可能会接收到与自身无关的配置变更消息
func (d *peerMsgHandler) onReadyChangePeer(cp *execResultChangePeer) {
	changeType := cp.confChange.ChangeType      // 变更类型
	d.RaftGroup.ApplyConfChange(*cp.confChange) // 应用配置变更
	if cp.confChange.NodeId == 0 {
		// Apply failed, skip.
		return
	}

	// 更新存储元数据
	meta := d.ctx.storeMeta
	meta.Lock()
	meta.setRegion(cp.region, d.peer) // 更新区域信息
	meta.Unlock()

	peerID := cp.peer.Id
	switch changeType {
	// 添加节点
	case eraftpb.ConfChangeType_AddNode:
		now := time.Now()
		// 如果当前节点是 Leader 节点，记录节点的开始等待时间
		if d.IsLeader() {
			d.PeersStartPendingTime[peerID] = now
		}
		d.insertPeerCache(cp.peer) // 将节点插入缓存
	// 移除节点
	case eraftpb.ConfChangeType_RemoveNode:
		// 如果当前节点是 Leader 节点，删除节点的开始等待时间
		if d.IsLeader() {
			delete(d.PeersStartPendingTime, peerID)
		}
		d.removePeerCache(peerID) // 将节点从缓存中移除
	}

	// In pattern matching above, if the peer is the leader,
	// it will push the change peer into `peers_start_pending_time`
	// without checking if it is duplicated. We move `heartbeat_pd` here
	// to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
	// adding the redundant peer.

	// 如果是 leader，发送心跳消息通知调度器
	if d.IsLeader() {
		// Notify scheduler immediately.
		log.Info(fmt.Sprintf("%s notify scheduler with change peer region %s", d.Tag, d.Region()))
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	myPeerID := d.PeerId()

	// 是否需要销毁当前节点
	// We only care remove itself now.
	// 如果变更类型是移除节点，并且变更节点的存储 ID 与当前节点的存储 ID 相同
	if changeType == eraftpb.ConfChangeType_RemoveNode && cp.peer.StoreId == d.storeID() {
		if myPeerID == peerID {
			d.destroyPeer()
		} else {
			panic(fmt.Sprintf("%s trying to remove unknown peer %s", d.Tag, cp.peer))
		}
	}
}

// 处理 Raft 日志压缩任务
func (d *peerMsgHandler) onReadyCompactLog(firstIndex uint64, truncatedIndex uint64) {
	// 创建 Raft 日志压缩任务
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId, // region = raft group
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx  // 更新最后压缩索引
	d.ctx.raftLogGCTaskSender <- raftLogGCTask // 发送日志压缩任务
}

// 处理区域分裂
func (d *peerMsgHandler) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()

	regionID := derived.Id
	meta.setRegion(derived, d.peer)
	d.SizeDiffHint = 0

	isLeader := d.IsLeader()
	if isLeader {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		// Notify scheduler immediately to let it update the region meta.
		log.Info(fmt.Sprintf("%s notify scheduler with split count %d", d.Tag, len(regions)))
	}

	// regions 数组中，第一个元素通常是原始父 Region 的数据状态。
	if meta.regionRanges.Delete(&regionItem{region: regions[0]}) == nil {
		panic(d.Tag + " original region should exist")
	}
	// It's not correct anymore, so set it to None to let split checker update it.
	d.ApproximateSize = nil

	// 删除的 regions[0] 是分裂前的状态。
	// 插入的 regions[0] 是分裂后派生出来的新状态，通常作为父 Region 的延续。
	for _, newRegion := range regions {
		// 将新 Region 的范围插入 regionRanges
		newRegionID := newRegion.Id
		notExist := meta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		if notExist != nil {
			panic(fmt.Sprintf("%v %v newregion:%v, region:%v", d.Tag, notExist.(*regionItem).region, newRegion, regions[0]))
		}
		if newRegionID == regionID {
			continue
		}

		// Insert new regions and validation
		log.Info(fmt.Sprintf("[region %d] inserts new region %s", regionID, newRegion))
		if r, ok := meta.regions[newRegionID]; ok {
			// Suppose a new node is added by conf change and the snapshot comes slowly.
			// Then, the region splits and the first vote message comes to the new node
			// before the old snapshot, which will create an uninitialized peer on the
			// store. After that, the old snapshot comes, followed with the last split
			// proposal. After it's applied, the uninitialized peer will be met.
			// We can remove this uninitialized peer directly.
			if len(r.Peers) > 0 {
				panic(fmt.Sprintf("[region %d] duplicated region %s for split region %s",
					newRegionID, r, newRegion))
			}
			d.ctx.router.close(newRegionID)
		}

		// 为新 Region 创建对应的 Peer 实例
		newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			// peer information is already written into db, can't recover.
			// there is probably a bug.
			panic(fmt.Sprintf("create new split region %s error %v", newRegion, err))
		}
		metaPeer := newPeer.Meta

		for _, p := range newRegion.GetPeers() {
			newPeer.insertPeerCache(p)
		}

		// New peer derive write flow from parent region,
		// this will be used by balance write flow.
		campaigned := newPeer.MaybeCampaign(isLeader)

		if isLeader {
			// The new peer is likely to become leader, send a heartbeat immediately to reduce
			// client query miss.
			newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}

		meta.regions[newRegionID] = newRegion // 将新 Region 注册到元数据中
		// 注册新 Peer 到路由器，并发送启动消息
		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeStart, newRegionID, nil))

		// 处理待定的投票
		if !campaigned {
			for i, msg := range meta.pendingVotes {
				if util.PeerEqual(msg.ToPeer, metaPeer) {
					meta.pendingVotes = append(meta.pendingVotes[:i], meta.pendingVotes[i+1:]...)
					_ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeRaftMessage, newRegionID, msg))
					break
				}
			}
		}
	}
}

// 在提交 Raft 命令前进行检查
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	// 检查请求中的 Store ID 是否与当前节点的 Store ID 匹配，确保请求被分发到正确的节点
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	// 确保当前节点具有处理请求的权限，只有 Leader 节点有权限接收和处理写操作请求。
	regionID := d.regionId
	leaderID := d.LeaderId()
	// 如果当前节点不是 Leader，会返回 ErrNotLeader，并附带当前的 Leader 信息，供客户端重新路由请求
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	// 检查请求中的 Peer ID 是否与当前节点的 Peer ID 匹配

	// 已经check是否为leader，为什么还要检查peer_id是否匹配？
	// 防范副本重叠或迁移期间的错误；
	// 保护 Leader 切换中的一致性（客户端请求被发送到旧 Leader，而此时新 Leader 尚未完全接管）
	// 防止 Region 分裂或合并中的混乱
	// 客户端缓存信息可能过期（客户端的缓存可能仍然指向旧的 Leader）

	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	// 检查请求中的 Term 是否与当前节点的 Term 匹配
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	// 检查请求中的 Region Epoch 是否与当前节点的 Region Epoch 匹配
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// panic("not implemented yet")

	// YOUR CODE HERE (lab1).
	// Hint1: do `preProposeRaftCommand` check for the command, if the check fails, need to execute the
	// callback function and return the error results. `ErrResp` is useful to generate error response.

	if err := d.preProposeRaftCommand(msg); err != nil {
		// 如果检查失败，调用回调函数并返回错误响应
		log.Warn(fmt.Sprintf("[region %d] Pre-propose raft command failed: %v", d.regionId, err))
		cb.Done(ErrResp(err))
		return
	}

	// Hint2: Check if peer is stopped already, if so notify the callback that the region is removed, check
	// the `destroy` function for related utilities. `NotifyReqRegionRemoved` is useful to generate error response.

	if d.stopped {
		log.Warn(fmt.Sprintf("[region %d] Peer is already stopped", d.regionId))
		err := d.Destroy(d.ctx.engine, false)
		if err != nil {
			// 通知回调region已经被移除
			NotifyReqRegionRemoved(d.regionId, cb)
			// return
		}
	}

	// Hint3: Bind the possible response with term then do the real requests propose using the `Propose` function.
	// 提议请求时，需要将当前的 Raft term（任期）信息绑定到响应中
	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed. There are some useful information in the `ctx` of the `peerMsgHandler`.

	// cmd_resp.go
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())

	// 提交实际的 Raft 提议
	success := d.peer.Propose(d.ctx.engine.Raft, d.ctx.cfg, cb, msg, resp)
	if !success {
		log.Warn(fmt.Sprintf("[region %d] Propose failed", d.regionId))
		return
	}

	// 提议成功日志
	log.Info(fmt.Sprintf("[region %d] Propose succeeded", d.regionId))
}

// 查找当前region的兄弟region
// 兄弟 Region 是指在键空间范围上连续的 Region。这些 Region 之间存在逻辑上的关联，通常是因为它们在某些操作（如分裂或合并）中产生了连续的键空间范围。
// 键空间是指在分布式系统中用于存储和管理数据的键的集合。键空间定义了数据的逻辑范围和分布方式。在分布式数据库或存储系统中，键空间通常被划分为多个 Region，每个数据分片负责管理一定范围内的键值对数据。
func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

// 处理 Raft 日志的垃圾回收任务
func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC) // 调度下一个垃圾回收任务
	// 只有 leader 节点执行垃圾回收任务
	if !d.IsLeader() {
		return
	}

	// 获取已应用的索引和第一个索引，用来确定可以进行垃圾回收的索引范围
	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()

	// 在 Raft 协议中，日志索引（Index）用于标识日志条目的位置。每个日志条目都有一个唯一的索引，表示它在日志中的位置。
	// 已应用的索引是指已经被应用到状态机的最新日志条目的索引
	// 第一个索引是指当前日志中最早的日志条目的索引

	// 如果已应用的索引大于第一个索引，并且已应用的索引与第一个索引之间的差值大于等于 RaftLogGcCountLimit，执行垃圾回收
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	// 调整压缩日志的索引
	y.Assert(compactIdx > 0)
	compactIdx -= 1 // 确保压缩操作不会影响到当前正在使用的日志条目
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	// 获取压缩日志的任期

	// 每个任期开始时，集群中的节点会进行领导者选举，选出一个新的领导者（Leader）。
	// 通过任期，可以确保在同一时间内只有一个有效的领导者，从而避免多个领导者导致的数据不一致问题。
	// 在每个任期内，领导者负责将客户端的请求转换为日志条目，并将这些日志条目复制到集群中的其他节点。
	// 通过任期，可以确保日志条目的顺序和一致性。只有在当前任期内的领导者才能提交日志条目，旧任期的领导者不能提交日志条目
	// 任期用于区分不同的领导者选举周期，并确保系统的一致性。
	// 通过任期，可以确保在领导者切换期间，系统的一致性得到维护。新的领导者在接管之前，会确保自己拥有最新的日志条目

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatal(fmt.Sprintf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx))
		panic(err)
	}

	// Create a compact log request and notify directly.
	// 创建 Raft 日志压缩请求并提交
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

// 定期检查当前 region 是否需要进行分裂
func (d *peerMsgHandler) onSplitRegionCheckTick() {
	// 调度下一个分裂检查任务
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	// 避免频繁扫描，只有在所有之前的任务都完成后才添加新的扫描任务
	if len(d.ctx.splitCheckTaskSender) > 0 { // 如果长度大于 0，表示有未完成的分裂检查任务
		return
	}

	if !d.IsLeader() {
		return
	}
	// 避免频繁分裂，只有在大小差异超过一定阈值时才触发分裂检查任务
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	// 发送分裂检查任务
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	// 重置大小差异提示
	d.SizeDiffHint = 0
}

// 准备 region 的分裂操作
func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	// 验证分裂请求的合法性
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	// 发送分裂任务到调度器
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

// 验证分裂请求的合法性
func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error("split key empty error", zap.Error(err))
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Info(fmt.Sprintf("%s not leader, skip", d.Tag))
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch() // 获取最新的 Region Epoch

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Info(fmt.Sprintf("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch))
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

// 更新 region 的大小信息
func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

// 处理调度器心跳定时器的触发信息
func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat) // 调度下一个心跳任务（在当前心跳任务执行完毕后，安排下一次心跳任务的执行）

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

// 创建一个新的管理员请求，用于生成包含管理员命令的 Raft 请求
func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

// 创建一个新的 Raft 日志压缩请求
func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
