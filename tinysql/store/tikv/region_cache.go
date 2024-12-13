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

// 定义了 TiKV 中的区域缓存（Region Cache）逻辑。
// 区域缓存用于缓存 TiKV 集群中的区域信息，以便快速定位和访问数据。

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	btreeDegree               = 32 // B-树的度数，用于区域缓存中的 B-树结构
	invalidatedLastAccessTime = -1 // 无效的最后访问时间，用于标记区域缓存中的无效条目
)

// RegionCacheTTLSec is the max idle time for regions in the region cache.
// 区域缓存的最大空闲时间（以秒为单位），用于控制区域缓存的生存时间
const RegionCacheTTLSec int64 = 600

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              //  need sync new region info.
)

// Region presents kv region
type Region struct {
	meta       *metapb.Region // raw region meta from PD immutable after init
	store      unsafe.Pointer // point to region store info, see RegionStore
	syncFlag   int32          // region need be sync in next turn
	lastAccess int64          // last region access time, see checkRegionCacheTTL
}

// RegionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type RegionStore struct {
	workTiKVIdx int32    // point to current work peer in meta.Peers and work store in stores(same idx) for tikv peer
	stores      []*Store // stores in this region
	storeFails  []uint32 // snapshots of store's fail, need reload when `storeFails[curr] != stores[cur].fail`
}

// clone clones region store struct.
// 克隆一个 RegionStore 对象
func (r *RegionStore) clone() *RegionStore {
	storeFails := make([]uint32, len(r.stores))
	copy(storeFails, r.storeFails)
	return &RegionStore{
		workTiKVIdx: r.workTiKVIdx,
		stores:      r.stores,
		storeFails:  storeFails,
	}
}

// return next follower store's index
// 返回下一个存储的索引，它不是当前正在工作的存储节点 (workTiKVIdx)。
// follower 方法的目的是在多个存储节点中找到一个有效的 follower 存储节点，并避免选择当前的工作存储节点。
func (r *RegionStore) follower(seed uint32) int32 {
	l := uint32(len(r.stores))
	if l <= 1 {
		return r.workTiKVIdx
	}

	for retry := l - 1; retry > 0; retry-- {
		followerIdx := int32(seed % (l - 1))
		if followerIdx >= r.workTiKVIdx {
			followerIdx++
		}
		if r.storeFails[followerIdx] == atomic.LoadUint32(&r.stores[followerIdx].fail) {
			return followerIdx
		}
		seed++
	}
	return r.workTiKVIdx
}

// init initializes region after constructed.
// 初始化一个 Region 对象
func (r *Region) init(c *RegionCache) {
	// region store pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &RegionStore{
		workTiKVIdx: 0,
		stores:      make([]*Store, 0, len(r.meta.Peers)),
		storeFails:  make([]uint32, 0, len(r.meta.Peers)),
	}
	for _, p := range r.meta.Peers {
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			store = c.getStoreByStoreID(p.StoreId)
		}
		rs.stores = append(rs.stores, store)
		rs.storeFails = append(rs.storeFails, atomic.LoadUint32(&store.fail))
	}
	atomic.StorePointer(&r.store, unsafe.Pointer(rs))

	// mark region has been init accessed.
	r.lastAccess = time.Now().Unix()
}

// 使用原子操作从 r.store 中加载一个指向 RegionStore 的指针，并返回该指针。
// 它确保在多线程环境中安全地读取 r.store。
func (r *Region) getStore() (store *RegionStore) {
	store = (*RegionStore)(atomic.LoadPointer(&r.store))
	return
}

// 使用原子操作比较并交换 r.store 的值（实际存储节点的信息）。
// 如果 r.store 当前的值是 oldStore，则将其替换为 newStore，并返回 true；否则返回 false
func (r *Region) compareAndSwapStore(oldStore, newStore *RegionStore) bool {
	return atomic.CompareAndSwapPointer(&r.store, unsafe.Pointer(oldStore), unsafe.Pointer(newStore))
}

// 检查 Region 的缓存是否过期
func (r *Region) checkRegionCacheTTL(ts int64) bool {
	for {
		lastAccess := atomic.LoadInt64(&r.lastAccess)
		if ts-lastAccess > RegionCacheTTLSec {
			return false
		}
		if atomic.CompareAndSwapInt64(&r.lastAccess, lastAccess, ts) {
			return true
		}
	}
}

// invalidate invalidates a region, next time it will got null result.
// 使 Region 无效
func (r *Region) invalidate() {
	atomic.StoreInt64(&r.lastAccess, invalidatedLastAccessTime)
}

// scheduleReload schedules reload region request in next LocateKey.
// 计划在下次 LocateKey 时重新加载 Region
// LocateKey 用于在分布式存储系统中定位特定键所在的 Region。
func (r *Region) scheduleReload() {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue != updated {
		return
	}
	atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, needSync)
}

// needReload checks whether region need reload.
// 检查 Region 是否需要重新加载
func (r *Region) needReload() bool {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue == updated {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, updated)
}

// RegionCache caches Regions loaded from PD.
// 存储从 PD（Placement Driver）加载的 Region 信息
// PD：管理和调度整个集群的元数据，包括 Region 的分布、负载均衡、故障恢复等
type RegionCache struct {
	pdClient pd.Client // 用于与 PD 通信的客户端

	// 互斥锁保护的结构体，包含缓存的 Region 信息
	mu struct {
		sync.RWMutex                         // mutex protect cached region
		regions      map[RegionVerID]*Region // cached regions be organized as regionVerID to region ref mapping
		sorted       *btree.BTree            // cache regions be organized as sorted key to region ref mapping
	}
	// 互斥锁保护的结构体，包含缓存的存储节点信息
	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
	notifyCheckCh chan struct{} // 通道，用于通知检查存储节点
	closeCh       chan struct{} // 通道，用于关闭缓存
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	c.notifyCheckCh = make(chan struct{}, 1)
	c.closeCh = make(chan struct{})
	go c.asyncCheckAndResolveLoop()
	return c
}

// Close releases region cache's resource.
// 关闭 closeCh 通道，释放 RegionCache 的资源
func (c *RegionCache) Close() {
	close(c.closeCh)
}

// asyncCheckAndResolveLoop with
// 异步循环，用于检查和解析存储节点的地址。
// 它监听 closeCh 和 notifyCheckCh 通道：
//
//	如果收到 closeCh 的信号，循环终止。
//	如果收到 notifyCheckCh 的信号，调用 checkAndResolve 函数检查和解析存储节点。
func (c *RegionCache) asyncCheckAndResolveLoop() {
	var needCheckStores []*Store
	for {
		select {
		case <-c.closeCh:
			return
		case <-c.notifyCheckCh:
			needCheckStores = needCheckStores[:0]
			c.checkAndResolve(needCheckStores)
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
// 检查和解析失败的存储节点的地址
func (c *RegionCache) checkAndResolve(needCheckStores []*Store) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("panic in the checkAndResolve goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		state := store.getResolveState()
		if state == needCheck {
			needCheckStores = append(needCheckStores, store)
		}
	}
	c.storeMu.RUnlock()

	for _, store := range needCheckStores {
		store.reResolve(c)
	}
}

// RPCContext contains data that is needed to send RPC to a region.
// 包含发送 RPC 请求到某个 Region 所需的数据
// RPC（Remote Procedure Call，远程过程调用）请求是一种通过网络从一台计算机调用另一台计算机上的函数或方法的技术。
// RPC 使得开发者可以像调用本地函数一样调用远程函数，而不需要关心底层的网络通信细节。
// 在分布式系统中，RPC 请求通常用于客户端与服务器之间的通信。
// 例如，在 TiKV 分布式存储系统中，客户端可能需要通过 RPC 请求与存储节点进行交互，以执行读写操作。
type RPCContext struct {
	Region  RegionVerID
	Meta    *metapb.Region
	Peer    *metapb.Peer
	PeerIdx int
	Store   *Store
	Addr    string
}

// GetStoreID returns StoreID.
// 返回 RPCContext 中存储节点的 ID
func (c *RPCContext) GetStoreID() uint64 {
	if c.Store != nil {
		return c.Store.storeID
	}
	return 0
}

// 返回 RPCContext 的字符串表示形式
func (c *RPCContext) String() string {
	return fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr, c.PeerIdx)
}

// GetTiKVRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
// 返回指定 Region 的 RPCContext
// 如果返回 nil，表示该 Region 已过期并从缓存中删除
func (c *RegionCache) GetTiKVRPCContext(bo *Backoffer, id RegionVerID, replicaRead kv.ReplicaReadType, followerStoreSeed uint32) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if !cachedRegion.checkRegionCacheTTL(ts) {
		return nil, nil
	}

	regionStore := cachedRegion.getStore()
	var store *Store
	var peer *metapb.Peer
	var storeIdx int
	switch replicaRead {
	case kv.ReplicaReadFollower:
		store, peer, storeIdx = cachedRegion.FollowerStorePeer(regionStore, followerStoreSeed)
	default:
		store, peer, storeIdx = cachedRegion.WorkStorePeer(regionStore)
	}
	addr, err := c.getStoreAddr(bo, cachedRegion, store, storeIdx)
	if err != nil {
		return nil, err
	}
	// enable by `curl -XPUT -d '1*return("[some-addr]")->return("")' http://host:port/github.com/pingcap/tidb/store/tikv/injectWrongStoreAddr`
	failpoint.Inject("injectWrongStoreAddr", func(val failpoint.Value) {
		if a, ok := val.(string); ok && len(a) > 0 {
			addr = a
		}
	})
	if store == nil || len(addr) == 0 {
		// Store not found, region must be out of date.
		cachedRegion.invalidate()
		return nil, nil
	}

	storeFailEpoch := atomic.LoadUint32(&store.fail)
	if storeFailEpoch != regionStore.storeFails[storeIdx] {
		cachedRegion.invalidate()
		logutil.BgLogger().Info("invalidate current region, because others failed on same store",
			zap.Uint64("region", id.GetID()),
			zap.String("store", store.addr))
		return nil, nil
	}

	return &RPCContext{
		Region:  id,
		Meta:    cachedRegion.meta,
		Peer:    peer,
		PeerIdx: storeIdx,
		Store:   store,
		Addr:    addr,
	}, nil
}

// KeyLocation is the region and range that a key is located.
// 表示一个键所在的 Region 和范围
type KeyLocation struct {
	Region   RegionVerID
	StartKey kv.Key
	EndKey   kv.Key
}

// Contains checks if key is in [StartKey, EndKey).
// 检查一个键是否在 KeyLocation 的范围内
// KeyLocation 是一个结构体，用于表示一个键所在的 Region 和范围
func (l *KeyLocation) Contains(key []byte) bool {
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

// LocateKey searches for the region and range that the key is located.
// 查找包含指定键的 Region 和范围
func (c *RegionCache) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, false)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// LocateEndKey searches for the region and range that the key is located.
// Unlike LocateKey, start key of a region is exclusive and end key is inclusive.
// 查找包含指定键的 Region 和范围，与 LocateKey 不同的是，Region 的起始键是排他的，结束键是包容的
func (c *RegionCache) LocateEndKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, true)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// 根据键查找 Region，如果缓存中没有或需要重新加载，则从 PD 加载。
func (c *RegionCache) findRegionByKey(bo *Backoffer, key []byte, isEndKey bool) (r *Region, err error) {
	r = c.searchCachedRegion(key, isEndKey)
	if r == nil {
		// load region when it is not exists or expired.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// no region data, return error if failure.
			return nil, err
		}
		r = lr
		c.mu.Lock()
		c.insertRegionToCache(r)
		c.mu.Unlock()
	} else if r.needReload() {
		// load region when it be marked as need reload.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// ignore error and use old region info.
			logutil.Logger(bo.ctx).Error("load region failure",
				zap.ByteString("key", key), zap.Error(err))
		} else {
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r)
			c.mu.Unlock()
		}
	}
	return r, nil
}

// OnSendFail handles send request fail logic.
// 处理发送请求失败的逻辑
func (c *RegionCache) OnSendFail(bo *Backoffer, ctx *RPCContext, scheduleReload bool, err error) {
	r := c.getCachedRegionWithRLock(ctx.Region)
	if r != nil {
		c.switchNextPeer(r, ctx.PeerIdx, err)
		if scheduleReload {
			r.scheduleReload()
		}
		logutil.Logger(bo.ctx).Info("switch region peer to next due to send request fail",
			zap.Stringer("current", ctx),
			zap.Bool("needReload", scheduleReload),
			zap.Error(err))
	}
}

// LocateRegionByID searches for the region with ID.
// 根据 Region ID 查找 Region 和范围
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	c.mu.RUnlock()
	if r != nil {
		if r.needReload() {
			lr, err := c.loadRegionByID(bo, regionID)
			if err != nil {
				// ignore error and use old region info.
				logutil.Logger(bo.ctx).Error("load region failure",
					zap.Uint64("regionID", regionID), zap.Error(err))
			} else {
				r = lr
				c.mu.Lock()
				c.insertRegionToCache(r)
				c.mu.Unlock()
			}
		}
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		return loc, nil
	}

	r, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.mu.Lock()
	c.insertRegionToCache(r)
	c.mu.Unlock()
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
// filter is used to filter some unwanted keys.
// The return values are the separated groups, first region and error
//
// Help function `RegionCache.LocateKey`

// GroupKeysByRegion 将键按所属的 Region 分组。
// 特别地，它还返回第一个键的 Region，该 Region 可能被用作 'PrimaryLockKey'，应该优先提交。
// filter 用于过滤一些不需要的键。
// 返回值是分组后的键，第一个 Region 和错误信息。
//
// 帮助函数 `RegionCache.LocateKey`
func (c *RegionCache) GroupKeysByRegion(bo *Backoffer, keys [][]byte, filter func(key, regionStartKey []byte) bool) (map[RegionVerID][][]byte, RegionVerID, error) {
	// YOUR CODE HERE (lab3).
	// panic("YOUR CODE HERE")
	// return nil, RegionVerID{}, nil

	// 创建一个空的 groups 映射，用于存储按 Region 分组的键
	groups := make(map[RegionVerID][][]byte)
	// 记录第一个键的 Region
	var firstRegionID RegionVerID
	var firstRegionSet bool

	// 遍历所有的键
	for _, key := range keys {
		// LocateKey 函数的作用是查找包含指定键的 Region 和范围。
		// 它首先会在缓存中查找，如果缓存中没有找到或者缓存中的数据无效，则会从 PD（Placement Driver）加载最新的 Region 信息
		// 故不存在缓存无效的情况
		loc, err := c.LocateKey(bo, key)
		if err != nil {
			return nil, RegionVerID{}, err
		}

		if filter != nil && filter(key, loc.StartKey) {
			break
		}

		regionID := loc.Region
		if !firstRegionSet {
			firstRegionID = regionID
			firstRegionSet = true
		}

		groups[regionID] = append(groups[regionID], key)
	}

	return groups, firstRegionID, nil
}

// ListRegionIDsInKeyRange lists ids of regions in [start_key,end_key].
// 列出指定键范围 [startKey, endKey] 内的所有 Region 的 ID
func (c *RegionCache) ListRegionIDsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regionIDs []uint64, err error) {
	for {
		curRegion, err := c.LocateKey(bo, startKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionIDs = append(regionIDs, curRegion.Region.id)
		if curRegion.Contains(endKey) {
			break
		}
		startKey = curRegion.EndKey
	}
	return regionIDs, nil
}

// LoadRegionsInKeyRange lists ids of regions in [start_key,end_key].
// 加载指定键范围 [startKey, endKey] 内的所有 Region，并将其插入缓存
func (c *RegionCache) LoadRegionsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regions []*Region, err error) {
	for {
		curRegion, err := c.loadRegion(bo, startKey, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.mu.Lock()
		c.insertRegionToCache(curRegion)
		c.mu.Unlock()

		regions = append(regions, curRegion)
		if curRegion.Contains(endKey) {
			break
		}
		startKey = curRegion.EndKey()
	}
	return regions, nil
}

// BatchLoadRegionsFromKey loads at most given numbers of regions to the RegionCache, from the given startKey. Returns
// the endKey of the last loaded region. If some of the regions has no leader, their entries in RegionCache will not be
// updated.
// 从指定的 startKey 开始，批量加载最多 count 个 Region 到缓存中，并返回最后一个加载的 Region 的 endKey
func (c *RegionCache) BatchLoadRegionsFromKey(bo *Backoffer, startKey []byte, count int) ([]byte, error) {
	regions, err := c.scanRegions(bo, startKey, count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(regions) == 0 {
		return nil, errors.New("PD returned no region")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, region := range regions {
		c.insertRegionToCache(region)
	}

	return regions[len(regions)-1].EndKey(), nil
}

// InvalidateCachedRegion removes a cached Region.
// 使指定的 Region 缓存无效
func (c *RegionCache) InvalidateCachedRegion(id RegionVerID) {
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return
	}
	cachedRegion.invalidate()
}

// UpdateLeader update some region cache with newer leader info.
// 更新指定 Region 的领导者信息
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64, currentPeerIdx int) {
	r := c.getCachedRegionWithRLock(regionID)
	if r == nil {
		logutil.BgLogger().Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}

	if leaderStoreID == 0 {
		c.switchNextPeer(r, currentPeerIdx, nil)
		logutil.BgLogger().Info("switch region peer to next due to NotLeader with NULL leader",
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("regionID", regionID.GetID()))
		return
	}

	if !c.switchToPeer(r, leaderStoreID) {
		logutil.BgLogger().Info("invalidate region cache due to cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("leaderStoreID", leaderStoreID))
		r.invalidate()
	} else {
		logutil.BgLogger().Info("switch region leader to specific leader due to kv return NotLeader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("leaderStoreID", leaderStoreID))
	}
}

// insertRegionToCache tries to insert the Region to cache.
// 尝试将 Region 插入缓存
func (c *RegionCache) insertRegionToCache(cachedRegion *Region) {
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		delete(c.mu.regions, old.(*btreeItem).cachedRegion.VerID())
	}
	c.mu.regions[cachedRegion.VerID()] = cachedRegion
}

// searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
// 通过键从缓存中查找 Region
func (c *RegionCache) searchCachedRegion(key []byte, isEndKey bool) *Region {
	ts := time.Now().Unix()
	var r *Region
	c.mu.RLock()
	c.mu.sorted.DescendLessOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		r = item.(*btreeItem).cachedRegion
		if isEndKey && bytes.Equal(r.StartKey(), key) {
			r = nil     // clear result
			return true // iterate next item
		}
		if !r.checkRegionCacheTTL(ts) {
			r = nil
			return true
		}
		return false
	})
	c.mu.RUnlock()
	if r != nil && (!isEndKey && r.Contains(key) || isEndKey && r.ContainsByEnd(key)) {
		return r
	}
	return nil
}

// getRegionByIDFromCache tries to get region by regionID from cache. Like
// `getCachedRegion`, it should be called with c.mu.RLock(), and the returned
// Region should not be used after c.mu is RUnlock().
// 通过 regionID 从缓存中获取 Region
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	for v, r := range c.mu.regions {
		if v.id == regionID {
			return r
		}
	}
	return nil
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
// 从 PD 客户端加载 Region，并选择第一个节点作为领导者
func (c *RegionCache) loadRegion(bo *Backoffer, key []byte, isEndKey bool) (*Region, error) {
	var backoffErr error
	searchPrev := false
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		var meta *metapb.Region
		var leader *metapb.Peer
		var err error
		if searchPrev {
			meta, leader, err = c.pdClient.GetPrevRegion(bo.ctx, key)
		} else {
			meta, leader, err = c.pdClient.GetRegion(bo.ctx, key)
		}
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, key: %q, err: %v", key, err)
			continue
		}
		if meta == nil {
			backoffErr = errors.Errorf("region not found for key %q", key)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		if isEndKey && !searchPrev && bytes.Equal(meta.StartKey, key) && len(meta.StartKey) != 0 {
			searchPrev = true
			continue
		}
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			c.switchToPeer(region, leader.StoreId)
		}
		return region, nil
	}
}

// loadRegionByID loads region from pd client, and picks the first peer as leader.
// 通过 regionID 从 PD 客户端加载 Region，并选择第一个节点作为领导者
func (c *RegionCache) loadRegionByID(bo *Backoffer, regionID uint64) (*Region, error) {
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		meta, leader, err := c.pdClient.GetRegionByID(bo.ctx, regionID)
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, regionID: %v, err: %v", regionID, err)
			continue
		}
		if meta == nil {
			backoffErr = errors.Errorf("region not found for regionID %q", regionID)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			c.switchToPeer(region, leader.GetStoreId())
		}
		return region, nil
	}
}

// scanRegions scans at most `limit` regions from PD, starts from the region containing `startKey` and in key order.
// Regions with no leader will not be returned.
// 从 PD 扫描最多 limit 个 Region，从包含 startKey 的 Region 开始，按键顺序扫描。
// 没有领导者的 Region 不会被返回
func (c *RegionCache) scanRegions(bo *Backoffer, startKey []byte, limit int) ([]*Region, error) {
	if limit == 0 {
		return nil, nil
	}

	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		metas, leaders, err := c.pdClient.ScanRegions(bo.ctx, startKey, nil, limit)
		if err != nil {
			backoffErr = errors.Errorf(
				"scanRegion from PD failed, startKey: %q, limit: %q, err: %v",
				startKey,
				limit,
				err)
			continue
		}

		if len(metas) == 0 {
			return nil, errors.New("PD returned no region")
		}
		if len(metas) != len(leaders) {
			return nil, errors.New("PD returned mismatching region metas and leaders")
		}
		regions := make([]*Region, 0, len(metas))
		for i, meta := range metas {
			region := &Region{meta: meta}
			region.init(c)
			leader := leaders[i]
			// Leader id = 0 indicates no leader.
			if leader.GetId() != 0 {
				c.switchToPeer(region, leader.GetStoreId())
				regions = append(regions, region)
			}
		}
		if len(regions) == 0 {
			return nil, errors.New("receive Regions with no peer")
		}
		if len(regions) < len(metas) {
			logutil.Logger(context.Background()).Debug(
				"regionCache: scanRegion finished but some regions has no leader.")
		}
		return regions, nil
	}
}

// 通过 regionID 获取缓存的 Region，使用读锁保护
func (c *RegionCache) getCachedRegionWithRLock(regionID RegionVerID) (r *Region) {
	c.mu.RLock()
	r = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

// 获取存储节点的地址，根据存储节点的解析状态进行不同的处理
func (c *RegionCache) getStoreAddr(bo *Backoffer, region *Region, store *Store, storeIdx int) (addr string, err error) {
	state := store.getResolveState()
	switch state {
	case resolved, needCheck:
		addr = store.addr
		return
	case unresolved:
		addr, err = store.initResolve(bo, c)
		return
	case deleted:
		addr = c.changeToActiveStore(region, store, storeIdx)
		return
	default:
		panic("unsupported resolve state")
	}
}

// 将存储节点切换为活动状态，并更新 Region 的存储信息
func (c *RegionCache) changeToActiveStore(region *Region, store *Store, storeIdx int) (addr string) {
	c.storeMu.RLock()
	store = c.storeMu.stores[store.storeID]
	c.storeMu.RUnlock()
	for {
		oldRegionStore := region.getStore()
		newRegionStore := oldRegionStore.clone()
		newRegionStore.stores = make([]*Store, 0, len(oldRegionStore.stores))
		for i, s := range oldRegionStore.stores {
			if i == storeIdx {
				newRegionStore.stores = append(newRegionStore.stores, store)
			} else {
				newRegionStore.stores = append(newRegionStore.stores, s)
			}
		}
		if region.compareAndSwapStore(oldRegionStore, newRegionStore) {
			break
		}
	}
	addr = store.addr
	return
}

// 通过 storeID 获取存储节点，如果不存在则创建新的存储节点
func (c *RegionCache) getStoreByStoreID(storeID uint64) (store *Store) {
	var ok bool
	c.storeMu.Lock()
	store, ok = c.storeMu.stores[storeID]
	if ok {
		c.storeMu.Unlock()
		return
	}
	store = &Store{storeID: storeID}
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

// OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
// 处理 Region 版本不匹配的情况，移除旧的 Region 并插入新的 Region 到缓存中
func (c *RegionCache) OnRegionEpochNotMatch(bo *Backoffer, ctx *RPCContext, currentRegions []*metapb.Region) error {
	// Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
	for _, meta := range currentRegions {
		if meta.GetId() == ctx.Region.id &&
			(meta.GetRegionEpoch().GetConfVer() < ctx.Region.confVer ||
				meta.GetRegionEpoch().GetVersion() < ctx.Region.ver) {
			err := errors.Errorf("region epoch is ahead of tikv. rpc ctx: %+v, currentRegions: %+v", ctx, currentRegions)
			logutil.BgLogger().Info("region epoch is ahead of tikv", zap.Error(err))
			return bo.Backoff(BoRegionMiss, err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	needInvalidateOld := true
	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		if _, ok := c.pdClient.(*codecPDClient); ok {
			if err := decodeRegionMetaKey(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{meta: meta}
		region.init(c)
		c.switchToPeer(region, ctx.Store.storeID)
		c.insertRegionToCache(region)
		if ctx.Region == region.VerID() {
			needInvalidateOld = false
		}
	}
	if needInvalidateOld {
		cachedRegion, ok := c.mu.regions[ctx.Region]
		if ok {
			cachedRegion.invalidate()
		}
	}
	return nil
}

// PDClient returns the pd.Client in RegionCache.
// 返回 RegionCache 中的 PD 客户端
func (c *RegionCache) PDClient() pd.Client {
	return c.pdClient
}

// btreeItem is BTree's Item that uses []byte to compare.
// 用于 B 树的项，使用 []byte 进行比较。它包含一个键和一个缓存的 Region
type btreeItem struct {
	key          []byte
	cachedRegion *Region
}

// 创建一个新的 btreeItem，用于插入 B 树
func newBtreeItem(cr *Region) *btreeItem {
	return &btreeItem{
		key:          cr.StartKey(),
		cachedRegion: cr,
	}
}

// 创建一个新的 btreeItem，用于在 B 树中查找
func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{
		key: key,
	}
}

// 用于比较两个 btreeItem 的键，以确定它们在 B 树中的顺序
func (item *btreeItem) Less(other btree.Item) bool {
	return bytes.Compare(item.key, other.(*btreeItem).key) < 0
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta returns region meta.
func (r *Region) GetMeta() *metapb.Region {
	return proto.Clone(r.meta).(*metapb.Region)
}

// GetLeaderID returns leader region ID.
func (r *Region) GetLeaderID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= len(r.meta.Peers) {
		return 0
	}
	return r.meta.Peers[int(r.getStore().workTiKVIdx)].Id
}

// GetLeaderStoreID returns the store ID of the leader region.
func (r *Region) GetLeaderStoreID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= len(r.meta.Peers) {
		return 0
	}
	return r.meta.Peers[int(r.getStore().workTiKVIdx)].StoreId
}

// 返回指定索引的存储节点和对应的副本
func (r *Region) getStorePeer(rs *RegionStore, pidx int32) (store *Store, peer *metapb.Peer, idx int) {
	store = rs.stores[pidx]
	peer = r.meta.Peers[pidx]
	idx = int(pidx)
	return
}

// WorkStorePeer returns current work store with work peer.
// 返回当前工作的存储节点和对应的副本
func (r *Region) WorkStorePeer(rs *RegionStore) (store *Store, peer *metapb.Peer, idx int) {
	return r.getStorePeer(rs, rs.workTiKVIdx)
}

// FollowerStorePeer returns a follower store with follower peer.
// 返回一个 follower 存储节点和对应的副本
func (r *Region) FollowerStorePeer(rs *RegionStore, followerStoreSeed uint32) (*Store, *metapb.Peer, int) {
	return r.getStorePeer(rs, rs.follower(followerStoreSeed))
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
// 唯一标识特定版本的 Region 的 ID
type RegionVerID struct {
	// Region id
	id uint64
	// Conf change version, auto increment when add or remove peer
	confVer uint64
	// Region version, auto increment when split or merge
	ver uint64
}

// GetID returns the id of the region
func (r *RegionVerID) GetID() uint64 {
	return r.id
}

// VerID returns the Region's RegionVerID.
func (r *Region) VerID() RegionVerID {
	return RegionVerID{
		id:      r.meta.GetId(),
		confVer: r.meta.GetRegionEpoch().GetConfVer(),
		ver:     r.meta.GetRegionEpoch().GetVersion(),
	}
}

// StartKey returns StartKey.
func (r *Region) StartKey() []byte {
	return r.meta.StartKey
}

// EndKey returns EndKey.
func (r *Region) EndKey() []byte {
	return r.meta.EndKey
}

// switchToPeer switches current store to the one on specific store. It returns
// false if no peer matches the storeID.
// 将当前存储节点切换到指定的存储节点。如果没有匹配的副本，则返回 false
func (c *RegionCache) switchToPeer(r *Region, targetStoreID uint64) (found bool) {
	leaderIdx, found := c.getPeerStoreIndex(r, targetStoreID)
	c.switchWorkIdx(r, leaderIdx)
	return
}

// 将当前存储节点切换到下一个副本
func (c *RegionCache) switchNextPeer(r *Region, currentPeerIdx int, err error) {
	rs := r.getStore()

	if err != nil { // TODO: refine err, only do this for some errors.
		s := rs.stores[currentPeerIdx]
		epoch := rs.storeFails[currentPeerIdx]
		if atomic.CompareAndSwapUint32(&s.fail, epoch, epoch+1) {
			logutil.BgLogger().Info("mark store's regions need be refill", zap.String("store", s.addr))
		}
		s.markNeedCheck(c.notifyCheckCh)
	}

	if int(rs.workTiKVIdx) != currentPeerIdx {
		return
	}

	nextIdx := (currentPeerIdx + 1) % len(rs.stores)
	newRegionStore := rs.clone()
	newRegionStore.workTiKVIdx = int32(nextIdx)
	r.compareAndSwapStore(rs, newRegionStore)
}

// 获取指定存储节点 ID 的副本索引
func (c *RegionCache) getPeerStoreIndex(r *Region, id uint64) (idx int, found bool) {
	if len(r.meta.Peers) == 0 {
		return
	}
	for i, p := range r.meta.Peers {
		if p.GetStoreId() == id {
			idx = i
			found = true
			return
		}
	}
	return
}

// 将当前工作存储节点切换到新的领导者
func (c *RegionCache) switchWorkIdx(r *Region, leaderIdx int) {
retry:
	// switch to new leader.
	oldRegionStore := r.getStore()
	if oldRegionStore.workTiKVIdx == int32(leaderIdx) {
		return
	}
	newRegionStore := oldRegionStore.clone()
	newRegionStore.workTiKVIdx = int32(leaderIdx)
	if !r.compareAndSwapStore(oldRegionStore, newRegionStore) {
		goto retry
	}
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// ContainsByEnd check the region contains the greatest key that is less than key.
// for the maximum region endKey is empty.
// startKey < key <= endKey.
func (r *Region) ContainsByEnd(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) <= 0 || len(r.meta.GetEndKey()) == 0)
}

// Store contains a kv process's address.
// 包含存储节点的地址、ID、状态等信息
type Store struct {
	addr         string     // loaded store address
	storeID      uint64     // store's id
	state        uint64     // unsafe store storeState
	resolveMutex sync.Mutex // protect pd from concurrent init requests
	fail         uint32     // store fail count, see RegionStore.storeFails
}

type resolveState uint64

const (
	unresolved resolveState = iota
	resolved
	needCheck
	deleted
)

// initResolve resolves addr for store that never resolved.
// 解析从未解析过的存储节点地址
func (s *Store) initResolve(bo *Backoffer, c *RegionCache) (addr string, err error) {
	s.resolveMutex.Lock()
	state := s.getResolveState()
	defer s.resolveMutex.Unlock()
	if state != unresolved {
		addr = s.addr
		return
	}
	var store *metapb.Store
	for {
		store, err = c.pdClient.GetStore(bo.ctx, s.storeID)
		if err != nil {
			// TODO: more refine PD error status handle.
			if errors.Cause(err) == context.Canceled {
				return
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(BoPDRPC, err); err != nil {
				return
			}
			continue
		}
		if store == nil {
			return
		}
		addr = store.GetAddress()
		s.addr = addr
	retry:
		state = s.getResolveState()
		if state != unresolved {
			addr = s.addr
			return
		}
		if !s.compareAndSwapState(state, resolved) {
			goto retry
		}
		return
	}
}

// reResolve try to resolve addr for store that need check.
// 尝试解析需要检查的存储节点地址
func (s *Store) reResolve(c *RegionCache) {
	var addr string
	store, err := c.pdClient.GetStore(context.Background(), s.storeID)
	if err != nil {
		logutil.BgLogger().Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return
	}
	if store == nil {
		// store has be removed in PD, we should invalidate all regions using those store.
		logutil.BgLogger().Info("invalidate regions in removed store",
			zap.Uint64("store", s.storeID), zap.String("add", s.addr))
		atomic.AddUint32(&s.fail, 1)
		return
	}

	addr = store.GetAddress()
	if s.addr != addr {
		state := resolved
		newStore := &Store{storeID: s.storeID, addr: addr}
		newStore.state = *(*uint64)(unsafe.Pointer(&state))
		c.storeMu.Lock()
		c.storeMu.stores[newStore.storeID] = newStore
		c.storeMu.Unlock()
	retryMarkDel:
		// all region used those
		oldState := s.getResolveState()
		if oldState == deleted {
			return
		}
		newState := deleted
		if !s.compareAndSwapState(oldState, newState) {
			goto retryMarkDel
		}
		return
	}
retryMarkResolved:
	oldState := s.getResolveState()
	if oldState != needCheck {
		return
	}
	newState := resolved
	if !s.compareAndSwapState(oldState, newState) {
		goto retryMarkResolved
	}
}

// 获取存储节点的解析状态
func (s *Store) getResolveState() resolveState {
	var state resolveState
	if s == nil {
		return state
	}
	return resolveState(atomic.LoadUint64(&s.state))
}

// 比较并交换存储节点的解析状态
func (s *Store) compareAndSwapState(oldState, newState resolveState) bool {
	return atomic.CompareAndSwapUint64(&s.state, uint64(oldState), uint64(newState))
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
// 标记已解析的存储节点，以异步解析检查存储节点地址的变化
func (s *Store) markNeedCheck(notifyCheckCh chan struct{}) {
retry:
	oldState := s.getResolveState()
	if oldState != resolved {
		return
	}
	if !s.compareAndSwapState(oldState, needCheck) {
		goto retry
	}
	select {
	case notifyCheckCh <- struct{}{}:
	default:
	}

}
