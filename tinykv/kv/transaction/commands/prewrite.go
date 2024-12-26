// 事务的预写阶段，包含事务中的所有写操作（但不包括读操作）。
// 如果整个事务可以原子地写入底层存储，并且不会与其他事务（完成的或进行中的）发生冲突，则返回成功给客户端。
// 如果客户端的所有预写操作都成功，则它将发送提交消息。预写是两阶段提交中的第一阶段。

package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Prewrite represents the prewrite stage of a transaction. A prewrite contains all writes (but not reads) in a transaction,
// if the whole transaction can be written to underlying storage atomically and without conflicting with other
// transactions (complete or in-progress) then success is returned to the client. If all a client's prewrites succeed,
// then it will send a commit message. I.e., prewrite is the first phase in a two phase commit.
type Prewrite struct {
	CommandBase
	request *kvrpcpb.PrewriteRequest // tinykv/proto/pkg/kvrpcpb/kvrpcpb.pb.go
}

func NewPrewrite(request *kvrpcpb.PrewriteRequest) Prewrite {
	return Prewrite{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

// PrepareWrites prepares the data to be written to the raftstore. The data flow is as follows.
// The tinysql part:
//
//			user client -> insert/delete query -> tinysql server
//	     query -> parser -> planner -> executor -> the transaction memory buffer
//			memory buffer -> kv muations -> kv client 2pc committer
//			committer -> prewrite all the keys
//			committer -> commit all the keys
//			tinysql server -> respond to the user client
//
// The tinykv part:
//
//			prewrite requests -> transaction mutations -> raft request
//			raft req -> raft router -> raft worker -> peer propose raft req
//			raft worker -> peer receive majority response for the propose raft req  -> peer raft committed entries
//	 	raft worker -> process committed entries -> send apply req to apply worker
//			apply worker -> apply the correspond requests to storage(the state machine) -> callback
//			callback -> signal the response action -> response to kv client

// PrepareWrites 准备要写入 raftstore 的数据。数据流如下。
// tinysql 部分:
//
//			用户客户端 -> 插入/删除查询 -> tinysql 服务器
//			查询 -> 解析器 -> 计划器 -> 执行器 -> 事务内存缓冲区
//			内存缓冲区 -> kv 变更 -> kv 客户端 2pc 提交器
//			提交器 -> 预写所有键
//			提交器 -> 提交所有键
//			tinysql 服务器 -> 响应用户客户端
//
// tinykv 部分:
//
//			预写请求 -> 事务变更 -> raft 请求
//			raft 请求 -> raft 路由器 -> raft 工作线程 -> peer 提出 raft 请求
//			raft 工作线程 -> peer 接收多数响应的提出的 raft 请求 -> peer raft 提交的条目
//			raft 工作线程 -> 处理提交的条目 -> 发送应用请求到应用工作线程
//			应用工作线程 -> 将相应的请求应用到存储（状态机） -> 回调
//			回调 -> 发出响应动作信号 -> 响应 kv 客户端

// 准备事务的预写操作
// 它会处理请求中的所有变更（mutations），并将结果返回给客户端
// 请求中的变更（mutations）指的是对数据库进行的具体操作，这些操作通常包括插入（insert）、删除（delete）和更新（update）等。
// 每个变更都包含了需要操作的键和值，以及操作的类型。

// mvcc.MvccTxn 是一个用于处理 多版本并发控制（MVCC） 的事务上下文对象，在分布式事务的实现中用于管理键值存储的读写操作和事务的相关状态。
func (p *Prewrite) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// 创建响应对象
	response := new(kvrpcpb.PrewriteResponse)

	// Prewrite all mutations in the request.
	// 遍历请求中的所有变更
	for _, m := range p.request.Mutations {
		// 处理每个变更
		keyError, err := p.prewriteMutation(txn, m)
		if keyError != nil {
			response.Errors = append(response.Errors, keyError)
		} else if err != nil {
			return nil, err
		}
	}

	return response, nil
}

// prewriteMutation prewrites mut to txn. It returns (nil, nil) on success, (err, nil) if the key in mut is already
// locked or there is any other key error, and (nil, err) if an internal error occurs.
// prewriteMutation 将变更预写到事务中。成功时返回 (nil, nil)，如果变更中的键已经被锁定或存在其他键错误，则返回 (err, nil)，
// 如果发生内部错误，则返回 (nil, err)。
func (p *Prewrite) prewriteMutation(txn *mvcc.MvccTxn, mut *kvrpcpb.Mutation) (*kvrpcpb.KeyError, error) {
	key := mut.Key

	log.Debug("prewrite key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	// YOUR CODE HERE (lab2).
	// Check for write conflicts.
	// Hint: Check the interafaces provided by `mvcc.MvccTxn`. The error type `kvrpcpb.WriteConflict` is used
	//		 denote to write conflict error, try to set error information properly in the `kvrpcpb.KeyError`
	//		 response.
	// 检查写入冲突。
	// 提示：检查 `mvcc.MvccTxn` 提供的接口。错误类型 `kvrpcpb.WriteConflict` 用于表示写入冲突错误，
	//      尝试在 `kvrpcpb.KeyError` 响应中正确设置错误信息。
	// tinykv/kv/transaction/mvcc/transaction.go

	// 如果一个事务发现目标键的最新提交版本的时间戳大于等于自己的 StartTS，则认为存在冲突
	write, commitTs, err := txn.MostRecentWrite(key)
	if err != nil {
		return nil, err
	}
	if write != nil && commitTs >= txn.StartTS {
		keyError := &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    txn.StartTS,
				ConflictTs: commitTs,
				Key:        key,
				Primary:    p.request.PrimaryLock,
			},
		}
		return keyError, nil
	}

	// YOUR CODE HERE (lab2).
	// Check if key is locked. Report key is locked error if lock does exist, note the key could be locked
	// by this transaction already and the current prewrite request is stale.
	// 检查键是否被锁定。如果存在锁定错误则报告，注意键可能已经被该事务锁定，而当前的预写请求是过时的。

	// panic("check lock in prewrite is not implemented yet")

	// 锁定错误的两种情况
	// （1）锁属于其他事务
	//		如果目标键的锁是由另一个事务设置的：当前事务无法继续操作，因为锁表示另一个事务正在修改该键。
	//		此时返回锁定错误（KeyError.Locked），并将锁的详细信息填入响应中，供客户端处理，客户端通常会等待锁释放或直接回滚事务。
	// （2）锁属于当前事务
	//		如果目标键的锁是由当前事务设置的：这种情况可能是因为该事务的重复请求导致的，例如客户端的重试。
	//		锁已经存在，说明之前的预写请求已经完成，而当前请求是冗余的，此时可以忽略当前请求，返回成功而不重复设置锁或写入值。
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		if lock.Ts != txn.StartTS {
			keyError := &kvrpcpb.KeyError{
				Locked: lock.Info(key),
			}
			return keyError, nil
		} else {
			return nil, nil
		}
	}

	// YOUR CODE HERE (lab2).
	// Write a lock and value.
	// Hint: Check the interfaces provided by `mvccTxn.Txn`.
	// YOUR CODE HERE (lab2).
	// 写入锁和值。
	// 提示：检查 `mvccTxn.Txn` 提供的接口。
	// tinykv/kv/transaction/mvcc/transaction.go

	// 写入锁
	lock = &mvcc.Lock{
		Primary: p.request.PrimaryLock,
		Ts:      txn.StartTS,
		Ttl:     p.request.LockTtl,
		Kind:    mvcc.WriteKindFromProto(mut.Op), // tinykv/kv/transaction/mvcc/write.go
	}
	txn.PutLock(key, lock)
	// 写入值
	// 操作类型包含写入（存储键的新值）和删除（删除键）
	if mut.Op == kvrpcpb.Op_Put {
		txn.PutValue(key, mut.Value)
	} else if mut.Op == kvrpcpb.Op_Del {
		txn.DeleteValue(key)
	}

	return nil, nil
}

func (p *Prewrite) WillWrite() [][]byte {
	result := [][]byte{}
	for _, m := range p.request.Mutations {
		result = append(result, m.Key)
	}
	return result
}
