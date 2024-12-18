// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// Copyright 2015 PingCAP, Inc.
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

// 文件定义了服务器和客户端之间的连接处理逻辑。
// 包含了处理客户端请求、进行握手、读取和写入数据包等功能的实现。
// 主要用于管理和维护服务器与客户端之间的连接状态和数据传输。

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

// newClientConn creates a *clientConn object.
// 创建一个新的 clientConn 对象。它接受一个 Server 实例作为参数
func newClientConn(s *Server) *clientConn {
	return &clientConn{
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		status:       connStatusDispatching,
	}
}

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
// 表示服务器和客户端之间的连接
// 维护连接的特定状态，并处理客户端查询
type clientConn struct {
	pkt          *packetIO         // a helper to read and write data in packet format.
	bufReadConn  *bufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn         // TLS connection, nil if not TLS.
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	user         string            // user of the client.
	dbname       string            // default database name.
	salt         []byte            // random bytes used for authentication.
	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
	lastPacket   []byte            // latest sql query string, currently used for logging error.
	ctx          QueryCtx          // an interface to execute sql statements.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	peerHost     string            // peer host
	peerPort     string            // peer port
	status       int32             // dispatching/reading/shutdown/waitshutdown
	lastCode     uint16            // last error code
	collation    uint8             // collation used by client, may be different from the collation used by database.
}

// 返回 clientConn 对象的字符串表示形式
// 包括连接 ID、远程地址、状态、排序规则和用户信息
func (cc *clientConn) String() string {
	collationStr := mysql.Collations[cc.collation]
	return fmt.Sprintf("id:%d, addr:%s status:%b, collation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	)
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send sql query to server.
// 类似于 TCP 握手，但在更高层次上进行。
// 它首先向客户端写入初始数据包，在握手过程中，客户端和服务器协商兼容特性并进行身份验证。
// 握手完成后，客户端可以向服务器发送 SQL 查询。
func (cc *clientConn) handshake(ctx context.Context) error {
	if err := cc.writeInitialHandshake(); err != nil {
		return err
	}
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(ctx); err != nil {
		err1 := cc.writeError(err)
		if err1 != nil {
			logutil.Logger(ctx).Debug("writeError failed", zap.Error(err1))
		}
		return err
	}
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, mysql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkt.sequence = 0
	if err != nil {
		return err
	}

	return cc.flush()
}

// 关闭 clientConn 对象
func (cc *clientConn) Close() error {
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	connections := len(cc.server.clients)
	cc.server.rwlock.Unlock()
	return closeConn(cc, connections)
}

// 关闭 clientConn 对象的连接。它关闭缓冲读取连接，并记录任何错误
func closeConn(cc *clientConn, connections int) error {
	err := cc.bufReadConn.Close()
	terror.Log(err)
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}

// 在不获取锁的情况下关闭 clientConn 对象（它从服务器的客户端列表中删除当前连接）
func (cc *clientConn) closeWithoutLock() error {
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc, len(cc.server.clients))
}

// writeInitialHandshake sends server version, connection ID, server capability, collation, server status
// and auth salt to the client.
// 向客户端发送初始握手数据包。数据包包括服务器版本、连接 ID、服务器能力、排序规则、服务器状态和身份验证盐
func (cc *clientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connectionID), byte(cc.connectionID>>8), byte(cc.connectionID>>16), byte(cc.connectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability), byte(cc.server.capability>>8))
	// charset
	if cc.collation == 0 {
		cc.collation = uint8(mysql.DefaultCollationID)
	}
	data = append(data, cc.collation)
	// status
	data = dumpUint16(data, mysql.ServerStatusAutocommit)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability>>16), byte(cc.server.capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(cc.salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	data = append(data, []byte("mysql_native_password")...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush()
}

// 从客户端读取数据包
func (cc *clientConn) readPacket() ([]byte, error) {
	return cc.pkt.readPacket()
}

// 向客户端写入数据包
func (cc *clientConn) writePacket(data []byte) error {
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	return cc.pkt.writePacket(data)
}

// getSessionVarsWaitTimeout get session variable wait_timeout
// 获取会话变量 wait_timeout 的值
// 会话变量 wait_timeout 用于指定服务器等待客户端活动的时间（以秒为单位）。
// 如果在指定的时间内没有收到客户端的任何请求，
// 服务器将关闭该连接。这个变量有助于管理服务器资源，防止长时间空闲的连接占用资源。
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	valStr, exists := cc.ctx.GetSessionVars().GetSystemVar(variable.WaitTimeout)
	if !exists {
		return variable.DefWaitTimeout
	}
	waitTimeout, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		logutil.Logger(ctx).Warn("get sysval wait_timeout failed, use default value", zap.Error(err))
		// if get waitTimeout error, use default value
		return variable.DefWaitTimeout
	}
	return waitTimeout
}

// 表示客户端在握手过程中发送的响应数据。
// 它包含客户端的能力标志、排序规则、用户名、数据库名称、身份验证数据和属性。
type handshakeResponse41 struct {
	Capability uint32
	Collation  uint8
	User       string
	DBName     string
	Auth       []byte
	Attrs      map[string]string
}

// parseOldHandshakeResponseHeader parses the old version handshake header HandshakeResponse320
// 解析旧版本握手响应头
func parseOldHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse320
	logutil.Logger(ctx).Debug("try to parse hanshake response as Protocol::HandshakeResponse320", zap.ByteString("packetData", data))
	if len(data) < 2+3 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}
	offset := 0
	// capability
	capability := binary.LittleEndian.Uint16(data[:2])
	packet.Capability = uint32(capability)

	// be compatible with Protocol::HandshakeResponse41
	packet.Capability = packet.Capability | mysql.ClientProtocol41

	offset += 2
	// skip max packet size
	offset += 3
	// usa default CharsetID
	packet.Collation = mysql.CollationNames["utf8mb4_general_ci"]

	return offset, nil
}

// parseOldHandshakeResponseBody parse the HandshakeResponse for Protocol::HandshakeResponse320 (except the common header part).
// 解析旧版本握手响应体
func parseOldHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
		if len(data[offset:]) > 0 {
			packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		}
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
	}

	return nil
}

// parseHandshakeResponseHeader parses the common header of SSLRequest and HandshakeResponse41.
// 解析 SSL 请求和握手响应的公共头部
func parseHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if len(data) < 4+4+1+23 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}

	offset := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	offset += 4
	// skip max packet size
	offset += 4
	// charset, skip, if you want to use another charset, use set names
	packet.Collation = data[offset]
	offset++
	// skip reserved 23[00]
	offset += 23

	return offset, nil
}

// parseHandshakeResponseBody parse the HandshakeResponse (except the common header part).
// 解析握手响应的主体部分
func parseHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientPluginAuthLenencClientData > 0 {
		// MySQL client sets the wrong capability, it will set this bit even server doesn't
		// support ClientPluginAuthLenencClientData.
		// https://github.com/mysql/mysql-server/blob/5.7/sql-common/client.c#L3478
		num, null, off := parseLengthEncodedInt(data[offset:])
		offset += off
		if !null {
			packet.Auth = data[offset : offset+int(num)]
			offset += int(num)
		}
	} else if packet.Capability&mysql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[offset])
		offset++
		packet.Auth = data[offset : offset+authLen]
		offset += authLen
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(packet.Auth) + 1
	}

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
	}

	if packet.Capability&mysql.ClientPluginAuth > 0 {
		// TODO: Support mysql.ClientPluginAuth, skip it now
		idx := bytes.IndexByte(data[offset:], 0)
		offset = offset + idx + 1
	}

	if packet.Capability&mysql.ClientConnectAtts > 0 {
		if len(data[offset:]) == 0 {
			// Defend some ill-formated packet, connection attribute is not important and can be ignored.
			return nil
		}
		if num, null, off := parseLengthEncodedInt(data[offset:]); !null {
			offset += off
			row := data[offset : offset+int(num)]
			attrs, err := parseAttrs(row)
			if err != nil {
				logutil.Logger(ctx).Warn("parse attrs failed", zap.Error(err))
				return nil
			}
			packet.Attrs = attrs
		}
	}

	return nil
}

// 解析握手响应中的属性
func parseAttrs(data []byte) (map[string]string, error) {
	attrs := make(map[string]string)
	pos := 0
	for pos < len(data) {
		key, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off
		value, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off

		attrs[string(key)] = string(value)
	}
	return attrs, nil
}

// 读取客户端发送的数据包，该数据包可能是 SSL 请求或握手响应
func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse(ctx context.Context) error {
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		return err
	}

	isOldVersion := false

	var resp handshakeResponse41
	var pos int

	if len(data) < 2 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return mysql.ErrMalformPacket
	}

	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 > 0 {
		pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
	} else {
		pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
		isOldVersion = true
	}

	if err != nil {
		return err
	}

	if (resp.Capability&mysql.ClientSSL > 0) && cc.server.tlsConfig != nil {
		// The packet is a SSLRequest, let's switch to TLS.
		if err = cc.upgradeToTLS(cc.server.tlsConfig); err != nil {
			return err
		}
		// Read the following HandshakeResponse packet.
		data, err = cc.readPacket()
		if err != nil {
			return err
		}
		if isOldVersion {
			pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
		} else {
			pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
		}
		if err != nil {
			return err
		}
	}

	// Read the remaining part of the packet.
	if isOldVersion {
		err = parseOldHandshakeResponseBody(ctx, &resp, data, pos)
	} else {
		err = parseHandshakeResponseBody(ctx, &resp, data, pos)
	}
	if err != nil {
		return err
	}

	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.collation = resp.Collation
	cc.attrs = resp.Attrs

	err = cc.openSessionAndDoAuth()
	return err
}

func (cc *clientConn) SessionStatusToString() string {
	status := cc.ctx.Status()
	inTxn, autoCommit := 0, 0
	if status&mysql.ServerStatusInTrans > 0 {
		inTxn = 1
	}
	if status&mysql.ServerStatusAutocommit > 0 {
		autoCommit = 1
	}
	return fmt.Sprintf("inTxn:%d, autocommit:%d",
		inTxn, autoCommit,
	)
}

// 打开会话并进行身份验证
func (cc *clientConn) openSessionAndDoAuth() error {
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	var err error
	cc.ctx, err = cc.server.driver.OpenCtx(uint64(cc.connectionID), cc.capability, cc.collation, cc.dbname, tlsStatePtr)
	if err != nil {
		return err
	}
	if cc.dbname != "" {
		err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			return err
		}
	}
	return nil
}

// 返回客户端的主机地址
func (cc *clientConn) PeerHost(hasPassword string) (host string, err error) {
	if len(cc.peerHost) > 0 {
		return cc.peerHost, nil
	}
	host = variable.DefHostname
	addr := cc.bufReadConn.RemoteAddr().String()
	var port string
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		err = errAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.peerHost = host
	cc.peerPort = port
	return
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	const size = 4096
	//在 run 方法返回时执行 defer 函数，用于处理 panic
	defer func() {
		r := recover() // 捕获可能的 panic，，防止程序崩溃
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			// 记录堆栈信息，用于后续调试和分析问题
			logutil.Logger(ctx).Error("connection running loop panic",
				zap.Stringer("lastSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf)),
			)

		}
		// 如果连接状态不是关闭状态，调用 Close 方法关闭连接
		if atomic.LoadInt32(&cc.status) != connStatusShutdown {
			err := cc.Close()
			terror.Log(err)
		}
	}()

	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	// 通常，客户端连接状态在 [dispatching] <=> [reading] 之间切换。
	// 当某些事件发生时，服务器可能会通过设置特殊值来通知此客户端连接，例如：终止或优雅关闭。
	// 客户端连接在 CAS 操作失败时会检测到这些事件，然后相应地采取一些行动。

	// 循环处理客户端请求
	for {
		// 确保连接的状态从 connStatusDispatching 切换到 connStatusReading
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) {
			return
		}

		// 从客户端读取数据包
		///
		cc.alloc.Reset()
		// 设置超时时间，如果超时未收到数据，记录日志并关闭连接
		waitTimeout := cc.getSessionVarsWaitTimeout(ctx)
		cc.pkt.setReadTimeout(time.Duration(waitTimeout) * time.Second)
		start := time.Now()
		data, err := cc.readPacket() // 开始读取
		// 错误处理
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
					idleTime := time.Since(start)
					logutil.Logger(ctx).Info("read packet timeout, close this connection",
						zap.Duration("idle", idleTime),
						zap.Uint64("waitTimeout", waitTimeout),
						zap.Error(err),
					)
				} else {
					errStack := errors.ErrorStack(err)
					if !strings.Contains(errStack, "use of closed network connection") {
						logutil.Logger(ctx).Warn("read packet failed, close this connection",
							zap.Error(errors.SuspendStack(err)))
					}
				}
			}
			return
		}

		// 如果读取到数据包，将状态设置为 connStatusDispatching
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) {
			return
		}

		// Hint: step I.2
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 数据分发（ dispatch() ）
		///
		// dispatch 函数的主要作用
		// 请求解析：分析从客户端接收到的数据包，提取命令或请求类型。
		// 逻辑分发：根据解析结果，将请求分发到对应的处理方法（如查询处理、事务管理、数据修改等）。
		// 错误管理：处理请求过程中可能发生的错误，并根据错误类型决定是否需要中断连接、返回错误响应或执行其他操作。
		// 返回结果：将请求处理的结果返回给客户端。
		err = cc.dispatch(ctx, data)
		// 错误处理
		if err != nil {
			if terror.ErrorEqual(err, io.EOF) {

				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Error("result undetermined, close this connection", zap.Error(err))
				return
			} else if terror.ErrCritical.Equal(err) {
				logutil.Logger(ctx).Error("critical error, stop the server listener", zap.Error(err))

				select {
				case cc.server.stopListenerCh <- struct{}{}:
				default:
				}
				return
			}
			logutil.Logger(ctx).Warn("command dispatched failed",
				zap.String("connInfo", cc.String()),
				zap.String("command", mysql.Command2Str[data[0]]),
				zap.String("status", cc.SessionStatusToString()),
				zap.Stringer("sql", getLastStmtInConn{cc}),
				zap.String("err", errStrForLog(err)),
			)
			err1 := cc.writeError(err)
			terror.Log(err1)
		}

		// 每次循环结束，重置数据包序列号，以准备处理下一个请求
		cc.pkt.sequence = 0
	}
}

// ShutdownOrNotify will Shutdown this client connection, or do its best to notify.
// 关闭客户端连接或尽量通知客户端连接关闭请求
func (cc *clientConn) ShutdownOrNotify() bool {
	if (cc.ctx.Status() & mysql.ServerStatusInTrans) > 0 {
		return false
	}
	// If the client connection status is reading, it's safe to shutdown it.
	if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusShutdown) {
		return true
	}
	// If the client connection status is dispatching, we can't shutdown it immediately,
	// so set the status to WaitShutdown as a notification, the client will detect it
	// and then exit.
	atomic.StoreInt32(&cc.status, connStatusWaitShutdown)
	return false
}

// 生成适合日志记录的查询字符串
func queryStrForLog(query string) string {
	const size = 4096
	if len(query) > size {
		return query[:size] + fmt.Sprintf("(len: %d)", len(query))
	}
	return query
}

// 生成适合日志记录的错误字符串
func errStrForLog(err error) string {
	if kv.ErrKeyExists.Equal(err) {
		// Do not log stack for duplicated entry error.
		return err.Error()
	}
	return errors.ErrorStack(err)
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
// dispatch 根据数据的第一个字节处理客户端请求。
// 它还从服务器获取一个令牌，用于限制并发处理的客户端数量。
// 最常用的命令是 ComQuery。
func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	// 保存最后一个数据包，在发生错误时，可以检查 lastPacket 来还原客户端的最后请求
	cc.lastPacket = data

	// 提取命令
	cmd := data[0]  // 命令类型
	data = data[1:] // 数据包内容

	// 获取并重置会话变量
	vars := cc.ctx.GetSessionVars()
	atomic.StoreUint32(&vars.Killed, 0) // 将变量 Killed 重置为 0，表示该会话没有被标记为需要终止
	// 如果命令在有效范围内，记录当前命令类型
	if cmd < mysql.ComEnd {
		cc.ctx.SetCommandValue(cmd)
	}

	dataStr := string(hack.String(data))

	// 处理不同命令
	switch cmd {

	// ComSleep 是 MySQL 内部使用的命令，通常不需要处理
	case mysql.ComSleep:
		// TODO: According to mysql document, this command is supposed to be used only internally.
		// So it's just a temp fix, not sure if it's done right.
		// Investigate this command and write test case later.
		return nil

	// 处理客户端断开连接的请求。
	// 返回 io.EOF，通知调用方连接已结束
	case mysql.ComQuit:
		return io.EOF

	// 处理客户端请求的查询结果
	case mysql.ComQuery: // Most frequently used command.
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		// 针对问题 1989，输入负载可能以字节 '\0' 结尾。
		// 虽然未找到相关的 MySQL 文档，但 MySQL 的实现接受这种情况。
		// 因此，这里移除最后的 '\0'，将负载视为以 EOF 结尾的字符串。
		if len(data) > 0 && data[len(data)-1] == 0 { // 如果数据包以 '\0' 结尾，则去掉最后一个字节
			data = data[:len(data)-1]
			dataStr = string(hack.String(data))
		}
		var err error
		// Hint: step I.2
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 调用 cc.handleQuery 方法处理 SQL 查询，并返回结果：
		err = cc.handleQuery(ctx, dataStr)
		return err

	// 检查服务器是否在线
	case mysql.ComPing:
		return cc.writeOK()

	// 切换当前连接的数据库
	case mysql.ComInitDB:
		// 调用 cc.useDB 方法设置数据库
		if err := cc.useDB(ctx, dataStr); err != nil {
			return err
		}
		return cc.writeOK()

	// 请求字段元数据
	case mysql.ComFieldList:
		return cc.handleFieldList(dataStr)

	// 对于不支持的命令类型，返回错误
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	}
}

// 切换数据库
func (cc *clientConn) useDB(ctx context.Context, db string) (err error) {
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	_, err = cc.ctx.Execute(ctx, "use `"+db+"`")
	if err != nil {
		return err
	}
	cc.dbname = db
	return
}

// 刷新数据包
func (cc *clientConn) flush() error {
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	return cc.pkt.flush()
}

// 向客户端发送 OK 数据包
func (cc *clientConn) writeOK() error {
	return cc.writeOkWith("", cc.ctx.AffectedRows(), cc.ctx.LastInsertID(), cc.ctx.Status(), cc.ctx.WarningCount())
}

// 向客户端发送带有详细信息的 OK 数据包
func (cc *clientConn) writeOkWith(msg string, affectedRows, lastInsertID uint64, status, warnCnt uint16) error {
	enclen := 0
	if len(msg) > 0 {
		enclen = lengthEncodedIntSize(uint64(len(msg))) + len(msg)
	}

	data := cc.alloc.AllocWithLen(4, 32+enclen)
	data = append(data, mysql.OKHeader)
	data = dumpLengthEncodedInt(data, affectedRows)
	data = dumpLengthEncodedInt(data, lastInsertID)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, status)
		data = dumpUint16(data, warnCnt)
	}
	if enclen > 0 {
		// although MySQL manual says the info message is string<EOF>(https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html),
		// it is actually string<lenenc>
		data = dumpLengthEncodedString(data, []byte(msg))
	}

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush()
}

// 向客户端发送错误数据包
func (cc *clientConn) writeError(e error) error {
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = te.ToSQLError()
	} else {
		e := errors.Cause(originErr)
		switch y := e.(type) {
		case *terror.Error:
			m = y.ToSQLError()
		default:
			m = mysql.NewErrf(mysql.ErrUnknown, "%s", e.Error())
		}
	}

	cc.lastCode = m.Code
	data := cc.alloc.AllocWithLen(4, 16+len(m.Message))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush()
}

// writeEOF writes an EOF packet.
// Note this function won't flush the stream because maybe there are more
// packets following it.
// serverStatus, a flag bit represents server information
// in the packet.
// 向客户端发送 EOF 数据包
func (cc *clientConn) writeEOF(serverStatus uint16) error {
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, cc.ctx.WarningCount())
		status := cc.ctx.Status()
		status |= serverStatus
		data = dumpUint16(data, status)
	}

	err := cc.writePacket(data)
	return err
}

// handleQuery executes the sql query string and writes result set or result ok to the client.
func (cc *clientConn) handleQuery(ctx context.Context, sql string) (err error) {

	// 结果集是指从数据库查询中返回的一组数据。
	// 结果集通常包含多行数据，每行数据包含多个列，可以用于遍历和处理查询返回的数据。
	var rss []ResultSet

	// Hint: step I.3
	// YOUR CODE HERE (lab4)
	// panic("YOUR CODE HERE")
	// 调用执行器的 Execute 函数执行 SQL 查询，并将结果集存储到 rss 中
	rss, err = cc.ctx.Execute(ctx, sql)
	if err != nil {
		return err
	}

	// 检查连接状态，如果连接状态为 connStatusShutdown 或 connStatusWaitShutdown，则关闭结果集
	// 关闭结果集是指释放与结果集相关的资源，以便系统可以回收这些资源。具体来说，关闭结果集通常包括以下操作：
	// 释放内存：释放存储结果集数据的内存。
	// 关闭游标：如果查询使用了数据库游标，关闭游标以释放数据库连接资源。
	// 清理临时数据：删除与结果集相关的临时数据或文件。
	status := atomic.LoadInt32(&cc.status)
	if rss != nil && (status == connStatusShutdown || status == connStatusWaitShutdown) {
		for _, rs := range rss {
			terror.Call(rs.Close)
		}
		return executor.ErrQueryInterrupted
	}
	// 如果结果集不为空，根据结果集的数量调用 writeResultset 或 writeMultiResultset 方法向客户端发送结果集
	// 为了流式的将执行的结果返回给客户端，而不是将所有结果存放在 DBMS 的内存中
	if rss != nil {
		if len(rss) == 1 {
			err = cc.writeResultset(ctx, rss[0], false, 0, 0)
		} else {
			err = cc.writeMultiResultset(ctx, rss, false)
		}
	} else {
		err = cc.writeOK()
	}
	return err
}

// handleFieldList returns the field list for a table.
// The sql string is composed of a table name and a terminating character \x00.
// 返回表的字段列表。
// 具体来说，它处理客户端发送的 COM_FIELD_LIST 命令，返回指定表的字段信息
func (cc *clientConn) handleFieldList(sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return err
	}
	data := cc.alloc.AllocWithLen(4, 1024)
	for _, column := range columns {
		// Current we doesn't output defaultValue but reserve defaultValue length byte to make mariadb client happy.
		// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
		// TODO: fill the right DefaultValues.
		column.DefaultValueLength = 0
		column.DefaultValue = []byte{}

		data = data[0:4]
		data = column.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(0); err != nil {
		return err
	}
	return cc.flush()
}

// writeResultset writes data into a resultset and uses rs.Next to get row data back.
// If binary is true, the data would be encoded in BINARY format.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
// resultsets, it's used to support the MULTI_RESULTS capability in mysql protocol.
// 将数据写入结果集，并使用 rs.Next 获取行数据
func (cc *clientConn) writeResultset(ctx context.Context, rs ResultSet, binary bool, serverStatus uint16, fetchSize int) (runErr error) {
	defer func() {
		// close ResultSet when cursor doesn't exist
		// 当游标不存在时关闭 ResultSet
		if !mysql.HasCursorExistsFlag(serverStatus) {
			terror.Call(rs.Close)
		}
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(string); !ok {
			panic(r)
		}

		runErr = errors.Errorf("%v", r)
		buf := make([]byte, 4096)
		stackSize := runtime.Stack(buf, false)
		buf = buf[:stackSize]
		logutil.Logger(ctx).Error("write query result panic", zap.Stringer("lastSQL", getLastStmtInConn{cc}), zap.String("stack", string(buf)))
	}()

	var err error
	// 如果 serverStatus 中有游标存在标志，调用 writeChunksWithFetchSize 方法写入数据
	// 否则，调用 writeChunks 方法写入数据
	///
	// 游标的存在与否代表了不同的查询处理方式和数据传输方式。
	/// 游标存在时，适用于逐行处理结果集、分页查询和长时间查询；游标不存在时，适用于一次性返回结果集和简化数据传输。
	if mysql.HasCursorExistsFlag(serverStatus) {
		err = cc.writeChunksWithFetchSize(ctx, rs, serverStatus, fetchSize)
	} else {
		err = cc.writeChunks(ctx, rs, binary, serverStatus)
	}
	if err != nil {
		return err
	}

	return cc.flush()
}

// 将列信息写入到客户端
// 将列信息编码成数据包并发送给客户端，最后发送一个 EOF 数据包表示列信息的结束。
func (cc *clientConn) writeColumnInfo(columns []*ColumnInfo, serverStatus uint16) error {
	data := cc.alloc.AllocWithLen(4, 1024)
	data = dumpLengthEncodedInt(data, uint64(len(columns)))
	if err := cc.writePacket(data); err != nil {
		return err
	}
	for _, v := range columns {
		data = data[0:4]
		data = v.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	return cc.writeEOF(serverStatus)
}

// writeChunks writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information
func (cc *clientConn) writeChunks(ctx context.Context, rs ResultSet, binary bool, serverStatus uint16) error {
	// 分配数据包的内存
	data := cc.alloc.AllocWithLen(4, 1024)

	// 创建一个 Chunk 用于存储从 ResultSet 获取的数据
	req := rs.NewChunk()

	// 初始化标志变量
	gotColumnInfo := false

	// 循环从 ResultSet 获取数据并写入到连接中
	for {
		var err error
		// Here server.tidbResultSet implements Next method.
		// Hint: step I.4.4
		// YOUR CODE HERE (lab4)
		// panic("YOUR CODE HERE")
		// 调用 ResultSet.Next 函数来执行
		// 每次调用会返回一条数据，直到返回的数据为空，说明执行完成
		err = rs.Next(ctx, req)
		if err != nil {
			return err
		}

		// 如果没有获取到列信息，调用 writeColumnInfo 方法写入列信息
		if !gotColumnInfo {
			// We need to call Next before we get columns.
			// Otherwise, we will get incorrect columns info.
			columns := rs.Columns()
			err = cc.writeColumnInfo(columns, serverStatus)
			if err != nil {
				return err
			}
			gotColumnInfo = true
		}

		// 获取当前 Chunk 中的行数
		rowCount := req.NumRows()
		// 如果行数为 0，说明数据已经读取完毕，退出循环
		if rowCount == 0 {
			break
		}
		// 遍历 Chunk 中的每一行数据
		for i := 0; i < rowCount; i++ {
			data = data[0:4]
			if binary {
				data, err = dumpBinaryRow(data, rs.Columns(), req.GetRow(i))
			} else {
				data, err = dumpTextRow(data, rs.Columns(), req.GetRow(i))
			}
			if err != nil {
				return err
			}
			if err = cc.writePacket(data); err != nil {
				return err
			}
		}
	}

	return cc.writeEOF(serverStatus)
}

// writeChunksWithFetchSize writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
func (cc *clientConn) writeChunksWithFetchSize(ctx context.Context, rs ResultSet, serverStatus uint16, fetchSize int) error {
	fetchedRows := rs.GetFetchedRows()

	// if fetchedRows is not enough, getting data from recordSet.
	req := rs.NewChunk()
	for len(fetchedRows) < fetchSize {
		// Here server.tidbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			return err
		}
		rowCount := req.NumRows()
		if rowCount == 0 {
			break
		}
		// filling fetchedRows with chunk
		for i := 0; i < rowCount; i++ {
			fetchedRows = append(fetchedRows, req.GetRow(i))
		}
		req = chunk.Renew(req, cc.ctx.GetSessionVars().MaxChunkSize)
	}

	// tell the client COM_STMT_FETCH has finished by setting proper serverStatus,
	// and close ResultSet.
	if len(fetchedRows) == 0 {
		serverStatus |= mysql.ServerStatusLastRowSend
		terror.Call(rs.Close)
		return cc.writeEOF(serverStatus)
	}

	// construct the rows sent to the client according to fetchSize.
	var curRows []chunk.Row
	if fetchSize < len(fetchedRows) {
		curRows = fetchedRows[:fetchSize]
		fetchedRows = fetchedRows[fetchSize:]
	} else {
		curRows = fetchedRows[:]
		fetchedRows = fetchedRows[:0]
	}
	rs.StoreFetchedRows(fetchedRows)

	data := cc.alloc.AllocWithLen(4, 1024)
	var err error
	for _, row := range curRows {
		data = data[0:4]
		data, err = dumpBinaryRow(data, rs.Columns(), row)
		if err != nil {
			return err
		}
		if err = cc.writePacket(data); err != nil {
			return err
		}
	}
	if cl, ok := rs.(fetchNotifier); ok {
		cl.OnFetchReturned()
	}
	return cc.writeEOF(serverStatus)
}

func (cc *clientConn) writeMultiResultset(ctx context.Context, rss []ResultSet, binary bool) error {
	for i, rs := range rss {
		lastRs := i == len(rss)-1
		if r, ok := rs.(*tidbResultSet).recordSet.(sqlexec.MultiQueryNoDelayResult); ok {
			status := r.Status()
			if !lastRs {
				status |= mysql.ServerMoreResultsExists
			}
			if err := cc.writeOkWith("", r.AffectedRows(), r.LastInsertID(), status, r.WarnCount()); err != nil {
				return err
			}
			continue
		}
		status := uint16(0)
		if !lastRs {
			status |= mysql.ServerMoreResultsExists
		}
		if err := cc.writeResultset(ctx, rs, binary, status, 0); err != nil {
			return err
		}
	}
	return nil
}

func (cc *clientConn) setConn(conn net.Conn) {
	cc.bufReadConn = newBufferedReadConn(conn)
	if cc.pkt == nil {
		cc.pkt = newPacketIO(cc.bufReadConn)
	} else {
		// Preserve current sequence number.
		cc.pkt.setBufferedReadConn(cc.bufReadConn)
	}
}

func (cc *clientConn) upgradeToTLS(tlsConfig *tls.Config) error {
	// Important: read from buffered reader instead of the original net.Conn because it may contain data we need.
	tlsConn := tls.Server(cc.bufReadConn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	cc.setConn(tlsConn)
	cc.tlsConn = tlsConn
	return nil
}

var _ fmt.Stringer = getLastStmtInConn{}

type getLastStmtInConn struct {
	*clientConn
}

func (cc getLastStmtInConn) String() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case mysql.ComInitDB:
		return "Use " + string(data)
	case mysql.ComFieldList:
		return "ListFields " + string(data)
	case mysql.ComQuery, mysql.ComStmtPrepare:
		return queryStrForLog(string(hack.String(data)))
	case mysql.ComStmtClose, mysql.ComStmtReset:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return mysql.Command2Str[cmd] + " " + strconv.Itoa(int(stmtID))
	default:
		if cmdStr, ok := mysql.Command2Str[cmd]; ok {
			return cmdStr
		}
		return string(hack.String(data))
	}
}
