// Copyright 2018 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// This file contains the implementation of the physical Projection Operator:
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
//
// NOTE:
// 1. The number of "projectionWorker" is controlled by the global session
//    variable "tidb_projection_concurrency".
// 2. Unparallel version is used when one of the following situations occurs:
//    a. "tidb_projection_concurrency" is set to 0.
//    b. The estimated input size is smaller than "tidb_max_chunk_size".
//    c. This projection can not be executed vectorially.

type projectionInput struct {
	chk          *chunk.Chunk
	targetWorker *projectionWorker
}

type projectionOutput struct {
	chk  *chunk.Chunk
	done chan error
}

// ProjectionExec implements the physical Projection Operator:
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
type ProjectionExec struct {
	baseExecutor

	evaluatorSuit *expression.EvaluatorSuite

	prepared    bool
	finishCh    chan struct{}
	outputCh    chan *projectionOutput
	fetcher     projectionInputFetcher
	numWorkers  int64
	workers     []*projectionWorker
	childResult *chunk.Chunk

	wg sync.WaitGroup

	// parentReqRows indicates how many rows the parent executor is
	// requiring. It is set when parallelExecute() is called and used by the
	// concurrent projectionInputFetcher.
	//
	// NOTE: It should be protected by atomic operations.
	parentReqRows int64
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *ProjectionExec) open(ctx context.Context) error {
	e.prepared = false
	e.parentReqRows = int64(e.maxChunkSize)

	// For now a Projection can not be executed vectorially only because it
	// contains "SetVar" or "GetVar" functions, in this scenario this
	// Projection can not be executed parallelly.
	if e.numWorkers > 0 && !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 0
	}

	if e.isUnparallelExec() {
		e.childResult = newFirstChunk(e.children[0])
	}

	return nil
}

// Next implements the Executor Next interface.
//
// Here we explain the execution flow of the parallel projection implementation.
// There are 3 main components:
//  1. "projectionInputFetcher": Fetch input "Chunk" from child.
//  2. "projectionWorker":       Do the projection work.
//  3. "ProjectionExec.Next":    Return result to parent.
//
// 1. "projectionInputFetcher" gets its input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it fetches child's result into "input.chk" and:
//
//	a. Dispatches this input to the worker specified in "input.targetWorker"
//	b. Dispatches this output to the main thread: "ProjectionExec.Next"
//	c. Dispatches this output to the worker specified in "input.targetWorker"
//
// It is finished and exited once:
//
//	a. There is no more input from child.
//	b. "ProjectionExec" close the "globalFinishCh"
//
// 2. "projectionWorker" gets its input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it calculates the projection result use "input.chk" as the input
// and "output.chk" as the output, once the calculation is done, it:
//
//	a. Sends "nil" or error to "output.done" to mark this input is finished.
//	b. Returns the "input" resource to "projectionInputFetcher.inputCh"
//
// They are finished and exited once:
//
//	a. "ProjectionExec" closes the "globalFinishCh"
//
// 3. "ProjectionExec.Next" gets its output resources from its "outputCh" channel.
// After receiving an output from "outputCh", it should wait to receive a "nil"
// or error from "output.done" channel. Once a "nil" or error is received:
//
//	a. Returns this output to its parent
//	b. Returns the "output" resource to "projectionInputFetcher.outputCh"
//
// 三个主要组件：
// projectionInputFetcher：从子执行器获取输入 Chunk。
// projectionWorker：执行投影工作。
// ProjectionExec.Next：将结果返回给父执行器。
//
// 1.projectionInputFetcher 从其 inputCh 和 outputCh 通道获取输入和输出资源，一旦获取到输入和输出资源，
// 它将子执行器的结果获取到 input.chk 中，并且：
//
//	a. 将此输入分派给 input.targetWorker 指定的工作线程。
//	b. 将此输出分派给主线程：ProjectionExec.Next。
//	c. 将此输出分派给 input.targetWorker 指定的工作线程。
//
// 当以下情况发生时，它将完成并退出：
//
//	a. 子执行器没有更多输入。
//	b. ProjectionExec 关闭 globalFinishCh。
//
// 2.projectionWorker 从其 inputCh 和 outputCh 通道获取输入和输出资源，一旦获取到输入和输出资源，
// 它使用 input.chk 作为输入，output.chk 作为输出来计算投影结果，一旦计算完成，它将：
//
//	a. 发送 nil 或错误到 output.done，以标记此输入已完成。
//	b. 将 input 资源返回给 projectionInputFetcher.inputCh。
//
// 当以下情况发生时，它们将完成并退出：
//
//	a. ProjectionExec 关闭 globalFinishCh。
//
// ProjectionExec.Next 从其 outputCh 通道获取输出资源。
// 在从 outputCh 接收到输出后，它应该等待从 output.done 通道接收 nil 或错误。一旦接收到 nil 或错误：
//
//		a. 将此输出返回给其父执行器。
//		b. 将 output 资源返回给 projectionInputFetcher.outputCh。
//	+-----------+----------------------+--------------------------+
//	|           |                      |                          |
//	|  +--------+---------+   +--------+---------+       +--------+---------+
//	|  | projectionWorker |   + projectionWorker |  ...  + projectionWorker |
//	|  +------------------+   +------------------+       +------------------+
//	|       ^       ^              ^       ^                  ^       ^
//	|       |       |              |       |                  |       |
//	|    inputCh outputCh       inputCh outputCh           inputCh outputCh
//	|       ^       ^              ^       ^                  ^       ^
//	|       |       |              |       |                  |       |
//	|                              |       |
//	|                              |       +----------------->outputCh
//	|                              |       |                      |
//	|                              |       |                      v
//	|                      +-------+-------+--------+   +---------------------+
//	|                      | projectionInputFetcher |   | ProjectionExec.Next |
//	|                      +------------------------+   +---------+-----------+
//	|                              ^       ^                      |
//	|                              |       |                      |
//	|                           inputCh outputCh                  |
//	|                              ^       ^                      |
//	|                              |       |                      |
//	+------------------------------+       +----------------------+
func (e *ProjectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.isUnparallelExec() {
		return e.unParallelExecute(ctx, req)
	}
	return e.parallelExecute(ctx, req)
}

func (e *ProjectionExec) isUnparallelExec() bool {
	return e.numWorkers <= 0
}

func (e *ProjectionExec) unParallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	// transmit the requiredRows
	e.childResult.SetRequiredRows(chk.RequiredRows(), e.maxChunkSize)
	err := Next(ctx, e.children[0], e.childResult)
	if err != nil {
		return err
	}
	if e.childResult.NumRows() == 0 {
		return nil
	}
	err = e.evaluatorSuit.Run(e.ctx, e.childResult, chk)
	return err
}

func (e *ProjectionExec) parallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	atomic.StoreInt64(&e.parentReqRows, int64(chk.RequiredRows()))
	if !e.prepared {
		e.prepare(ctx)
		e.prepared = true
	}

	var (
		output *projectionOutput
		ok     bool
	)
	// Get the output from fetcher
	// Hint: step III.3.1
	// YOUR CODE HERE (lab4)
	// 外部线程不停的调用 ProjectionExec.Next 获取处理完成的数据，
	// 在并行处理时会调用 ProjectionExec.parallelExecute。
	// 从 ProjectionExec.outputCh 中拿到数据
	output, ok = <-e.outputCh
	if !ok {
		return nil
	}

	err := <-output.done
	if err != nil {
		return err
	}

	// 通过 Chunk.SwapColumns 将数据写入外部传入的 Chunk 中
	chk.SwapColumns(output.chk)
	// 返回给 projectionInputFetcher，以便重复使用
	e.fetcher.outputCh <- output
	return nil
}

func (e *ProjectionExec) prepare(ctx context.Context) {
	e.finishCh = make(chan struct{})
	e.outputCh = make(chan *projectionOutput, e.numWorkers)

	// Initialize projectionInputFetcher.
	// 初始化 projectionInputFetcher
	e.fetcher = projectionInputFetcher{
		proj:           e,
		child:          e.children[0],
		globalFinishCh: e.finishCh,
		globalOutputCh: e.outputCh,
		inputCh:        make(chan *projectionInput, e.numWorkers),
		outputCh:       make(chan *projectionOutput, e.numWorkers),
	}

	// Initialize projectionWorker.
	// 初始化 projectionWorker
	e.workers = make([]*projectionWorker, 0, e.numWorkers)
	for i := int64(0); i < e.numWorkers; i++ {
		e.workers = append(e.workers, &projectionWorker{
			proj:            e,
			sctx:            e.ctx,
			evaluatorSuit:   e.evaluatorSuit,
			globalFinishCh:  e.finishCh,
			inputGiveBackCh: e.fetcher.inputCh,
			inputCh:         make(chan *projectionInput, 1),
			outputCh:        make(chan *projectionOutput, 1),
		})

		inputChk := newFirstChunk(e.children[0])
		e.fetcher.inputCh <- &projectionInput{
			chk:          inputChk,
			targetWorker: e.workers[i],
		}

		outputChk := newFirstChunk(e)
		e.fetcher.outputCh <- &projectionOutput{
			chk:  outputChk,
			done: make(chan error, 1),
		}
	}

	// 启动 projectionInputFetcher 和 projectionWorker
	e.wg.Add(1)
	go e.fetcher.run(ctx)

	for i := range e.workers {
		e.wg.Add(1)
		go e.workers[i].run(ctx)
	}
}

func (e *ProjectionExec) drainInputCh(ch chan *projectionInput) {
	close(ch)
	for range ch {
	}
}

func (e *ProjectionExec) drainOutputCh(ch chan *projectionOutput) {
	close(ch)
	for range ch {
	}
}

// Close implements the Executor Close interface.
func (e *ProjectionExec) Close() error {
	if e.isUnparallelExec() {
		e.childResult = nil
	}
	if e.prepared {
		close(e.finishCh)
		e.wg.Wait() // Wait for fetcher and workers to finish and exit.

		// clear fetcher
		e.drainInputCh(e.fetcher.inputCh)
		e.drainOutputCh(e.fetcher.outputCh)

		// clear workers
		for _, w := range e.workers {
			e.drainInputCh(w.inputCh)
			e.drainOutputCh(w.outputCh)
		}
	}
	return e.baseExecutor.Close()
}

type projectionInputFetcher struct {
	proj           *ProjectionExec
	child          Executor
	globalFinishCh <-chan struct{}
	globalOutputCh chan<- *projectionOutput

	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

// run gets projectionInputFetcher's input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it fetches child's result into "input.chk" and:
//
//	a. Dispatches this input to the worker specified in "input.targetWorker"
//	b. Dispatches this output to the main thread: "ProjectionExec.Next"
//	c. Dispatches this output to the worker specified in "input.targetWorker"
//
// It is finished and exited once:
//
//	a. There is no more input from child.
//	b. "ProjectionExec" close the "globalFinishCh"
func (f *projectionInputFetcher) run(ctx context.Context) {
	var output *projectionOutput
	defer func() {
		if r := recover(); r != nil {
			recoveryProjection(output, r)
		}
		close(f.globalOutputCh)
		f.proj.wg.Done()
	}()

	for {
		// 从 projectionInputFetcher.inputCh 通道获取 projectionInput（空的）
		input := readProjectionInput(f.inputCh, f.globalFinishCh)
		if input == nil {
			return
		}
		targetWorker := input.targetWorker
		logutil.Logger(ctx).Debug("targetWorker", zap.Int("targetWorker", len(targetWorker.inputCh)))

		// 从 outputCh 通道获取工作线程已经处理好的 projectionOutput
		output = readProjectionOutput(f.outputCh, f.globalFinishCh)
		if output == nil {
			return
		}

		// Send processed output to global output
		// Hint: step III.3.2
		// YOUR CODE HERE (lab4)
		// 将上一个任务的处理结果发往全局输出通道，供主线程消费
		f.globalOutputCh <- output

		requiredRows := atomic.LoadInt64(&f.proj.parentReqRows) // 获取父执行器所需的行数
		// 设置读取需求（所需的行数）
		input.chk.SetRequiredRows(int(requiredRows), f.proj.maxChunkSize)
		// 调用子执行器读取数据
		err := Next(ctx, f.child, input.chk)
		if err != nil || input.chk.NumRows() == 0 {
			output.done <- err
			return
		}

		// Give the input and output back to worker
		// Hint: step III.3.2
		// YOUR CODE HERE (lab4)
		// 将填充了数据的 projectionInput 发送到目标工作线程的 inputCh
		targetWorker.inputCh <- input
		// 将空闲的 projectionOutput 发送到目标工作线程的 outputCh，供工作线程存储计算结果
		targetWorker.outputCh <- output
	}
}

type projectionWorker struct {
	proj            *ProjectionExec
	sctx            sessionctx.Context
	evaluatorSuit   *expression.EvaluatorSuite
	globalFinishCh  <-chan struct{}
	inputGiveBackCh chan<- *projectionInput

	// channel "input" and "output" is :
	// a. initialized by "ProjectionExec.prepare"
	// b. written	  by "projectionInputFetcher.run"
	// c. read    	  by "projectionWorker.run"
	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

// run gets projectionWorker's input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it calculate the projection result use "input.chk" as the input
// and "output.chk" as the output, once the calculation is done, it:
//
//	a. Sends "nil" or error to "output.done" to mark this input is finished.
//	b. Returns the "input" resource to "projectionInputFetcher.inputCh".
//
// It is finished and exited once:
//
//	a. "ProjectionExec" closes the "globalFinishCh".
func (w *projectionWorker) run(ctx context.Context) {
	var output *projectionOutput
	defer func() {
		if r := recover(); r != nil {
			recoveryProjection(output, r)
		}
		w.proj.wg.Done()
	}()
	for {
		var (
			input  *projectionInput
			output *projectionOutput
		)
		// Get input data
		// Hint: step III.3.3
		// YOUR CODE HERE (lab4)
		// 从 Fetcher 分发的任务中（worker.inputCh）读取数据
		input = readProjectionInput(w.inputCh, w.globalFinishCh)
		if input == nil {
			return
		}

		// Get output data
		// Hint: step III.3.3
		// YOUR CODE HERE (lab4)
		// 从 w.outputCh 获取空闲的 projectionOutput
		output = readProjectionOutput(w.outputCh, w.globalFinishCh)
		if output == nil {
			return
		}

		// calculate the projection expression
		// TODO: trace memory used by the evaluatorSuit including all temporal buffers it uses
		// 执行投影计算，投影计算结果存储到 projectionOutput.chk
		err := w.evaluatorSuit.Run(w.sctx, input.chk, output.chk)
		output.done <- err

		if err != nil {
			return
		}

		// Give the input channel back
		// Hint: step III.3.3
		// YOUR CODE HERE (lab4)
		// 将处理完成的 projectionInput 发送到 w.inputGiveBackCh，供 Fetcher 重用
		w.inputGiveBackCh <- input
	}
}

func recoveryProjection(output *projectionOutput, r interface{}) {
	if output != nil {
		output.done <- errors.Errorf("%v", r)
	}
	buf := util.GetStack()
	logutil.BgLogger().Error("projection executor panicked", zap.String("error", fmt.Sprintf("%v", r)), zap.String("stack", string(buf)))
}

func readProjectionInput(inputCh <-chan *projectionInput, finishCh <-chan struct{}) *projectionInput {
	select {
	case <-finishCh:
		return nil
	case input, ok := <-inputCh:
		if !ok {
			return nil
		}
		return input
	}
}

func readProjectionOutput(outputCh <-chan *projectionOutput, finishCh <-chan struct{}) *projectionOutput {
	select {
	case <-finishCh:
		return nil
	case output, ok := <-outputCh:
		if !ok {
			return nil
		}
		return output
	}
}
