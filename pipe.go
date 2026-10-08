// Copyright 2025 VNXME
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A significant part of the code in this file was copied from
// https://github.com/golang/go/tree/go1.25.5 in December 2025.
// Source: https://github.com/golang/go/blob/go1.25.5/src/net/pipe.go
// License: https://github.com/golang/go/blob/go1.25.5/LICENSE
// The original copyright is preserved below.

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hybrid

import (
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// pipeAddr implements the net.Addr interface for the pipe network type.
type pipeAddr struct{}

// Network returns the network name for the pipe address.
func (pipeAddr) Network() string { return pipeNetwork }

// String returns the network name for the pipe address.
func (pipeAddr) String() string { return pipeNetwork }

// pipeChunk is an internal type that represents a write, or its part left unread by a sync pipe,
// along with the reference to its pooled buffer, if any. It is used to transfer data between
// the two ends of a pipe via channels.
type pipeChunk struct {
	ref *[]byte
	buf []byte
}

// pipe represents a stream-oriented communication channel between local and remote endpoints.
type pipe struct {
	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	// Used by local Read to interact with remote Write.
	// For a sync/semi-sync pipe, receive on rdRx is always followed by send on rdTx
	rdRx <-chan pipeChunk
	rdTx chan<- int

	// Used by local Write to interact with remote Read.
	// For a sync/semi-sync pipe, send on wrTx is always followed by receive on wrRx
	wrTx chan<- pipeChunk
	wrRx <-chan int

	localDone  chan struct{}
	remoteDone <-chan struct{}

	rdRef *[]byte // Reference to the pooled buffer of rdBuf, if any
	rdBuf []byte  // Unread bytes retained from a remote Write

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
	wrMu sync.Mutex // Serialize Write operations

	retain bool // Read retains unread bytes, so Write needn't wait for them to be read
}

// BufferedPipe creates an in-memory, full duplex network connection
// that is buffered and asynchronous; both ends implement the StreamConn interface.
// A write on one end may be consumed by several reads on the other. It returns
// once it has been queued, unless the queue already holds queueSize writes,
// in which case it blocks. Data are copied internally, so the write buffer
// may be reused once a write returns. When the remote end is closed, reads
// return io.EOF once all the bytes queued internally have been read.
//
// BufferedPipe panics if queueSize isn't positive.
func BufferedPipe(queueSize int) (StreamConn, StreamConn) {
	if queueSize <= 0 {
		panic("hybrid: BufferedPipe: non-positive queueSize")
	}
	return newPipe(queueSize, true)
}

// UnbufferedPipe creates an in-memory, full duplex network connection
// that is unbuffered; both ends implement the StreamConn interface.
// A write on one end may be consumed by several reads on the other,
// but the moment the write returns depends on semiSync:
//
// (a) Synchronous (semiSync == false).
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal
// buffering. A write returns once all its bytes have been read,
// the same way as with [net.Pipe].
//
// (b) Semi-synchronous (semiSync == true).
// A write returns once a read on the other end has received it,
// even if the read buffer is shorter than the write buffer: the
// remaining bytes are retained internally for subsequent reads,
// and the next write blocks until they have been read. Data are
// copied internally, so the write buffer may be reused once a write
// returns, which is trivially true for (a) as well.
//
// When the remote end is closed, reads return io.EOF once all the bytes
// retained internally in (b) have been read.
func UnbufferedPipe(semiSync bool) (StreamConn, StreamConn) {
	return newPipe(0, semiSync)
}

// newPipe creates a pipe of the mode specified by queueSize and semiSync,
// see [BufferedPipe] and [UnbufferedPipe]. A positive queueSize implies semiSync.
func newPipe(queueSize int, semiSync bool) (StreamConn, StreamConn) {
	retain := semiSync || queueSize > 0 // Read retains unread bytes unless the pipe is sync

	cb1 := make(chan pipeChunk, queueSize)
	cb2 := make(chan pipeChunk, queueSize)
	cn1 := make(chan int)
	cn2 := make(chan int)
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	p1 := &pipe{
		rdRx: cb1, rdTx: cn1,
		wrTx: cb2, wrRx: cn2,
		localDone: done1, remoteDone: done2,
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
		retain:     retain,
	}
	p2 := &pipe{
		rdRx: cb2, rdTx: cn2,
		wrTx: cb1, wrRx: cn1,
		localDone: done2, remoteDone: done1,
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
		retain:     retain,
	}
	return p1, p2
}

// LocalAddr returns the local address of the pipe end. The address is the same for all pipes.
func (*pipe) LocalAddr() net.Addr { return onePipeAddr }

// RemoteAddr returns the remote address of the pipe end. The address is the same for all pipes.
func (*pipe) RemoteAddr() net.Addr { return onePipeAddr }

// Read reads up to len(b) bytes from the pipe, returning the number of bytes read and an error.
func (p *pipe) Read(b []byte) (int, error) {
	n, err := p.read(b)
	if err != nil && err != io.EOF { // Like TCP, io.EOF isn't wrapped, as callers compare it with ==
		err = p.opError("read", err)
	}
	return n, err
}

// read reads up to len(b) bytes from the pipe and returns the number of bytes copied and any error.
//
// It serializes reads, since bytes retained by a semi-sync or async pipe are shared by them, and
// blocks until any bytes are received or the pipe is closed or a deadline expires. It returns
// bytes retained from a previous write first, and retains any bytes of a write left unread.
func (p *pipe) read(b []byte) (n int, err error) {
	// Fail fast without waiting for rdMu. Remote closure isn't checked here, because
	// io.EOF depends on rdBuf guarded by rdMu. A concurrent Read holding rdMu observes
	// remoteDone as well, so it releases rdMu promptly in this case.
	switch {
	case isClosedChan(p.localDone):
		return 0, errPipeClosed
	case len(b) == 0: // Return immediately like TCP does, without consuming any remote Write
		return 0, nil
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, errPipeDeadlineExceeded
	}

	p.rdMu.Lock()
	defer p.rdMu.Unlock()
	switch {
	case isClosedChan(p.localDone):
		return 0, errPipeClosed
	case isClosedChan(p.rdDeadline.Wait()): // Like TCP, an exceeded deadline prevails over io.EOF and retained bytes
		return 0, errPipeDeadlineExceeded
	case isClosedChan(p.remoteDone) && p.rdBuf == nil && len(p.rdRx) == 0:
		return 0, io.EOF
	case p.rdBuf != nil: // Only a semi-sync/async pipe retains unread bytes
		nr := copy(b, p.rdBuf)
		if nr < len(p.rdBuf) {
			p.rdBuf = p.rdBuf[nr:]
		} else {
			putBuf(p.rdRef) // Nothing refers to a pooled buffer once its bytes are read
			p.rdBuf, p.rdRef = nil, nil
		}
		return nr, nil
	}

	var chunk pipeChunk
	select { // Receive a pending write without a blocking select, which is much slower
	case chunk = <-p.rdRx:
	default:
		select {
		case chunk = <-p.rdRx:
		case <-p.localDone:
			return 0, errPipeClosed
		case <-p.remoteDone:
			// Only an async pipe may have queued bytes. No need to check p.rdBuf here:
			// unless it was empty, the function would return above
			if len(p.rdRx) == 0 {
				return 0, io.EOF
			}
			chunk = <-p.rdRx
		case <-p.rdDeadline.Wait():
			return 0, errPipeDeadlineExceeded
		}
	}

	bw := chunk.buf
	nr := copy(b, bw)
	if cap(p.rdRx) == 0 { // An unbuffered pipe can be sync or semi-sync
		if p.retain { // A semi-sync pipe retains unread bytes and reports all of them written
			if nr < len(bw) { // Copy before reporting, as remote Write returns and b may be reused
				p.rdBuf, p.rdRef = getBuf(len(bw) - nr)
				copy(p.rdBuf, bw[nr:])
			}
			p.rdTx <- len(bw)
		} else { // A sync pipe reports only read bytes, so Write resends the rest
			p.rdTx <- nr
		}
	} else if nr < len(bw) { // A buffered pipe is async by definition and retains unread bytes
		p.rdBuf, p.rdRef = bw[nr:], chunk.ref
	} else {
		putBuf(chunk.ref) // Nothing refers to a pooled buffer once its bytes are read
	}
	return nr, nil
}

// Write writes len(b) bytes to the pipe, returning the number of bytes written and an error.
func (p *pipe) Write(b []byte) (int, error) {
	n, err := p.write(b)
	if err != nil {
		err = p.opError("write", err)
	}
	return n, err
}

// write sends the given byte slice b to the remote end and returns the number of bytes written and any error.
//
// It serializes writes, so that the entirety of b is written together, and blocks until all the bytes
// have been read by a sync pipe, received by a semi-sync one, or queued by an async one, or until
// the pipe is closed or a deadline expires. For async pipes, it copies b to avoid slice aliasing issues.
func (p *pipe) write(b []byte) (n int, err error) {
	switch {
	case isClosedChan(p.localDone):
		return 0, errPipeClosed
	case isClosedChan(p.remoteDone):
		return 0, errPipeBroken
	case isClosedChan(p.wrDeadline.Wait()):
		return 0, errPipeDeadlineExceeded
	case len(b) == 0: // Send nothing like TCP does, so that no remote Read returns 0, nil
		return 0, nil
	}

	p.wrMu.Lock() // Ensure entirety of b is written together
	defer p.wrMu.Unlock()
	// An async pipe requires copy on write, as Write returns before b is read
	var ref *[]byte // Reference to the pooled buffer of b, if any
	if cap(p.wrTx) > 0 {
		src := b
		b, ref = getBuf(len(src))
		copy(b, src)
	}
	for len(b) > 0 {
		// Check again before every send, as the pipe may have been closed or the deadline exceeded
		// while waiting for wrMu or for the previous part of b to be read. Otherwise, select would
		// choose randomly between them and a send ready to proceed, letting the write succeed.
		switch {
		case isClosedChan(p.localDone):
			return n, errPipeClosed
		case isClosedChan(p.remoteDone):
			return n, errPipeBroken
		case isClosedChan(p.wrDeadline.Wait()):
			return n, errPipeDeadlineExceeded
		}

		select { // Send without a blocking select if possible, which is much slower
		case p.wrTx <- pipeChunk{ref: ref, buf: b}:
		default:
			select {
			case p.wrTx <- pipeChunk{ref: ref, buf: b}:
			case <-p.localDone:
				return n, errPipeClosed
			case <-p.remoteDone:
				return n, errPipeBroken
			case <-p.wrDeadline.Wait():
				return n, errPipeDeadlineExceeded
			}
		}

		if cap(p.wrTx) > 0 { // A buffered pipe is async by definition
			return len(b), nil
		}
		nw := <-p.wrRx // An unbuffered pipe can be sync or semi-sync
		b = b[nw:]
		n += nw
	}
	return n, nil
}

// SetDeadline sets the read and write deadlines. Like TCP and unlike [net.Pipe], it fails
// only once the local end is closed, since bytes retained or queued may be read after remote closure.
func (p *pipe) SetDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return p.opError("set", errPipeClosed)
	}
	p.rdDeadline.Set(t)
	p.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline. Like TCP and unlike [net.Pipe], it fails only
// once the local end is closed, since bytes retained or queued may be read after remote closure.
func (p *pipe) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return p.opError("set", errPipeClosed)
	}
	p.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline. Like TCP and unlike [net.Pipe], it fails only
// once the local end is closed, while a write fails with a broken pipe error after remote closure.
func (p *pipe) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return p.opError("set", errPipeClosed)
	}
	p.wrDeadline.Set(t)
	return nil
}

// Close closes the pipe end. Like TCP and unlike [net.Pipe], it returns net.ErrClosed
// if the pipe end is already closed.
func (p *pipe) Close() error {
	closed := false
	p.once.Do(func() {
		close(p.localDone)
		closed = true
	})
	if !closed {
		return p.opError("close", errPipeClosed)
	}
	return nil
}

// opError wraps err in net.OpError the same way as a TCP connection does: Source is the local
// address, and Addr is the remote one. Setting a deadline reports the local address as Addr only.
func (*pipe) opError(op string, err error) error {
	if op == "set" {
		return &net.OpError{Op: op, Net: pipeNetwork, Addr: onePipeAddr, Err: err}
	}
	return &net.OpError{Op: op, Net: pipeNetwork, Source: onePipeAddr, Addr: onePipeAddr, Err: err}
}

const pipeNetwork = "pipe"

// Shared variables
var (
	errPipeClosed           = net.ErrClosed // Reported by using a closed pipe end, the same way as by TCP
	errPipeDeadlineExceeded = os.ErrDeadlineExceeded

	onePipeAddr = pipeAddr{}
)

// Interface guards
var (
	_ net.Addr   = pipeAddr{}
	_ StreamConn = (*pipe)(nil)
)
