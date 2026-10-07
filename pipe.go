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

// Major changes to the standard pipe implementation:
// - read and write channels can be buffered or unbuffered
// - pipes can be sync, semi-sync or async

package hybrid

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return pipeNetwork }
func (pipeAddr) String() string  { return pipeNetwork }

type pipe struct {
	rdDeadline Deadline
	wrDeadline Deadline

	// Used by local Read to interact with remote Write.
	// For a sync/semi-sync pipe, receive on rdRx is always followed by send on rdTx
	rdRx <-chan []byte
	rdTx chan<- int

	// Used by local Write to interact with remote Read.
	// For a sync/semi-sync pipe, send on wrTx is always followed by receive on wrRx
	wrTx chan<- []byte
	wrRx <-chan int

	localDone  chan struct{}
	remoteDone <-chan struct{}

	rdBuf []byte // Unread bytes retained from a remote Write

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
	cb1 := make(chan []byte, queueSize)
	cb2 := make(chan []byte, queueSize)
	cn1 := make(chan int)
	cn2 := make(chan int)
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	p1 := &pipe{
		rdRx: cb1, rdTx: cn1,
		wrTx: cb2, wrRx: cn2,
		localDone: done1, remoteDone: done2,
		rdDeadline: SimpleDeadline(),
		wrDeadline: SimpleDeadline(),
		retain:     semiSync || queueSize > 0,
	}
	p2 := &pipe{
		rdRx: cb2, rdTx: cn2,
		wrTx: cb1, wrRx: cn1,
		localDone: done2, remoteDone: done1,
		rdDeadline: SimpleDeadline(),
		wrDeadline: SimpleDeadline(),
		retain:     semiSync || queueSize > 0,
	}
	return p1, p2
}

func (*pipe) LocalAddr() net.Addr  { return pipeAddr{} }
func (*pipe) RemoteAddr() net.Addr { return pipeAddr{} }

func (p *pipe) Read(b []byte) (int, error) {
	n, err := p.read(b)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "read", Net: pipeNetwork, Err: err}
	}
	return n, err
}

func (p *pipe) read(b []byte) (n int, err error) {
	// Fail fast without waiting for rdMu. Remote closure isn't checked here, because
	// io.EOF depends on rdBuf guarded by rdMu. A concurrent Read holding rdMu observes
	// remoteDone as well, so it releases rdMu promptly in this case.
	switch {
	case isClosedChan(p.localDone):
		return 0, io.ErrClosedPipe
	case len(b) == 0: // Return immediately like TCP does, without consuming any remote Write
		return 0, nil
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	}

	p.rdMu.Lock()
	defer p.rdMu.Unlock()
	switch {
	case isClosedChan(p.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone) && p.rdBuf == nil && len(p.rdRx) == 0:
		return 0, io.EOF
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	case p.rdBuf != nil: // Only a semi-sync/async pipe retains unread bytes
		nr := copy(b, p.rdBuf)
		if nr < len(p.rdBuf) {
			p.rdBuf = p.rdBuf[nr:]
		} else {
			p.rdBuf = nil
		}
		return nr, nil
	}

	select {
	case bw := <-p.rdRx:
		nr := copy(b, bw)
		if cap(p.rdRx) == 0 { // An unbuffered pipe can be sync or semi-sync
			if p.retain { // A semi-sync pipe retains unread bytes and reports all of them written
				if nr < len(bw) { // Copy before reporting, as remote Write returns and b may be reused
					p.rdBuf = bytes.Clone(bw[nr:])
				}
				p.rdTx <- len(bw)
			} else { // A sync pipe reports only read bytes, so Write resends the rest
				p.rdTx <- nr
			}
		} else { // A buffered pipe is async by definition and retains unread bytes
			if nr < len(bw) {
				p.rdBuf = bw[nr:]
			}
		}
		return nr, nil
	case <-p.localDone:
		return 0, io.ErrClosedPipe
	case <-p.remoteDone:
		// Only an async pipe may have queued bytes. No need to check p.rdBuf here:
		// unless it was empty, the function would return above
		if len(p.rdRx) > 0 {
			bw := <-p.rdRx
			nr := copy(b, bw)
			if nr < len(bw) {
				p.rdBuf = bw[nr:]
			}
			return nr, nil
		}
		return 0, io.EOF
	case <-p.rdDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	}
}

func (p *pipe) Write(b []byte) (int, error) {
	n, err := p.write(b)
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "write", Net: pipeNetwork, Err: err}
	}
	return n, err
}

func (p *pipe) write(b []byte) (n int, err error) {
	switch {
	case isClosedChan(p.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.wrDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	case len(b) == 0: // Send nothing like TCP does, so that no remote Read returns 0, nil
		return 0, nil
	}

	p.wrMu.Lock() // Ensure entirety of b is written together
	defer p.wrMu.Unlock()
	if cap(p.wrTx) > 0 { // An async pipe requires copy on write, as Write returns before b is read
		b = bytes.Clone(b) // TODO(vnxme): use a pool?
	}
	for len(b) > 0 {
		select {
		case p.wrTx <- b:
			if cap(p.wrTx) == 0 { // An unbuffered pipe can be sync or semi-sync
				nw := <-p.wrRx
				b = b[nw:]
				n += nw
			} else { // A buffered pipe is async by definition
				return len(b), nil
			}
		case <-p.localDone:
			return n, io.ErrClosedPipe
		case <-p.remoteDone:
			return n, io.ErrClosedPipe
		case <-p.wrDeadline.Wait():
			return n, os.ErrDeadlineExceeded
		}
	}
	return n, nil
}

// SetDeadline sets the read and write deadlines. Unlike [net.Pipe], it fails only once
// the local end is closed, since bytes retained or queued may be read after remote closure.
func (p *pipe) SetDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return io.ErrClosedPipe
	}
	p.rdDeadline.Set(t)
	p.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline. Unlike [net.Pipe], it fails only once
// the local end is closed, since bytes retained or queued may be read after remote closure.
func (p *pipe) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return io.ErrClosedPipe
	}
	p.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline. It fails once either end is closed,
// since nothing may be written after that.
func (p *pipe) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.wrDeadline.Set(t)
	return nil
}

func (p *pipe) Close() error {
	p.once.Do(func() { close(p.localDone) })
	return nil
}

const pipeNetwork = "pipe"

// Interface guards
var (
	_ net.Addr   = (*pipeAddr)(nil)
	_ StreamConn = (*pipe)(nil)
)
