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

	rdBuf []byte // Bytes saved for partial reading

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
	wrMu sync.Mutex // Serialize Write operations

	rdPart bool // Partial reading switch
}

// Pipe creates an in-memory, full duplex network connection;
// both ends implement the [net.Conn] interface. It can be:
//
// (a) Unbuffered and synchronous.
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal
// buffering.
//
// (b) Unbuffered and semi-synchronous.
// When the target read buffer is shorter than the source write buffer,
// the latter is saved internally so that its remaining bytes could be
// asynchronously read later, i.e. partial reading is supported.
//
// (c) Buffered and asynchronous.
// Reads on one end aren't matched with writes on the other;
// data are copied internally to prevent slice aliasing issues;
// partial reading is also supported.
//
// Note: when the remote end is closed, (a) returns io.EOF immediately
// after that, but (b) and (c) return io.EOF only once there are no more
// packets left in the read channel and no more bytes partially unread.
func Pipe(queueSize int, partialRead bool) (net.Conn, net.Conn) {
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
		rdDeadline: HookDeadline(nil),
		wrDeadline: HookDeadline(nil),
		rdPart:     partialRead,
	}
	p2 := &pipe{
		rdRx: cb2, rdTx: cn2,
		wrTx: cb1, wrRx: cn1,
		localDone: done2, remoteDone: done1,
		rdDeadline: HookDeadline(nil),
		wrDeadline: HookDeadline(nil),
		rdPart:     partialRead,
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
	switch {
	case isClosedChan(p.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone) && p.rdBuf == nil && len(p.rdRx) == 0:
		return 0, io.EOF
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
	case p.rdPart && p.rdBuf != nil:
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
			if p.rdPart { // A semi-sync unbuffered pipe supports partial read
				sz := len(bw)
				p.rdTx <- sz
				if nr < sz {
					p.rdBuf = bw[nr:]
				}
			} else { // A sync unbuffered pipe drops unread bytes
				p.rdTx <- nr
			}
		} else { // A buffered pipe is async by definition and supports partial read
			if nr < len(bw) {
				p.rdBuf = bw[nr:]
			}
		}
		return nr, nil
	case <-p.localDone:
		return 0, io.ErrClosedPipe
	case <-p.remoteDone:
		if p.rdPart {
			// No need to check p.rdBuf here: unless it was empty, the function would return above
			if len(p.rdRx) > 0 {
				bw := <-p.rdRx
				nr := copy(b, bw)
				if nr < len(bw) {
					p.rdBuf = bw[nr:]
				}
				return nr, nil
			}
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
	}

	p.wrMu.Lock() // Ensure entirety of b is written together
	defer p.wrMu.Unlock()
	if p.rdPart { // A semi-sync unbuffered/async buffered pipe requires copy on write
		b = append([]byte{}, b...)
	}
	for once := true; once || len(b) > 0; once = false {
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

func (p *pipe) SetDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.rdDeadline.Set(t)
	p.wrDeadline.Set(t)
	return nil
}

func (p *pipe) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.rdDeadline.Set(t)
	return nil
}

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
	_ net.Addr = (*pipeAddr)(nil)
	_ net.Conn = (*pipe)(nil)
)
