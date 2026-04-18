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

package hybrid

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type trunkAddr struct{}

func (trunkAddr) Network() string { return trunkNetwork }
func (trunkAddr) String() string  { return trunkNetwork }

type trunkPacket struct {
	r net.Addr
	l net.Addr
	b []byte
	i int
}

type trunk struct {
	rdDeadline Deadline
	wrDeadline Deadline

	// Used by local Read to interact with remote Write.
	// For a sync/semi-sync trunk, receive on rdRx is always followed by send on rdTx
	rdRx <-chan trunkPacket
	rdTx chan<- int

	// Used by local Write to interact with remote Read.
	// For a sync/semi-sync trunk, send on wrTx is always followed by receive on wrRx
	wrTx chan<- trunkPacket
	wrRx <-chan int

	localDone  chan struct{}
	remoteDone <-chan struct{}

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
	wrMu sync.Mutex // Serialize Write operations
}

// BufferedTrunk returns a buffered asynchronous Trunk.
func BufferedTrunk(queueSize int) (SelectivePacketConn, SelectivePacketConn) {
	return Trunk(queueSize)
}

// UnbufferedTrunk returns an unbuffered synchronous Trunk.
func UnbufferedTrunk() (SelectivePacketConn, SelectivePacketConn) {
	return Trunk(0)
}

// Trunk creates an in-memory, full duplex network connection;
// both ends implement the SelectivePacketConn interface. It can be:
//
// (a) Unbuffered and synchronous.
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal
// buffering, and partial reading isn't supported.
//
// (b) Buffered and asynchronous.
// Reads on one end aren't matched with writes on the other;
// data are copied internally to prevent slice aliasing issues;
// partial reading isn't supported, i.e. unread bytes are dropped.
//
// Note: when the remote end is closed, (a) returns io.EOF immediately
// after that, but (b) returns io.EOF only once there are no more
// packets left in the read channel. When a read buffer is shorter than
// a packet buffer, both (a) and (b) return ErrPacketTruncated.
func Trunk(queueSize int) (SelectivePacketConn, SelectivePacketConn) {
	cb1 := make(chan trunkPacket, queueSize)
	cb2 := make(chan trunkPacket, queueSize)
	cn1 := make(chan int)
	cn2 := make(chan int)
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	p1 := &trunk{
		rdRx: cb1, rdTx: cn1,
		wrTx: cb2, wrRx: cn2,
		localDone: done1, remoteDone: done2,
		rdDeadline: HookDeadline(nil),
		wrDeadline: HookDeadline(nil),
	}
	p2 := &trunk{
		rdRx: cb2, rdTx: cn2,
		wrTx: cb1, wrRx: cn1,
		localDone: done2, remoteDone: done1,
		rdDeadline: HookDeadline(nil),
		wrDeadline: HookDeadline(nil),
	}
	return p1, p2
}

func (*trunk) LocalAddr() net.Addr { return trunkAddr{} }

func (p *trunk) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, _, _, err := p.ReadFromTo(b)
	return n, addr, err
}

func (p *trunk) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, r, l, i, err := p.read(b)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "read", Net: trunkNetwork, Source: l, Addr: r, Err: err}
	}
	if r == nil {
		r = trunkAddr{}
	}
	if l == nil {
		l = trunkAddr{}
	}
	return n, r, l, i, err
}

func (p *trunk) read(b []byte) (int, net.Addr, net.Addr, int, error) {
	switch {
	case isClosedChan(p.localDone):
		return 0, nil, nil, 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone) && len(p.rdRx) == 0:
		return 0, nil, nil, 0, io.EOF
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, nil, nil, 0, os.ErrDeadlineExceeded
	}

	p.rdMu.Lock()
	defer p.rdMu.Unlock()
	switch {
	case isClosedChan(p.localDone):
		return 0, nil, nil, 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone) && len(p.rdRx) == 0:
		return 0, nil, nil, 0, io.EOF
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, nil, nil, 0, os.ErrDeadlineExceeded
	}

	select {
	case pkt := <-p.rdRx:
		nr := copy(b, pkt.b)
		if cap(p.rdRx) == 0 { // An unbuffered trunk is sync, while a buffered trunk is async
			p.rdTx <- nr
		}
		var err error
		if nr < len(pkt.b) { // Unread bytes are dropped, i.e. a trunk doesn't support partial read
			err = ErrPacketTruncated
		}
		return nr, pkt.l, pkt.r, pkt.i, err
	case <-p.localDone:
		return 0, nil, nil, 0, io.ErrClosedPipe
	case <-p.remoteDone:
		return 0, nil, nil, 0, io.EOF
	case <-p.rdDeadline.Wait():
		return 0, nil, nil, 0, os.ErrDeadlineExceeded
	}
}

func (p *trunk) WriteTo(b []byte, addr net.Addr) (int, error) {
	return p.WriteToFrom(b, addr, nil, 0)
}

func (p *trunk) WriteToFrom(b []byte, r, l net.Addr, i int) (int, error) {
	n, err := p.write(b, r, l, i)
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "write", Net: trunkNetwork, Source: l, Addr: r, Err: err}
	}
	return n, err
}

func (p *trunk) write(b []byte, r, l net.Addr, i int) (int, error) {
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
	pkt := trunkPacket{b: b, r: r, l: l, i: i}
	if cap(p.wrTx) > 0 { // An async buffered trunk requires copy on write
		pkt.b = append([]byte{}, b...)
	}
	select {
	case p.wrTx <- pkt:
		if cap(p.wrTx) == 0 { // An unbuffered trunk is sync, while a buffered trunk is async
			return <-p.wrRx, nil
		}
		return len(b), nil
	case <-p.localDone:
		return 0, io.ErrClosedPipe
	case <-p.remoteDone:
		return 0, io.ErrClosedPipe
	case <-p.wrDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	}
}

func (p *trunk) SetDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.rdDeadline.Set(t)
	p.wrDeadline.Set(t)
	return nil
}

func (p *trunk) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.rdDeadline.Set(t)
	return nil
}

func (p *trunk) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.wrDeadline.Set(t)
	return nil
}

func (p *trunk) Close() error {
	p.once.Do(func() { close(p.localDone) })
	return nil
}

const trunkNetwork = "trunk"

var ErrPacketTruncated = errors.New("packet truncated")

// Interface guards
var (
	_ net.Addr            = (*trunkAddr)(nil)
	_ SelectivePacketConn = (*trunk)(nil)
)
