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
// - a trunk deals with packets instead of byte slices
// - a trunk implements SelectivePacketConn interface

package hybrid

import (
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"time"
)

// trunkAddr implements the net.Addr interface for the trunk network type.
type trunkAddr struct{}

// Network returns the network name for the trunk address.
func (trunkAddr) Network() string { return trunkNetwork }

// String returns the network name for the trunk address.
func (trunkAddr) String() string { return trunkNetwork }

// trunkPacket is an internal type that represents a single packet with its payload,
// source address, destination address, and the interface index. It is used to transfer
// data between the two ends of a trunk connection via channels.
type trunkPacket struct {
	src net.Addr
	dst net.Addr
	b   []byte
	i   int
}

// cloneAddr returns a copy of a standard address, so that they don't share memory.
// Any other address, including a nil pointer of a standard type, is returned as is.
func cloneAddr(addr net.Addr) net.Addr {
	switch a := addr.(type) {
	case *net.UDPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.TCPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.IPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.UnixAddr:
		if a != nil {
			c := *a
			return &c
		}
	}
	return addr
}

// trunk represents a packet-oriented communication channel between local and remote endpoints.
type trunk struct {
	addr net.Addr // Local address

	// Used by local read to interact with remote write. For an unbuffered trunk,
	// receive on rdRx is always followed by send on rdTx, once the packet is copied
	rdRx <-chan trunkPacket
	rdTx chan<- struct{}

	// Used by local write to interact with remote read. For an unbuffered trunk,
	// send on wrTx is always followed by receive on wrRx
	wrTx chan<- trunkPacket
	wrRx <-chan struct{}

	localDone  chan struct{}
	remoteDone <-chan struct{}

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once  // Protects closing localDone
	wrMu sync.Mutex // Serialize write operations
}

// BufferedTrunk creates an in-memory, full duplex packet-oriented network connection
// that is buffered and asynchronous; both ends implement the SelectivePacketConn interface.
// Reads on one end aren't matched with writes on the other: a write returns once its
// packet has been queued, unless the queue already holds queueSize packets, in which
// case it blocks. Packets are copied internally, so the write buffer may be reused
// once a write returns. When the remote end is closed, reads return io.EOF once
// all the packets queued have been read.
//
// The ends have local addresses addr1 and addr2 respectively, or a generic
// trunk address if nil. A local address is returned by LocalAddr, used as
// the source address of a packet written without one, e.g. by WriteTo, and
// as the destination address of a packet read that was written without one.
// A packet is delivered to the other end whatever its destination address is,
// so that multiple packet flows may share a trunk, told apart by addresses.
//
// A packet longer than the read buffer is truncated: a read returns the bytes
// copied along with io.ErrShortBuffer, and the rest of the packet is dropped.
//
// BufferedTrunk panics if queueSize isn't positive.
func BufferedTrunk(queueSize int, addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn) {
	if queueSize <= 0 {
		panic("hybrid: BufferedTrunk: non-positive queueSize")
	}
	return newTrunk(queueSize, addr1, addr2)
}

// UnbufferedTrunk creates an in-memory, full duplex packet-oriented network connection
// that is unbuffered and synchronous; both ends implement the SelectivePacketConn interface.
// Reads on one end are matched with writes on the other, copying packets directly
// between the two; there is no internal buffering. A write returns once its packet
// has been copied by a read, so the write buffer may be reused after that. When the
// remote end is closed, reads return io.EOF. Local addresses and packet truncation
// are handled the same way as by [BufferedTrunk].
func UnbufferedTrunk(addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn) {
	return newTrunk(0, addr1, addr2)
}

// newTrunk creates a trunk with the given queue size, see [BufferedTrunk] and [UnbufferedTrunk].
func newTrunk(queueSize int, addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn) {
	if addr1 == nil {
		addr1 = oneTrunkAddr
	}
	if addr2 == nil {
		addr2 = oneTrunkAddr
	}

	chan1 := make(chan trunkPacket, queueSize)
	chan2 := make(chan trunkPacket, queueSize)
	ack1 := make(chan struct{})
	ack2 := make(chan struct{})
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	p1 := &trunk{
		addr: addr1,
		rdRx: chan1, rdTx: ack1,
		wrTx: chan2, wrRx: ack2,
		localDone: done1, remoteDone: done2,
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
	p2 := &trunk{
		addr: addr2,
		rdRx: chan2, rdTx: ack2,
		wrTx: chan1, wrRx: ack1,
		localDone: done2, remoteDone: done1,
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
	return p1, p2
}

// LocalAddr returns the local address of the trunk end.
func (p *trunk) LocalAddr() net.Addr { return p.addr }

// ReadFrom reads a packet from the trunk, returning the number of bytes, the remote address, and an error.
func (p *trunk) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, _, _, err := p.ReadFromTo(b)
	return n, addr, err
}

// ReadFromTo reads a packet from the trunk and returns the number of bytes read, the remote
// address, the local address, the interface index, and an error.
func (p *trunk) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, r, l, i, err := p.read(b)
	if err != nil && !errors.Is(err, errTrunkEOF) && !errors.Is(err, errTrunkClosed) {
		err = &net.OpError{Op: "read", Net: trunkNetwork, Source: l, Addr: r, Err: err}
	}
	return n, r, l, i, err
}

// read reads a packet from the trunk and returns the number of bytes copied,
// the source and destination addresses, the interface index, and any error.
//
// It blocks until a packet is received or the trunk is closed or a deadline expires. Reads needn't
// be serialized, since each of them receives a whole packet, the same way as UDP reads do.
// If the provided buffer is too small, it returns io.ErrShortBuffer and drops the excess bytes.
func (p *trunk) read(b []byte) (int, net.Addr, net.Addr, int, error) {
	var pkt trunkPacket

	switch {
	case isClosedChan(p.localDone):
		return 0, nil, nil, 0, errTrunkClosed
	case isClosedChan(p.rdDeadline.Wait()): // An exceeded deadline prevails over pending packets
		return 0, nil, nil, 0, errTrunkDeadlineExceeded
	case isClosedChan(p.remoteDone):
		select {
		case pkt = <-p.rdRx:
			goto success
		default:
			return 0, nil, nil, 0, errTrunkEOF
		}
	}

	select {
	case pkt = <-p.rdRx:
		goto success
	default:
		select {
		case pkt = <-p.rdRx:
			goto success
		case <-p.localDone:
			return 0, nil, nil, 0, errTrunkClosed
		case <-p.remoteDone:
			return 0, nil, nil, 0, errTrunkEOF
		case <-p.rdDeadline.Wait():
			return 0, nil, nil, 0, errTrunkDeadlineExceeded
		}
	}

success:
	if pkt.dst == nil { // A packet written without a destination address is destined to this end
		pkt.dst = cloneAddr(p.addr)
	}
	nr := copy(b, pkt.b)
	if cap(p.rdRx) == 0 { // An unbuffered trunk lets remote write return only once the packet is copied
		p.rdTx <- struct{}{}
	}
	if nr < len(pkt.b) { // Unread bytes are dropped, i.e. a trunk doesn't support partial read
		return nr, pkt.src, pkt.dst, pkt.i, errTrunkShortBuffer
	}
	return nr, pkt.src, pkt.dst, pkt.i, nil
}

// WriteTo writes a packet of len(b) bytes to the remote addr.
// It returns the number of bytes written and any error.
func (p *trunk) WriteTo(b []byte, addr net.Addr) (int, error) {
	return p.WriteToFrom(b, addr, nil, 0)
}

// WriteToFrom writes len(b) bytes to remote addr r from local addr l on interface i.
// It returns the number of bytes written and any error.
func (p *trunk) WriteToFrom(b []byte, r, l net.Addr, i int) (int, error) {
	n, err := p.write(b, r, l, i)
	if err != nil && !errors.Is(err, errTrunkClosed) {
		err = &net.OpError{Op: "write", Net: trunkNetwork, Source: l, Addr: r, Err: err}
	}
	return n, err
}

// write sends a packet with the given byte slice b from local address l to remote address r on interface i.
//
// It blocks until a packet is sent or the trunk is closed or a deadline expires. Writes are serialized,
// so that a single packet is in flight in an unbuffered trunk, and its acknowledgement can't be mixed up.
// For buffered trunks, it copies b to avoid slice aliasing issues.
func (p *trunk) write(b []byte, r, l net.Addr, i int) (int, error) {
	switch {
	case isClosedChan(p.localDone):
		return 0, errTrunkClosed
	case isClosedChan(p.remoteDone):
		return 0, errTrunkClosed
	case isClosedChan(p.wrDeadline.Wait()):
		return 0, errTrunkDeadlineExceeded
	}

	p.wrMu.Lock()
	defer p.wrMu.Unlock()
	switch {
	case isClosedChan(p.localDone):
		return 0, errTrunkClosed
	case isClosedChan(p.remoteDone):
		return 0, errTrunkClosed
	case isClosedChan(p.wrDeadline.Wait()):
		return 0, errTrunkDeadlineExceeded
	}

	if l == nil { // A packet written without a source address originates from this end
		l = p.addr
	}
	// Copy addresses, as the caller may modify them once write returns, and so may remote read
	pkt := trunkPacket{b: b, src: cloneAddr(l), dst: cloneAddr(r), i: i}
	if cap(p.wrTx) > 0 { // An async buffered trunk requires copy on write
		pkt.b = slices.Clone(b) // TODO(vnxme): use a pool?
	}

	select {
	case p.wrTx <- pkt:
		goto success
	default:
		select {
		case p.wrTx <- pkt:
			goto success
		case <-p.localDone:
			return 0, errTrunkClosed
		case <-p.remoteDone:
			return 0, errTrunkClosed
		case <-p.wrDeadline.Wait():
			return 0, errTrunkDeadlineExceeded
		}
	}

success:
	if cap(p.wrTx) == 0 { // An unbuffered trunk doesn't copy on write, so wait until remote read copies the packet
		<-p.wrRx
	}
	return len(b), nil
}

// SetDeadline sets the read and write deadlines to t. It returns io.ErrClosedPipe
// only once the local end is closed, since packets queued may be read after remote closure.
func (p *trunk) SetDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return errTrunkClosed
	}
	p.rdDeadline.Set(t)
	p.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline to t. It returns io.ErrClosedPipe
// only once the local end is closed, since packets queued may be read after remote closure.
func (p *trunk) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.localDone) {
		return errTrunkClosed
	}
	p.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline to t. It returns io.ErrClosedPipe
// once either end is closed, since nothing may be written after that.
func (p *trunk) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return errTrunkClosed
	}
	p.wrDeadline.Set(t)
	return nil
}

// Close closes the trunk, signaling completion and preventing further I/O.
// Returns io.ErrClosedPipe if the trunk is closed.
func (p *trunk) Close() (err error) {
	err = errTrunkClosed
	p.once.Do(func() {
		close(p.localDone)
		err = nil
	})
	return
}

const trunkNetwork = "trunk"

// Shared variables
var (
	errTrunkClosed           = io.ErrClosedPipe
	errTrunkDeadlineExceeded = os.ErrDeadlineExceeded
	errTrunkEOF              = io.EOF
	errTrunkShortBuffer      = io.ErrShortBuffer

	oneTrunkAddr = trunkAddr{}
)

// Interface guards
var (
	_ net.Addr            = (*trunkAddr)(nil)
	_ SelectivePacketConn = (*trunk)(nil)
)
