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
	ref *[]byte // Reference to the pooled buffer of buf, if any
	buf []byte
	idx int
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
// once a write returns. When the remote end is closed, reads return net.ErrClosed
// once all the packets queued have been read, and writes return net.ErrClosed.
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
// remote end is closed, reads and writes return net.ErrClosed. Local addresses and packet truncation
// are handled the same way as by [BufferedTrunk].
func UnbufferedTrunk(addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn) {
	return newTrunk(0, addr1, addr2)
}

// newTrunk creates a trunk with the given queue size, see [BufferedTrunk] and [UnbufferedTrunk].
func newTrunk(queueSize int, addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn) {
	// Copy addresses, as the caller may modify them once the trunk is created
	addr1, addr2 = cloneAddr(addr1), cloneAddr(addr2)
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

	tr1 := &trunk{
		addr: addr1,
		rdRx: chan1, rdTx: ack1,
		wrTx: chan2, wrRx: ack2,
		localDone: done1, remoteDone: done2,
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
	tr2 := &trunk{
		addr: addr2,
		rdRx: chan2, rdTx: ack2,
		wrTx: chan1, wrRx: ack1,
		localDone: done2, remoteDone: done1,
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
	return tr1, tr2
}

// LocalAddr returns the local address of the trunk end.
func (tr *trunk) LocalAddr() net.Addr { return tr.addr }

// ReadFrom reads a packet from the trunk, returning the number of bytes, the source address, and an error.
func (tr *trunk) ReadFrom(b []byte) (int, net.Addr, error) {
	n, src, _, _, err := tr.ReadFromTo(b)
	return n, src, err
}

// ReadFromTo reads a packet from the trunk and returns the number of bytes read, the source
// address, the destination address, the interface index, and an error.
func (tr *trunk) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, src, dst, idx, err := tr.read(b)
	if err != nil {
		err = tr.opError("read", dst, src, err)
	}
	return n, src, dst, idx, err
}

// read reads a packet from the trunk and returns the number of bytes copied,
// the source and destination addresses, the interface index, and any error.
//
// It blocks until a packet is received or the trunk is closed or a deadline expires. Reads needn't
// be serialized, since each of them receives a whole packet, the same way as UDP reads do.
// If the provided buffer is too small, it returns io.ErrShortBuffer and drops the excess bytes.
func (tr *trunk) read(b []byte) (int, net.Addr, net.Addr, int, error) {
	switch {
	case isClosedChan(tr.localDone):
		return 0, nil, nil, 0, errTrunkClosed
	case isClosedChan(tr.rdDeadline.Wait()): // An exceeded deadline prevails over pending packets
		return 0, nil, nil, 0, errTrunkDeadlineExceeded
	case isClosedChan(tr.remoteDone) && len(tr.rdRx) == 0: // Queued packets may be read after remote closure
		return 0, nil, nil, 0, errTrunkClosed
	}

	var pkt trunkPacket
	select { // Receive a pending packet without a blocking select, which is much slower
	case pkt = <-tr.rdRx:
	default:
		select {
		case pkt = <-tr.rdRx:
		case <-tr.localDone:
			return 0, nil, nil, 0, errTrunkClosed
		case <-tr.remoteDone:
			select { // A packet may have been queued just before remote closure
			case pkt = <-tr.rdRx:
			default:
				return 0, nil, nil, 0, errTrunkClosed
			}
		case <-tr.rdDeadline.Wait():
			return 0, nil, nil, 0, errTrunkDeadlineExceeded
		}
	}

	if pkt.dst == nil { // A packet written without a destination address is destined to this end
		pkt.dst = cloneAddr(tr.addr)
	}
	nr := copy(b, pkt.buf)
	// Nothing refers to a pooled buffer once its packet is copied, as unread bytes are dropped
	putBuf(pkt.ref)
	if cap(tr.rdRx) == 0 { // An unbuffered trunk lets remote write return only once the packet is copied
		tr.rdTx <- struct{}{}
	}
	if nr < len(pkt.buf) { // Unread bytes are dropped, i.e. a trunk doesn't support partial read
		return nr, pkt.src, pkt.dst, pkt.idx, errTrunkShortBuffer
	}
	return nr, pkt.src, pkt.dst, pkt.idx, nil
}

// WriteTo writes a packet of len(b) bytes to the destination address dst.
// It returns the number of bytes written and any error.
func (tr *trunk) WriteTo(b []byte, dst net.Addr) (int, error) {
	return tr.WriteToFrom(b, dst, nil, 0)
}

// WriteToFrom writes a packet of len(b) bytes to the destination address dst
// from the source address src on the interface with index idx.
// It returns the number of bytes written and any error.
func (tr *trunk) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
	n, err := tr.write(b, dst, src, idx)
	if err != nil {
		err = tr.opError("write", src, dst, err)
	}
	return n, err
}

// write sends a packet with the given byte slice b to the destination address dst
// from the source address src on the interface with index idx.
//
// It blocks until a packet is sent or the trunk is closed or a deadline expires. Writes are serialized,
// so that a single packet is in flight in an unbuffered trunk, and its acknowledgement can't be mixed up.
// For buffered trunks, it copies b to avoid slice aliasing issues.
func (tr *trunk) write(b []byte, dst, src net.Addr, idx int) (int, error) {
	switch {
	case isClosedChan(tr.localDone):
		return 0, errTrunkClosed
	case isClosedChan(tr.remoteDone):
		return 0, errTrunkClosed
	case isClosedChan(tr.wrDeadline.Wait()):
		return 0, errTrunkDeadlineExceeded
	}

	tr.wrMu.Lock()
	defer tr.wrMu.Unlock()
	switch {
	case isClosedChan(tr.localDone):
		return 0, errTrunkClosed
	case isClosedChan(tr.remoteDone):
		return 0, errTrunkClosed
	case isClosedChan(tr.wrDeadline.Wait()):
		return 0, errTrunkDeadlineExceeded
	}

	if src == nil { // A packet written without a source address originates from this end
		src = tr.addr
	}
	// Copy addresses, as the caller may modify them once write returns, and so may remote read
	pkt := trunkPacket{buf: b, src: cloneAddr(src), dst: cloneAddr(dst), idx: idx}
	if cap(tr.wrTx) > 0 { // An async buffered trunk requires copy on write
		pkt.buf, pkt.ref = getBuf(len(b))
		copy(pkt.buf, b)
	}

	select { // Send without a blocking select if possible, which is much slower
	case tr.wrTx <- pkt:
	default:
		select {
		case tr.wrTx <- pkt:
		case <-tr.localDone:
			return 0, errTrunkClosed
		case <-tr.remoteDone:
			return 0, errTrunkClosed
		case <-tr.wrDeadline.Wait():
			return 0, errTrunkDeadlineExceeded
		}
	}

	if cap(tr.wrTx) == 0 { // An unbuffered trunk doesn't copy on write, so wait until remote read copies the packet
		<-tr.wrRx
	}
	return len(b), nil
}

// SetDeadline sets the read and write deadlines to t. It returns net.ErrClosed
// only once the local end is closed, since packets queued may be read after remote closure.
func (tr *trunk) SetDeadline(t time.Time) error {
	if isClosedChan(tr.localDone) {
		return tr.opError("set", nil, nil, errTrunkClosed)
	}
	tr.rdDeadline.Set(t)
	tr.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline to t. It returns net.ErrClosed
// only once the local end is closed, since packets queued may be read after remote closure.
func (tr *trunk) SetReadDeadline(t time.Time) error {
	if isClosedChan(tr.localDone) {
		return tr.opError("set", nil, nil, errTrunkClosed)
	}
	tr.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline to t. Like UDP, it returns net.ErrClosed
// only once the local end is closed, while a write fails with it after remote closure too.
func (tr *trunk) SetWriteDeadline(t time.Time) error {
	if isClosedChan(tr.localDone) {
		return tr.opError("set", nil, nil, errTrunkClosed)
	}
	tr.wrDeadline.Set(t)
	return nil
}

// Close closes the trunk, signaling completion and preventing further I/O.
// It returns net.ErrClosed if the trunk end is already closed.
func (tr *trunk) Close() error {
	closed := false
	tr.once.Do(func() {
		close(tr.localDone)
		closed = true
	})
	if !closed {
		return tr.opError("close", nil, nil, errTrunkClosed)
	}
	return nil
}

// opError wraps err in net.OpError the same way as a UDP connection does: Source is the local
// address, or the address of this end if nil, and Addr is the remote address, if any. Setting
// a deadline reports the address of this end as Addr only.
func (tr *trunk) opError(op string, local, remote net.Addr, err error) error {
	if op == "set" {
		return &net.OpError{Op: op, Net: trunkNetwork, Addr: tr.addr, Err: err}
	}
	if local == nil {
		local = tr.addr
	}
	return &net.OpError{Op: op, Net: trunkNetwork, Source: local, Addr: remote, Err: err}
}

const trunkNetwork = "trunk"

// Shared variables
var (
	errTrunkClosed           = net.ErrClosed // Reported by using a trunk after closure of either end, the same way as by UDP after local closure
	errTrunkDeadlineExceeded = os.ErrDeadlineExceeded
	errTrunkShortBuffer      = io.ErrShortBuffer // Reported by reading a packet longer than the read buffer

	oneTrunkAddr = trunkAddr{}
)

// Interface guards
var (
	_ net.Addr            = trunkAddr{}
	_ SelectivePacketConn = (*trunk)(nil)
)
