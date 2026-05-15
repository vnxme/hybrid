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
// remote address, local address, and the interface index. It is used to transfer
// data between the two ends of a trunk connection via channels.
type trunkPacket struct {
	r net.Addr
	l net.Addr
	b []byte
	i int
}

// trunk represents a packet-oriented communication channel between local and remote endpoints.
type trunk struct {
	rdDeadline Deadline
	wrDeadline Deadline

	rdRx <-chan trunkPacket // Used by local read to interact with remote write
	wrTx chan<- trunkPacket // Used by local write to interact with remote read

	locDone chan struct{}
	remDone <-chan struct{}

	once sync.Once  // Protects closing locDone
	rdMu sync.Mutex // Serialize read operations
	wrMu sync.Mutex // Serialize write operations
}

// BufferedTrunk creates an in-memory, full duplex network connection with buffered
// and asynchronous behavior, using the specified internal queue size for each end.
// See Trunk for more details.
func BufferedTrunk(queueSize int) (SelectivePacketConn, SelectivePacketConn) {
	return Trunk(queueSize)
}

// UnbufferedTrunk creates an unbuffered, synchronous in-memory full-duplex connection pair.
// See Trunk for more details.
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
//
// Note: the queue size must be within the range of 0 to 1024.
// Any negative value is replaced with 0, and any value exceeding
// the upper limit is adjusted to that limit.
func Trunk(queueSize int) (SelectivePacketConn, SelectivePacketConn) {
	queueSize = min(1<<10, max(0, queueSize))

	chan1 := make(chan trunkPacket, queueSize)
	chan2 := make(chan trunkPacket, queueSize)
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	p1 := &trunk{
		rdRx:    chan1,
		wrTx:    chan2,
		locDone: done1, remDone: done2,
		rdDeadline: HookDeadline(nil),
		wrDeadline: HookDeadline(nil),
	}
	p2 := &trunk{
		rdRx:    chan2,
		wrTx:    chan1,
		locDone: done2, remDone: done1,
		rdDeadline: HookDeadline(nil),
		wrDeadline: HookDeadline(nil),
	}
	return p1, p2
}

// LocalAddr returns the local address of the trunk. The address is constant for all trunks.
func (*trunk) LocalAddr() net.Addr { return oneTrunkAddr }

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
	if r == nil {
		r = oneTrunkAddr
	}
	if l == nil {
		l = oneTrunkAddr
	}
	return n, r, l, i, err
}

// read reads a packet from the trunk and returns the number of bytes copied,
// the local and remote addresses, the interface index, and any error.
//
// It serializes reads and blocks until a packet is received or the trunk is closed or a deadline expires.
// If the provided buffer is too small, it returns io.ErrShortBuffer and drops the excess bytes.
func (p *trunk) read(b []byte) (int, net.Addr, net.Addr, int, error) {
	switch {
	case isClosedChan(p.locDone):
		return 0, nil, nil, 0, errTrunkClosed
	case isClosedChan(p.remDone) && len(p.rdRx) == 0:
		return 0, nil, nil, 0, errTrunkEOF // TODO(vnxme): racy len check?
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, nil, nil, 0, errTrunkDeadlineExceeded
	}

	p.rdMu.Lock()
	defer p.rdMu.Unlock()
	switch {
	case isClosedChan(p.locDone):
		return 0, nil, nil, 0, errTrunkClosed
	case isClosedChan(p.remDone) && len(p.rdRx) == 0:
		return 0, nil, nil, 0, errTrunkEOF
	case isClosedChan(p.rdDeadline.Wait()):
		return 0, nil, nil, 0, errTrunkDeadlineExceeded
	}

	select {
	case pkt := <-p.rdRx:
		nr := copy(b, pkt.b)
		var err error
		if nr < len(pkt.b) { // Unread bytes are dropped, i.e. a trunk doesn't support partial read
			err = errTrunkShortBuffer
		}
		return nr, pkt.l, pkt.r, pkt.i, err
	case <-p.locDone:
		return 0, nil, nil, 0, errTrunkClosed
	case <-p.remDone:
		return 0, nil, nil, 0, errTrunkEOF
	case <-p.rdDeadline.Wait():
		return 0, nil, nil, 0, errTrunkDeadlineExceeded
	}
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
// It serializes writes and blocks until a packet is sent or the trunk is closed or a deadline expires.
// For buffered trunks, it copies b to avoid slice aliasing issues.
func (p *trunk) write(b []byte, r, l net.Addr, i int) (int, error) {
	switch {
	case isClosedChan(p.locDone):
		return 0, errTrunkClosed
	case isClosedChan(p.remDone):
		return 0, errTrunkClosed
	case isClosedChan(p.wrDeadline.Wait()):
		return 0, errTrunkDeadlineExceeded
	}

	p.wrMu.Lock() // Ensure entirety of b is written together
	defer p.wrMu.Unlock()
	pkt := trunkPacket{b: b, r: r, l: l, i: i}
	if cap(p.wrTx) > 0 { // An async buffered trunk requires copy on write
		pkt.b = slices.Clone(b) // TODO(vnxme): use a pool?
	}
	select {
	case p.wrTx <- pkt:
		return len(b), nil
	case <-p.locDone:
		return 0, errTrunkClosed
	case <-p.remDone:
		return 0, errTrunkClosed
	case <-p.wrDeadline.Wait():
		return 0, errTrunkDeadlineExceeded
	}
}

// SetDeadline sets the read and write deadlines to t.
// Returns io.ErrClosedPipe if the trunk is closed.
func (p *trunk) SetDeadline(t time.Time) error {
	if isClosedChan(p.locDone) || isClosedChan(p.remDone) {
		return errTrunkClosed
	}
	p.rdDeadline.Set(t)
	p.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline to t.
// Returns io.ErrClosedPipe if the trunk is closed.
func (p *trunk) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.locDone) || isClosedChan(p.remDone) {
		return errTrunkClosed
	}
	p.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline to t.
// Returns io.ErrClosedPipe if the trunk is closed.
func (p *trunk) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.locDone) || isClosedChan(p.remDone) {
		return errTrunkClosed
	}
	p.wrDeadline.Set(t)
	return nil
}

// Close closes the trunk, signaling completion and preventing further I/O.
// Returns no error if the trunk is closed.
func (p *trunk) Close() error {
	p.once.Do(func() { close(p.locDone) })
	return nil
}

const trunkNetwork = "trunk"

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
