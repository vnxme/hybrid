// Copyright 2026 VNXME
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

package hybrid

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// inletConnBase is what reading and writing a connection of an inlet needs, embedded in inletConn
// and inletFallback.
type inletConnBase struct {
	parent    *inlet
	rdRx      chan *inletPacket // Packets waiting to be read
	localDone chan struct{}     // Closed once the connection is closed

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serializes reads
}

// closeLocal closes localDone, and reports whether it's done so, rather than found it closed.
func (ic *inletConnBase) closeLocal() (closed bool) {
	ic.once.Do(func() {
		close(ic.localDone)
		closed = true
	})
	return closed
}

type inletConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr

	key flowKey

	inletConnBase // With the packets of the flow in rdRx, unless it's been released

	last       atomic.Int64 // The clock of the inlet at the latest packet read or written
	localIfIdx atomic.Int64 // The latest one, used for replies

	mu sync.Mutex // Keeps the packets of the flow in order while it's being released

	released atomic.Bool // Set before closing localDone, if the flow goes on to the fallback connection
}

func (ic *inletConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, _, _, err := ic.ReadFromTo(b)
	return n, addr, err
}

func (ic *inletConn) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, src, dst, idx, err := ic.readFromTo(b)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "read", Net: ic.parent.pc.LocalAddr().Network(), Err: err, Source: ic.LocalAddr(), Addr: ic.remoteAddr}
	}
	return n, src, dst, idx, err
}

func (ic *inletConn) readFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, _, err := ic.receive(b)
	if err != nil {
		return 0, nil, nil, 0, err
	}
	return n, ic.remoteAddr, ic.localAddr, ic.LocalIfIdx(), nil
}

func (ic *inletConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	return ic.WriteToFrom(b, nil, nil, 0)
}

func (ic *inletConn) WriteToFrom(b []byte, _, _ net.Addr, _ int) (int, error) {
	n, err := ic.send(b, ic, ic.remoteAddr, ic.localAddr, ic.LocalIfIdx())
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "write", Net: ic.parent.pc.LocalAddr().Network(), Err: err, Source: ic.LocalAddr(), Addr: ic.remoteAddr}
	}
	return n, err
}

// Close closes the connection and ends its flow, so that the next packet of the flow starts a new one.
func (ic *inletConn) Close() error {
	if !ic.closeLocal() {
		return net.ErrClosed
	}
	ic.parent.deleteConnection(ic)
	return nil
}

// Release hands the flow of the connection over to the fallback connection of the inlet: the packets
// queued and all the later packets of the flow go there, or are dropped if the inlet has no fallback
// connection. The connection is closed, but its flow goes on until it's idle.
func (ic *inletConn) Release() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if isClosedChan(ic.localDone) {
		return net.ErrClosed
	}

	ic.released.Store(true)
	for {
		select {
		case pkt := <-ic.rdRx:
			ic.parent.toFallback(pkt)
		default:
			ic.closeLocal()
			return nil
		}
	}
}

// touch records the activity of the flow of the connection, at the time of the clock of the inlet.
func (ic *inletConn) touch() { ic.last.Store(ic.parent.clock.Load()) }

// expire ends the flow of a connection that has been idle: it closes the connection, unless it's been
// closed already, and reports the flow for deletion, even if it's been released.
func (ic *inletConn) expire() {
	ic.closeLocal()
	ic.parent.deleteConnection(ic)
}

// ended reports whether the flow of the connection has ended, i.e. it's been closed without being released.
func (ic *inletConn) ended() bool { return isClosedChan(ic.localDone) && !ic.released.Load() }

func (ic *inletConn) LocalIfIdx() int { return int(ic.localIfIdx.Load()) }

func (ic *inletConn) LocalAddr() net.Addr {
	if ic.localAddr == nil {
		return ic.parent.pc.LocalAddr()
	}
	return ic.localAddr
}

func (ic *inletConn) RemoteAddr() net.Addr { return ic.remoteAddr }

func (ic *inletConn) SetDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.parent.done) {
		return io.ErrClosedPipe
	}
	ic.rdDeadline.Set(t)
	ic.wrDeadline.Set(t)
	return nil
}

func (ic *inletConn) SetReadDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.parent.done) {
		return io.ErrClosedPipe
	}
	ic.rdDeadline.Set(t)
	return nil
}

func (ic *inletConn) SetWriteDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.parent.done) {
		return io.ErrClosedPipe
	}
	ic.wrDeadline.Set(t)
	return nil
}

// inletFallback is the fallback connection of an inlet: a plain PacketConn serving the flows released by
// their handlers. It deliberately gives no access to the socket.
type inletFallback struct {
	inletConnBase // With the packets of released flows in rdRx
}

func (i *inlet) newFallback() *inletFallback {
	return &inletFallback{i.newConnBase(i.cfg.fallbackQueueSize)}
}

// newConnBase returns the base of a connection of the inlet, with a read queue of the given size.
func (i *inlet) newConnBase(queueSize int) inletConnBase {
	return inletConnBase{
		parent:     i,
		rdRx:       make(chan *inletPacket, queueSize),
		localDone:  make(chan struct{}),
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
}

// ReadFrom reads a packet of a released flow, and returns the remote address of the flow.
func (ic *inletFallback) ReadFrom(b []byte) (int, net.Addr, error) {
	n, src, _, _, err := ic.ReadFromTo(b)
	return n, src, err
}

// ReadFromTo reads a packet of a released flow, and returns its remote address, the local address
// it came to, and the latest interface index of its flow.
func (ic *inletFallback) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, c, err := ic.receive(b)
	if err != nil {
		return 0, nil, nil, 0, ic.opError("read", nil, err)
	}
	return n, c.remoteAddr, c.localAddr, c.LocalIfIdx(), nil
}

// WriteTo writes a packet to addr, as WriteToFrom does with no local address.
func (ic *inletFallback) WriteTo(b []byte, addr net.Addr) (int, error) {
	return ic.WriteToFrom(b, addr, nil, 0)
}

// WriteToFrom writes a packet to dst from src and through the interface idx. If src is nil, it writes
// from the local address and through the interface of the latest flow of dst, or from those the system
// chooses if the inlet hasn't seen dst. A write keeps the flow of dst going, but doesn't start one,
// so a reply from a dst without a flow is queued for Accept, rather than read by the fallback connection.
func (ic *inletFallback) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
	if dst == nil {
		return 0, ic.opError("write", nil, errMissingAddress)
	}
	c := ic.parent.lookupRemote(dst)
	if src == nil && c != nil {
		src, idx = c.localAddr, c.LocalIfIdx()
	}
	n, err := ic.send(b, c, dst, src, idx)
	return n, ic.opError("write", dst, err)
}

// Close closes the fallback connection, so that the packets queued and those of released flows are dropped
// from now on.
func (ic *inletFallback) Close() error {
	var err error
	if !ic.closeLocal() {
		err = net.ErrClosed
	}
	ic.discard()
	return err
}

// discard drops and counts the packets left unread on the closed fallback connection.
func (ic *inletFallback) discard() {
	for {
		select {
		case pkt := <-ic.rdRx:
			putBuf(pkt.ref)
			ic.parent.drops.fallbackClosed.Add(1)
		default:
			return
		}
	}
}

func (ic *inletFallback) LocalAddr() net.Addr { return ic.parent.pc.LocalAddr() }

func (ic *inletFallback) SetDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.parent.done) {
		return io.ErrClosedPipe
	}
	ic.rdDeadline.Set(t)
	ic.wrDeadline.Set(t)
	return nil
}

func (ic *inletFallback) SetReadDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.parent.done) {
		return io.ErrClosedPipe
	}
	ic.rdDeadline.Set(t)
	return nil
}

func (ic *inletFallback) SetWriteDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.parent.done) {
		return io.ErrClosedPipe
	}
	ic.wrDeadline.Set(t)
	return nil
}

// opError wraps an error of an operation on the fallback connection, except for the end of reading.
func (ic *inletFallback) opError(op string, addr net.Addr, err error) error {
	if err == nil || err == io.EOF || errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return &net.OpError{Op: op, Net: ic.LocalAddr().Network(), Source: ic.LocalAddr(), Addr: addr, Err: err}
}

var errMissingAddress = errors.New("hybrid: missing address")

// Interface guards
var (
	_ FromToPacketConn = (*inletFallback)(nil)
	_ InletConn        = (*inletConn)(nil)
)
