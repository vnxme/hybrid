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
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// inletConnBase is what reading and writing a connection of an inlet needs, embedded in inletConn
// and inletFallback.
type inletConnBase struct {
	inlet     *inlet
	localAddr net.Addr          // Local address of the flow, if any
	rdRx      chan *inletPacket // Packets waiting to be read
	localDone chan struct{}     // Closed once the connection is closed

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once // Protects closing localDone
}

// closeLocal closes localDone, and reports whether it's done so, rather than found it closed.
func (ic *inletConnBase) closeLocal() (closed bool) {
	ic.once.Do(func() {
		close(ic.localDone)
		closed = true
	})
	return closed
}

// opError wraps err in net.OpError the same way as a trunk, and so a UDP connection, does: Source is
// the local address, and Addr is the remote one. Setting a deadline reports the local address as Addr only.
func (ic *inletConnBase) opError(op string, local, remote net.Addr, err error) error {
	if err == nil {
		return nil
	}
	network := ic.inlet.pc.LocalAddr().Network()
	if op == "set" {
		return &net.OpError{Op: op, Net: network, Addr: local, Err: err}
	}
	return &net.OpError{Op: op, Net: network, Source: local, Addr: remote, Err: err}
}

// LocalAddr returns the local address of the flow of the connection, or that of the underlying PacketConn
// if there's none, e.g. for the fallback connection, which serves many flows.
func (ic *inletConnBase) LocalAddr() net.Addr {
	if ic.localAddr == nil {
		return ic.inlet.pc.LocalAddr()
	}
	return ic.localAddr
}

// SetDeadline sets the read and write deadlines to t. It returns net.ErrClosed
// only once the connection is closed, since packets queued may be read after the inlet stops.
func (ic *inletConnBase) SetDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) {
		return ic.opError("set", ic.LocalAddr(), nil, errInletClosed)
	}
	ic.rdDeadline.Set(t)
	ic.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline to t. It returns net.ErrClosed
// only once the connection is closed, since packets queued may be read after the inlet stops.
func (ic *inletConnBase) SetReadDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) {
		return ic.opError("set", ic.LocalAddr(), nil, errInletClosed)
	}
	ic.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline to t. Like UDP, it returns net.ErrClosed
// only once the connection is closed, while a write fails with it after the inlet stops too.
func (ic *inletConnBase) SetWriteDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) {
		return ic.opError("set", ic.LocalAddr(), nil, errInletClosed)
	}
	ic.wrDeadline.Set(t)
	return nil
}

// inletConn is an inlet connection: it reads the packets of a single flow, queued by the read loop,
// and writes to the remote address of the flow from its local address, until it's closed or released.
type inletConn struct {
	remoteAddr net.Addr

	key flowKey

	inletConnBase // With the packets of the flow in rdRx, unless it's been released

	touched    atomic.Int64 // The clock of the inlet at the latest packet read or written
	localIfIdx atomic.Int64 // The latest one, used for replies

	releaseMu sync.Mutex // Keeps the packets of the flow in order while it's being released

	released atomic.Bool // Set before closing localDone, if the flow goes on to the fallback connection
}

// ReadFrom reads a packet of the flow, and returns a copy of the remote address of the flow.
func (ic *inletConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, _, _, err := ic.ReadFromTo(b)
	return n, addr, err
}

// ReadFromTo reads a packet of the flow, and returns copies of the remote and local addresses of the flow,
// which the caller may change, and its latest interface index.
func (ic *inletConn) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, c, err := ic.receive(b)
	err = ic.opError("read", ic.LocalAddr(), ic.remoteAddr, err)
	if c == nil { // No packet read
		return 0, nil, nil, 0, err
	}
	return n, cloneAddr(ic.remoteAddr), cloneAddr(ic.localAddr), ic.LocalIfIdx(), err // Copies the caller may change
}

// WriteTo writes a packet to the remote address of the flow, as WriteToFrom does with no local address.
func (ic *inletConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	return ic.WriteToFrom(b, addr, nil, 0)
}

// WriteToFrom writes a packet to the remote address of the flow from its local address, through its latest
// interface rather than idx. Like a connected UDP socket, it fails with net.ErrWriteToConnected if dst or src
// is another address.
func (ic *inletConn) WriteToFrom(b []byte, dst, src net.Addr, _ int) (int, error) {
	if dst != nil && addrKey(dst) != ic.key.remote() || src != nil && addrKey(src) != addrKey(ic.LocalAddr()) {
		return 0, ic.opError("write", ic.LocalAddr(), dst, errInletWriteToConnected)
	}
	n, err := ic.send(b, ic, ic.remoteAddr, ic.localAddr, ic.LocalIfIdx())
	return n, ic.opError("write", ic.LocalAddr(), ic.remoteAddr, err)
}

// Close closes the connection and ends its flow, so that the next packet of the flow starts a new one.
func (ic *inletConn) Close() error {
	if !ic.closeLocal() {
		return ic.opError("close", ic.LocalAddr(), ic.remoteAddr, errInletClosed)
	}
	ic.inlet.deleteConnection(ic)
	return nil
}

// Release hands the flow of the connection over to the fallback connection of the inlet: the packets
// queued and all the later packets of the flow go there, or are dropped if the inlet has no fallback
// connection. The connection is closed, but its flow goes on until it's idle.
func (ic *inletConn) Release() error {
	ic.releaseMu.Lock()
	defer ic.releaseMu.Unlock()
	if isClosedChan(ic.localDone) {
		return ic.opError("release", ic.LocalAddr(), ic.remoteAddr, errInletClosed)
	}

	ic.released.Store(true)
	for {
		select {
		case pkt := <-ic.rdRx:
			ic.inlet.toFallback(pkt)
		default:
			ic.closeLocal()
			return nil
		}
	}
}

// touch records the activity of the flow of the connection, at the time of the clock of the inlet.
func (ic *inletConn) touch() { ic.touched.Store(ic.inlet.clock.Load()) }

// expire ends the flow of a connection that has been idle: it closes the connection, unless it's been
// closed already, and reports the flow for deletion, even if it's been released.
func (ic *inletConn) expire() {
	ic.closeLocal()
	ic.inlet.deleteConnection(ic)
}

// ended reports whether the flow of the connection has ended, i.e. it's been closed without being released,
// or released while the fallback connection is closed.
func (ic *inletConn) ended() bool {
	if !isClosedChan(ic.localDone) {
		return false
	}
	// A released flow goes on, unless its fallback connection has been closed, so that its next packet starts
	// a new flow, offered to Accept again, rather than being dropped while the flow is kept going by its packets
	return !ic.released.Load() || ic.inlet.fallback != nil && isClosedChan(ic.inlet.fallback.localDone)
}

// LocalIfIdx returns the index of the interface the latest packet of the flow came through, used for replies.
func (ic *inletConn) LocalIfIdx() int { return int(ic.localIfIdx.Load()) }

// RemoteAddr returns the remote address of the flow.
func (ic *inletConn) RemoteAddr() net.Addr { return ic.remoteAddr }

// inletFallback is the fallback connection of an inlet: a plain PacketConn serving the flows released by
// their handlers. It deliberately gives no access to the socket.
type inletFallback struct {
	inletConnBase // With the packets of released flows in rdRx
}

// newFallback returns the fallback connection of the inlet.
func (i *inlet) newFallback() *inletFallback {
	return &inletFallback{i.newConnBase(i.cfg.fallbackQueueSize, nil)}
}

// newConnBase returns the base of a connection of the inlet, with a read queue of the given size
// and the local address of its flow, if any.
func (i *inlet) newConnBase(queueSize int, local net.Addr) inletConnBase {
	return inletConnBase{
		inlet:      i,
		localAddr:  local,
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
	if c == nil { // No packet read
		return 0, nil, nil, 0, ic.opError("read", ic.LocalAddr(), nil, err)
	}
	err = ic.opError("read", ic.LocalAddr(), c.remoteAddr, err)
	return n, cloneAddr(c.remoteAddr), cloneAddr(c.localAddr), c.LocalIfIdx(), err // Copies the caller may change
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
		return 0, ic.opError("write", ic.LocalAddr(), nil, errInletMissingAddress)
	}
	// The packet is written once this write returns, so don't queue the addresses of the caller, which may
	// change them by then: use those of the latest flow of dst if any, or copies otherwise, as trunks do
	var to net.Addr
	c := ic.inlet.lookupRemote(dst)
	switch {
	case c == nil:
		to, src = cloneAddr(dst), cloneAddr(src)
	case src == nil:
		to, src, idx = c.remoteAddr, c.localAddr, c.LocalIfIdx()
	default:
		to, src = c.remoteAddr, cloneAddr(src)
	}
	n, err := ic.send(b, c, to, src, idx)
	return n, ic.opError("write", ic.LocalAddr(), dst, err)
}

// Close closes the fallback connection, so that the packets queued are dropped, and the flows released end:
// the next packet of each starts a new flow, queued for Accept.
func (ic *inletFallback) Close() error {
	var err error
	if !ic.closeLocal() {
		err = ic.opError("close", ic.LocalAddr(), nil, errInletClosed)
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
			ic.inlet.drops.fallbackClosed.Add(1)
		default:
			return
		}
	}
}

// Interface guards
var (
	_ FromToPacketConn = (*inletFallback)(nil)
	_ InletConn        = (*inletConn)(nil)
)
