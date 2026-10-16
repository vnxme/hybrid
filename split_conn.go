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

// splitConnBase is what reading and writing a connection of a splitter needs, embedded in splitConn
// and splitFallback.
type splitConnBase struct {
	splitter  *splitter
	localAddr net.Addr          // Local address of the flow, if any
	rdRx      chan *splitPacket // Packets waiting to be read
	localDone chan struct{}     // Closed once the connection is closed

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once // Protects closing localDone
}

// newConnBase returns the base of a connection of the splitter, with a read queue of the given size
// and the local address of its flow, if any.
func (s *splitter) newConnBase(queueSize int, local net.Addr) splitConnBase {
	return splitConnBase{
		splitter:   s,
		localAddr:  local,
		rdRx:       make(chan *splitPacket, queueSize),
		localDone:  make(chan struct{}),
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
}

// LocalAddr returns the local address of the flow of the connection, or that of the underlying PacketConn
// if there's none, e.g. for the fallback connection, which serves many flows.
func (sc *splitConnBase) LocalAddr() net.Addr {
	if sc.localAddr == nil {
		return sc.splitter.pc.LocalAddr()
	}
	return sc.localAddr
}

// SetDeadline sets the read and write deadlines to t. It returns net.ErrClosed
// only once the connection is closed, since packets queued may be read after the splitter stops.
func (sc *splitConnBase) SetDeadline(t time.Time) error {
	if isClosedChan(sc.localDone) {
		return sc.opError("set", sc.LocalAddr(), nil, errSplitClosed)
	}
	sc.rdDeadline.Set(t)
	sc.wrDeadline.Set(t)
	return nil
}

// SetReadDeadline sets the read deadline to t. It returns net.ErrClosed
// only once the connection is closed, since packets queued may be read after the splitter stops.
func (sc *splitConnBase) SetReadDeadline(t time.Time) error {
	if isClosedChan(sc.localDone) {
		return sc.opError("set", sc.LocalAddr(), nil, errSplitClosed)
	}
	sc.rdDeadline.Set(t)
	return nil
}

// SetWriteDeadline sets the write deadline to t. Like UDP, it returns net.ErrClosed
// only once the connection is closed, while a write fails with it after the splitter stops too.
func (sc *splitConnBase) SetWriteDeadline(t time.Time) error {
	if isClosedChan(sc.localDone) {
		return sc.opError("set", sc.LocalAddr(), nil, errSplitClosed)
	}
	sc.wrDeadline.Set(t)
	return nil
}

// closeLocal closes localDone, and reports whether it's done so, rather than found it closed.
func (sc *splitConnBase) closeLocal() (closed bool) {
	sc.once.Do(func() {
		close(sc.localDone)
		closed = true
	})
	return closed
}

// opError wraps err in net.OpError the same way as a trunk, and so a UDP connection, does: Source is
// the local address, and Addr is the remote one. Setting a deadline reports the local address as Addr only.
func (sc *splitConnBase) opError(op string, local, remote net.Addr, err error) error {
	if err == nil {
		return nil
	}
	network := sc.splitter.pc.LocalAddr().Network()
	if op == "set" {
		return &net.OpError{Op: op, Net: network, Addr: local, Err: err}
	}
	return &net.OpError{Op: op, Net: network, Source: local, Addr: remote, Err: err}
}

// splitConn is a split connection: it reads the packets of a single flow, queued by the read loop,
// and writes to the remote address of the flow from its local address, until it's closed or released.
type splitConn struct {
	remoteAddr net.Addr

	key flowKey

	splitConnBase // With the packets of the flow in rdRx, unless it's been released

	touched    atomic.Int64 // The clock of the splitter at the latest packet read or written
	localIfIdx atomic.Int64 // The latest one, used for replies

	releaseMu sync.Mutex // Keeps the packets of the flow in order while it's being released

	released atomic.Bool // Set before closing localDone, if the flow goes on to the fallback connection
}

// newConnection returns a new split connection for the flow of the given packet.
func (s *splitter) newConnection(p *splitPacketInfo) *splitConn {
	src, dst := p.src, p.dst
	if s.udp != nil {
		src, dst = addrPortToAddr(p.key.src), addrPortToAddr(p.key.dst)
	}

	sc := &splitConn{
		splitConnBase: s.newConnBase(s.cfg.readQueueSize, dst),
		remoteAddr:    src,
		key:           p.key,
	}
	sc.localIfIdx.Store(int64(p.idx))
	sc.touch()
	return sc
}

// Close closes the connection and ends its flow, so that the next packet of the flow starts a new one.
func (sc *splitConn) Close() error {
	if !sc.closeLocal() {
		return sc.opError("close", sc.LocalAddr(), sc.remoteAddr, errSplitClosed)
	}
	sc.splitter.deleteConnection(sc)
	return nil
}

// LocalIfIdx returns the index of the interface the latest packet of the flow came through, used for replies.
func (sc *splitConn) LocalIfIdx() int { return int(sc.localIfIdx.Load()) }

// ReadFrom reads a packet of the flow, and returns a copy of the remote address of the flow.
func (sc *splitConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, _, _, err := sc.ReadFromTo(b)
	return n, addr, err
}

// ReadFromTo reads a packet of the flow, and returns copies of the remote and local addresses of the flow,
// which the caller may change, and its latest interface index.
func (sc *splitConn) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, c, err := sc.receive(b)
	err = sc.opError("read", sc.LocalAddr(), sc.remoteAddr, err)
	if c == nil { // No packet read
		return 0, nil, nil, 0, err
	}
	return n, cloneAddr(sc.remoteAddr), cloneAddr(sc.localAddr), sc.LocalIfIdx(), err // Copies the caller may change
}

// Release hands the flow of the connection over to the fallback connection of the splitter: the packets
// queued and all the later packets of the flow go there, or are dropped if the splitter has no fallback
// connection. The connection is closed, but its flow goes on until it's idle.
func (sc *splitConn) Release() error {
	sc.releaseMu.Lock()
	defer sc.releaseMu.Unlock()
	if isClosedChan(sc.localDone) {
		return sc.opError("release", sc.LocalAddr(), sc.remoteAddr, errSplitClosed)
	}

	sc.released.Store(true)
	for {
		select {
		case pkt := <-sc.rdRx:
			sc.splitter.toFallback(pkt)
		default:
			sc.closeLocal()
			return nil
		}
	}
}

// RemoteAddr returns the remote address of the flow.
func (sc *splitConn) RemoteAddr() net.Addr { return sc.remoteAddr }

// WriteTo writes a packet to the remote address of the flow, as WriteToFrom does with no local address.
func (sc *splitConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	return sc.WriteToFrom(b, addr, nil, 0)
}

// WriteToFrom writes a packet to the remote address of the flow from its local address, through its latest
// interface rather than idx. Like a connected UDP socket, it fails with net.ErrWriteToConnected if dst or src
// is another address.
func (sc *splitConn) WriteToFrom(b []byte, dst, src net.Addr, _ int) (int, error) {
	if dst != nil && addrKey(dst) != sc.key.remote() || src != nil && addrKey(src) != addrKey(sc.LocalAddr()) {
		return 0, sc.opError("write", sc.LocalAddr(), dst, errSplitWriteToConnected)
	}
	n, err := sc.send(b, sc, sc.remoteAddr, sc.localAddr, sc.LocalIfIdx())
	return n, sc.opError("write", sc.LocalAddr(), sc.remoteAddr, err)
}

// ended reports whether the flow of the connection has ended, i.e. it's been closed without being released,
// or released while the fallback connection is closed.
func (sc *splitConn) ended() bool {
	if !isClosedChan(sc.localDone) {
		return false
	}
	// A released flow goes on, unless its fallback connection has been closed, so that its next packet starts
	// a new flow, offered to Accept again, rather than being dropped while the flow is kept going by its packets
	return !sc.released.Load() || sc.splitter.fallback != nil && isClosedChan(sc.splitter.fallback.localDone)
}

// expire ends the flow of a connection that has been idle: it closes the connection, unless it's been
// closed already, and reports the flow for deletion, even if it's been released.
func (sc *splitConn) expire() {
	sc.closeLocal()
	sc.splitter.deleteConnection(sc)
}

// touch records the activity of the flow of the connection, at the time of the clock of the splitter.
func (sc *splitConn) touch() { sc.touched.Store(sc.splitter.clock.Load()) }

// splitFallback is the fallback connection of a splitter: a plain PacketConn serving the flows released by
// their handlers. It deliberately gives no access to the socket.
type splitFallback struct {
	splitConnBase // With the packets of released flows in rdRx
}

// newFallback returns the fallback connection of the splitter.
func (s *splitter) newFallback() *splitFallback {
	return &splitFallback{s.newConnBase(s.cfg.fallbackQueueSize, nil)}
}

// Close closes the fallback connection, so that the packets queued are dropped, and the flows released end:
// the next packet of each starts a new flow, queued for Accept.
func (sc *splitFallback) Close() error {
	var err error
	if !sc.closeLocal() {
		err = sc.opError("close", sc.LocalAddr(), nil, errSplitClosed)
	}
	sc.discard()
	return err
}

// ReadFrom reads a packet of a released flow, and returns the remote address of the flow.
func (sc *splitFallback) ReadFrom(b []byte) (int, net.Addr, error) {
	n, src, _, _, err := sc.ReadFromTo(b)
	return n, src, err
}

// ReadFromTo reads a packet of a released flow, and returns its remote address, the local address
// it came to, and the latest interface index of its flow.
func (sc *splitFallback) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, c, err := sc.receive(b)
	if c == nil { // No packet read
		return 0, nil, nil, 0, sc.opError("read", sc.LocalAddr(), nil, err)
	}
	err = sc.opError("read", sc.LocalAddr(), c.remoteAddr, err)
	return n, cloneAddr(c.remoteAddr), cloneAddr(c.localAddr), c.LocalIfIdx(), err // Copies the caller may change
}

// WriteTo writes a packet to addr, as WriteToFrom does with no local address.
func (sc *splitFallback) WriteTo(b []byte, addr net.Addr) (int, error) {
	return sc.WriteToFrom(b, addr, nil, 0)
}

// WriteToFrom writes a packet to dst from src and through the interface idx. If src is nil, it writes
// from the local address and through the interface of the latest flow of dst, or from those the system
// chooses if the splitter hasn't seen dst. A write keeps the flow of dst going, but doesn't start one,
// so a reply from a dst without a flow is queued for Accept, rather than read by the fallback connection.
func (sc *splitFallback) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
	if dst == nil {
		return 0, sc.opError("write", sc.LocalAddr(), nil, errSplitMissingAddress)
	}
	// The packet is written once this write returns, so don't queue the addresses of the caller, which may
	// change them by then: use those of the latest flow of dst if any, or copies otherwise, as trunks do
	var to net.Addr
	c := sc.splitter.lookupRemote(dst)
	switch {
	case c == nil:
		to, src = cloneAddr(dst), cloneAddr(src)
	case src == nil:
		to, src, idx = c.remoteAddr, c.localAddr, c.LocalIfIdx()
	default:
		to, src = c.remoteAddr, cloneAddr(src)
	}
	n, err := sc.send(b, c, to, src, idx)
	return n, sc.opError("write", sc.LocalAddr(), dst, err)
}

// discard drops and counts the packets left unread on the closed fallback connection.
func (sc *splitFallback) discard() {
	for {
		select {
		case pkt := <-sc.rdRx:
			putBuf(pkt.ref)
			sc.splitter.drops.fallbackClosed.Add(1)
		default:
			return
		}
	}
}

// Interface guards
var (
	_ FromToPacketConn = (*splitFallback)(nil)
	_ SplitConn        = (*splitConn)(nil)
)
