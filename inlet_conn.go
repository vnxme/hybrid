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
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// receive copies the next packet of q into b, waiting until one comes, until done is closed, or until
// the deadline d passes, and returns the inlet connection of its flow. Once remoteDone is closed, it still
// returns the packets queued, then io.EOF.
func receive(b []byte, q chan *inletPacket, done, remoteDone <-chan struct{}, mu *sync.Mutex, d *deadline[noHook]) (int, *inletConn, error) {
	switch {
	case isClosedChan(done):
		return 0, nil, io.ErrClosedPipe
	case isClosedChan(remoteDone) && len(q) == 0:
		return 0, nil, io.EOF
	case isClosedChan(d.Wait()):
		return 0, nil, os.ErrDeadlineExceeded
	}

	mu.Lock()
	defer mu.Unlock()
	switch {
	case isClosedChan(done):
		return 0, nil, io.ErrClosedPipe
	case isClosedChan(remoteDone) && len(q) == 0:
		return 0, nil, io.EOF
	case isClosedChan(d.Wait()):
		return 0, nil, os.ErrDeadlineExceeded
	}

	var pkt *inletPacket
	select {
	case pkt = <-q:
	case <-done:
		return 0, nil, io.ErrClosedPipe
	case <-remoteDone:
		select {
		case pkt = <-q:
		default:
			return 0, nil, io.EOF
		}
	case <-d.Wait():
		return 0, nil, os.ErrDeadlineExceeded
	}
	n := copy(b, pkt.buf)
	putBuf(pkt.ref)
	return n, pkt.conn, nil
}

type inletConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr

	localDone chan struct{}
	rdRx      chan *inletPacket // Packets of the flow, unless it's been released

	parent     *inlet
	remoteDone <-chan struct{}

	key flowKey

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	last       atomic.Int64 // The clock of the inlet at the latest packet read or written
	localIfIdx atomic.Int64 // The latest one, used for replies

	once sync.Once  // Protects closing localDone
	mu   sync.Mutex // Keeps the packets of the flow in order while it's being released
	rdMu sync.Mutex // Serialize Read operations

	released atomic.Bool // Set before closing localDone, if the flow goes on to the fallback connection
}

func (ic *inletConn) Read(b []byte) (int, error) {
	n, _, err := ic.ReadFrom(b)
	return n, err
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
	n, _, err := receive(b, ic.rdRx, ic.localDone, ic.remoteDone, &ic.rdMu, &ic.rdDeadline)
	if err != nil {
		return 0, nil, nil, 0, err
	}
	return n, ic.remoteAddr, ic.localAddr, ic.LocalIfIdx(), nil
}

func (ic *inletConn) Write(b []byte) (int, error) { return ic.WriteTo(b, ic.remoteAddr) }

func (ic *inletConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	return ic.WriteToFrom(b, nil, nil, 0)
}

func (ic *inletConn) WriteToFrom(b []byte, _, _ net.Addr, _ int) (int, error) {
	n, err := ic.parent.send(b, ic, nil, ic.localDone, &ic.wrDeadline)
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

// closeLocal closes localDone, and reports whether it's done so, rather than found it closed.
func (ic *inletConn) closeLocal() (closed bool) {
	ic.once.Do(func() {
		close(ic.localDone)
		closed = true
	})
	return closed
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
	if isClosedChan(ic.localDone) || isClosedChan(ic.remoteDone) {
		return io.ErrClosedPipe
	}
	ic.rdDeadline.Set(t)
	ic.wrDeadline.Set(t)
	return nil
}

func (ic *inletConn) SetReadDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.remoteDone) {
		return io.ErrClosedPipe
	}
	ic.rdDeadline.Set(t)
	return nil
}

func (ic *inletConn) SetWriteDeadline(t time.Time) error {
	if isClosedChan(ic.localDone) || isClosedChan(ic.remoteDone) {
		return io.ErrClosedPipe
	}
	ic.wrDeadline.Set(t)
	return nil
}

// inletFallback is the fallback connection of an inlet: a plain PacketConn serving the flows released by
// their handlers. It deliberately gives no access to the socket.
type inletFallback struct {
	parent *inlet

	rdRx chan *inletPacket // Packets of released flows

	localDone chan struct{}

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
}

func (i *inlet) newFallback() *inletFallback {
	return &inletFallback{
		parent:     i,
		rdRx:       make(chan *inletPacket, i.cfg.fallbackQueueSize),
		localDone:  make(chan struct{}),
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
}

// ReadFrom reads a packet of a released flow, and returns the remote address of the flow.
func (d *inletFallback) ReadFrom(b []byte) (int, net.Addr, error) {
	n, c, err := receive(b, d.rdRx, d.localDone, d.parent.done, &d.rdMu, &d.rdDeadline)
	if err != nil {
		return 0, nil, d.opError("read", nil, err)
	}
	return n, c.remoteAddr, nil
}

// WriteTo writes a packet to addr, from the local address and through the interface of the latest flow
// of addr, or from those the system chooses if the inlet hasn't seen addr.
func (d *inletFallback) WriteTo(b []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, d.opError("write", nil, errMissingAddress)
	}
	n, err := d.parent.send(b, d.parent.lookupRemote(addr), addr, d.localDone, &d.wrDeadline)
	return n, d.opError("write", addr, err)
}

// Close closes the fallback connection, so that the packets queued and those of released flows are dropped
// from now on.
func (d *inletFallback) Close() error {
	err := net.ErrClosed
	d.once.Do(func() {
		close(d.localDone)
		err = nil
	})
	d.discard()
	return err
}

// discard drops and counts the packets left unread on the closed fallback connection.
func (d *inletFallback) discard() {
	for {
		select {
		case pkt := <-d.rdRx:
			putBuf(pkt.ref)
			d.parent.drops.fallbackClosed.Add(1)
		default:
			return
		}
	}
}

func (d *inletFallback) LocalAddr() net.Addr { return d.parent.pc.LocalAddr() }

func (d *inletFallback) SetDeadline(t time.Time) error {
	if isClosedChan(d.localDone) || isClosedChan(d.parent.done) {
		return io.ErrClosedPipe
	}
	d.rdDeadline.Set(t)
	d.wrDeadline.Set(t)
	return nil
}

func (d *inletFallback) SetReadDeadline(t time.Time) error {
	if isClosedChan(d.localDone) || isClosedChan(d.parent.done) {
		return io.ErrClosedPipe
	}
	d.rdDeadline.Set(t)
	return nil
}

func (d *inletFallback) SetWriteDeadline(t time.Time) error {
	if isClosedChan(d.localDone) || isClosedChan(d.parent.done) {
		return io.ErrClosedPipe
	}
	d.wrDeadline.Set(t)
	return nil
}

// opError wraps an error of an operation on the fallback connection, except for the end of reading.
func (d *inletFallback) opError(op string, addr net.Addr, err error) error {
	if err == nil || err == io.EOF || errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return &net.OpError{Op: op, Net: d.LocalAddr().Network(), Source: d.LocalAddr(), Addr: addr, Err: err}
}

var errMissingAddress = errors.New("hybrid: missing address")
