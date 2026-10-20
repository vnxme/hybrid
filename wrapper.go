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

package hybrid

import (
	"math"
	"net"
	"sync/atomic"
	"time"
)

// Wrapper design.
//
// A wrapper changes a single aspect of a connection, e.g. its addresses or its lifetime, or adapts it to another
// interface, e.g. a PacketConn to a StreamConn, and Unwrap returns the connection as given. A wrapper embeds
// the interface it wraps, so it has the methods of that interface only, and hides any other methods of the
// connection, e.g. those of FromToPacketConn or UDPConn, as forwarding them would bypass the aspect it changes.
// Hence, a wrapper goes on top: anything that needs such methods, e.g. to enable packet info on a UDPConn,
// should be given the connection before it's wrapped, and a method unrelated to the aspect changed, e.g.
// SyscallConn, may still be reached through Unwrap.

// fakeAddressPacketConn is a Wrapper[PacketConn] with customizable local and remote addresses.
type fakeAddressPacketConn struct {
	PacketConn

	local  net.Addr
	remote net.Addr
}

// FakeAddressPacketConn returns a WrappingPacketConn that shadows local and remote addresses.
//
// Unless the local address is nil, it is returned by LocalAddr instead of the native local address.
// Similarly, unless the remote address is nil, it is returned by ReadFrom instead of the address a packet
// comes from, and WriteTo sends every packet to it, whatever address it is given.
func FakeAddressPacketConn(pc PacketConn, local, remote net.Addr) WrappingPacketConn {
	return &fakeAddressPacketConn{pc, local, remote}
}

// LocalAddr returns the local address given, or that of the underlying PacketConn if nil.
func (w *fakeAddressPacketConn) LocalAddr() net.Addr {
	if w.local == nil {
		return w.PacketConn.LocalAddr()
	}
	return w.local
}

// ReadFrom reads a packet from the underlying PacketConn, and returns the remote address given
// instead of the address it comes from, unless either is nil, e.g. as the read has failed.
func (w *fakeAddressPacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, addr, err = w.PacketConn.ReadFrom(b)
	if addr != nil && w.remote != nil {
		addr = w.remote
	}
	return
}

// WriteTo writes a packet to the remote address given, or to addr if nil.
func (w *fakeAddressPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if w.remote != nil {
		return w.PacketConn.WriteTo(b, w.remote)
	}
	return w.PacketConn.WriteTo(b, addr)
}

// Unwrap returns the underlying PacketConn.
func (w *fakeAddressPacketConn) Unwrap() PacketConn { return w.PacketConn }

// fakeAddressStreamConn is a Wrapper[StreamConn] with customizable local and remote addresses.
type fakeAddressStreamConn struct {
	StreamConn

	local  net.Addr
	remote net.Addr
}

// FakeAddressStreamConn returns a WrappingStreamConn that shadows local and remote addresses.
//
// Unless the local address is nil, it is returned by LocalAddr instead of the native local address.
// Similarly, unless the remote address is nil, it is returned by RemoteAddr instead of the native remote address.
func FakeAddressStreamConn(sc StreamConn, local, remote net.Addr) WrappingStreamConn {
	return &fakeAddressStreamConn{sc, local, remote}
}

// LocalAddr returns the local address given, or that of the underlying StreamConn if nil.
func (w *fakeAddressStreamConn) LocalAddr() net.Addr {
	if w.local == nil {
		return w.StreamConn.LocalAddr()
	}
	return w.local
}

// RemoteAddr returns the remote address given, or that of the underlying StreamConn if nil.
func (w *fakeAddressStreamConn) RemoteAddr() net.Addr {
	if w.remote == nil {
		return w.StreamConn.RemoteAddr()
	}
	return w.remote
}

// Unwrap returns the underlying StreamConn.
func (w *fakeAddressStreamConn) Unwrap() StreamConn { return w.StreamConn }

// fakeClosePacketConn is a Wrapper[PacketConn] with a customizable Close.
type fakeClosePacketConn struct {
	PacketConn

	closeFunc func() error
}

// FakeClosePacketConn returns a WrappingPacketConn that shadows Close.
//
// Unless closeFunc is nil, Close calls it instead of closing the underlying PacketConn,
// and returns its error. Otherwise, Close does nothing and returns nil. Either way, the underlying
// PacketConn stays open: reads and writes go on, and Close neither unblocks them nor makes them fail,
// so whoever owns the PacketConn closes it, e.g. after Unwrap.
func FakeClosePacketConn(pc PacketConn, closeFunc func() error) WrappingPacketConn {
	return &fakeClosePacketConn{pc, closeFunc}
}

// Close calls the close function given, if any, leaving the underlying PacketConn open.
func (w *fakeClosePacketConn) Close() error {
	if w.closeFunc == nil {
		return nil
	}
	return w.closeFunc()
}

// Unwrap returns the underlying PacketConn.
func (w *fakeClosePacketConn) Unwrap() PacketConn { return w.PacketConn }

// fakeCloseStreamConn is a Wrapper[StreamConn] with a customizable Close.
type fakeCloseStreamConn struct {
	StreamConn

	closeFunc func() error
}

// FakeCloseStreamConn returns a WrappingStreamConn that shadows Close.
//
// Unless closeFunc is nil, Close calls it instead of closing the underlying StreamConn,
// and returns its error. Otherwise, Close does nothing and returns nil. Either way, the underlying
// StreamConn stays open: reads and writes go on, and Close neither unblocks them nor makes them fail,
// so whoever owns the StreamConn closes it, e.g. after Unwrap.
func FakeCloseStreamConn(sc StreamConn, closeFunc func() error) WrappingStreamConn {
	return &fakeCloseStreamConn{sc, closeFunc}
}

// Close calls the close function given, if any, leaving the underlying StreamConn open.
func (w *fakeCloseStreamConn) Close() error {
	if w.closeFunc == nil {
		return nil
	}
	return w.closeFunc()
}

// Unwrap returns the underlying StreamConn.
func (w *fakeCloseStreamConn) Unwrap() StreamConn { return w.StreamConn }

// idleTimer closes a connection once it has been idle for its timeout. Reads and writes only record the time
// of their activity, which the timer checks when it fires: it closes the connection, or fires again once
// the timeout will have passed since the latest activity, so that activity costs no reset of the timer.
type idleTimer struct {
	timer   *time.Timer
	close   func() error
	start   time.Time     // Origin of touched, with a monotonic clock reading
	timeout time.Duration // Positive if the timer is armed
	touched atomic.Int64  // Time of the latest activity, since start
	stopped atomic.Bool   // Set once, by whichever of fire and stop comes first
}

// init arms the timer to call closer once idle for the timeout, unless the timeout isn't positive.
func (t *idleTimer) init(timeout time.Duration, closer func() error) {
	if timeout <= 0 {
		return
	}
	t.timeout, t.close, t.start = timeout, closer, time.Now()
	t.timer = time.AfterFunc(math.MaxInt64, t.fire)
	t.timer.Reset(timeout) // Only once t.timer is set, as fire resets it
}

// touch records activity, which postpones closing the connection.
func (t *idleTimer) touch() {
	if t.timeout > 0 {
		t.touched.Store(int64(time.Since(t.start)))
	}
}

// fire closes the connection if it has been idle for the timeout, or makes the timer fire again
// once it will have been.
func (t *idleTimer) fire() {
	if t.stopped.Load() {
		return
	}
	if idle := time.Since(t.start) - time.Duration(t.touched.Load()); idle < t.timeout {
		t.timer.Reset(t.timeout - idle) // If stop comes meanwhile, the next fire returns at once
		return
	}
	if t.stopped.CompareAndSwap(false, true) {
		_ = t.close()
	}
}

// stop disarms the timer for good, so that it never closes the connection afterwards. It reports whether
// it has done so, i.e. neither the timer has closed the connection nor stop has been called before.
func (t *idleTimer) stop() bool {
	if !t.stopped.CompareAndSwap(false, true) {
		return false
	}
	if t.timer != nil {
		t.timer.Stop()
	}
	return true
}

// idleTimeoutPacketConn is a Wrapper[PacketConn] that closes the underlying connection once idle for its timeout.
type idleTimeoutPacketConn struct {
	PacketConn

	idle idleTimer
}

// IdleTimeoutPacketConn returns a WrappingPacketConn that closes the underlying PacketConn
// asynchronously from a separate goroutine once it has been idle for the timeout.
//
// The timeout is ignored if it's zero or negative. The idle time restarts on each successful
// read and write operation, i.e. when n > 0 or err == nil.
//
// The idle time starts when the underlying connection is wrapped, i.e. before this function returns,
// and the timer stops for good once Stop is called, leaving the underlying connection open, or once
// the connection is closed, by Close or by the timer.
func IdleTimeoutPacketConn(pc PacketConn, timeout time.Duration) WrappingPacketConn {
	w := &idleTimeoutPacketConn{PacketConn: pc}
	w.idle.init(timeout, pc.Close)
	return w
}

// Close stops the timer and closes the underlying PacketConn.
func (w *idleTimeoutPacketConn) Close() error {
	w.idle.stop()
	return w.PacketConn.Close()
}

// ReadFrom reads a packet from the underlying PacketConn, restarting the idle time if successful.
func (w *idleTimeoutPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addr, err = w.PacketConn.ReadFrom(p)
	if n > 0 || err == nil {
		w.idle.touch()
	}
	return
}

// WriteTo writes a packet to the underlying PacketConn, restarting the idle time if successful.
func (w *idleTimeoutPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	n, err = w.PacketConn.WriteTo(p, addr)
	if n > 0 || err == nil {
		w.idle.touch()
	}
	return
}

// Stop stops the timer for good, leaving the underlying PacketConn open. It returns net.ErrClosed if the timer
// has stopped already, by Close or Stop, or by closing the underlying PacketConn.
func (w *idleTimeoutPacketConn) Stop() error {
	if !w.idle.stop() {
		return net.ErrClosed
	}
	return nil
}

// Unwrap returns the underlying PacketConn, which the timer closes once idle, unless stopped.
func (w *idleTimeoutPacketConn) Unwrap() PacketConn { return w.PacketConn }

// idleTimeoutStreamConn is a Wrapper[StreamConn] that closes the underlying connection once idle for its timeout.
type idleTimeoutStreamConn struct {
	StreamConn

	idle idleTimer
}

// IdleTimeoutStreamConn returns a WrappingStreamConn that closes the underlying StreamConn
// asynchronously from a separate goroutine once it has been idle for the timeout.
//
// The timeout is ignored if it's zero or negative. The idle time restarts on each successful
// read and write operation, i.e. when n > 0 or err == nil.
//
// The idle time starts when the underlying connection is wrapped, i.e. before this function returns,
// and the timer stops for good once Stop is called, leaving the underlying connection open, or once
// the connection is closed, by Close or by the timer.
func IdleTimeoutStreamConn(sc StreamConn, timeout time.Duration) WrappingStreamConn {
	w := &idleTimeoutStreamConn{StreamConn: sc}
	w.idle.init(timeout, sc.Close)
	return w
}

// Close stops the timer and closes the underlying StreamConn.
func (w *idleTimeoutStreamConn) Close() error {
	w.idle.stop()
	return w.StreamConn.Close()
}

// Read reads from the underlying StreamConn, restarting the idle time if successful.
func (w *idleTimeoutStreamConn) Read(b []byte) (n int, err error) {
	n, err = w.StreamConn.Read(b)
	if n > 0 || err == nil {
		w.idle.touch()
	}
	return
}

// Write writes to the underlying StreamConn, restarting the idle time if successful.
func (w *idleTimeoutStreamConn) Write(b []byte) (n int, err error) {
	n, err = w.StreamConn.Write(b)
	if n > 0 || err == nil {
		w.idle.touch()
	}
	return
}

// Stop stops the timer for good, leaving the underlying StreamConn open. It returns net.ErrClosed if the timer
// has stopped already, by Close or Stop, or by closing the underlying StreamConn.
func (w *idleTimeoutStreamConn) Stop() error {
	if !w.idle.stop() {
		return net.ErrClosed
	}
	return nil
}

// Unwrap returns the underlying StreamConn, which the timer closes once idle, unless stopped.
func (w *idleTimeoutStreamConn) Unwrap() StreamConn { return w.StreamConn }

// packetToFromToPacketConn is a Wrapper[PacketConn] that implements FromToPacketConn.
type packetToFromToPacketConn struct {
	PacketConn
}

// PacketToFromToPacketConn returns a FromToPacketConn implementation based on the underlying PacketConn.
//
// If the given PacketConn satisfies FromToPacketConn, no wrapping is done at all. If tryPacketInfoUDP is true,
// the PacketConn implements UDPConn, and system calls succeed, a FromToUDPConn made by [PacketInfoUDPConn]
// is returned. Otherwise, this function creates a simple wrapper that returns zero values for the destination
// address and the local interface index on read, and ignores any values assigned to these parameters on write.
func PacketToFromToPacketConn(pc PacketConn, tryPacketInfoUDP bool) FromToPacketConn {
	if fpc, ok := pc.(FromToPacketConn); ok {
		return fpc
	}

	if tryPacketInfoUDP {
		if conn, ok := pc.(UDPConn); ok {
			udp, err := PacketInfoUDPConn(conn)
			if err == nil {
				return udp
			}
		}
	}

	return &packetToFromToPacketConn{pc}
}

// ReadFromTo reads a packet with ReadFrom, and returns no destination address and a zero interface index.
func (w *packetToFromToPacketConn) ReadFromTo(p []byte) (n int, src, dst net.Addr, idx int, err error) {
	n, src, err = w.ReadFrom(p)
	return n, src, nil, 0, err
}

// WriteToFrom writes a packet with WriteTo, ignoring the source address and the interface index.
func (w *packetToFromToPacketConn) WriteToFrom(p []byte, dst, _ net.Addr, _ int) (n int, err error) {
	return w.WriteTo(p, dst)
}

// Unwrap returns the underlying PacketConn.
func (w *packetToFromToPacketConn) Unwrap() PacketConn {
	return w.PacketConn
}

// packetToStreamConn is a Wrapper[PacketConn] that implements StreamConn.
type packetToStreamConn struct {
	PacketConn

	remote net.Addr
}

// PacketToStreamConn returns a StreamConn implementation based on the underlying PacketConn
// with the given remote address.
//
// The remote address given is returned by RemoteAddr, and Write sends every packet to it,
// while Read accepts a packet from any address, as no filter applies to the remote address.
// Each Read reads a single packet, which is truncated if longer than the buffer, silently or with
// an error, e.g. io.ErrShortBuffer, depending on the underlying PacketConn, so that reading a packet
// in parts, e.g. with io.ReadFull, fails.
func PacketToStreamConn(pc PacketConn, remote net.Addr) StreamConn {
	return &packetToStreamConn{pc, remote}
}

// Read reads a packet from the underlying PacketConn, from any address.
func (w *packetToStreamConn) Read(b []byte) (n int, err error) {
	n, _, err = w.ReadFrom(b)
	return
}

// Write writes a packet to the remote address given.
func (w *packetToStreamConn) Write(b []byte) (n int, err error) {
	return w.WriteTo(b, w.remote)
}

// RemoteAddr returns the remote address given.
func (w *packetToStreamConn) RemoteAddr() net.Addr { return w.remote }

// Unwrap returns the underlying PacketConn.
func (w *packetToStreamConn) Unwrap() PacketConn { return w.PacketConn }

// streamToPacketConn is a Wrapper[StreamConn] that implements PacketConn.
type streamToPacketConn struct {
	StreamConn
}

// StreamToPacketConn returns a PacketConn implementation based on the underlying StreamConn.
//
// ReadFrom returns the remote address of the StreamConn, while WriteTo writes to it whatever address
// it's given, as the address argument is ignored. A packet read is what a single Read returns, so packets
// keep their boundaries only if the StreamConn keeps those of writes, as a pipe does with a buffer as long
// as any write, and TCP doesn't.
func StreamToPacketConn(sc StreamConn) PacketConn {
	return &streamToPacketConn{sc}
}

// ReadFrom reads from the underlying StreamConn, and returns its remote address.
func (w *streamToPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = w.Read(p)
	addr = w.RemoteAddr()
	return
}

// WriteTo writes to the underlying StreamConn, ignoring addr.
func (w *streamToPacketConn) WriteTo(p []byte, _ net.Addr) (n int, err error) {
	return w.Write(p)
}

// Unwrap returns the underlying StreamConn.
func (w *streamToPacketConn) Unwrap() StreamConn { return w.StreamConn }

// Interface guards
var (
	_ FromToPacketConn = (*packetToFromToPacketConn)(nil)

	_ PacketConn = (*streamToPacketConn)(nil)

	_ Stopper = (*idleTimeoutPacketConn)(nil)
	_ Stopper = (*idleTimeoutStreamConn)(nil)

	_ StreamConn = (*packetToStreamConn)(nil)

	_ WrappingPacketConn = (*fakeAddressPacketConn)(nil)
	_ WrappingPacketConn = (*fakeClosePacketConn)(nil)
	_ WrappingPacketConn = (*idleTimeoutPacketConn)(nil)

	_ WrappingStreamConn = (*fakeAddressStreamConn)(nil)
	_ WrappingStreamConn = (*fakeCloseStreamConn)(nil)
	_ WrappingStreamConn = (*idleTimeoutStreamConn)(nil)

	_ Wrapper[PacketConn] = (*packetToFromToPacketConn)(nil)
	_ Wrapper[PacketConn] = (*packetToStreamConn)(nil)

	_ Wrapper[StreamConn] = (*streamToPacketConn)(nil)
)
