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
	"net"
	"time"
)

// fakeAddressPacketConn is a PacketConn wrapper that has
// customizable local and remote addresses.
type fakeAddressPacketConn struct {
	PacketConn

	local  net.Addr
	remote net.Addr
}

// FakeAddressPacketConn returns a PacketConn implementation that shadows
// local and remote addresses.
//
// Unless the local address is nil, it is returned by LocalAddr instead of
// the native local address. Similarly, unless the remote address is nil,
// it is returned by ReadFrom and used by WriteTo instead of any real address.
func FakeAddressPacketConn(pc PacketConn, local, remote net.Addr) PacketConn {
	return &fakeAddressPacketConn{pc, local, remote}
}

func (w *fakeAddressPacketConn) LocalAddr() net.Addr {
	if w.local == nil {
		return w.PacketConn.LocalAddr()
	}
	return w.local
}

func (w *fakeAddressPacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, addr, err = w.PacketConn.ReadFrom(b)
	if w.remote != nil {
		addr = w.remote
	}
	return
}

func (w *fakeAddressPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if w.remote != nil {
		return w.PacketConn.WriteTo(b, w.remote)
	}
	return w.PacketConn.WriteTo(b, addr)
}

func (w *fakeAddressPacketConn) Unwrap() PacketConn { return w.PacketConn }

// fakeAddressStreamConn is a StreamConn wrapper that has
// customizable local and remote addresses.
type fakeAddressStreamConn struct {
	StreamConn

	local  net.Addr
	remote net.Addr
}

// FakeAddressStreamConn returns a StreamConn implementation that shadows
// local and remote addresses.
//
// Unless the local address is nil, it is returned by LocalAddr instead of
// the native local address. Similarly, unless the remote address is nil,
// it is returned by RemoteAddr instead of the native remote address.
func FakeAddressStreamConn(sc StreamConn, local, remote net.Addr) StreamConn {
	return &fakeAddressStreamConn{sc, local, remote}
}

func (w *fakeAddressStreamConn) LocalAddr() net.Addr {
	if w.local == nil {
		return w.StreamConn.LocalAddr()
	}
	return w.local
}

func (w *fakeAddressStreamConn) RemoteAddr() net.Addr {
	if w.remote == nil {
		return w.StreamConn.RemoteAddr()
	}
	return w.remote
}

func (w *fakeAddressStreamConn) Unwrap() StreamConn { return w.StreamConn }

// idleTimeoutPacketConn is a PacketConn wrapper that closes
// the underlying connection when its idle timeout has occurred.
type idleTimeoutPacketConn struct {
	PacketConn

	deadline Deadline
	timeout  time.Duration
}

// IdleTimeoutPacketConn returns a PacketConn implementation that closes itself
// asynchronously from a separate goroutine once its idle timeout has occurred.
//
// The timeout is completely ignored if its value is nil or negative. The timer is
// reset on each successful read and write operation, i.e. when n > 0 or err == nil.
//
// The timer launches when the underlying connection is wrapped, i.e. before this
// function returns, and stops when the underlying connection is returned by Unwrap,
// or closed by Close, or closed by the timer itself.
func IdleTimeoutPacketConn(pc PacketConn, timeout time.Duration) PacketConn {
	w := &idleTimeoutPacketConn{PacketConn: pc, deadline: HookDeadline(func() { _ = pc.Close() }), timeout: timeout}
	w.deadline.Set(time.Now().Add(timeout))
	return w
}

func (w *idleTimeoutPacketConn) Close() (err error) {
	if w.timeout > 0 {
		w.deadline.Set(time.Time{})
	}
	return w.PacketConn.Close()
}

func (w *idleTimeoutPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addr, err = w.PacketConn.ReadFrom(p)
	if w.timeout > 0 && (n > 0 || err == nil) {
		w.deadline.Set(time.Now().Add(w.timeout))
	}
	return
}

func (w *idleTimeoutPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	n, err = w.PacketConn.WriteTo(p, addr)
	if w.timeout > 0 && (n > 0 || err == nil) {
		w.deadline.Set(time.Now().Add(w.timeout))
	}
	return
}

func (w *idleTimeoutPacketConn) Unwrap() PacketConn {
	if w.timeout > 0 {
		w.deadline.Set(time.Time{})
	}
	return w.PacketConn
}

// idleTimeoutStreamConn is a StreamConn wrapper that closes
// the underlying connection when its idle timeout has occurred.
type idleTimeoutStreamConn struct {
	StreamConn

	deadline Deadline
	timeout  time.Duration
}

// IdleTimeoutStreamConn returns a StreamConn implementation that closes itself
// asynchronously from a separate goroutine once its idle timeout has occurred.
//
// The timeout is completely ignored if its value is nil or negative. The timer is
// reset on each successful read and write operation, i.e. when n > 0 or err == nil.
//
// The timer launches when the underlying connection is wrapped, i.e. before this
// function returns, and stops when the underlying connection is returned by Unwrap,
// or closed by Close, or closed by the timer itself.
func IdleTimeoutStreamConn(sc StreamConn, timeout time.Duration) StreamConn {
	w := &idleTimeoutStreamConn{StreamConn: sc, deadline: HookDeadline(func() { _ = sc.Close() }), timeout: timeout}
	w.deadline.Set(time.Now().Add(timeout))
	return w
}

func (w *idleTimeoutStreamConn) Close() (err error) {
	if w.timeout > 0 {
		w.deadline.Set(time.Time{})
	}
	return w.StreamConn.Close()
}

func (w *idleTimeoutStreamConn) Read(b []byte) (n int, err error) {
	n, err = w.StreamConn.Read(b)
	if w.timeout > 0 && (n > 0 || err == nil) {
		w.deadline.Set(time.Now().Add(w.timeout))
	}
	return
}

func (w *idleTimeoutStreamConn) Write(b []byte) (n int, err error) {
	n, err = w.StreamConn.Write(b)
	if w.timeout > 0 && (n > 0 || err == nil) {
		w.deadline.Set(time.Now().Add(w.timeout))
	}
	return
}

func (w *idleTimeoutStreamConn) Unwrap() StreamConn {
	if w.timeout > 0 {
		w.deadline.Set(time.Time{})
	}
	return w.StreamConn
}

// packetToFromToPacketConn is a PacketConnWrapper that implements FromToPacketConn.
type packetToFromToPacketConn struct {
	PacketConn
}

// PacketToFromToPacketConn returns a FromToPacketConn implementation
// based on the underlying PacketConn.
//
// If the given PacketConn satisfies FromToPacketConn, no wrapping is done at all.
// If tryPacketInfoUDP is true, the PacketConn implements UDPConn, and system calls succeed,
// a FromToUDPConn made by [PacketInfoUDPConn] is returned. Otherwise, this function creates a simple wrapper
// that returns zero values for the destination address and the local interface index
// on read, and ignores any values assigned to these parameters on write.
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

func (w *packetToFromToPacketConn) ReadFromTo(p []byte) (n int, src, dst net.Addr, idx int, err error) {
	n, src, err = w.ReadFrom(p)
	return n, src, nil, 0, err
}

func (w *packetToFromToPacketConn) WriteToFrom(p []byte, dst, _ net.Addr, _ int) (n int, err error) {
	return w.WriteTo(p, dst)
}

func (w *packetToFromToPacketConn) Unwrap() PacketConn {
	return w.PacketConn
}

// packetToStreamConn is a PacketConnWrapper that implements StreamConn.
type packetToStreamConn struct {
	PacketConn

	remote net.Addr
}

// PacketToStreamConn returns a StreamConn implementation based on
// the underlying PacketConn with the given remote address.
//
// The pre-defined remote address is used by the RemoteAddr and Write methods.
// The Read method is unsafe because no filters apply to the remote address.
func PacketToStreamConn(pc PacketConn, remote net.Addr) StreamConn {
	return &packetToStreamConn{pc, remote}
}

func (w *packetToStreamConn) Read(b []byte) (n int, err error) {
	n, _, err = w.ReadFrom(b)
	return
}

func (w *packetToStreamConn) Write(b []byte) (n int, err error) {
	return w.WriteTo(b, w.remote)
}

func (w *packetToStreamConn) RemoteAddr() net.Addr { return w.remote }

func (w *packetToStreamConn) Unwrap() PacketConn { return w.PacketConn }

type packetToStreamListener struct {
	PacketListener
}

func PacketToStreamListener(pl PacketListener) StreamListener {
	return &packetToStreamListener{pl}
}

func (w *packetToStreamListener) Accept() (sc StreamConn, err error) {
	return w.PacketListener.Accept()
}

func (w *packetToStreamListener) Unwrap() PacketListener { return w.PacketListener }

// streamToPacketConn is a StreamConnWrapper that implements PacketConn.
type streamToPacketConn struct {
	StreamConn
}

// StreamToPacketConn returns a PacketConn implementation based on
// the underlying StreamConn with the given remote address.
//
// The underlying remote address is used by the Read method. The Write method
// is unsafe because the remote address argument is completely ignored.
func StreamToPacketConn(sc StreamConn) PacketConn {
	return &streamToPacketConn{sc}
}

func (w *streamToPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, err = w.Read(p)
	addr = w.RemoteAddr()
	return
}

func (w *streamToPacketConn) WriteTo(p []byte, _ net.Addr) (n int, err error) {
	return w.Write(p)
}

func (w *streamToPacketConn) Unwrap() StreamConn { return w.StreamConn }

// Interface guards
var (
	_ PacketConn = (*fakeAddressPacketConn)(nil)
	_ PacketConn = (*idleTimeoutPacketConn)(nil)
	_ PacketConn = (*streamToPacketConn)(nil)

	_ PacketConnWrapper = (*fakeAddressPacketConn)(nil)
	_ PacketConnWrapper = (*idleTimeoutPacketConn)(nil)
	_ PacketConnWrapper = (*packetToFromToPacketConn)(nil)
	_ PacketConnWrapper = (*packetToStreamConn)(nil)

	_ PacketListenerWrapper = (*packetToStreamListener)(nil)

	_ FromToPacketConn = (*packetToFromToPacketConn)(nil)

	_ StreamConn = (*fakeAddressStreamConn)(nil)
	_ StreamConn = (*idleTimeoutStreamConn)(nil)
	_ StreamConn = (*packetToStreamConn)(nil)

	_ StreamConnWrapper = (*fakeAddressStreamConn)(nil)
	_ StreamConnWrapper = (*idleTimeoutStreamConn)(nil)
	_ StreamConnWrapper = (*streamToPacketConn)(nil)

	_ StreamListener = (*packetToStreamListener)(nil)
)
