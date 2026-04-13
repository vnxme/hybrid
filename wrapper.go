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

// Interface guards
var (
	_ PacketConn = (*fakeAddressPacketConn)(nil)

	_ PacketConnWrapper = (*fakeAddressPacketConn)(nil)

	_ StreamConn = (*fakeAddressStreamConn)(nil)

	_ StreamConnWrapper = (*fakeAddressStreamConn)(nil)
)
