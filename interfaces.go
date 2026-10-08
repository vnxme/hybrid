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
	"net/netip"
	"syscall"
	"time"
)

// Deadline is an abstraction for handling timeouts.
type Deadline interface {
	// Set sets the point in time when the deadline will time out.
	// A timeout event is signaled by closing the channel returned by Wait.
	// Once a timeout has occurred, the deadline can be refreshed by specifying
	// a time value in the future. A zero time value prevents timeout.
	Set(t time.Time)
	// Wait returns a channel that is closed when the deadline is exceeded.
	Wait() <-chan struct{}
}

// MixedConn is a network connection that implements both PacketConn and StreamConn interfaces.
type MixedConn interface {
	PacketConn
	StreamConn
}

type DataPacket interface {
	Cap() int
	Len() int

	ReadableBuffer() []byte
	WritableBuffer() []byte

	Reset()
}

type NetworkPacket interface {
	DataPacket

	Data() DataPacket

	LocalIfIdx() int

	LocalAddr() net.Addr
	RemoteAddr() net.Addr

	Reset()
}

// PacketConn is an alias for net.PacketConn, i.e. a generic packet-oriented network connection.
type PacketConn = net.PacketConn

// PacketConnWrapper can unwrap the underlying PacketConn.
type PacketConnWrapper interface {
	Unwrap() PacketConn
}

// PacketListener is a generic network listener for packet-oriented protocols, similar to StreamListener.
type PacketListener interface {
	// Accept waits for and returns the next connection to the listener.
	Accept() (MixedConn, error)
	// Close closes the listener. Any operations blocked by Accept will be unblocked and return errors.
	Close() error
	// Addr returns the listener's network address.
	Addr() net.Addr
}

// PacketListenerWrapper can unwrap the underlying PacketListener.
type PacketListenerWrapper interface {
	Unwrap() PacketListener
}

// FromToReader is a capability of a generic packet-oriented network connection
// to return the local address and the local interface index in addition to the
// remote address while reading packets.
type FromToReader interface {
	// ReadFromTo reads a packet from the connection, copying the payload into p.
	// It returns the number of bytes copied into p, the return address that was
	// on the packet, the local address the packet came to and the local interface
	// index. It returns the number of bytes read (0 <= n <= len(p)) and any error
	// encountered. Callers should always process the n > 0 bytes returned before
	// considering the error err.
	//
	// ReadFromTo can be made to time out and return an error after a fixed time limit;
	// see [net.PacketConn.SetDeadline] and [net.PacketConn.SetReadDeadline].
	ReadFromTo(p []byte) (n int, src, dst net.Addr, idx int, err error)
}

// FromToPacketConn is a generic packet-oriented network connection that
// is aware of the local addresses and the local interface indices when it
// comes to reading and writing packets.
type FromToPacketConn interface {
	PacketConn
	FromToReader
	FromToWriter
}

// FromToUDPConn is a UDP connection that is aware of the local addresses
// and the local interface indices when it comes to reading and writing packets.
type FromToUDPConn interface {
	UDPConn
	FromToReader
	FromToWriter

	ReadFromToUDP(b []byte) (n int, src *net.UDPAddr, dst *net.UDPAddr, idx int, err error)
	ReadFromToUDPAddrPort(b []byte) (n int, src netip.AddrPort, dst netip.AddrPort, idx int, err error)
	WriteToFromUDP(b []byte, dst, src *net.UDPAddr, idx int) (n int, err error)
	WriteToFromUDPAddrPort(b []byte, dst, src netip.AddrPort, idx int) (n int, err error)
}

// StreamConn is an alias for net.Conn, i.e. a generic stream-oriented network connection.
type StreamConn = net.Conn

// StreamConnWrapper can unwrap the underlying StreamConn.
type StreamConnWrapper interface {
	Unwrap() StreamConn
}

// StreamListener is an alias for net.Listener, i.e. a generic network listener for stream-oriented protocols.
type StreamListener = net.Listener

// UDPConn is an interface that is implicitly implemented by *net.UDPConn.
type UDPConn interface {
	PacketConn

	SyscallConn() (syscall.RawConn, error)

	ReadFromUDP(b []byte) (n int, addr *net.UDPAddr, err error)
	ReadFromUDPAddrPort(b []byte) (n int, addr netip.AddrPort, err error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
	WriteToUDPAddrPort(b []byte, addr netip.AddrPort) (int, error)

	ReadMsgUDP(b []byte, oob []byte) (n int, oobn int, flags int, addr *net.UDPAddr, err error)
	ReadMsgUDPAddrPort(b []byte, oob []byte) (n int, oobn int, flags int, addr netip.AddrPort, err error)
	WriteMsgUDP(b []byte, oob []byte, addr *net.UDPAddr) (n int, oobn int, err error)
	WriteMsgUDPAddrPort(b []byte, oob []byte, addr netip.AddrPort) (n int, oobn int, err error)
}

// FromToWriter is a capability of a generic packet-oriented network connection
// to consider the local address and the local interface index in addition to the
// remote address while writing packets.
type FromToWriter interface {
	// WriteToFrom writes a packet with payload p to the remote address from
	// the local address assigned to the local interface with the given index.
	//
	// WriteToFrom can be made to time out and return an Error after a fixed time limit;
	// see [net.PacketConn.SetDeadline] and [net.PacketConn.SetWriteDeadline].
	// On packet-oriented connections, write timeouts are rare.
	WriteToFrom(p []byte, dst, src net.Addr, idx int) (n int, err error)
}
