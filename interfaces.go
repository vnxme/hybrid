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

// Conn is a hybrid of a PacketConn and a StreamConn: a packet-oriented network connection serving a single
// remote address, which it reads packets from with Read and writes packets to with Write, so it's a StreamConn
// as well, but one keeping packet boundaries. Any *net.UDPConn, *net.IPConn or *net.UnixConn satisfies it,
// but serves a single remote address only if dialed.
type Conn interface {
	PacketConn

	// Read reads a single packet from the remote address into b. A packet longer than b is truncated,
	// silently or with an error, e.g. io.ErrShortBuffer, depending on the connection, and the rest of it
	// is lost, so reading a packet in parts, e.g. with io.ReadFull, fails.
	Read(b []byte) (n int, err error)
	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr
	// Write writes b as a single packet to the remote address.
	Write(b []byte) (n int, err error)
}

// Deadline signals that a point in time has passed by closing a channel, which a select may wait on
// along with its other cases. [SimpleDeadline] and [HookDeadline] return implementations of it.
type Deadline interface {
	// Set sets the point in time when the deadline will time out.
	// A timeout event is signaled by closing the channel returned by Wait.
	// Once a timeout has occurred, the deadline can be refreshed by specifying
	// a time value in the future. A zero time value prevents timeout.
	Set(t time.Time)
	// Wait returns a channel that is closed when the deadline is exceeded. As Set replaces
	// a closed channel with a new one, Wait should be called again after Set, rather than
	// its channel kept.
	Wait() <-chan struct{}
}

// FromToPacketConn is a generic packet-oriented network connection that
// is aware of the local addresses and the local interface indices when it
// comes to reading and writing packets.
type FromToPacketConn interface {
	PacketConn
	FromToReader
	FromToWriter
}

// FromToReader is a capability of a generic packet-oriented network connection
// to return the local address and the local interface index in addition to the
// remote address while reading packets.
type FromToReader interface {
	// ReadFromTo reads a packet from the connection, copying the payload into p.
	// It returns the number of bytes copied into p (0 <= n <= len(p)), the return
	// address that was on the packet, the local address the packet came to,
	// the local interface index, and any error encountered. The local address
	// is nil and the index is zero if the connection doesn't know them, e.g. a UDP
	// connection without packet info. Callers should always process the n > 0 bytes
	// returned before considering the error err.
	//
	// ReadFromTo can be made to time out and return an error after a fixed time limit;
	// see [net.PacketConn.SetDeadline] and [net.PacketConn.SetReadDeadline].
	ReadFromTo(p []byte) (n int, src, dst net.Addr, idx int, err error)
}

// FromToUDPConn is a UDP connection that is aware of the local addresses
// and the local interface indices when it comes to reading and writing packets.
// Its UDP-specific methods work the same way as ReadFromTo and WriteToFrom,
// with a nil *net.UDPAddr or an invalid netip.AddrPort for an address unknown
// on read, or left to the system on write.
type FromToUDPConn interface {
	UDPConn
	FromToReader
	FromToWriter

	ReadFromToUDP(b []byte) (n int, src *net.UDPAddr, dst *net.UDPAddr, idx int, err error)
	ReadFromToUDPAddrPort(b []byte) (n int, src netip.AddrPort, dst netip.AddrPort, idx int, err error)
	WriteToFromUDP(b []byte, dst, src *net.UDPAddr, idx int) (n int, err error)
	WriteToFromUDPAddrPort(b []byte, dst, src netip.AddrPort, idx int) (n int, err error)
}

// FromToWriter is a capability of a generic packet-oriented network connection
// to consider the local address and the local interface index in addition to the
// remote address while writing packets.
type FromToWriter interface {
	// WriteToFrom writes a packet with payload p to the remote address dst from
	// the local address src through the local interface with index idx. A nil src
	// and a zero idx leave them to the system. A connection that can't choose them,
	// e.g. one made by [PacketToFromToPacketConn] without packet info, ignores them.
	//
	// WriteToFrom can be made to time out and return an error after a fixed time limit;
	// see [net.PacketConn.SetDeadline] and [net.PacketConn.SetWriteDeadline].
	// On packet-oriented connections, write timeouts are rare.
	WriteToFrom(p []byte, dst, src net.Addr, idx int) (n int, err error)
}

// PacketConn is an alias for net.PacketConn, i.e. a generic packet-oriented network connection.
type PacketConn = net.PacketConn

// PacketListener is a generic network listener for packet-oriented protocols, similar to StreamListener,
// accepting connections of type C, each serving a single remote address. [PacketToStreamListener] makes
// a StreamListener of it.
type PacketListener[C Conn] interface {
	// Accept waits for and returns the next connection to the listener.
	Accept() (C, error)
	// Close closes the listener. Any blocked Accept operations will be unblocked and return errors.
	Close() error
	// Addr returns the listener's network address.
	Addr() net.Addr
}

// StreamConn is an alias for net.Conn, i.e. a generic stream-oriented network connection.
type StreamConn = net.Conn

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

// Wrapper can unwrap the underlying value of type T, e.g. a PacketConn, a StreamConn or a PacketListener.
// T must be the very type Unwrap returns, e.g. a wrapper returning a FromToPacketConn isn't a Wrapper[PacketConn].
type Wrapper[T any] interface {
	// Unwrap returns the underlying value as given to the wrapper.
	Unwrap() T
}

// Interface guards
var (
	_ StreamConn = Conn(nil)
	_ UDPConn    = (*net.UDPConn)(nil)
)
