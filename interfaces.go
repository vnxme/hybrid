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

// Deadline is an abstraction for handling timeouts.
type Deadline interface {
	// Set sets the point in time when the deadline will time out.
	// A timeout event is signaled by closing the channel returned by Wait.
	// Once a timeout has occurred, the deadline can be refreshed by specifying
	// a time value in the future. A zero time value prevents timeout.
	Set(t time.Time)
	// Wait returns a channel that is closed when the deadline is exceeded.
	Wait() chan struct{}
}

// PacketConn is an alias for net.PacketConn, i.e. a generic packet-oriented network connection.
type PacketConn = net.PacketConn

// PacketConnWrapper can unwrap the underlying PacketConn.
type PacketConnWrapper interface {
	Unwrap() PacketConn
}

// ReaderFromTo is a capability of a generic packet-oriented network connection
// to return the local address and the local interface index in addition to the
// remote address while reading packets.
type ReaderFromTo interface {
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

// SelectivePacketConn is a generic packet-oriented network connection that
// is aware of the local addresses and the local interface indices when it
// comes to reading and writing packets.
type SelectivePacketConn interface {
	PacketConn
	ReaderFromTo
	WriterToFrom
}

// StreamConn is an alias for net.Conn, i.e. a generic stream-oriented network connection.
type StreamConn = net.Conn

// StreamConnWrapper can unwrap the underlying StreamConn.
type StreamConnWrapper interface {
	Unwrap() StreamConn
}

// WriterToFrom is a capability of a generic packet-oriented network connection
// to consider the local address and the local interface index in addition to the
// remote address while writing packets.
type WriterToFrom interface {
	// WriteToFrom writes a packet with payload p to the remote address from
	// the local address assigned to the local interface with the given index.
	//
	// WriteToFrom can be made to time out and return an Error after a fixed time limit;
	// see [net.PacketConn.SetDeadline] and [net.PacketConn.SetWriteDeadline].
	// On packet-oriented connections, write timeouts are rare.
	WriteToFrom(p []byte, dst, src net.Addr, idx int) (n int, err error)
}
