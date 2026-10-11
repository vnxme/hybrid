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
	"sync"
	"sync/atomic"
	"time"
)

// Inlet design. This is the target the code below is being brought in line with.
//
// An inlet serves a packet connection, typically a UDP socket, to many handlers at once, the way
// a TCP listener serves a socket to many connections. Its read loop reads every packet, tells
// the flows apart, and passes each flow either to an inlet connection, which a handler serves
// like an accepted TCP connection, or to the fallback connection, which serves all the other
// flows together, e.g. as the packet connection of the QUIC server of Caddy, which tells QUIC
// connections apart itself. Its write loop writes the packets of all those connections.
//
// Flows. A flow of a UDP connection that reports the destination address of each packet, e.g. with
// packet info, is identified by its source and destination addresses, as netip.AddrPort values.
// A flow of any other connection, e.g. of a UDP connection without packet info or of a trunk, is
// identified by its source address alone, as a netip.AddrPort value for a *net.UDPAddr, or as the
// string of any other address. One comparable key type holds either, so a UDP flow is looked up
// without allocating. Which addresses identify a flow is decided once per inlet, not per packet,
// so that a flow never splits: an inlet that uses destination addresses drops a packet without one.
// The interface index isn't part of a flow, as it may change, e.g. with asymmetric routing: the
// latest one is kept and used for replies. Trunks carry the addresses their writers give, so
// a test can have many flows over a trunk by writing from distinct source addresses: a trunk end
// writing without one gives its own local address, which makes a single flow.
//
// Lifecycle. The first packet of an unknown flow creates an inlet connection, queues it for Accept,
// and queues the packet on it. A handler accepting the connection either serves it or releases it,
// e.g. as no matcher recognizes the flow: then the packets queued on the connection and all the later
// packets of the flow go to the fallback connection if there is one, or are dropped if there isn't.
// A flow ends when its connection is closed, by its handler or by its idle timeout, and its next
// packet starts a new flow straight away: UDP has nothing like TCP's TIME_WAIT and RST to tell a late
// packet of an old flow from the first packet of a new one.
//
// Deadlines. Inlet connections and the fallback connection have read and write deadlines of their own,
// like any net.Conn or net.PacketConn. Each flow has an idle timeout, which every packet read or written
// refreshes: an atomic timestamp per flow, checked by a periodic sweeper, keeps that cheap. Only the read
// loop sets the read deadline of the underlying connection, to be interrupted when the inlet closes, and
// only the write loop sets its write deadline, to apply the write timeout of the inlet: as the deadlines
// of a socket are shared by everything using it, each has a single user.
//
// Reading. Only the read loop changes the flow table, so it takes no lock on the way of a packet:
// closed and expired flows reach it over a channel. It reads into a buffer as large as the largest
// datagram, and copies each packet into a pooled buffer of its size before queueing it.
//
// Writing. Inlet connections and the fallback connection hand their packets to the write loop, which
// writes them to the underlying connection one by one, with the addresses of their flows. A write copies
// its packet into a pooled buffer of its size and returns once the packet is queued, the way a UDP write
// returns once the system has copied it: if the write queue is full, it waits for room until the write
// deadline of its connection. So a write can't report an error of the underlying connection: the write
// loop counts it, and closes the inlet if the underlying connection fails for good. The write timeout
// of the inlet limits how long the write loop waits on the underlying connection for one packet, so
// that a stuck write can't hold every flow up. The fallback connection is a plain net.PacketConn, whose
// WriteTo knows only the remote address. So it looks the remote address up in an index of the latest
// flow of each remote address, safe for concurrent use, to write from the local address and through
// the interface that flow came to, and lets the system choose them for a remote address it hasn't seen.
// It deliberately gives no access to the socket, e.g. no SyscallConn, so that a QUIC server can't change
// socket options or read from the socket past the inlet.
//
// Dropping. Like a UDP socket whose buffer is full, the inlet never blocks its read loop on a slow reader:
// it drops a packet if the queue of its connection is full, if the accept queue is full (without creating
// a flow, so that a retry by the client is a new flow), if the flow table is full, or if the packet has no
// destination address while the inlet uses destination addresses. The write loop drops a packet whose write
// times out or fails. Every drop is counted by its reason, and the counters can be read with Stats.

// InletConfig configures an inlet. The zero value of a field gives its default, as does a nil *InletConfig.
type InletConfig struct {
	// AcceptQueueSize is the maximum number of inlet connections waiting for Accept,
	// [DefaultInletAcceptQueueSize] by default. The first packet of a new flow is dropped
	// if the accept queue is full.
	AcceptQueueSize int

	// ReadQueueSize is the maximum number of packets waiting to be read from each inlet connection,
	// [DefaultInletReadQueueSize] by default. A packet is dropped if the queue of its inlet connection is full.
	ReadQueueSize int

	// FallbackQueueSize is the maximum number of packets waiting to be read from the fallback connection,
	// [DefaultInletFallbackQueueSize] by default. A packet is dropped if the queue of the fallback connection
	// is full.
	FallbackQueueSize int

	// IdleTimeout is how long a flow may go without a packet read or written before it ends,
	// [DefaultInletIdleTimeout] by default: its inlet connection is closed, and its next packet starts
	// a new flow. A negative timeout keeps every flow until its inlet connection is closed, and a released
	// flow as long as the inlet is open.
	IdleTimeout time.Duration

	// MaxFlows is the maximum number of flows, [DefaultInletMaxFlows] by default.
	// The first packet of a new flow is dropped if there are as many flows already.
	MaxFlows int

	// ReadBufferSize is the maximum size of a packet read from the underlying PacketConn,
	// [DefaultInletReadBufferSize] by default, and at least 512. A larger packet is dropped.
	ReadBufferSize int

	// WriteQueueSize is the maximum number of packets waiting to be written to the underlying PacketConn,
	// [DefaultInletWriteQueueSize] by default. A write waits for room until the write deadline of its
	// connection if the queue is full.
	WriteQueueSize int

	// WriteTimeout is how long the write loop waits for the underlying PacketConn to write a packet before
	// dropping it, [DefaultInletWriteTimeout] by default. A negative timeout makes it wait as long as it takes.
	WriteTimeout time.Duration

	// FallbackConn gives the inlet a fallback connection, which serves the flows released by their handlers.
	// Without one, the packets of a released flow are dropped.
	FallbackConn bool

	// PacketInfo makes the inlet enable packet info with [PacketInfoUDPConn] if the underlying PacketConn
	// satisfies UDPConn, so that flows are told apart by their destination addresses as well, and replies
	// are sent from the address each flow came to, e.g. for a UDPConn listening on an unspecified address.
	// Without it, flows are told apart by their source addresses alone.
	PacketInfo bool
}

// inletConfig is an InletConfig with its defaults filled in, and with a zero timeout meaning none.
type inletConfig struct {
	idleTimeout  time.Duration
	writeTimeout time.Duration

	acceptQueueSize   int
	fallbackQueueSize int
	maxFlows          int
	readBufferSize    int
	readQueueSize     int
	writeQueueSize    int

	fallbackConn bool
	packetInfo   bool
}

// resolve returns the configuration of an inlet, with the defaults of the fields not set.
func (c *InletConfig) resolve() inletConfig {
	if c == nil {
		c = &InletConfig{}
	}
	orDefault := func(v, def int) int {
		if v > 0 {
			return v
		}
		return def
	}
	timeout := func(v, def time.Duration) time.Duration {
		switch {
		case v < 0:
			return 0
		case v == 0:
			return def
		}
		return v
	}

	cfg := inletConfig{
		idleTimeout:       timeout(c.IdleTimeout, DefaultInletIdleTimeout),
		writeTimeout:      timeout(c.WriteTimeout, DefaultInletWriteTimeout),
		acceptQueueSize:   orDefault(c.AcceptQueueSize, DefaultInletAcceptQueueSize),
		fallbackQueueSize: orDefault(c.FallbackQueueSize, DefaultInletFallbackQueueSize),
		maxFlows:          orDefault(c.MaxFlows, DefaultInletMaxFlows),
		readBufferSize:    orDefault(c.ReadBufferSize, DefaultInletReadBufferSize),
		readQueueSize:     orDefault(c.ReadQueueSize, DefaultInletReadQueueSize),
		writeQueueSize:    orDefault(c.WriteQueueSize, DefaultInletWriteQueueSize),
		fallbackConn:      c.FallbackConn,
		packetInfo:        c.PacketInfo,
	}
	cfg.readBufferSize = max(cfg.readBufferSize, minInletReadBufferSize)
	return cfg
}

// inletPacket is a packet queued for reading from an inlet connection or from the fallback connection,
// or for writing by the write loop.
type inletPacket struct {
	conn *inletConn // The inlet connection of the flow of the packet, if any
	dst  net.Addr   // The address to write to, if there is no conn
	ref  *[]byte    // Reference to the pooled buffer of buf, if any
	buf  []byte
}

// InletStats counts the packets an inlet has dropped, by reason.
type InletStats struct {
	// Reading, in the order a packet meets the checks
	Truncated       uint64 // The packet was larger than ReadBufferSize
	NoDestination   uint64 // The packet had no destination address, while the inlet uses them
	TableFull       uint64 // The flow table was full when the packet started a new flow
	AcceptQueueFull uint64 // The accept queue was full when the packet started a new flow
	ConnQueueFull   uint64 // The queue of the inlet connection of the packet was full
	ConnClosed      uint64 // The inlet connection of the packet was closed before reading it

	// Releasing
	NoFallback        uint64 // The packet of a released flow had no open fallback connection to go to
	FallbackQueueFull uint64 // The queue of the fallback connection was full
	FallbackClosed    uint64 // The fallback connection was closed before reading the packet

	// Writing
	WriteTimedOut uint64 // The write loop timed out writing the packet
	WriteFailed   uint64 // The write loop failed to write the packet, e.g. to an unreachable address
	WriteUnsent   uint64 // The inlet closed, or its underlying connection did, before writing the packet
}

// InletListener is a PacketListener of an inlet, with its fallback connection and statistics.
type InletListener interface {
	PacketListener

	// Unwrap stops the inlet like Close does, but leaves the underlying PacketConn open, and returns it
	// as given to Inlet, with no deadlines.
	PacketConnWrapper

	// Fallback returns the fallback connection of the inlet, or nil if it has none.
	Fallback() PacketConn

	// Stats returns the numbers of packets dropped so far, by reason.
	Stats() InletStats
}

// inlet implements PacketListener for any underlying PacketConn.
type inlet struct {
	start time.Time // When the inlet started, for its clock

	raw PacketConn       // Underlying PacketConn, as given to Inlet
	pc  FromToPacketConn // The same, wrapped as a FromToPacketConn if it isn't one
	udp FromToUDPConn    // The same as pc if it's a FromToUDPConn, which reports destination addresses
	fb  *inletFallback   // Fallback connection, if any

	acChan chan *inletConn   // Accept channel
	wrChan chan *inletPacket // Write channel
	done   chan struct{}     // Done channel

	rt map[flowKey]*inletConn // Routing table

	closed struct { // Connections closed or expired, for the read loop to delete their flows
		s []*inletConn
		sync.Mutex
	}

	flows struct { // Every flow in the routing table, for the sweeper
		m map[*inletConn]struct{}
		sync.Mutex
	}

	remotes struct { // The latest flow of each remote address, for the writes of the fallback connection
		m map[flowKey]*inletConn
		sync.RWMutex
	}

	cfg inletConfig

	drops struct {
		truncated, noDestination, tableFull, acceptQueueFull, connQueueFull, connClosed atomic.Uint64 // Reading
		noFallback, fallbackQueueFull, fallbackClosed                                   atomic.Uint64 // Releasing
		writeTimedOut, writeFailed, writeUnsent                                         atomic.Uint64 // Writing
	}

	wg    sync.WaitGroup
	clock atomic.Int64 // Time since start, kept by the sweeper, coarse but cheap to read
	once  sync.Once
}

// Inlet returns an InletListener for the given PacketConn, configured by config, which may be nil for the defaults,
// and starts its internal loops.
func Inlet(pc PacketConn, config *InletConfig) InletListener {
	cfg := config.resolve()

	i := &inlet{
		raw:    pc,
		pc:     PacketToFromToPacketConn(pc, cfg.packetInfo),
		acChan: make(chan *inletConn, cfg.acceptQueueSize),
		wrChan: make(chan *inletPacket, cfg.writeQueueSize),
		done:   make(chan struct{}),
		rt:     make(map[flowKey]*inletConn),
		start:  time.Now(),
		cfg:    cfg,
	}
	i.udp, _ = i.pc.(FromToUDPConn)
	i.remotes.m = make(map[flowKey]*inletConn)
	i.flows.m = make(map[*inletConn]struct{})
	if cfg.fallbackConn {
		i.fb = i.newFallback()
	}

	i.wg.Add(2)
	go i.readLoop()
	go i.writeLoop()
	if cfg.idleTimeout > 0 {
		i.wg.Add(1)
		go i.sweepLoop()
	}
	return i
}

// Accept waits for and returns the next connection to the listener.
func (i *inlet) Accept() (MixedConn, error) {
	if isClosedChan(i.done) {
		return nil, net.ErrClosed
	}

	select {
	case conn := <-i.acChan:
		return conn, nil
	case <-i.done:
		return nil, net.ErrClosed
	}
}

// Addr returns the local network address, if known.
func (i *inlet) Addr() net.Addr { return i.pc.LocalAddr() }

// Fallback returns the fallback connection of the inlet, or nil if it has none.
func (i *inlet) Fallback() PacketConn {
	if i.fb == nil {
		return nil // Not a nil *inletFallback, which wouldn't compare equal to nil
	}
	return i.fb
}

// Stats returns the numbers of packets dropped so far, by reason.
func (i *inlet) Stats() InletStats {
	return InletStats{
		Truncated:         i.drops.truncated.Load(),
		NoDestination:     i.drops.noDestination.Load(),
		TableFull:         i.drops.tableFull.Load(),
		AcceptQueueFull:   i.drops.acceptQueueFull.Load(),
		ConnQueueFull:     i.drops.connQueueFull.Load(),
		ConnClosed:        i.drops.connClosed.Load(),
		NoFallback:        i.drops.noFallback.Load(),
		FallbackQueueFull: i.drops.fallbackQueueFull.Load(),
		FallbackClosed:    i.drops.fallbackClosed.Load(),
		WriteTimedOut:     i.drops.writeTimedOut.Load(),
		WriteFailed:       i.drops.writeFailed.Load(),
		WriteUnsent:       i.drops.writeUnsent.Load(),
	}
}

// Close closes the underlying PacketConn, the outlets and terminates the internal goroutines.
// Any operations blocked by Accept will be unblocked and return errors.
func (i *inlet) Close() (err error) {
	return i.closeWith(true)
}

// Unwrap stops the inlet and returns the underlying PacketConn, as given to Inlet.
func (i *inlet) Unwrap() PacketConn {
	// Stop all internal loops, but keep the underlying PacketConn open,
	// unless it has been closed otherwise. Ignore errors if any.
	_ = i.closeWith(false)

	// Clear the deadlines that have interrupted the loops, which have returned by now
	_ = i.raw.SetDeadline(time.Time{})
	return i.raw
}

func (i *inlet) closeWith(closeUnderlying bool) (err error) {
	err = net.ErrClosed
	i.once.Do(func() {
		// Close the done channel in order to:
		// - unblock Accept and let the listener return;
		// - unblock Read and Write operations in the outlets;
		// - let the readLoop goroutine return
		//   (a) before blocking at PacketConn.ReadFrom, or
		//   (b) on sending a MixedConn to the accept channel, or
		//   (c) on sending a packet to an outlet;
		// - let the writeLoop goroutine return
		//   on receiving a packet from an outlet.
		close(i.done)

		// Unless this function is called by Unwrap(), it should be true.
		if closeUnderlying {
			// Close the underlying PacketConn in order to:
			// - let the readLoop goroutine return
			//   by unblocking PacketConn.ReadFrom operations;
			// - let the writeLoop goroutine return
			//   by unblocking PacketConn.WriteTo operations.
			if err = i.pc.Close(); err != nil {
				// TODO(vnxme): log an unexpected closing error
			}
		} else {
			// Interrupt the reading and writing of the loops, keeping the underlying PacketConn open
			_ = i.pc.SetDeadline(time.Unix(1, 0))
		}

		// Wait until the internal goroutines return.
		i.wg.Wait()

		// Drop the packets queued once the write loop has returned, e.g. as the underlying connection has closed.
		i.drainWrites()

		// Return nil to report a successful closing.
		err = nil
	})
	return
}

func (i *inlet) logError(err error) {
	// TODO(vnxme): Log an unexpected reading/writing error
}

// Defaults of the fields of InletConfig, which apply when a field isn't set
const (
	DefaultInletAcceptQueueSize   = 8
	DefaultInletFallbackQueueSize = 1024 // As the fallback connection may serve many flows
	DefaultInletIdleTimeout       = 30 * time.Second
	DefaultInletMaxFlows          = 8192
	DefaultInletReadBufferSize    = math.MaxUint16 - (20 + 8) // The largest IPv4 UDP datagram is 64 KiB net of headers
	DefaultInletReadQueueSize     = 32
	DefaultInletWriteQueueSize    = 64
	DefaultInletWriteTimeout      = 500 * time.Millisecond

	minInletReadBufferSize = 512 // A read buffer of less than 512 bytes seems impractical
)

// Interface guards
var (
	_ InletListener = (*inlet)(nil)
	_ PacketConn    = (*inletFallback)(nil)
)
