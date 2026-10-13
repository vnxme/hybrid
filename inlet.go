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
	"errors"
	"io"
	"math"
	"net"
	"os"
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
// packet of an old flow from the first packet of a new one. Only packets read start flows: a write
// keeps the flow of its remote address going, but a reply to a remote address without one, e.g. written
// by the fallback connection, starts a new flow, queued for Accept like any other.
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
	// flow as long as the inlet is open. As the next packet of an expired released flow goes to Accept
	// again, the timeout should be no shorter than that of the application serving the fallback connection.
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
	conn *inletConn // The inlet connection of the flow of a packet read
	dst  net.Addr   // The remote address to write a packet to
	src  net.Addr   // The local address to write a packet from, if any
	ref  *[]byte    // Reference to the pooled buffer of buf, if any
	buf  []byte
	idx  int // The interface index to write a packet through, if any
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

// InletConn is an inlet connection: a FromToPacketConn serving the packets of a single flow,
// which it reads from and writes to its remote address.
type InletConn interface {
	FromToPacketConn
	ConnectedPacketConn // With the remote address of the flow

	// Release hands the flow over to the fallback connection, e.g. as no handler recognizes it,
	// and closes the inlet connection.
	Release() error
}

// InletListener is a listener of an inlet, accepting inlet connections, with its fallback connection
// and statistics.
type InletListener interface {
	// Accept returns the inlet connection of the next new flow, Addr returns the local address
	// of the underlying PacketConn, and Close closes it and stops the inlet.
	PacketListener[InletConn]

	// Unwrap stops the inlet like Close does, but leaves the underlying PacketConn open, and returns it
	// as given to Inlet, with no deadlines.
	PacketConnWrapper

	// Fallback returns the fallback connection of the inlet, or nil if it has none.
	Fallback() FromToPacketConn

	// Stats returns the numbers of packets dropped so far, by reason.
	Stats() InletStats
}

// inlet implements InletListener for any underlying PacketConn.
type inlet struct {
	// Set by Inlet, and read-only afterwards, as is cfg
	orig        PacketConn        // Underlying PacketConn, as given to Inlet
	pc          FromToPacketConn  // The same, wrapped as a FromToPacketConn if it isn't one
	udp         FromToUDPConn     // The same as pc if it's a FromToUDPConn, which reports destination addresses
	fallback    *inletFallback    // Fallback connection, if any
	acceptQueue chan *inletConn   // Inlet connections of new flows, for Accept
	writeQueue  chan *inletPacket // Packets of every connection, for the write loop
	done        chan struct{}     // Closed once the inlet stops

	// Owned by the read loop
	flows map[flowKey]*inletConn // Flow table

	// Shared under locks
	closed struct { // Connections closed or expired, for the read loop to delete their flows
		conns []*inletConn
		sync.Mutex
	}
	swept struct { // Every connection in the flow table, for the sweeper to check
		m map[*inletConn]struct{}
		sync.Mutex
	}
	remotes struct { // The latest flow of each remote address, for the writes of the fallback connection
		m map[flowKey]*inletConn
		sync.RWMutex
	}

	// Free of pointers, so placed after the fields holding them, for the garbage collector to scan less
	cfg   inletConfig
	clock atomic.Int64 // Time since the sweeper started, kept by it, coarse but cheap to read
	drops struct {
		truncated, noDestination, tableFull, acceptQueueFull, connQueueFull, connClosed atomic.Uint64 // Reading
		noFallback, fallbackQueueFull, fallbackClosed                                   atomic.Uint64 // Releasing
		writeTimedOut, writeFailed, writeUnsent                                         atomic.Uint64 // Writing
	}

	// Stopping
	wg   sync.WaitGroup
	once sync.Once
}

// Inlet returns an InletListener for the given PacketConn, configured by config, which may be nil for the defaults,
// and starts its internal loops.
func Inlet(pc PacketConn, config *InletConfig) InletListener {
	cfg := config.resolve()

	i := &inlet{
		cfg:         cfg,
		orig:        pc,
		pc:          PacketToFromToPacketConn(pc, cfg.packetInfo),
		acceptQueue: make(chan *inletConn, cfg.acceptQueueSize),
		writeQueue:  make(chan *inletPacket, cfg.writeQueueSize),
		done:        make(chan struct{}),
		flows:       make(map[flowKey]*inletConn),
	}
	i.udp, _ = i.pc.(FromToUDPConn)
	i.remotes.m = make(map[flowKey]*inletConn)
	i.swept.m = make(map[*inletConn]struct{})
	if cfg.fallbackConn {
		i.fallback = i.newFallback()
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
func (i *inlet) Accept() (InletConn, error) {
	if isClosedChan(i.done) {
		return nil, errInletClosed
	}

	select {
	case conn := <-i.acceptQueue:
		return conn, nil
	case <-i.done:
		return nil, errInletClosed
	}
}

// Addr returns the local network address, if known.
func (i *inlet) Addr() net.Addr { return i.pc.LocalAddr() }

// Fallback returns the fallback connection of the inlet, or nil if it has none.
func (i *inlet) Fallback() FromToPacketConn {
	if i.fallback == nil {
		return nil // Not a nil *inletFallback, which wouldn't compare equal to nil
	}
	return i.fallback
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

// Close closes the underlying PacketConn and stops the internal goroutines. Accept returns an error,
// while the inlet connections and the fallback connection return the packets queued, then net.ErrClosed.
// It returns the error of closing the underlying PacketConn, or net.ErrClosed if the inlet has stopped.
func (i *inlet) Close() error {
	return i.stop(i.pc.Close)
}

// Unwrap stops the inlet and returns the underlying PacketConn, as given to Inlet.
func (i *inlet) Unwrap() PacketConn {
	// Interrupt the read and write in progress with deadlines rather than by closing, so that the underlying
	// PacketConn stays open, unless it has been closed otherwise. Ignore errors if any.
	_ = i.stop(func() error { return i.pc.SetDeadline(time.Unix(1, 0)) })

	// Clear the deadlines that have interrupted the loops, which have returned by now
	_ = i.orig.SetDeadline(time.Time{})
	return i.orig
}

// stop stops the inlet once: it closes the done channel, interrupts the loops with interrupt, waits for them
// to return, and drops the writes left. It returns the error of interrupt, or net.ErrClosed if the inlet
// has been stopped already.
func (i *inlet) stop(interrupt func() error) error {
	err := errInletClosed
	i.once.Do(func() {
		// Close the done channel in order to:
		// - unblock Accept and make it return an error;
		// - make reads of the inlet connections and of the fallback connection return
		//   the packets queued, then net.ErrClosed, and make their writes fail;
		// - let the readLoop goroutine return before it reads again;
		// - let the writeLoop goroutine return before it writes again.
		close(i.done)

		// Interrupt the read and write in progress, if any, so that the loops return.
		err = interrupt()

		// Wait until the internal goroutines return.
		i.wg.Wait()

		// Drop the packets queued once the write loop has returned, e.g. as the underlying connection has closed.
		i.drainWrites()
	})
	return err
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

// Shared variables
var (
	errInletClosed           = net.ErrClosed // Reported by using a closed connection, or one of a stopped inlet, the same way as by UDP after local closure
	errInletDeadlineExceeded = os.ErrDeadlineExceeded
	errInletMissingAddress   = errors.New("hybrid: missing address") // Reported by writing to the fallback connection with no remote address
	errInletShortBuffer      = io.ErrShortBuffer                     // Reported by reading a packet longer than the read buffer
)

// Interface guards
var (
	_ InletListener = (*inlet)(nil)
)
