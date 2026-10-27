// Copyright 2025-2026 VNXME
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
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Splitter design.
//
// A splitter serves a packet connection, typically a UDP socket, to many handlers at once, the way
// a TCP listener serves a socket to many connections. Its read loop reads every packet, tells
// the flows apart, and passes each flow either to a split connection, which a handler serves
// like an accepted TCP connection, or to the fallback connection, which serves all the other
// flows together, e.g. as the packet connection of the QUIC server of Caddy, which tells QUIC
// connections apart itself. Its write loop writes the packets of all those connections.
//
// Flows. A flow of a connection that reports the destination address of each packet, e.g. of a UDP
// connection with packet info or of a trunk, is identified by its source and destination addresses.
// A flow of any other connection, e.g. of a UDP connection without packet info, is identified by its
// source address alone. An address is held as a netip.AddrPort value for a *net.UDPAddr, or as the
// string of any other address, in one comparable key type, so a UDP flow is looked up without allocating.
// A UDP connection is read with netip.AddrPort values too, so that reading it allocates nothing, and its
// addresses are made net.Addr values only once a flow starts. IPv4 addresses read from a dual-stack socket
// are reported unmapped, with packet info or without, as flows are keyed by them. How to read from and write to the connection
// is the mode Split picks once for the interfaces the connection implements: a FromToUDPConn, any other
// FromToPacketConn, a UDPConn, with packet info enabled on it if configured, or any other PacketConn.
// Which addresses identify a flow is decided once per splitter, not per packet, so that a flow stays
// whole: a splitter that uses destination addresses drops a packet without one, and a connection reporting
// them is split by source address alone only if wrapped so as to hide them, e.g. by FakeAddressPacketConn.
// The interface index isn't part of a flow, as it may change, e.g. with asymmetric routing: the
// latest one is kept and used for replies. Trunks carry the addresses their writers give, so a test
// can have many flows over a trunk by writing from distinct source addresses or to distinct destination
// addresses: a trunk end writing without a source address gives its own local address, and an end reading
// a packet without a destination address gives its own, which makes a single flow.
//
// Lifecycle. The first packet of an unknown flow creates a split connection, queues it for Accept,
// and queues the packet on it. A handler accepting the connection either serves it or releases it,
// e.g. as no matcher recognizes the flow: then the packets it has read and hands back, the packets queued
// on the connection and all the later packets of the flow go to the fallback connection if there is one,
// or are dropped if there isn't.
// Once the fallback connection is closed, a released flow ends instead, so that its next packet is offered
// to Accept again, e.g. as the server behind the fallback connection has stopped.
// A flow ends when its connection is closed, by its handler or by its idle timeout, and its next
// packet starts a new flow straight away: UDP has nothing like TCP's TIME_WAIT and RST to tell a late
// packet of an old flow from the first packet of a new one. Only packets read start flows: a write
// keeps the flow of its remote address going, but doesn't start one, so the response to a packet written
// to a remote address without a flow, e.g. by the fallback connection, starts a new flow, queued for Accept
// like any other.
//
// Deadlines. Split connections and the fallback connection have read and write deadlines of their own,
// like any net.Conn or net.PacketConn. Each flow has an idle timeout, which every packet read or written
// refreshes: an atomic timestamp per flow, checked by a periodic sweeper, keeps that cheap. Only the read
// loop sets the read deadline of the underlying connection, to be interrupted when the splitter closes, and
// only the write loop sets its write deadline, to apply the write timeout of the splitter. As the deadlines
// of a socket are shared by everything using it, other readers of the socket, e.g. another splitter during
// a reload, may set its read deadline too: the read loop backs off from a timeout it hasn't caused, for them
// to take their own, and clears the read deadline itself only after a few such timeouts in a row.
//
// Reading. Only the read loop changes the flow table, so it takes no lock on the way of a packet:
// closed and expired flows reach it over a channel. It reads into a buffer as large as the largest
// datagram, and copies each packet into a pooled buffer of its size before queueing it.
//
// Writing. Split connections and the fallback connection hand their packets to the write loop, which
// writes them to the underlying connection one by one, with the addresses of their flows. A write copies
// its packet into a pooled buffer of its size and returns once the packet is queued, the way a UDP write
// returns once the system has copied it: if the write queue is full, it waits for room until the write
// deadline of its connection. So a write can't report an error of the underlying connection: the write
// loop counts it, and closes the splitter if the underlying connection fails for good. The write timeout
// of the splitter limits how long the write loop waits on the underlying connection for one packet, so
// that a stuck write can't hold every flow up. The fallback connection is a plain net.PacketConn, whose
// WriteTo knows only the remote address. So it looks the remote address up in an index of the latest
// flow of each remote address, safe for concurrent use, to write from the local address and through
// the interface that flow came to, and lets the system choose them for a remote address it hasn't seen.
// It deliberately gives no access to the socket, e.g. no SyscallConn, so that a QUIC server can't change
// socket options or read from the socket past the splitter.
//
// Dropping. Like a UDP socket whose buffer is full, the splitter never blocks its read loop on a slow reader:
// it drops a packet if the queue of its connection is full, if the accept queue is full (without creating
// a flow, so that a retry by the client is a new flow), if the flow table is full, or if the packet has no
// destination address while the splitter uses destination addresses. The write loop drops a packet whose write
// times out or fails. Every drop is counted by its reason, and the counters can be read with Stats.

// SplitConfig configures a splitter. The zero value of a field gives its default, as does a nil *SplitConfig.
type SplitConfig struct {
	// ErrorFunc, if set, is called with each error of the underlying PacketConn that the splitter can't return
	// otherwise: the one that stops the splitter, e.g. as the PacketConn fails, or is closed other than by Close,
	// which matches net.ErrClosed, as Accept returns it then, and that of each write counted as WriteFailed.
	// It's called on the goroutines of the splitter, so it shouldn't block.
	ErrorFunc func(error)

	// AcceptQueueSize is the maximum number of split connections waiting for Accept,
	// [DefaultSplitAcceptQueueSize] by default. The first packet of a new flow is dropped
	// if the accept queue is full.
	AcceptQueueSize int

	// ReadQueueSize is the maximum number of packets waiting to be read from each split connection,
	// [DefaultSplitReadQueueSize] by default. A packet is dropped if the queue of its split connection is full.
	ReadQueueSize int

	// FallbackQueueSize is the maximum number of packets waiting to be read from the fallback connection,
	// [DefaultSplitFallbackQueueSize] by default. A packet is dropped if the queue of the fallback connection
	// is full.
	FallbackQueueSize int

	// IdleTimeout is how long a flow may go without a packet read or written before it ends,
	// [DefaultSplitIdleTimeout] by default: its split connection is closed, and its next packet starts
	// a new flow. A negative timeout keeps every flow until its split connection is closed, and a released
	// flow as long as the splitter is open. As the next packet of an expired released flow goes to Accept
	// again, the timeout should be no shorter than that of the application serving the fallback connection.
	IdleTimeout time.Duration

	// MaxFlows is the maximum number of flows, [DefaultSplitMaxFlows] by default.
	// The first packet of a new flow is dropped if there are as many flows already.
	MaxFlows int

	// ReadBufferSize is the maximum size of a packet read from the underlying PacketConn,
	// [DefaultSplitReadBufferSize] by default, and at least 512. A larger packet is dropped.
	ReadBufferSize int

	// WriteQueueSize is the maximum number of packets waiting to be written to the underlying PacketConn,
	// [DefaultSplitWriteQueueSize] by default. A write waits for room until the write deadline of its
	// connection if the queue is full.
	WriteQueueSize int

	// WriteTimeout is how long the write loop waits for the underlying PacketConn to write a packet before
	// dropping it, [DefaultSplitWriteTimeout] by default. A negative timeout makes it wait as long as it takes.
	WriteTimeout time.Duration

	// FallbackConn gives the splitter a fallback connection, which serves the flows released by their handlers.
	// Without one, the packets of a released flow are dropped. Once it's closed, a released flow ends.
	FallbackConn bool

	// PacketInfo makes the splitter enable packet info with [PacketInfoUDPConn] if the underlying PacketConn
	// satisfies UDPConn, so that flows are told apart by their destination addresses as well, and replies
	// are sent from the address each flow came to, e.g. for a UDPConn listening on an unspecified address.
	// Without it, flows of a UDPConn are told apart by their source addresses alone.
	PacketInfo bool
}

// resolve returns the configuration of a splitter, with the defaults of the fields not set.
func (c *SplitConfig) resolve() splitConfig {
	if c == nil {
		c = &SplitConfig{}
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

	cfg := splitConfig{
		idleTimeout:       timeout(c.IdleTimeout, DefaultSplitIdleTimeout),
		writeTimeout:      timeout(c.WriteTimeout, DefaultSplitWriteTimeout),
		acceptQueueSize:   orDefault(c.AcceptQueueSize, DefaultSplitAcceptQueueSize),
		fallbackQueueSize: orDefault(c.FallbackQueueSize, DefaultSplitFallbackQueueSize),
		maxFlows:          orDefault(c.MaxFlows, DefaultSplitMaxFlows),
		readBufferSize:    orDefault(c.ReadBufferSize, DefaultSplitReadBufferSize),
		readQueueSize:     orDefault(c.ReadQueueSize, DefaultSplitReadQueueSize),
		writeQueueSize:    orDefault(c.WriteQueueSize, DefaultSplitWriteQueueSize),
		fallbackConn:      c.FallbackConn,
		packetInfo:        c.PacketInfo,
	}
	cfg.readBufferSize = max(cfg.readBufferSize, minSplitReadBufferSize)
	return cfg
}

// SplitConn is a split connection: a FromToPacketConn serving the packets of a single flow,
// which it reads from and writes to its remote address.
type SplitConn interface {
	Conn // With the remote address of the flow
	FromToPacketConn

	// Release hands the flow over to the fallback connection, e.g. as no handler recognizes it,
	// and closes the split connection. The packets given, e.g. those read while matching, go first,
	// as the packets of the flow, so that the fallback connection misses none of them.
	Release(pkts ...[]byte) error
}

// SplitListener is a listener of a splitter, accepting split connections, with its fallback connection
// and statistics.
type SplitListener interface {
	// Accept returns the split connection of the next new flow as a StreamConn, as AcceptSplit does,
	// Addr returns the local address of the underlying PacketConn, and Close closes it and stops the splitter.
	PacketListener

	// Stop stops the splitter like Close does, but leaves the underlying PacketConn open, with no deadlines,
	// for the caller to take it over, e.g. with Unwrap.
	Stopper

	// Unwrap returns the underlying PacketConn as given to Split, which the splitter reads from and writes to
	// until it's stopped by Close or Stop.
	Wrapper[PacketConn]

	// AcceptSplit returns the split connection of the next new flow.
	AcceptSplit() (SplitConn, error)

	// Fallback returns the fallback connection of the splitter, or nil if it has none. Like a split connection,
	// it reads a packet longer than the buffer in part, with io.ErrShortBuffer as a temporary net.Error.
	Fallback() FromToPacketConn

	// Stats returns the numbers of packets dropped so far, by reason.
	Stats() SplitStats
}

// SplitStats counts the packets a splitter has dropped, by reason.
type SplitStats struct {
	// Reading, in the order a packet meets the checks
	Truncated       uint64 // The packet was larger than ReadBufferSize
	NoSource        uint64 // The packet had no source address
	NoDestination   uint64 // The packet had no destination address, while the splitter uses them
	TableFull       uint64 // The flow table was full when the packet started a new flow
	AcceptQueueFull uint64 // The accept queue was full when the packet started a new flow
	ConnQueueFull   uint64 // The queue of the split connection of the packet was full
	ConnClosed      uint64 // The split connection of the packet was closed before reading it

	// Releasing
	NoFallback        uint64 // The packet of a released flow had no open fallback connection to go to
	FallbackQueueFull uint64 // The queue of the fallback connection was full
	FallbackClosed    uint64 // The fallback connection was closed before reading the packet

	// Writing
	WriteTimedOut uint64 // The write loop timed out writing the packet
	WriteFailed   uint64 // The write loop failed to write the packet, e.g. to an unreachable address
	WriteUnsent   uint64 // The splitter closed, or its underlying connection did, before writing the packet
}

// splitter implements SplitListener for any underlying PacketConn.
type splitter struct {
	// Set by Split, and read-only afterwards, as is cfg
	orig        PacketConn        // Underlying PacketConn, as given to Split
	mode        splitMode         // How to read from and write to orig, or to a FromToUDPConn wrapping it
	fallback    *splitFallback    // Fallback connection, if any
	acceptQueue chan *splitConn   // Split connections of new flows, for Accept
	writeQueue  chan *splitPacket // Packets of every connection, for the write loop
	done        chan struct{}     // Closed once the splitter stops
	errorFunc   func(error)       // SplitConfig.ErrorFunc, if any

	// Set by stop before done is closed, and read-only afterwards
	err error // Returned by Accept once the splitter has stopped

	// Owned by the read loop
	flows map[flowKey]*splitConn // Flow table

	// Shared under locks
	closed struct { // Connections closed or expired, for the read loop to delete their flows
		conns []*splitConn
		sync.Mutex
	}
	swept struct { // Every connection in the flow table, for the sweeper to check
		m map[*splitConn]struct{}
		sync.Mutex
	}
	remotes struct { // The latest flow of each remote address, for the writes of the fallback connection
		m map[addrKey]*splitConn
		sync.RWMutex
	}

	// Free of pointers, so placed after the fields holding them, for the garbage collector to scan less
	cfg   splitConfig
	clock atomic.Int64 // Time since the sweeper started, kept by it, coarse but cheap to read
	drops struct {
		truncated, noSource, noDestination, tableFull, acceptQueueFull, connQueueFull, connClosed atomic.Uint64 // Reading
		noFallback, fallbackQueueFull, fallbackClosed                                             atomic.Uint64 // Releasing
		writeTimedOut, writeFailed, writeUnsent                                                   atomic.Uint64 // Writing
	}

	// Stopping
	wg   sync.WaitGroup
	once sync.Once
	mu   sync.Mutex // Held to interrupt the loops, or to clear a read deadline the splitter hasn't set

	// Set by Split, and placed last, so as to take the padding of once
	byDst bool // Set if mode reports destination addresses, so that they identify flows too
}

// Split returns a SplitListener for the given PacketConn, configured by config, which may be nil for the defaults,
// and starts its internal loops. It takes pc over, clearing its write deadline, until Close or Stop.
//
// The splitter may share a socket with other readers, e.g. in another splitter over another PacketConn of it,
// which set its read deadline as they need: it backs off from a read timeout it hasn't caused, for them to take
// their own, and clears the read deadline after 20 of them in a row, e.g. one left passed
// before Split. Stop sets a deadline passed in order to interrupt its read, so it interrupts theirs as well.
//
// The splitter leaves the socket options of pc as they are. Notably, macOS and the BSDs refuse to send a datagram
// larger than the send buffer of a socket, which is 9216 bytes by default, so a larger packet written to a split
// connection is counted as WriteFailed, unless the send buffer has been raised, e.g. with
// [net.UDPConn.SetWriteBuffer].
func Split(pc PacketConn, config *SplitConfig) SplitListener {
	cfg := config.resolve()

	s := &splitter{
		cfg:         cfg,
		orig:        pc,
		acceptQueue: make(chan *splitConn, cfg.acceptQueueSize),
		writeQueue:  make(chan *splitPacket, cfg.writeQueueSize),
		done:        make(chan struct{}),
		flows:       make(map[flowKey]*splitConn),
	}
	if config != nil {
		s.errorFunc = config.ErrorFunc
	}
	s.mode, s.byDst = newSplitMode(pc, cfg.packetInfo)
	s.remotes.m = make(map[addrKey]*splitConn)
	s.swept.m = make(map[*splitConn]struct{})
	if cfg.fallbackConn {
		s.fallback = s.newFallback()
	}

	// Clear the write deadline the underlying PacketConn may have, since a deadline passed would fail every write
	// without a write timeout. Its read deadline may be another reader's, e.g. interrupting its own read, so it's
	// left to the read loop, which clears it only if it seems to be no one's
	_ = s.orig.SetWriteDeadline(time.Time{})

	s.wg.Add(2)
	go s.readLoop()
	go s.writeLoop()
	if cfg.idleTimeout > 0 {
		s.wg.Add(1)
		go s.sweepLoop()
	}
	return s
}

// Accept waits for and returns the split connection of the next new flow as a StreamConn, as AcceptSplit does.
func (s *splitter) Accept() (StreamConn, error) { return s.AcceptSplit() }

// AcceptSplit waits for and returns the split connection of the next new flow, or fails once the splitter stops,
// with net.ErrClosed, wrapping the error of the underlying PacketConn that has stopped it, if any.
func (s *splitter) AcceptSplit() (SplitConn, error) {
	if isClosedChan(s.done) {
		return nil, s.err
	}

	select {
	case conn := <-s.acceptQueue:
		return conn, nil
	case <-s.done:
		return nil, s.err
	}
}

// Addr returns the local address of the underlying PacketConn.
func (s *splitter) Addr() net.Addr { return s.orig.LocalAddr() }

// Close closes the underlying PacketConn and stops the internal goroutines. Accept returns an error, and the split
// connections it hasn't returned are closed, while the others and the fallback connection return the packets
// queued, then net.ErrClosed. It returns the error of closing the underlying PacketConn, or net.ErrClosed
// if the splitter has stopped.
func (s *splitter) Close() error {
	return s.stop(s.orig.Close, nil)
}

// Fallback returns the fallback connection of the splitter, or nil if it has none.
func (s *splitter) Fallback() FromToPacketConn {
	if s.fallback == nil {
		return nil // Not a nil *splitFallback, which wouldn't compare equal to nil
	}
	return s.fallback
}

// Stats returns the numbers of packets dropped so far, by reason.
func (s *splitter) Stats() SplitStats {
	return SplitStats{
		Truncated:         s.drops.truncated.Load(),
		NoSource:          s.drops.noSource.Load(),
		NoDestination:     s.drops.noDestination.Load(),
		TableFull:         s.drops.tableFull.Load(),
		AcceptQueueFull:   s.drops.acceptQueueFull.Load(),
		ConnQueueFull:     s.drops.connQueueFull.Load(),
		ConnClosed:        s.drops.connClosed.Load(),
		NoFallback:        s.drops.noFallback.Load(),
		FallbackQueueFull: s.drops.fallbackQueueFull.Load(),
		FallbackClosed:    s.drops.fallbackClosed.Load(),
		WriteTimedOut:     s.drops.writeTimedOut.Load(),
		WriteFailed:       s.drops.writeFailed.Load(),
		WriteUnsent:       s.drops.writeUnsent.Load(),
	}
}

// Stop stops the internal goroutines like Close does, but leaves the underlying PacketConn open, with no deadlines.
// It returns the error of interrupting the underlying PacketConn, or net.ErrClosed if the splitter has stopped.
func (s *splitter) Stop() error {
	// Interrupt the read and write in progress with deadlines rather than by closing, so that the underlying
	// PacketConn stays open, unless it has been closed otherwise
	var stopping bool
	err := s.stop(func() error {
		stopping = true
		return s.orig.SetDeadline(time.Unix(1, 0))
	}, nil)

	// Clear the deadlines that have interrupted the loops, which have returned by now, unless the splitter
	// had stopped before, so as to keep any deadlines the caller has set since
	if stopping {
		_ = s.orig.SetDeadline(time.Time{})
	}
	return err
}

// Unwrap returns the underlying PacketConn, as given to Split.
func (s *splitter) Unwrap() PacketConn { return s.orig }

// fail stops the splitter as the underlying PacketConn has failed with err, or has been closed other than by Close,
// unless the splitter is stopping already, so that Accept returns err, which ErrorFunc gets as well. As stopping
// waits until the loops return, a loop calls it on a goroutine of its own.
func (s *splitter) fail(err error) {
	go func() {
		_ = s.stop(s.orig.Close, err)
	}()
}

// reportError passes an error of the underlying PacketConn that the splitter can't return to ErrorFunc, if any.
func (s *splitter) reportError(err error) {
	if s.errorFunc != nil {
		s.errorFunc(err)
	}
}

// stop stops the splitter once: it closes the done channel, interrupts the loops with interrupt, waits for them
// to return, and drops the writes left and the packets of the split connections the read loop won't serve.
// If cause is set, i.e. the underlying PacketConn has failed, Accept returns it, wrapped, and so does ErrorFunc.
// It returns the error of interrupt, or net.ErrClosed if the splitter has been stopped already.
func (s *splitter) stop(interrupt func() error, cause error) error {
	err := errSplitClosed
	s.once.Do(func() {
		s.err = errSplitClosed
		if cause != nil {
			s.err = splitStoppedError{cause}
		}

		// Close the done channel in order to:
		// - unblock Accept and make it return an error;
		// - make reads of the split connections and of the fallback connection return
		//   the packets queued, then net.ErrClosed, and make their writes fail;
		// - let the readLoop goroutine return before it reads again;
		// - let the writeLoop goroutine return before it writes again.
		close(s.done)

		// Interrupt the read and write in progress, if any, so that the loops return. The read loop never clears
		// a read deadline interrupt sets, e.g. one passed, which a PacketConn may set to close as well, since it
		// checks the done channel before, holding the same lock.
		s.mu.Lock()
		err = interrupt()
		s.mu.Unlock()

		// Wait until the internal goroutines return.
		s.wg.Wait()

		// Drop the packets queued once the write loop has returned, e.g. as the underlying connection has closed,
		// and those of the split connections left to the read loop, which won't serve them any more.
		s.drainWrites()
		s.drainReads()

		if cause != nil {
			s.reportError(s.err)
		}
	})
	return err
}

// splitConfig is a SplitConfig with its defaults filled in, and with a zero timeout meaning none.
type splitConfig struct {
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

// splitPacket is a packet queued for reading from a split connection or from the fallback connection,
// or for writing by the write loop.
type splitPacket struct {
	conn *splitConn // The split connection of the flow of a packet read
	dst  net.Addr   // The remote address to write a packet to
	src  net.Addr   // The local address to write a packet from, if any
	ref  *[]byte    // Reference to the pooled buffer of buf, if any
	buf  []byte
	idx  int // The interface index to write a packet through, if any
}

// splitPackets holds reusable packets, so that queuing a packet allocates nothing.
var splitPackets sync.Pool

// newSplitPacket returns a packet with a copy of b, in a pooled buffer, reused if possible.
func newSplitPacket(b []byte) *splitPacket {
	pkt, ok := splitPackets.Get().(*splitPacket)
	if !ok {
		pkt = new(splitPacket)
	}
	if len(b) > 0 {
		pkt.buf, pkt.ref = getBuf(len(b))
		copy(pkt.buf, b)
	}
	return pkt
}

// free makes a packet and its buffer reusable. Neither must be used afterwards.
func (pkt *splitPacket) free() {
	putBuf(pkt.ref)
	*pkt = splitPacket{} // Holds no connection or address back from collection
	splitPackets.Put(pkt)
}

// Defaults of the fields of SplitConfig, which apply when a field isn't set
const (
	DefaultSplitAcceptQueueSize   = 8
	DefaultSplitFallbackQueueSize = 1024 // As the fallback connection may serve many flows
	DefaultSplitIdleTimeout       = 30 * time.Second
	DefaultSplitMaxFlows          = 8192
	DefaultSplitReadBufferSize    = math.MaxUint16 - 8 // The largest UDP payload, over IPv6, as it is 20 bytes less over IPv4
	DefaultSplitReadQueueSize     = 32
	DefaultSplitWriteQueueSize    = 64
	DefaultSplitWriteTimeout      = 500 * time.Millisecond

	minSplitReadBufferSize = 512 // A read buffer of less than 512 bytes seems impractical

	// The read loop backs off from a read timeout it hasn't caused, for another reader of a shared socket
	// to take its own, and clears the read deadline after that many of them in a row
	splitForeignTimeoutBackoff = time.Millisecond
	splitForeignTimeouts       = 20
)

// Shared variables
var (
	errSplitClosed           = net.ErrClosed // Reported by using a closed connection, or one of a stopped splitter, the same way as by UDP after local closure
	errSplitDeadlineExceeded = os.ErrDeadlineExceeded
	errSplitMissingAddress   = errors.New("hybrid: missing address") // Reported by writing to the fallback connection with no remote address
	errSplitShortBuffer      = shortBufferError{}                    // Reported by reading a packet longer than the read buffer
	errSplitWriteToConnected = net.ErrWriteToConnected               // Reported by writing to an address other than that of the flow
)

// splitStoppedError reports that the underlying PacketConn has stopped the splitter, as it has failed, or has been
// closed other than by Close. It's net.ErrClosed, so that a server stops accepting as usual, and its cause as well.
type splitStoppedError struct{ cause error }

func (e splitStoppedError) Error() string   { return "hybrid: splitter stopped: " + e.cause.Error() }
func (e splitStoppedError) Unwrap() []error { return []error{errSplitClosed, e.cause} }

// Interface guards
var (
	_ SplitListener = (*splitter)(nil)
)
