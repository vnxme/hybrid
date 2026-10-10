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
	"net/netip"
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
// like an accepted TCP connection, or to the default connection, which serves all the other
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
// packets of the flow go to the default connection if there is one, or are dropped if there isn't.
// Optionally, every new flow goes to the default connection straight away, for an inlet nobody
// accepts connections from. A flow ends when its connection is closed, by its handler or by its idle
// timeout, and its next packet starts a new flow straight away: UDP has nothing like TCP's TIME_WAIT
// and RST to tell a late packet of an old flow from the first packet of a new one.
//
// Deadlines. Inlet connections and the default connection have read and write deadlines of their own,
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
// Writing. Inlet connections and the default connection hand their packets to the write loop, which
// writes them to the underlying connection one by one, with the addresses of their flows. A write copies
// its packet into a pooled buffer of its size and returns once the packet is queued, the way a UDP write
// returns once the system has copied it: if the write queue is full, it waits for room until the write
// deadline of its connection. So a write can't report an error of the underlying connection: the write
// loop counts it, and closes the inlet if the underlying connection fails for good. The write timeout
// of the inlet limits how long the write loop waits on the underlying connection for one packet, so
// that a stuck write can't hold every flow up. The default connection is a plain net.PacketConn, whose
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

type inletConfig struct {
	idleTimeout  time.Duration
	writeTimeout time.Duration

	acceptQueueSize  int
	defaultQueueSize int
	readBufferSize   int
	readQueueSize    int
	writeQueueSize   int

	activeConnLimit int

	defaultConn      bool
	defaultOnly      bool
	tryPacketInfoUDP bool
}

var defaultInletConfig = inletConfig{
	idleTimeout:      poIdleTimeoutDef,
	writeTimeout:     piWriteTimeoutDef,
	acceptQueueSize:  piAcceptQueueSizeDef,
	defaultQueueSize: piDefaultQueueSizeDef,
	readBufferSize:   piReadBufferSizeDef,
	readQueueSize:    poReadQueueSizeDef,
	writeQueueSize:   piWriteQueueSizeDef,
	activeConnLimit:  piActiveConnLimitDef,
	tryPacketInfoUDP: piTryPacketInfoUDPDef,
}

type InletOption interface {
	apply(*inletConfig)
}

type inletOptionFunc func(*inletConfig)

func (f inletOptionFunc) apply(cfg *inletConfig) { f(cfg) }

var noOpInletOption = inletOptionFunc(func(p *inletConfig) {})

// WithAcceptQueueSize sets the maximum number of inlet connections waiting to be accepted.
// The first packet of a new flow is dropped if the accept queue is full.
func WithAcceptQueueSize(value int) InletOption {
	if value < piAcceptQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.acceptQueueSize = value })
}

// WithDefaultConn sets whether the inlet has a default connection, which serves the flows
// released by their handlers. Without one, the packets of a released flow are dropped.
func WithDefaultConn(value bool) InletOption {
	return inletOptionFunc(func(cfg *inletConfig) { cfg.defaultConn = value })
}

// WithDefaultOnly sets whether every new flow goes to the default connection straight away rather than
// to Accept, e.g. for an inlet nobody accepts connections from. It implies WithDefaultConn(true).
func WithDefaultOnly(value bool) InletOption {
	return inletOptionFunc(func(cfg *inletConfig) { cfg.defaultOnly = value })
}

// WithDefaultQueueSize sets the maximum number of packets waiting to be read from the default connection.
// A packet is dropped if the queue of the default connection is full.
func WithDefaultQueueSize(value int) InletOption {
	if value < piDefaultQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.defaultQueueSize = value })
}

// WithReadBufferSize sets the maximum number of bytes read as a single packet
// from the underlying connection.
func WithReadBufferSize(value int) InletOption {
	if value < piReadBufferSizeMin || value > piReadBufferSizeMax {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.readBufferSize = value })
}

// WithReadQueueSize sets the maximum number of packets waiting to be read from each inlet connection.
// A packet is dropped if the queue of its inlet connection is full.
func WithReadQueueSize(value int) InletOption {
	if value < poReadQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.readQueueSize = value })
}

// WithIdleTimeout sets how long a flow may go without a packet read or written before it ends: its inlet
// connection is closed, and its next packet starts a new flow. A zero timeout keeps every flow until its
// inlet connection is closed, and a released flow as long as the inlet is open.
func WithIdleTimeout(value time.Duration) InletOption {
	if value < poIdleTimeoutMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.idleTimeout = value })
}

// WithTryPacketInfoUDP sets whether to try enabling packet info with [PacketInfoUDPConn]
// if the underlying PacketConn satisfies UDPConn.
func WithTryPacketInfoUDP(value bool) InletOption {
	if value == piTryPacketInfoUDPDef {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.tryPacketInfoUDP = value })
}

// WithWriteQueueSize sets the maximum number of packets waiting to be written to the underlying PacketConn.
// A write waits for room until the write deadline of its connection if the write queue is full.
func WithWriteQueueSize(value int) InletOption {
	if value < piWriteQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.writeQueueSize = value })
}

// WithWriteTimeout sets how long the write loop waits for the underlying PacketConn to write a packet,
// before dropping the packet. A zero timeout makes it wait as long as it takes.
func WithWriteTimeout(value time.Duration) InletOption {
	if value < 0 {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.writeTimeout = value })
}

type inletPacketSmall struct {
	c   *inletConn // The flow read from or written for, if any
	to  net.Addr   // The address written to, if there is no flow
	ref *[]byte    // Reference for putBuf, if b comes from getBuf
	b   []byte     // Reusable buffer
	n   int        // Bytes consumed
}

// flowKey identifies a flow: by its source and destination addresses for a UDP connection
// that reports destination addresses, or by its source address alone otherwise, as src
// for a *net.UDPAddr, or as the string of any other address in addr.
type flowKey struct {
	src, dst netip.AddrPort
	addr     string
}

// remote returns the key of the remote address of a flow, i.e. of its source address.
func (k flowKey) remote() flowKey { return flowKey{src: k.src, addr: k.addr} }

// remoteKey returns the key of a remote address given to a write, the way flowKey.remote does.
func remoteKey(a net.Addr) flowKey {
	if u, ok := a.(*net.UDPAddr); ok {
		return flowKey{src: u.AddrPort()}
	}
	return flowKey{addr: a.String()}
}

// inletPacketInfo describes a packet read from the underlying connection.
type inletPacketInfo struct {
	src, dst net.Addr // Only if read from a connection other than a FromToUDPConn
	key      flowKey
	idx      int
}

// InletStats counts the packets an inlet has dropped, by reason.
type InletStats struct {
	ConnQueueFull    uint64 // The queue of the inlet connection of the packet was full
	AcceptQueueFull  uint64 // The accept queue was full when the packet started a new flow
	TableFull        uint64 // The flow table was full when the packet started a new flow
	NoDestination    uint64 // The packet had no destination address, while the inlet uses them
	DefaultQueueFull uint64 // The queue of the default connection was full
	NoDefault        uint64 // The packet of a released flow had no open default connection to go to
	WriteTimedOut    uint64 // The write loop timed out writing the packet
	WriteFailed      uint64 // The write loop failed to write the packet, e.g. to an unreachable address
}

// InletListener is a PacketListener of an inlet, with its default connection and statistics.
type InletListener interface {
	PacketListener

	// Default returns the default connection of the inlet, or nil if it has none.
	Default() PacketConn

	// Stats returns the numbers of packets dropped so far, by reason.
	Stats() InletStats
}

// inlet implements PacketListener for any underlying PacketConn.
type inlet struct {
	start time.Time // When the inlet started, for its clock

	pc  FromToPacketConn // Underlying PacketConn
	udp FromToUDPConn    // The same as pc if it's a FromToUDPConn, which reports destination addresses
	def *inletDefault    // Default connection, if any

	acChan chan *inletConn        // Accept channel
	wrChan chan *inletPacketSmall // Write channel
	done   chan struct{}          // Done channel

	rt map[flowKey]*inletConn // Routing table

	closed struct { // Connections closed or expired, for the read loop to delete their flows
		s []*inletConn
		sync.Mutex
	}

	flows struct { // Every flow in the routing table, for the sweeper
		m map[*inletConn]struct{}
		sync.Mutex
	}

	remotes struct { // The latest flow of each remote address, for the writes of the default connection
		m map[flowKey]*inletConn
		sync.RWMutex
	}

	cfg inletConfig

	drops struct {
		connQueueFull, acceptQueueFull, tableFull, noDestination atomic.Uint64 // Reading
		defaultQueueFull, noDefault                              atomic.Uint64 // Releasing
		writeTimedOut, writeFailed                               atomic.Uint64 // Writing
	}

	wg    sync.WaitGroup
	clock atomic.Int64 // Time since start, kept by the sweeper, coarse but cheap to read
	once  sync.Once
}

// Inlet returns an InletListener for the given PacketConn and starts its internal loops.
func Inlet(pc PacketConn, opts ...InletOption) InletListener {
	cfg := defaultInletConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if cfg.defaultOnly {
		cfg.defaultConn = true
	}

	i := &inlet{
		pc:     PacketToFromToPacketConn(pc, cfg.tryPacketInfoUDP),
		acChan: make(chan *inletConn, cfg.acceptQueueSize),
		wrChan: make(chan *inletPacketSmall, cfg.writeQueueSize),
		done:   make(chan struct{}),
		rt:     make(map[flowKey]*inletConn),
		start:  time.Now(),
		cfg:    cfg,
	}
	i.udp, _ = i.pc.(FromToUDPConn)
	i.remotes.m = make(map[flowKey]*inletConn)
	i.flows.m = make(map[*inletConn]struct{})
	if cfg.defaultConn {
		i.def = i.newDefault()
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

// Default returns the default connection of the inlet, or nil if it has none.
func (i *inlet) Default() PacketConn {
	if i.def == nil {
		return nil // Not a nil *inletDefault, which wouldn't compare equal to nil
	}
	return i.def
}

// Stats returns the numbers of packets dropped so far, by reason.
func (i *inlet) Stats() InletStats {
	return InletStats{
		ConnQueueFull:    i.drops.connQueueFull.Load(),
		AcceptQueueFull:  i.drops.acceptQueueFull.Load(),
		TableFull:        i.drops.tableFull.Load(),
		NoDestination:    i.drops.noDestination.Load(),
		DefaultQueueFull: i.drops.defaultQueueFull.Load(),
		NoDefault:        i.drops.noDefault.Load(),
		WriteTimedOut:    i.drops.writeTimedOut.Load(),
		WriteFailed:      i.drops.writeFailed.Load(),
	}
}

// Close closes the underlying PacketConn, the outlets and terminates the internal goroutines.
// Any operations blocked by Accept will be unblocked and return errors.
func (i *inlet) Close() (err error) {
	return i.closeWith(true)
}

// Unwrap returns the underlying PacketConn.
func (i *inlet) Unwrap() PacketConn {
	// Stop all internal loops, but keep the underlying PacketConn open,
	// unless it has been closed otherwise. Ignore errors if any.
	_ = i.closeWith(false)
	return i.pc
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

		// Return nil to report a successful closing.
		err = nil
	})
	return
}

func (i *inlet) logError(err error) {
	// TODO(vnxme): Log an unexpected reading/writing error
}

// newConnection returns a new inlet connection for the flow of the given packet.
func (i *inlet) newConnection(p *inletPacketInfo) *inletConn {
	src, dst := p.src, p.dst
	if i.udp != nil {
		src, dst = addrPortToAddr(p.key.src), addrPortToAddr(p.key.dst)
	}

	ic := &inletConn{
		rdRx: make(chan *inletPacketSmall, i.cfg.readQueueSize),

		localDone:  make(chan struct{}),
		remoteDone: i.done,

		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),

		localAddr:  dst,
		remoteAddr: src,

		parent: i,
		key:    p.key,
	}
	ic.localIfIdx.Store(int64(p.idx))
	ic.touch()
	return ic
}

// deleteConnection reports a closed or expired connection, for the read loop to delete its flow. It needs no
// goroutine, so that it can't race with closing the inlet, which waits for the goroutines of the inlet.
func (i *inlet) deleteConnection(c *inletConn) {
	i.closed.Lock()
	i.closed.s = append(i.closed.s, c)
	i.closed.Unlock()
}

// deleteLoop deletes the flows whose connections have been closed and reported so far.
func (i *inlet) deleteLoop() {
	i.closed.Lock()
	closed := i.closed.s
	i.closed.s = nil
	i.closed.Unlock()

	for _, c := range closed {
		// Ignore a connection that no longer has a flow, e.g. as it's been deleted already,
		// and whose flow may have a new connection.
		if e, ok := i.rt[c.key]; ok && e == c {
			i.deleteFlow(c)
		} else {
			i.untrack(c)
		}
	}
}

// deleteFlow drains the read channel of a closed connection, and deletes its flow from the routing table,
// from the flows of the sweeper and from the index of remote addresses.
func (i *inlet) deleteFlow(c *inletConn) {
	for {
		select {
		case pkt := <-c.rdRx:
			putBuf(pkt.ref)
		default:
			delete(i.rt, c.key)
			i.untrack(c)
			i.remotes.Lock()
			if r := c.key.remote(); i.remotes.m[r] == c {
				delete(i.remotes.m, r)
			}
			i.remotes.Unlock()
			return
		}
	}
}

// untrack removes a connection from the flows of the sweeper.
func (i *inlet) untrack(c *inletConn) {
	i.flows.Lock()
	delete(i.flows.m, c)
	i.flows.Unlock()
}

// sweepLoop keeps the clock of the inlet, and ends the flows idle for the idle timeout, checking them every
// quarter of it, so that a flow ends within one and a quarter idle timeouts of its last packet.
func (i *inlet) sweepLoop() {
	defer i.wg.Done()

	ticker := time.NewTicker(max(i.cfg.idleTimeout/4, time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-i.done:
			return
		case now := <-ticker.C:
			clock := int64(now.Sub(i.start)) // Monotonic, unlike the wall clock
			i.clock.Store(clock)

			i.flows.Lock()
			for c := range i.flows.m {
				if clock-c.last.Load() >= int64(i.cfg.idleTimeout) {
					delete(i.flows.m, c)
					c.expire()
				}
			}
			i.flows.Unlock()
		}
	}
}

// readLoop reads packets from the underlying connection and routes them to their inlet connections,
// until the inlet closes or the underlying connection fails.
func (i *inlet) readLoop() {
	defer i.wg.Done()

	var nrr net.Error
	var p inletPacketInfo
	buf := make([]byte, i.cfg.readBufferSize) // Read into, then copied into a buffer of the packet's size
	for !isClosedChan(i.done) {
		// Read from the underlying FromToPacketConn and check for errors.
		// Note: any error is ignored, if at least one byte has been read.
		n, err := i.read(buf, &p)
		if n == 0 && err != nil {
			// A timeout interrupts reading when the inlet closes, so check it at the top of the loop
			if errors.As(err, &nrr) && nrr.Timeout() {
				continue
			}

			// Log an error, unless the underlying FromToPacketConn is closed.
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
				i.logError(err)
			}

			// If the underlying FromToPacketConn has unexpectedly closed while
			// the inlet's done channel remains open, spawn a closing goroutine.
			// Close() waits until readLoop() returns, so it can't be done in sync.
			if !isClosedChan(i.done) {
				go func() {
					_ = i.Close()
				}()
			}
			return
		}

		// Drain the read channels of the connections closed so far, e.g. while reading,
		// and delete them from the routing table, so that their flows end before routing.
		i.deleteLoop()

		if i.udp != nil && !p.key.dst.IsValid() { // Packet info is missing, e.g. as it has been truncated
			i.drops.noDestination.Add(1)
			continue
		}
		i.route(buf[:n], &p)
	}
}

// read reads a packet into b and fills in p, the key of its flow, its addresses and its interface index.
// It returns the number of bytes read and an error, which matters only if no bytes have been read.
func (i *inlet) read(b []byte, p *inletPacketInfo) (n int, err error) {
	*p = inletPacketInfo{}
	if i.udp != nil {
		n, p.key.src, p.key.dst, p.idx, err = i.udp.ReadFromToUDPAddrPort(b)
		return n, err
	}

	n, p.src, p.dst, p.idx, err = i.pc.ReadFromTo(b)
	if a, ok := p.src.(*net.UDPAddr); ok {
		p.key.src = a.AddrPort()
	} else if p.src != nil {
		p.key.addr = p.src.String()
	}
	return n, err
}

// route passes a packet to the inlet connection of its flow, or to the default connection if the flow
// has been released. The first packet of a flow creates its inlet connection, and queues it for Accept,
// or releases it straight away with WithDefaultOnly. It drops the packet rather than wait if a queue
// is full or the table is.
func (i *inlet) route(data []byte, p *inletPacketInfo) {
	c, ok := i.rt[p.key]
	if !ok || c.ended() {
		if !ok && len(i.rt) >= i.cfg.activeConnLimit {
			// Delete the flows of closed connections whose closing hasn't been reported yet, then check again
			for _, e := range i.rt {
				if e.ended() {
					i.deleteFlow(e)
				}
			}
			if len(i.rt) >= i.cfg.activeConnLimit {
				i.drops.tableFull.Add(1)
				return
			}
		}

		if ok { // An ended flow whose closing hasn't been reported yet
			i.untrack(c)
		}
		c = i.newConnection(p)
		if i.cfg.defaultOnly {
			c.released.Store(true)
			c.closeLocal()
		} else {
			select {
			case i.acChan <- c:
			default: // Don't create a flow, so that a retry of the packet is a new flow
				i.drops.acceptQueueFull.Add(1)
				return
			}
		}

		i.rt[p.key] = c
		i.flows.Lock()
		i.flows.m[c] = struct{}{}
		i.flows.Unlock()
		i.remotes.Lock()
		i.remotes.m[p.key.remote()] = c
		i.remotes.Unlock()
	}
	c.touch()
	c.localIfIdx.Store(int64(p.idx)) // The latest one is used for replies

	pkt := &inletPacketSmall{c: c, n: len(data)}
	if len(data) > 0 {
		pkt.b, pkt.ref = getBuf(len(data))
		copy(pkt.b, data)
	}

	c.mu.Lock() // Keeps the packets of a flow in order while it's being released
	if c.released.Load() {
		i.toDefault(pkt)
	} else {
		select {
		case c.rdRx <- pkt:
		default:
			putBuf(pkt.ref)
			i.drops.connQueueFull.Add(1)
		}
	}
	c.mu.Unlock()
}

// toDefault passes a packet of a released flow to the default connection, or drops it if there is none.
func (i *inlet) toDefault(pkt *inletPacketSmall) {
	if i.def == nil || isClosedChan(i.def.localDone) {
		putBuf(pkt.ref)
		i.drops.noDefault.Add(1)
		return
	}
	select {
	case i.def.rdRx <- pkt:
	default:
		putBuf(pkt.ref)
		i.drops.defaultQueueFull.Add(1)
	}
}

// lookupRemote returns the inlet connection of the latest flow of the given remote address, if any.
func (i *inlet) lookupRemote(addr net.Addr) *inletConn {
	i.remotes.RLock()
	defer i.remotes.RUnlock()
	return i.remotes.m[remoteKey(addr)]
}

// receive copies the next packet of q into b, waiting until one comes, until done is closed, or until
// the deadline d passes, and returns the inlet connection of its flow. Once remoteDone is closed, it still
// returns the packets queued, then io.EOF.
func receive(b []byte, q chan *inletPacketSmall, done, remoteDone <-chan struct{}, mu *sync.Mutex, d *deadline[noHook]) (int, *inletConn, error) {
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

	var pkt *inletPacketSmall
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
	n := copy(b, pkt.b[:pkt.n])
	putBuf(pkt.ref)
	return n, pkt.c, nil
}

// send copies b into a packet for the write loop to write for the flow of c, or to the address to if c
// is nil, and returns once the packet is queued. If the write queue is full, it waits for room, and fails
// if done is closed, the inlet closes or the deadline d passes first. As the packet is written later,
// an error of the underlying connection can't be returned: the write loop counts it.
func (i *inlet) send(b []byte, c *inletConn, to net.Addr, done <-chan struct{}, d *deadline[noHook]) (int, error) {
	switch {
	case isClosedChan(done):
		return 0, io.ErrClosedPipe
	case isClosedChan(i.done):
		return 0, io.ErrClosedPipe
	case isClosedChan(d.Wait()):
		return 0, os.ErrDeadlineExceeded
	}

	pkt := &inletPacketSmall{c: c, to: to, n: len(b)}
	if len(b) > 0 {
		pkt.b, pkt.ref = getBuf(len(b))
		copy(pkt.b, b)
	}
	select {
	case i.wrChan <- pkt:
		if c != nil {
			c.touch()
		}
		return len(b), nil
	case <-done:
		putBuf(pkt.ref)
		return 0, io.ErrClosedPipe
	case <-i.done:
		putBuf(pkt.ref)
		return 0, io.ErrClosedPipe
	case <-d.Wait():
		putBuf(pkt.ref)
		return 0, os.ErrDeadlineExceeded
	}
}

// writeLoop writes the packets of the write queue to the underlying connection, until the inlet closes
// or the underlying connection does.
func (i *inlet) writeLoop() {
	defer i.wg.Done()

	for {
		select {
		case <-i.done:
			i.drainWrites()
			return
		case pkt := <-i.wrChan:
			if !i.write(pkt) {
				i.drainWrites()
				return
			}
		}
	}
}

// write writes a packet to the underlying connection, with the addresses of its flow if it has one,
// and drops it if the write times out or fails. It reports false if the underlying connection is closed,
// and closes the inlet then.
func (i *inlet) write(pkt *inletPacketSmall) bool {
	defer putBuf(pkt.ref)

	remote, local, idx := pkt.to, net.Addr(nil), 0
	if pkt.c != nil {
		remote, local, idx = pkt.c.remoteAddr, pkt.c.localAddr, pkt.c.LocalIfIdx()
	}

	var err error
	if i.cfg.writeTimeout > 0 { // The write loop is the only user of the write deadline of the underlying connection
		err = i.pc.SetWriteDeadline(time.Now().Add(i.cfg.writeTimeout))
	}
	if err == nil {
		_, err = i.pc.WriteToFrom(pkt.b[:pkt.n], remote, local, idx)
	}

	var nrr net.Error
	switch {
	case err == nil:
		return true
	case errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe):
		// If the underlying FromToPacketConn has unexpectedly closed while
		// the inlet's done channel remains open, spawn a closing goroutine.
		// Close() waits until writeLoop() returns, so it can't be done in sync.
		if !isClosedChan(i.done) {
			go func() {
				_ = i.Close()
			}()
		}
		return false
	case errors.As(err, &nrr) && nrr.Timeout():
		i.drops.writeTimedOut.Add(1)
	default: // E.g. an address unreachable or of the wrong IP version, which another packet may not have
		i.drops.writeFailed.Add(1)
		i.logError(err)
	}
	return true
}

// drainWrites releases the buffers of the packets left in the write queue.
func (i *inlet) drainWrites() {
	for {
		select {
		case pkt := <-i.wrChan:
			putBuf(pkt.ref)
		default:
			return
		}
	}
}

type inletConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr

	localDone chan struct{}
	rdRx      chan *inletPacketSmall // Packets of the flow, unless it's been released

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

	released atomic.Bool // Set before closing localDone, if the flow goes on to the default connection
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

// Release hands the flow of the connection over to the default connection of the inlet: the packets
// queued and all the later packets of the flow go there, or are dropped if the inlet has no default
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
			ic.parent.toDefault(pkt)
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

// inletDefault is the default connection of an inlet: a plain PacketConn serving the flows released by
// their handlers, or every flow with WithDefaultOnly. It deliberately gives no access to the socket.
type inletDefault struct {
	parent *inlet

	rdRx chan *inletPacketSmall // Packets of released flows

	localDone chan struct{}

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
}

func (i *inlet) newDefault() *inletDefault {
	return &inletDefault{
		parent:     i,
		rdRx:       make(chan *inletPacketSmall, i.cfg.defaultQueueSize),
		localDone:  make(chan struct{}),
		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),
	}
}

// ReadFrom reads a packet of a released flow, and returns the remote address of the flow.
func (d *inletDefault) ReadFrom(b []byte) (int, net.Addr, error) {
	n, c, err := receive(b, d.rdRx, d.localDone, d.parent.done, &d.rdMu, &d.rdDeadline)
	if err != nil {
		return 0, nil, d.opError("read", nil, err)
	}
	return n, c.remoteAddr, nil
}

// WriteTo writes a packet to addr, from the local address and through the interface of the latest flow
// of addr, or from those the system chooses if the inlet hasn't seen addr.
func (d *inletDefault) WriteTo(b []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, d.opError("write", nil, errMissingAddress)
	}
	n, err := d.parent.send(b, d.parent.lookupRemote(addr), addr, d.localDone, &d.wrDeadline)
	return n, d.opError("write", addr, err)
}

// Close closes the default connection, so that the packets of released flows are dropped from now on.
func (d *inletDefault) Close() error {
	err := net.ErrClosed
	d.once.Do(func() {
		close(d.localDone)
		err = nil
	})
	return err
}

func (d *inletDefault) LocalAddr() net.Addr { return d.parent.pc.LocalAddr() }

func (d *inletDefault) SetDeadline(t time.Time) error {
	if isClosedChan(d.localDone) || isClosedChan(d.parent.done) {
		return io.ErrClosedPipe
	}
	d.rdDeadline.Set(t)
	d.wrDeadline.Set(t)
	return nil
}

func (d *inletDefault) SetReadDeadline(t time.Time) error {
	if isClosedChan(d.localDone) || isClosedChan(d.parent.done) {
		return io.ErrClosedPipe
	}
	d.rdDeadline.Set(t)
	return nil
}

func (d *inletDefault) SetWriteDeadline(t time.Time) error {
	if isClosedChan(d.localDone) || isClosedChan(d.parent.done) {
		return io.ErrClosedPipe
	}
	d.wrDeadline.Set(t)
	return nil
}

// opError wraps an error of an operation on the default connection, except for the end of reading.
func (d *inletDefault) opError(op string, addr net.Addr, err error) error {
	if err == nil || err == io.EOF || errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return &net.OpError{Op: op, Net: d.LocalAddr().Network(), Source: d.LocalAddr(), Addr: addr, Err: err}
}

var errMissingAddress = errors.New("hybrid: missing address")

// Defaults
const (
	poIdleTimeoutDef = 30 * time.Second
	poIdleTimeoutMin = 0 * time.Second

	poReadQueueSizeDef = 32
	poReadQueueSizeMin = 1 // As the read loop drops a packet rather than wait for a reader

	piAcceptQueueSizeDef = 8
	piAcceptQueueSizeMin = 1 // As the read loop drops a new flow rather than wait for Accept

	piDefaultQueueSizeDef = 1024 // As the default connection may serve many flows
	piDefaultQueueSizeMin = 1    // As the read loop drops a packet rather than wait for a reader

	piReadBufferSizeDef = math.MaxUint16 - (20 + 8) // Maximum size of an IPv4 UDP datagram is 64 KiB net of headers
	piReadBufferSizeMax = math.MaxInt32             // The largest int on 32-bit platforms, while an IPv6 jumbogram may take 4 GiB
	piReadBufferSizeMin = 512                       // A read buffer of less than 512 bytes seems impractical

	piWriteTimeoutDef = 500 * time.Millisecond

	piTryPacketInfoUDPDef = true

	piActiveConnLimitDef = 8192
	piActiveConnLimitMin = 64

	piWriteQueueSizeDef = 64
	piWriteQueueSizeMin = 1 // As a write returns once its packet is queued
)

// Interface guards
var (
	_ PacketConnWrapper = (*inlet)(nil)
	_ InletListener     = (*inlet)(nil)
	_ PacketConn        = (*inletDefault)(nil)
)
