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
	"strconv"
	"strings"
	"sync"
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
	readTimeout  time.Duration
	writeTimeout time.Duration

	acceptQueueSize int
	deleteQueueSize int
	readBufferSize  int
	readQueueSize   int
	writeQueueSize  int

	activeConnLimit int

	tryPacketInfoUDP bool
}

var defaultInletConfig = inletConfig{
	idleTimeout:      poIdleTimeoutDef,
	readTimeout:      piReadTimeoutDef,
	writeTimeout:     piWriteTimeoutDef,
	acceptQueueSize:  piAcceptQueueSizeDef,
	deleteQueueSize:  piDeleteQueueSizeDef,
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

// WithAcceptQueueSize sets the maximum number of virtual connections (outlets)
// waiting to be accepted.
func WithAcceptQueueSize(value int) InletOption {
	if value < piAcceptQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.acceptQueueSize = value })
}

// WithReadBufferSize sets the maximum number of bytes read as a single packet
// from the underlying connection.
func WithReadBufferSize(value int) InletOption {
	if value < piReadBufferSizeMin || value > piReadBufferSizeMax {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.readBufferSize = value })
}

// WithReadQueueSize sets the maximum number of packets waiting to be read
// from each virtual connection (outlet).
func WithReadQueueSize(value int) InletOption {
	if value < poReadQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.readQueueSize = value })
}

// WithIdleTimeout sets the amount of time to wait since the last read/write
// before closing a virtual connection (outlet).
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

// WithWriteQueueSize sets the maximum number of packets waiting to be written
// to the underlying PacketConn.
func WithWriteQueueSize(value int) InletOption {
	if value < piWriteQueueSizeMin {
		return noOpInletOption
	}
	return inletOptionFunc(func(cfg *inletConfig) { cfg.writeQueueSize = value })
}

type inletBinding struct {
	c *inletConn
	d Deadline
}

type inletPacketSmall struct {
	c *inletConn // Origin / target
	b []byte     // Reusable buffer
	n int        // Bytes consumed
}

type inletPacketLarge struct {
	r net.Addr // Remote address
	l net.Addr // Local address
	b []byte   // Reusable buffer
	n int      // Bytes consumed
	i int      // Interface index
}

// inlet implements PacketListener for any underlying PacketConn.
type inlet struct {
	pc FromToPacketConn // Underlying PacketConn

	acChan chan *inletConn        // Accept channel
	deChan chan *inletConn        // Delete channel
	rdChan chan *inletPacketLarge // Read channel
	wrChan chan *inletPacketSmall // Write channel
	done   chan struct{}          // Done channel

	rt map[string]*inletBinding // Routing table

	slicePool sync.Pool

	wg   sync.WaitGroup
	mu   sync.RWMutex
	once sync.Once

	cfg inletConfig
}

// Inlet returns a PacketListener implementation and starts its internal loops.
func Inlet(pc PacketConn, opts ...InletOption) PacketListener {
	cfg := defaultInletConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	i := &inlet{
		pc:     PacketToFromToPacketConn(pc, cfg.tryPacketInfoUDP),
		acChan: make(chan *inletConn, cfg.acceptQueueSize),
		deChan: make(chan *inletConn, cfg.deleteQueueSize),
		wrChan: make(chan *inletPacketSmall, cfg.writeQueueSize),
		done:   make(chan struct{}),
		rt:     make(map[string]*inletBinding),
		slicePool: sync.Pool{
			New: func() any {
				return make([]byte, cfg.readBufferSize)
			},
		},
		cfg: cfg,
	}

	i.wg.Add(2)
	go i.readLoop()
	go i.writeLoop()
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

// addrString returns the string of the given address, or an empty string if there is none,
// e.g. as the underlying connection doesn't report destination addresses.
func addrString(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}

func (i *inlet) newBinding(src, dst net.Addr, idx int, key string) *inletBinding {
	bdg := &inletBinding{c: i.newConnection(src, dst, idx, key)}
	if i.cfg.idleTimeout > 0 {
		bdg.d = HookDeadline(func() {
			_ = bdg.c.Close()
		})
		bdg.d.Set(time.Now().Add(i.cfg.idleTimeout))
	}
	return bdg
}

func (i *inlet) newConnection(src, dst net.Addr, idx int, key string) *inletConn {
	return &inletConn{
		rdRx: make(chan *inletPacketSmall, i.cfg.readQueueSize),
		wrTx: i.wrChan,
		wrRx: make(chan int),

		localDone:  make(chan struct{}),
		remoteDone: i.done,

		rdDeadline: makeDeadline(noHook{}),
		wrDeadline: makeDeadline(noHook{}),

		localAddr:  dst,
		remoteAddr: src,

		localIfIdx: idx,

		parent: i,
		key:    key,
	}
}

func (i *inlet) deleteConnection(conn *inletConn) {
	i.wg.Add(1)
	go func() {
		select {
		case i.deChan <- conn:
		case <-i.done:
		}
		i.wg.Done()
	}()
}

func (i *inlet) cleanLoop() {
	defer i.wg.Done()

	ticker := time.NewTicker(piCleanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-i.done:
			return
		case <-ticker.C:
			i.mu.Lock()
			for k, v := range i.rt {
				if isClosedChan(v.c.localDone) {
					delete(i.rt, k)
				}
			}
			i.mu.Unlock()
		}
	}
}

//
//func (i *inlet) readLoop2() {
//	defer i.wg.Done()
//
//	var buf []byte
//	var err error
//	var dst, src net.Addr
//	var idx, n int
//	var nrr net.Error
//	var pkt NetworkPacket
//
//loop:
//	for {
//		if isClosedChan(i.done) {
//			return
//		}
//
//		// Obtain a reusable slice from the pool.
//		buf = i.slicePool.Get().([]byte)
//
//	readWithDeadline:
//		err = i.pc.SetReadDeadline(time.Now().Add(piReadTimeoutDef))
//		if err != nil {
//			// Log an error, unless the underlying FromToPacketConn is closed.
//			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
//				i.logError(err)
//			}
//
//			// If the underlying FromToPacketConn has unexpectedly closed while
//			// the inlet's done channel remains open, spawn a closing goroutine.
//			// Close() waits until readLoop() returns, so it can't be done in sync.
//			if !isClosedChan(i.done) {
//				go func() {
//					_ = i.Close()
//				}()
//			}
//
//			break
//		}
//
//		// Read from the underlying FromToPacketConn and check for errors.
//		// Note: any error is ignored, if at least one byte has been read.
//		n, src, dst, idx, err = i.pc.ReadFromTo(buf)
//		if n == 0 {
//			if err != nil {
//				// If it's a timeout, release the empty packet to the pool and return, or continue the loop.
//				if errors.As(err, &nrr) && nrr.Timeout() {
//					if isClosedChan(i.done) {
//						break
//					}
//
//					goto readWithDeadline
//				}
//
//				// Log an error, unless the underlying FromToPacketConn is closed.
//				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
//					i.logError(err)
//				}
//
//				// If the underlying FromToPacketConn has unexpectedly closed while
//				// the inlet's done channel remains open, spawn a closing goroutine.
//				// Close() waits until readLoop() returns, so it can't be done in sync.
//				if !isClosedChan(i.done) {
//					go func() {
//						_ = i.Close()
//					}()
//				}
//
//				break
//			}
//
//			pkt = &dataPacketWithSocketInfo{DataPacket: emptyPacket, remoteAddr: src, localAddr: dst, localIfIdx: idx}
//		} else {
//			// Give a warning, if all available bytes have been filled.
//			if n == len(buf) {
//				// TODO(vnxme): Warn that some bytes might have been truncated
//				// 				and recommend increasing the read buffer size
//			}
//
//			pkt = &dataPacketWithSocketInfo{DataPacket: &uniformDataPacket{b: buf, n: n}, remoteAddr: src, localAddr: dst, localIfIdx: idx}
//		}
//
//		select {
//		case i.rdChan <- pkt:
//		case <-i.done:
//			break loop
//		}
//	}
//
//	// Release the buffer to the pool and return.
//	i.slicePool.Put(buf)
//}
//
//func (i *inlet) routeLoop() {
//	defer i.wg.Done()
//
//	var key string
//	var nbd, ok bool
//	var pbd *inletBinding
//	var sb strings.Builder
//
//	for {
//		select {
//		case pkt := <-i.rdChan:
//			// Use remote and local addresses and local interface indices as keys to track active connections.
//			sb.Grow(piKeyBufferSize)
//			sb.WriteString(pkt.RemoteAddr().String())
//			sb.WriteString("|")
//			sb.WriteString(pkt.LocalAddr().String())
//			sb.WriteString("|")
//			sb.WriteString(strconv.Itoa(pkt.LocalIfIdx()))
//			key = sb.String()
//			sb.Reset()
//
//			// Look up a binding in the routing table.
//			i.mu.RLock()
//			pbd, ok = i.rt[key]
//			i.mu.RUnlock()
//
//			// Determine if a new binding has to be created.
//			nbd = !ok || isClosedChan(pbd.c.localDone)
//
//		createNewBinding:
//			if nbd { // TODO(vnxme): limit creating new bindings
//				// Create a new binding.
//				pbd = i.newBinding(pkt.RemoteAddr(), pkt.LocalAddr(), pkt.LocalIfIdx())
//
//				// Add the binding to the routing table.
//				i.mu.Lock()
//				i.rt[key] = pbd
//				i.mu.Unlock()
//
//				// Send the new connection to the accept channel or drop it.
//				select {
//				case i.acChan <- pbd.c:
//				case <-pbd.c.localDone:
//					// If this connection is closed, jump back and create a new binding
//					// in order to ensure no packets are prematurely dropped.
//					nbd = true
//					goto createNewBinding
//				case <-i.done:
//					break
//				}
//			}
//		case outlet := <-i.deChan:
//
//		case <-i.done:
//			return
//		}
//	}
//}

func (i *inlet) deleteLoop() {
	var bdg *inletBinding
	var c *inletConn
	var ok bool
	var pkt *inletPacketSmall

deleteLoop:
	for {
		select {
		case c = <-i.deChan:
			// Ignore any delete requests for non-closed connections.
			if !isClosedChan(c.localDone) {
				continue
			}

			// Find a binding and verify it points to the same connection,
			// otherwise ignore this delete request as well.
			bdg, ok = i.rt[c.key]
			if ok && bdg.c == c {
				// Ensure no packets remain in the connection's read channel.
			drainLoop:
				for {
					select {
					case pkt = <-c.rdRx:
						i.slicePool.Put(pkt.b)
					default:
						break drainLoop
					}
				}

				// Delete this connection from the routing table.
				delete(i.rt, c.key)
			}
		default:
			break deleteLoop
		}
	}
}

func (i *inlet) readLoop() {
	defer i.wg.Done()

	var bdg *inletBinding
	var buf []byte
	var err error
	var idx, n int
	var key string
	var nrr net.Error
	var ok, needNewBinding bool
	var pkt *inletPacketSmall
	var sb strings.Builder
	var src, dst net.Addr

loop:
	for {
		// Obtain a reusable buffer from the pool.
		buf = i.slicePool.Get().([]byte)

	repeatReading:
		if isClosedChan(i.done) {
			break loop
		}

		// If there are any closed connections, drain their read channels
		// and delete these connections from the routing table.
		i.deleteLoop()

		if i.cfg.readTimeout > 0 {
			err = i.pc.SetReadDeadline(time.Now().Add(i.cfg.readTimeout))
			if err != nil {
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

				break loop
			}
		}

		// Read from the underlying FromToPacketConn and check for errors.
		// Note: any error is ignored, if at least one byte has been read.
		n, src, dst, idx, err = i.pc.ReadFromTo(buf)
		if n == 0 {
			if err != nil {
				// If it's a timeout, release the empty packet to the pool and return, or continue the loop.
				if errors.As(err, &nrr) && nrr.Timeout() {
					goto repeatReading
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

				break loop
			}
		} else {
			// Give a warning, if all available bytes have been filled.
			if n == len(buf) {
				// TODO(vnxme): Warn that some bytes might have been truncated
				// 				and recommend increasing the read buffer size
			}
		}

		// Use remote and local addresses and local interface indices as keys to track active connections.
		sb.Grow(piKeyBufferSize)
		sb.WriteString(addrString(src))
		sb.WriteString("|")
		sb.WriteString(addrString(dst))
		sb.WriteString("|")
		sb.WriteString(strconv.Itoa(idx))
		key = sb.String()
		sb.Reset()

		// Look up a connection in the routing table and determine
		// if a new connection binding has to be created.
		bdg, ok = i.rt[key]
		if ok {
			needNewBinding = isClosedChan(bdg.c.localDone)
		} else {
			if len(i.rt) >= i.cfg.activeConnLimit {
				// TODO(vnxme): Warn that a new connection could have been processed
				// 				and recommend increasing the active connection limit
				goto repeatReading
			}
			needNewBinding = true
		}

		// Replace a uniformDataPacket with an emptyDataPacket if possible.
		if n == 0 {
			pkt = &inletPacketSmall{}
		} else {
			pkt = &inletPacketSmall{b: buf, n: n}
		}

	repeatBinding:
		if needNewBinding {
			// Create a new connection binding.
			bdg = i.newBinding(src, dst, idx, key)

			// Add the new connection binding to the routing table.
			i.rt[key] = bdg

			// Send the new connection to the accept channel or drop it.
			select {
			case i.acChan <- bdg.c:
			case <-bdg.c.localDone:
				// If this connection is closed, jump back and create a new binding
				// in order to ensure no packets are prematurely dropped.
				needNewBinding = true
				goto repeatBinding
			case <-i.done:
				break loop
			}
		}

		// Fill a pointer to the outlet.
		pkt.c = bdg.c

		// Send the packet to the connection's read channel or drop it.
		select {
		case bdg.c.rdRx <- pkt:
			// Reuse the empty buffer when it's possible.
			if n == 0 {
				goto repeatReading
			}
		case <-bdg.c.localDone:
			// If this connection is closed, jump back and create a new binding
			// in order to ensure no packets are prematurely dropped.
			needNewBinding = true
			goto repeatBinding
		case <-i.done:
			break loop
		}
	}

	// Release the buffer to the pool.
	i.slicePool.Put(buf)
}

func (i *inlet) writeLoop() {
	defer i.wg.Done()

	var err error
	var n int
	var nrr net.Error
	var pkt *inletPacketSmall

loop:
	for {
		select {
		case <-i.done:
			break loop
		case pkt = <-i.wrChan:
			// Drop the packet if its origin is unknown.
			if pkt.c == nil {
				continue
			}

		repeatWriting:
			if i.cfg.writeTimeout > 0 {
				err = i.pc.SetWriteDeadline(time.Now().Add(i.cfg.writeTimeout))
				if err != nil {
					// Log an error, unless the underlying FromToPacketConn is closed.
					if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
						i.logError(err)
					}

					// If the underlying FromToPacketConn has unexpectedly closed while
					// the inlet's done channel remains open, spawn a closing goroutine.
					// Close() waits until writeLoop() returns, so it can't be done in sync.
					if !isClosedChan(i.done) {
						go func() {
							_ = i.Close()
						}()
					}

					break loop
				}
			}

			// Write to the underlying FromToPacketConn and check for errors.
			// Note: any error is ignored, if at least one byte has been written.
			n, err = i.pc.WriteToFrom(pkt.b[:pkt.n], pkt.c.remoteAddr, pkt.c.localAddr, pkt.c.localIfIdx)
			if n == 0 && err != nil {
				// If it's a timeout, drop the packet and return, or continue the loop.
				if errors.As(err, &nrr) && nrr.Timeout() {
					if isClosedChan(i.done) {
						break loop
					}

					goto repeatWriting
				}

				// Log an error, unless the underlying FromToPacketConn is closed.
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
					i.logError(err)
				}

				// If the underlying FromToPacketConn has unexpectedly closed while
				// the inlet's done channel remains open, spawn a closing goroutine.
				// Close() waits until writeLoop() returns, so it can't be done in sync.
				if !isClosedChan(i.done) {
					go func() {
						_ = i.Close()
					}()
				}

				break loop
			}

			// Report the number of bytes written, if the write channel is unbuffered.
			if cap(i.wrChan) == 0 {
				select {
				case pkt.c.wrRx <- n:
				default:
				}
			}
		}
	}

	if len(i.wrChan) > 0 { // It means cap(i.wrChan) > 0 as well.
	drainLoop:
		for {
			select {
			case pkt = <-i.wrChan:
				i.slicePool.Put(pkt.b)
			default:
				break drainLoop
			}
		}
	}
}

type inletConn struct {
	parent *inlet

	localIfIdx int

	localAddr  net.Addr
	remoteAddr net.Addr

	// Used by local Read to interact with remote Write.
	// For a sync/semi-sync pipe, receive on rdRx is always followed by send on rdTx
	rdRx chan *inletPacketSmall
	// rdTx chan<- int

	// Used by local Write to interact with remote Read.
	// For a sync/semi-sync pipe, send on wrTx is always followed by receive on wrRx
	wrTx chan *inletPacketSmall
	wrRx chan int

	localDone  chan struct{}
	remoteDone <-chan struct{}

	rdDeadline deadline[noHook]
	wrDeadline deadline[noHook]

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
	wrMu sync.Mutex // Serialize Write operations

	key string
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
	switch {
	case isClosedChan(ic.localDone):
		return 0, nil, nil, 0, io.ErrClosedPipe
	case isClosedChan(ic.remoteDone) && len(ic.rdRx) == 0:
		return 0, nil, nil, 0, io.EOF
	case isClosedChan(ic.rdDeadline.Wait()):
		return 0, nil, nil, 0, os.ErrDeadlineExceeded
	}

	ic.rdMu.Lock()
	defer ic.rdMu.Unlock()
	switch {
	case isClosedChan(ic.localDone):
		return 0, nil, nil, 0, io.ErrClosedPipe
	case isClosedChan(ic.remoteDone) && len(ic.rdRx) == 0:
		return 0, nil, nil, 0, io.EOF
	case isClosedChan(ic.rdDeadline.Wait()):
		return 0, nil, nil, 0, os.ErrDeadlineExceeded
	}

	select {
	case pkt := <-ic.rdRx:
		nr := copy(b, pkt.b[:pkt.n])
		ic.parent.slicePool.Put(pkt.b)
		return nr, ic.remoteAddr, ic.localAddr, ic.localIfIdx, nil
	case <-ic.localDone:
		return 0, nil, nil, 0, io.ErrClosedPipe
	case <-ic.remoteDone:
		if len(ic.rdRx) > 0 {
			pkt := <-ic.rdRx
			nr := copy(b, pkt.b[:pkt.n])
			ic.parent.slicePool.Put(pkt.b)
			return nr, ic.remoteAddr, ic.localAddr, ic.localIfIdx, nil
		}
		return 0, nil, nil, 0, io.EOF
	case <-ic.rdDeadline.Wait():
		return 0, nil, nil, 0, os.ErrDeadlineExceeded
	}
}

func (ic *inletConn) Write(b []byte) (int, error) { return ic.WriteTo(b, ic.remoteAddr) }

func (ic *inletConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	return ic.WriteToFrom(b, nil, nil, 0)
}

func (ic *inletConn) WriteToFrom(b []byte, _, _ net.Addr, _ int) (int, error) {
	n, err := ic.writeToFrom(b, nil, nil, 0)
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "write", Net: ic.parent.pc.LocalAddr().Network(), Err: err, Source: ic.LocalAddr(), Addr: ic.remoteAddr}
	}
	return n, err
}

func (ic *inletConn) writeToFrom(b []byte, _, _ net.Addr, _ int) (int, error) {
	switch {
	case isClosedChan(ic.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(ic.remoteDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(ic.wrDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	}

	ic.wrMu.Lock()
	defer ic.wrMu.Unlock()
	deadlineChan := ic.wrDeadline.Wait()

	pkt := &inletPacketSmall{c: ic}
	if cap(ic.wrTx) == 0 {
		if len(b) > 0 {
			pkt.b, pkt.n = b, len(b)
		}
		select {
		case ic.wrTx <- pkt:
			select {
			case n := <-ic.wrRx:
				return n, nil
			case <-ic.localDone:
				return 0, io.ErrClosedPipe
			case <-ic.remoteDone:
				return 0, io.ErrClosedPipe
			case <-deadlineChan:
				return 0, os.ErrDeadlineExceeded
			}
		case <-ic.localDone:
			return 0, io.ErrClosedPipe
		case <-ic.remoteDone:
			return 0, io.ErrClosedPipe
		case <-deadlineChan:
			return 0, os.ErrDeadlineExceeded
		}
	} else {
		if len(b) > 0 {
			pkt.b = ic.parent.slicePool.Get().([]byte)
			pkt.n = copy(pkt.b, b)
		}
		select {
		case ic.wrTx <- pkt:
			return pkt.n, nil
		case <-ic.localDone:
			return 0, io.ErrClosedPipe
		case <-ic.remoteDone:
			return 0, io.ErrClosedPipe
		case <-deadlineChan:
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (ic *inletConn) Close() (err error) {
	err = net.ErrClosed
	ic.once.Do(func() {
		close(ic.localDone)
		ic.parent.deleteConnection(ic)
		err = nil
	})
	return
}

func (ic *inletConn) LocalIfIdx() int { return ic.localIfIdx }

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

// Defaults
const (
	poIdleTimeoutDef = 30 * time.Second
	poIdleTimeoutMin = 0 * time.Second

	poReadQueueSizeDef = 32
	poReadQueueSizeMin = 0 // A read channel of size 0 is unbuffered

	hcWriteQueueDefSize = 32
	hcWriteQueueMinSize = 0 // A read channel of size 0 is unbuffered

	piAcceptQueueSizeDef = 8
	piAcceptQueueSizeMin = 0 // An accept channel of size 0 is unbuffered

	piDeleteQueueSizeDef = 8
	piDeleteQueueSizeMin = 0

	piReadBufferSizeDef = math.MaxUint16 - (20 + 8) // Maximum size of an IPv4 UDP datagram is 64 KiB net of headers
	piReadBufferSizeMax = math.MaxInt32             // The largest int on 32-bit platforms, while an IPv6 jumbogram may take 4 GiB
	piReadBufferSizeMin = 512                       // A read buffer of less than 512 bytes seems impractical

	piReadTimeoutDef  = 500 * time.Millisecond
	piWriteTimeoutDef = 500 * time.Millisecond

	piKeyBufferSize = len("[fe80:0000:0000:0000:0000:0000:0001%enp8s0f0np0.100]:65535")*2 + len("4294967295") + 2

	piCleanInterval = 120 * time.Second

	piTryPacketInfoUDPDef = true

	piActiveConnLimitDef = 8192
	piActiveConnLimitMin = 64

	piWriteQueueSizeDef = 0
	piWriteQueueSizeMin = 0
)

// Interface guards
var (
	_ PacketConnWrapper = (*inlet)(nil)
	_ PacketListener    = (*inlet)(nil)
)
