// Copyright 2026 VNXME
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
	"net"
	"net/netip"
	"time"
)

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

// newConnection returns a new inlet connection for the flow of the given packet.
func (i *inlet) newConnection(p *inletPacketInfo) *inletConn {
	src, dst := p.src, p.dst
	if i.udp != nil {
		src, dst = addrPortToAddr(p.key.src), addrPortToAddr(p.key.dst)
	}

	ic := &inletConn{
		rdRx: make(chan *inletPacket, i.cfg.readQueueSize),

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
		// Read from the underlying FromToPacketConn and check for errors. Note: any error is ignored if at least
		// one byte has been read, while a negative count, e.g. of a socket closed on Linux, reads nothing.
		n, err := i.read(buf, &p)
		if n <= 0 && err != nil {
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

// route passes a packet to the inlet connection of its flow, or to the fallback connection if the flow
// has been released. The first packet of a flow creates its inlet connection, and queues it for Accept.
// It drops the packet rather than wait if a queue is full or the table is.
func (i *inlet) route(data []byte, p *inletPacketInfo) {
	c, ok := i.rt[p.key]
	if !ok || c.ended() {
		if !ok && len(i.rt) >= i.cfg.maxFlows {
			// Delete the flows of closed connections whose closing hasn't been reported yet, then check again
			for _, e := range i.rt {
				if e.ended() {
					i.deleteFlow(e)
				}
			}
			if len(i.rt) >= i.cfg.maxFlows {
				i.drops.tableFull.Add(1)
				return
			}
		}

		if ok { // An ended flow whose closing hasn't been reported yet
			i.untrack(c)
		}
		c = i.newConnection(p)
		select {
		case i.acChan <- c:
		default: // Don't create a flow, so that a retry of the packet is a new flow
			i.drops.acceptQueueFull.Add(1)
			return
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

	pkt := &inletPacket{conn: c}
	if len(data) > 0 {
		pkt.buf, pkt.ref = getBuf(len(data))
		copy(pkt.buf, data)
	}

	c.mu.Lock() // Keeps the packets of a flow in order while it's being released
	if c.released.Load() {
		i.toFallback(pkt)
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

// toFallback passes a packet of a released flow to the fallback connection, or drops it if there is none.
func (i *inlet) toFallback(pkt *inletPacket) {
	if i.fb == nil || isClosedChan(i.fb.localDone) {
		putBuf(pkt.ref)
		i.drops.noFallback.Add(1)
		return
	}
	select {
	case i.fb.rdRx <- pkt:
	default:
		putBuf(pkt.ref)
		i.drops.fallbackQueueFull.Add(1)
	}
}
