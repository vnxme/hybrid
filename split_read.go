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
	"time"
)

// flowKey identifies a flow: by its source and destination addresses for a connection that reports
// destination addresses, or by its source address alone otherwise, with dst left empty.
type flowKey struct {
	src, dst addrKey
}

// splitPacketInfo describes a packet read from the underlying connection.
type splitPacketInfo struct {
	src, dst net.Addr // Only if read from a connection other than a FromToUDPConn
	key      flowKey
	idx      int
}

// readLoop reads packets from the underlying connection and routes them to their split connections,
// until the splitter closes or the underlying connection fails.
func (s *splitter) readLoop() {
	defer s.wg.Done()

	var nrr net.Error
	var p splitPacketInfo
	// Read into, then copied into a buffer of the packet's size. A byte more than the maximum size of a packet
	// tells a larger packet, as filling the buffer is the only sign of truncation every connection gives:
	// Linux only flags it, Windows and trunks report errors, and a net.PacketConn reports nothing at all.
	buf := make([]byte, s.cfg.readBufferSize+1)
	for !isClosedChan(s.done) {
		// Read from the underlying FromToPacketConn and check for errors. Note: any error is ignored if at least
		// one byte has been read, e.g. of a truncated packet, while a negative count, e.g. of a socket closed
		// on Linux, reads nothing.
		n, err := s.read(buf, &p)
		if n <= 0 && err != nil {
			// A timeout interrupts reading when the splitter closes, so check it at the top of the loop
			if errors.As(err, &nrr) && nrr.Timeout() {
				continue
			}

			// Log an error, unless the underlying FromToPacketConn is closed.
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
				s.logError(err)
			}

			// If the underlying FromToPacketConn has unexpectedly closed while
			// the splitter's done channel remains open, spawn a closing goroutine.
			// Close() waits until readLoop() returns, so it can't be done synchronously.
			if !isClosedChan(s.done) {
				go func() {
					_ = s.Close()
				}()
			}
			return
		}

		// Drain the read channels of the connections closed so far, e.g. while reading,
		// and delete them from the flow table, so that their flows end before routing.
		s.deleteLoop()

		if n > s.cfg.readBufferSize { // Checked first, as the addresses of a truncated packet may be missing
			s.drops.truncated.Add(1)
			continue
		}
		if s.byDst && !p.key.dst.valid() { // E.g. packet info is missing, as it has been truncated
			s.drops.noDestination.Add(1)
			continue
		}
		s.route(buf[:n], &p)
	}
}

// read reads a packet into b and fills in p, the key of its flow, its addresses and its interface index.
// It returns the number of bytes read and an error, which matters only if no bytes have been read.
func (s *splitter) read(b []byte, p *splitPacketInfo) (n int, err error) {
	*p = splitPacketInfo{}
	if s.udp != nil {
		n, p.key.src.ap, p.key.dst.ap, p.idx, err = s.udp.ReadFromToUDPAddrPort(b)
		return n, err
	}

	n, p.src, p.dst, p.idx, err = s.pc.ReadFromTo(b)
	if p.src != nil {
		p.key.src = addrKeyOf(p.src)
	}
	if s.byDst && p.dst != nil {
		p.key.dst = addrKeyOf(p.dst)
	}
	return n, err
}

// route passes a packet to the split connection of its flow, or to the fallback connection if the flow
// has been released. The first packet of a flow creates its split connection, and queues it for Accept.
// It drops the packet rather than wait if a queue is full or the table is.
func (s *splitter) route(data []byte, p *splitPacketInfo) {
	c, ok := s.flows[p.key]
	if !ok || c.ended() {
		if !ok && len(s.flows) >= s.cfg.maxFlows {
			// Delete the flows of closed connections whose closing hasn't been reported yet, then check again
			for _, e := range s.flows {
				if e.ended() {
					s.deleteFlow(e)
				}
			}
			if len(s.flows) >= s.cfg.maxFlows {
				s.drops.tableFull.Add(1)
				return
			}
		}

		if ok { // An ended flow whose closing hasn't been reported yet
			s.discard(c)
		}
		c = s.newConnection(p)
		select {
		case s.acceptQueue <- c:
		default: // Don't create a flow, so that a retry of the packet is a new flow
			s.drops.acceptQueueFull.Add(1)
			return
		}

		s.flows[p.key] = c
		s.swept.Lock()
		s.swept.m[c] = struct{}{}
		s.swept.Unlock()
		s.remotes.Lock()
		s.remotes.m[p.key.src] = c
		s.remotes.Unlock()
	}
	c.touch()
	c.localIfIdx.Store(int64(p.idx)) // The latest one is used for replies

	pkt := &splitPacket{conn: c}
	if len(data) > 0 {
		pkt.buf, pkt.ref = getBuf(len(data))
		copy(pkt.buf, data)
	}

	c.releaseMu.Lock() // Keeps the packets of a flow in order while it's being released
	if c.released.Load() {
		s.toFallback(pkt)
	} else {
		select {
		case c.rdRx <- pkt:
		default:
			putBuf(pkt.ref)
			s.drops.connQueueFull.Add(1)
		}
	}
	c.releaseMu.Unlock()
}

// toFallback passes a packet of a released flow to the fallback connection, or drops it if there is none.
func (s *splitter) toFallback(pkt *splitPacket) {
	if s.fallback == nil || isClosedChan(s.fallback.localDone) {
		putBuf(pkt.ref)
		s.drops.noFallback.Add(1)
		return
	}
	select {
	case s.fallback.rdRx <- pkt:
		if isClosedChan(s.fallback.localDone) { // Closed while queuing, maybe after Close has drained the queue
			s.fallback.discard()
		}
	default:
		putBuf(pkt.ref)
		s.drops.fallbackQueueFull.Add(1)
	}
}

// receive copies the next packet of the read queue into b, waiting until one comes, until the connection
// is closed, or until its read deadline passes, and returns the split connection of its flow. Once the splitter
// stops, it still returns the packets queued, then net.ErrClosed. Reads needn't be serialized, since each
// of them receives a whole packet, the same way as UDP reads do. If b is too small, it returns
// io.ErrShortBuffer and drops the excess bytes.
func (sc *splitConnBase) receive(b []byte) (int, *splitConn, error) {
	switch {
	case isClosedChan(sc.localDone):
		return 0, nil, errSplitClosed
	case isClosedChan(sc.rdDeadline.Wait()): // An exceeded deadline prevails over pending packets
		return 0, nil, errSplitDeadlineExceeded
	case isClosedChan(sc.splitter.done) && len(sc.rdRx) == 0: // Queued packets may be read after the splitter stops
		return 0, nil, errSplitClosed
	}

	var pkt *splitPacket
	select { // Receive a pending packet without a blocking select, which is much slower
	case pkt = <-sc.rdRx:
	default:
		select {
		case pkt = <-sc.rdRx:
		case <-sc.localDone:
			return 0, nil, errSplitClosed
		case <-sc.splitter.done:
			select { // A packet may have been queued just before the splitter stopped
			case pkt = <-sc.rdRx:
			default:
				return 0, nil, errSplitClosed
			}
		case <-sc.rdDeadline.Wait():
			return 0, nil, errSplitDeadlineExceeded
		}
	}

	n := copy(b, pkt.buf)
	putBuf(pkt.ref)
	if n < len(pkt.buf) { // Unread bytes are dropped, i.e. a split connection doesn't support partial reads
		return n, pkt.conn, errSplitShortBuffer
	}
	return n, pkt.conn, nil
}

// deleteConnection reports a closed or expired connection, for the read loop to delete its flow. It needs no
// goroutine, so that it can't race with closing the splitter, which waits for the goroutines of the splitter.
func (s *splitter) deleteConnection(c *splitConn) {
	if isClosedChan(s.done) { // With no read loop to delete the flow any more, only drop and count the packets left
		s.discard(c)
		return
	}
	s.closed.Lock()
	s.closed.conns = append(s.closed.conns, c)
	s.closed.Unlock()
}

// deleteLoop deletes the flows whose connections have been closed and reported so far.
func (s *splitter) deleteLoop() {
	s.closed.Lock()
	closed := s.closed.conns
	s.closed.conns = nil
	s.closed.Unlock()

	for _, c := range closed {
		// Ignore a connection that no longer has a flow, e.g. as it's been deleted already,
		// and whose flow may have a new connection.
		if e, ok := s.flows[c.key]; ok && e == c {
			s.deleteFlow(c)
		} else {
			s.discard(c)
		}
	}
}

// deleteFlow discards a closed connection, and deletes its flow from the flow table
// and from the index of remote addresses.
func (s *splitter) deleteFlow(c *splitConn) {
	s.discard(c)
	delete(s.flows, c.key)
	s.remotes.Lock()
	if s.remotes.m[c.key.src] == c {
		delete(s.remotes.m, c.key.src)
	}
	s.remotes.Unlock()
}

// discard drops and counts the packets left unread on a closed connection, and removes the connection
// from those the sweeper checks.
func (s *splitter) discard(c *splitConn) {
	for {
		select {
		case pkt := <-c.rdRx:
			putBuf(pkt.ref)
			s.drops.connClosed.Add(1)
		default:
			s.swept.Lock()
			delete(s.swept.m, c)
			s.swept.Unlock()
			return
		}
	}
}

// sweepLoop keeps the clock of the splitter, and ends the flows idle for the idle timeout, checking them every
// quarter of it, so that a flow ends within one and a quarter idle timeouts of its last packet.
func (s *splitter) sweepLoop() {
	defer s.wg.Done()

	start := time.Now() // The clock is zero until the first tick, as is the last packet time of a new flow
	ticker := time.NewTicker(max(s.cfg.idleTimeout/4, time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			clock := int64(now.Sub(start)) // Monotonic, unlike the wall clock
			s.clock.Store(clock)

			s.swept.Lock()
			for c := range s.swept.m {
				if clock-c.touched.Load() >= int64(s.cfg.idleTimeout) {
					delete(s.swept.m, c)
					c.expire()
				}
			}
			s.swept.Unlock()
		}
	}
}
