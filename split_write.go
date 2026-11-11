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
	"slices"
	"time"
)

// send copies b into a packet for the write loop to write to dst, from src and through idx if given,
// and returns once the packet is queued, keeping the flow of c going if c isn't nil. If the write queue
// is full, it waits for room, and fails if the connection is closed, the splitter stops or the write deadline
// passes first. As the packet is written later, an error of the underlying connection can't be returned:
// the write loop counts it.
func (sc *splitConnBase) send(b []byte, c *splitConn, dst, src net.Addr, idx int) (int, error) {
	switch {
	case isClosedChan(sc.localDone):
		return 0, errSplitClosed
	case isClosedChan(sc.splitter.done):
		return 0, errSplitClosed
	case isClosedChan(sc.wrDeadline.Wait()):
		return 0, errSplitDeadlineExceeded
	}

	pkt := newSplitPacket(b)
	pkt.dst, pkt.src, pkt.idx = dst, src, idx
	select { // Queue without a blocking select if possible, which is much slower
	case sc.splitter.writeQueue <- pkt:
	default:
		select {
		case sc.splitter.writeQueue <- pkt:
		case <-sc.localDone:
			pkt.free()
			return 0, errSplitClosed
		case <-sc.splitter.done:
			pkt.free()
			return 0, errSplitClosed
		case <-sc.wrDeadline.Wait():
			pkt.free()
			return 0, errSplitDeadlineExceeded
		}
	}

	if isClosedChan(sc.splitter.done) { // Stopped while queuing, maybe after the write loop has drained the queue
		sc.splitter.drainWrites()
	}
	if c != nil {
		c.touch()
	}
	return len(b), nil
}

// lookupRemote returns the split connection of the released flow of the given remote address touched last,
// or of the one released last among those touched at the same coarse time, if any.
func (s *splitter) lookupRemote(addr net.Addr) *splitConn {
	s.remotes.RLock()
	defer s.remotes.RUnlock()
	var latest *splitConn
	for _, c := range s.remotes.m[addrKeyOf(addr)] {
		if latest == nil || c.touched.Load() >= latest.touched.Load() {
			latest = c
		}
	}
	return latest
}

// indexRemote adds a connection being released to the index of remote addresses, so that the fallback connection
// writes to its remote address from its local address. Only released flows are indexed, as the fallback connection
// serves no other flows. A connection is released only while open, so it's never discarded before being indexed.
func (s *splitter) indexRemote(c *splitConn) {
	if s.fallback == nil { // Nothing to write from
		return
	}
	s.remotes.Lock()
	defer s.remotes.Unlock()
	s.remotes.m[c.key.src] = append(s.remotes.m[c.key.src], c)
}

// unindexRemote removes a discarded connection from the index of remote addresses, if it's there. As it's closed,
// so can't be released anymore, it's there only if released, so that closing one never released takes no lock
// that the writes of the fallback connection wait for.
func (s *splitter) unindexRemote(c *splitConn) {
	if s.fallback == nil || !c.released.Load() { // Nothing indexed
		return
	}
	s.remotes.Lock()
	defer s.remotes.Unlock()
	if conns := slices.DeleteFunc(s.remotes.m[c.key.src], func(e *splitConn) bool { return e == c }); len(conns) > 0 {
		s.remotes.m[c.key.src] = conns
	} else {
		delete(s.remotes.m, c.key.src)
	}
}

// endReleased reports every released flow for deletion, as the fallback connection has closed, so that their
// next packets start new flows, and the flow table needn't be searched for them once it's full.
func (s *splitter) endReleased() {
	s.remotes.RLock()
	var ended []*splitConn
	for _, conns := range s.remotes.m {
		ended = append(ended, conns...)
	}
	s.remotes.RUnlock()

	for _, c := range ended { // Once remotes is unlocked, as discarding a connection locks it
		s.deleteConnection(c)
	}
}

// writeLoop writes the packets of the write queue to the underlying connection, until the splitter closes
// or the underlying connection does.
func (s *splitter) writeLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.done:
			s.drainWrites()
			return
		case pkt := <-s.writeQueue:
			if isClosedChan(s.done) { // Picked at random, as the splitter has closed meanwhile
				pkt.free()
				s.drops.writeUnsent.Add(1)
				s.drainWrites()
				return
			}
			if !s.write(pkt) {
				s.drainWrites()
				return
			}
		}
	}
}

// write writes a packet to the underlying connection, with the addresses it has been queued with,
// and drops it if the write times out or fails. It reports false if the underlying connection is closed,
// and closes the splitter then.
func (s *splitter) write(pkt *splitPacket) bool {
	defer pkt.free()

	var err error
	if s.cfg.writeTimeout > 0 { // The write loop is the only user of the write deadline of the underlying connection
		err = s.orig.SetWriteDeadline(time.Now().Add(s.cfg.writeTimeout))
	}
	if err == nil {
		err = s.mode.write(pkt)
	}

	switch {
	case err == nil:
		return true
	case errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe):
		// The underlying PacketConn has been closed other than by the splitter, unless the splitter is stopping
		if !isClosedChan(s.done) {
			s.fail(err)
		}
		s.drops.writeUnsent.Add(1)
		return false
	case isTimeout(err):
		s.drops.writeTimedOut.Add(1)
	default: // E.g. an address unreachable or of the wrong IP version, which another packet may not have
		s.drops.writeFailed.Add(1)
		s.reportError(err)
	}
	return true
}

// drainWrites drops and counts the packets left in the write queue, which their writers took for written.
func (s *splitter) drainWrites() {
	for {
		select {
		case pkt := <-s.writeQueue:
			pkt.free()
			s.drops.writeUnsent.Add(1)
		default:
			return
		}
	}
}
