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
	"os"
	"time"
)

// lookupRemote returns the inlet connection of the latest flow of the given remote address, if any.
func (i *inlet) lookupRemote(addr net.Addr) *inletConn {
	i.remotes.RLock()
	defer i.remotes.RUnlock()
	return i.remotes.m[remoteKey(addr)]
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
