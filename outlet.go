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
	"net"
	"os"
	"sync"
	"time"
)

//type packetOutlet struct {
//	parent *inlet
//
//	localIfIdx int
//
//	localAddr  net.Addr
//	remoteAddr net.Addr
//
//	// Used by local Read to interact with remote Write.
//	// For a sync/semi-sync pipe, receive on rdRx is always followed by send on rdTx
//	rdRx chan *inletPacketSmall
//	// rdTx chan<- int
//
//	// Used by local Write to interact with remote Read.
//	// For a sync/semi-sync pipe, send on wrTx is always followed by receive on wrRx
//	wrTx chan *inletPacketSmall
//	wrRx chan int
//
//	localDone  chan struct{}
//	remoteDone <-chan struct{}
//
//	rdDeadline Deadline
//	wrDeadline Deadline
//
//	once sync.Once  // Protects closing localDone
//	rdMu sync.Mutex // Serialize Read operations
//	wrMu sync.Mutex // Serialize Write operations
//}
//
//func (pc *packetOutlet) Read(b []byte) (int, error) {
//	n, _, err := pc.ReadFrom(b)
//	return n, err
//}
//
//func (pc *packetOutlet) ReadFrom(b []byte) (int, net.Addr, error) {
//	n, addr, _, _, err := pc.ReadFromTo(b)
//	return n, addr, err
//}
//
//func (pc *packetOutlet) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
//	n, rAddr, lAddr, lIfIdx, err := pc.readFromTo(b)
//	if err != nil && err != io.EOF && !errors.Is(err, io.ErrClosedPipe) {
//		err = &net.OpError{Op: "read", Net: pc.parent.pc.LocalAddr().Network(), Err: err, Source: pc.LocalAddr(), Addr: pc.remoteAddr}
//	}
//	return n, rAddr, lAddr, lIfIdx, err
//}
//
//func (pc *packetOutlet) readFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
//	switch {
//	case isClosedChan(pc.localDone):
//		return 0, nil, nil, 0, io.ErrClosedPipe
//	case isClosedChan(pc.remoteDone) && len(pc.rdRx) == 0:
//		return 0, nil, nil, 0, io.EOF
//	case isClosedChan(pc.rdDeadline.Wait()):
//		return 0, nil, nil, 0, os.ErrDeadlineExceeded
//	}
//
//	pc.rdMu.Lock()
//	defer pc.rdMu.Unlock()
//	switch {
//	case isClosedChan(pc.localDone):
//		return 0, nil, nil, 0, io.ErrClosedPipe
//	case isClosedChan(pc.remoteDone) && len(pc.rdRx) == 0:
//		return 0, nil, nil, 0, io.EOF
//	case isClosedChan(pc.rdDeadline.Wait()):
//		return 0, nil, nil, 0, os.ErrDeadlineExceeded
//	}
//
//	select {
//	case pkt := <-pc.rdRx:
//		nr := copy(b, pkt.ReadableBuffer())
//		rAddr, lAddr, lIfIdx := pkt.RemoteAddr(), pkt.LocalAddr(), pkt.LocalIfIdx()
//		pc.putNetworkPacket(pkt)
//		return nr, rAddr, lAddr, lIfIdx, nil
//	case <-pc.localDone:
//		return 0, nil, nil, 0, io.ErrClosedPipe
//	case <-pc.remoteDone:
//		if len(pc.rdRx) > 0 {
//			pkt := <-pc.rdRx
//			nr := copy(b, pkt.ReadableBuffer())
//			rAddr, lAddr, lIfIdx := pkt.RemoteAddr(), pkt.LocalAddr(), pkt.LocalIfIdx()
//			pc.putNetworkPacket(pkt)
//			return nr, rAddr, lAddr, lIfIdx, nil
//		}
//		return 0, nil, nil, 0, io.EOF
//	case <-pc.rdDeadline.Wait():
//		return 0, nil, nil, 0, os.ErrDeadlineExceeded
//	}
//}
//
//func (pc *packetOutlet) Write(b []byte) (int, error) { return pc.WriteTo(b, pc.remoteAddr) }
//
//func (pc *packetOutlet) WriteTo(b []byte, addr net.Addr) (int, error) {
//	return pc.WriteToFrom(b, addr, pc.localAddr, pc.localIfIdx)
//}
//
//func (pc *packetOutlet) WriteToFrom(b []byte, rAddr, lAddr net.Addr, lIfIdx int) (int, error) {
//	n, err := pc.writeToFrom(b, rAddr, lAddr, lIfIdx)
//	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
//		if lAddr == nil {
//			lAddr = pc.LocalAddr()
//		}
//		err = &net.OpError{Op: "write", Net: pc.parent.pc.LocalAddr().Network(), Err: err, Source: lAddr, Addr: rAddr}
//	}
//	return n, err
//}
//
//func (pc *packetOutlet) writeToFrom(b []byte, rAddr, lAddr net.Addr, lIfIdx int) (int, error) {
//	switch {
//	case isClosedChan(pc.localDone):
//		return 0, io.ErrClosedPipe
//	case isClosedChan(pc.remoteDone):
//		return 0, io.ErrClosedPipe
//	case isClosedChan(pc.wrDeadline.Wait()):
//		return 0, os.ErrDeadlineExceeded
//	}
//
//	pc.wrMu.Lock()
//	defer pc.wrMu.Unlock()
//	deadlineChan := pc.wrDeadline.Wait()
//	pkt := pc.getNetworkPacket(b, rAddr, lAddr, lIfIdx)
//	select {
//	case pc.wrTx <- pkt:
//		select {
//		case n := <-pc.wrRx:
//			return n, nil
//		case <-pc.localDone:
//			return 0, io.ErrClosedPipe
//		case <-pc.remoteDone:
//			return 0, io.ErrClosedPipe
//		case <-deadlineChan:
//			return 0, os.ErrDeadlineExceeded
//		}
//	case <-pc.localDone:
//		return 0, io.ErrClosedPipe
//	case <-pc.remoteDone:
//		return 0, io.ErrClosedPipe
//	case <-deadlineChan:
//		return 0, os.ErrDeadlineExceeded
//	}
//}
//
//func (pc *packetOutlet) getNetworkPacket(b []byte, rAddr, lAddr net.Addr, lIfIdx int) NetworkPacket {
//	if rAddr == nil || pc.remoteAddr == nil || rAddr.Network() != pc.remoteAddr.Network() || rAddr.String() != pc.remoteAddr.String() {
//		if len(b) == 0 {
//			return &dataPacketWithSocketInfo{DataPacket: emptyPacket, remoteAddr: rAddr}
//		}
//		return &dataPacketWithSocketInfo{DataPacket: (*sliceDataPacket)(&b), remoteAddr: rAddr}
//	}
//
//	if len(b) == 0 {
//		return &dataPacketWithOutletPtr{DataPacket: emptyPacket, outlet: pc}
//	}
//	return &dataPacketWithOutletPtr{DataPacket: (*sliceDataPacket)(&b), outlet: pc}
//}
//
//func (pc *packetOutlet) putNetworkPacket(pkt NetworkPacket) {
//	if pkt == nil {
//		return
//	}
//
//	dpkt := pkt.Data()
//	if dpkt == nil || dpkt == emptyPacket {
//		return
//	}
//
//	if dp, ok := dpkt.(*uniformDataPacket); ok {
//		pc.parent.packetPool.Put(dp)
//	}
//}
//
//func (pc *packetOutlet) reportBytesWritten(n int) {
//	select {
//	case pc.wrRx <- n:
//	default:
//	}
//}
//
//func (pc *packetOutlet) Close() error {
//	pc.once.Do(func() { close(pc.localDone) })
//	return nil
//}
//
//func (pc *packetOutlet) LocalIfIdx() int { return pc.localIfIdx }
//
//func (pc *packetOutlet) LocalAddr() net.Addr {
//	if pc.localAddr == nil {
//		return pc.parent.pc.LocalAddr()
//	}
//	return pc.localAddr
//}
//
//func (pc *packetOutlet) RemoteAddr() net.Addr { return pc.remoteAddr }
//
//func (pc *packetOutlet) SetDeadline(t time.Time) error {
//	if isClosedChan(pc.localDone) || isClosedChan(pc.remoteDone) {
//		return io.ErrClosedPipe
//	}
//	pc.rdDeadline.Set(t)
//	pc.wrDeadline.Set(t)
//	return nil
//}
//
//func (pc *packetOutlet) SetReadDeadline(t time.Time) error {
//	if isClosedChan(pc.localDone) || isClosedChan(pc.remoteDone) {
//		return io.ErrClosedPipe
//	}
//	pc.rdDeadline.Set(t)
//	return nil
//}
//
//func (pc *packetOutlet) SetWriteDeadline(t time.Time) error {
//	if isClosedChan(pc.localDone) || isClosedChan(pc.remoteDone) {
//		return io.ErrClosedPipe
//	}
//	pc.wrDeadline.Set(t)
//	return nil
//}

type readablePacket struct {
	a net.Addr // Contains a remote address, if known
	b []byte   // Contains a number of bytes to read from
	n int      // Total readable bytes; 0 <= n <= len(b)
}

type writablePacket struct {
	a net.Addr
	n chan int
	b []byte
}

type streamOutlet struct {
	remoteAddr net.Addr
	rdPkt      *readablePacket // Bytes saved for partial reading

	// Used by local Read to interact with remote Write.
	// For a sync/semi-sync pipe, receive on rdRx is always followed by send on rdTx
	rdRx chan readablePacket
	// rdTx chan<- int

	// Used by local Write to interact with remote Read.
	// For a sync/semi-sync pipe, send on wrTx is always followed by receive on wrRx
	wrTx chan writablePacket
	wrRx chan int

	localDone  chan struct{}
	remoteDone <-chan struct{}

	rdDeadline Deadline
	wrDeadline Deadline

	// rdPart bool   // Partial reading switch
	rdIdx int

	once sync.Once  // Protects closing localDone
	rdMu sync.Mutex // Serialize Read operations
	wrMu sync.Mutex // Serialize Write operations
}

func (c *streamOutlet) Read(b []byte) (int, error) {
	n, err := c.read(b)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "read", Net: streamOutletNetwork, Err: err, Source: c.LocalAddr(), Addr: c.remoteAddr}
	}
	return n, err
}

func (c *streamOutlet) read(b []byte) (int, error) {
	switch {
	case isClosedChan(c.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(c.remoteDone) && c.rdPkt == nil && len(c.rdRx) == 0:
		return 0, io.EOF
	case isClosedChan(c.rdDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	}

	c.rdMu.Lock()
	defer c.rdMu.Unlock()
	switch {
	case isClosedChan(c.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(c.remoteDone) && c.rdPkt == nil && len(c.rdRx) == 0:
		return 0, io.EOF
	case isClosedChan(c.rdDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	case c.rdPkt != nil:
		nr := copy(b, c.rdPkt.b[c.rdIdx:c.rdPkt.n])
		c.rdIdx += nr
		if c.rdIdx >= c.rdPkt.n {
			c.rdPkt = nil
		}
		return nr, nil
	}

	select {
	case pkt := <-c.rdRx:
		nr := copy(b, pkt.b[:pkt.n])
		if nr < pkt.n {
			c.rdIdx, c.rdPkt = nr, &pkt
		}
		return nr, nil
	case <-c.localDone:
		return 0, io.ErrClosedPipe
	case <-c.remoteDone:
		// No need to check c.rdPkt here: unless it was empty, the function would return above
		if len(c.rdRx) > 0 {
			pkt := <-c.rdRx
			nr := copy(b, pkt.b[:pkt.n])
			if nr < pkt.n {
				c.rdIdx, c.rdPkt = nr, &pkt
			}
			return nr, nil
		}
		return 0, io.EOF
	case <-c.rdDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	}
}

func (c *streamOutlet) Write(b []byte) (int, error) {
	n, err := c.write(b)
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		err = &net.OpError{Op: "write", Net: streamOutletNetwork, Err: err, Source: c.LocalAddr(), Addr: c.remoteAddr}
	}
	return n, err
}

func (c *streamOutlet) write(b []byte) (int, error) {
	switch {
	case isClosedChan(c.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(c.remoteDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(c.wrDeadline.Wait()):
		return 0, os.ErrDeadlineExceeded
	}

	c.wrMu.Lock()
	defer c.wrMu.Unlock()
	deadlineChan := c.wrDeadline.Wait()
	pkt := writablePacket{a: c.remoteAddr, b: b, n: c.wrRx}
	select {
	case c.wrTx <- pkt:
		select {
		case n := <-c.wrRx:
			return n, nil
		case <-c.localDone:
			return 0, io.ErrClosedPipe
		case <-c.remoteDone:
			return 0, io.ErrClosedPipe
		case <-deadlineChan:
			return 0, os.ErrDeadlineExceeded
		}
	case <-c.localDone:
		return 0, io.ErrClosedPipe
	case <-c.remoteDone:
		return 0, io.ErrClosedPipe
	case <-deadlineChan:
		return 0, os.ErrDeadlineExceeded
	}
}

func (c *streamOutlet) Close() error {
	c.once.Do(func() { close(c.localDone) })
	return nil
}

func (c *streamOutlet) LocalAddr() net.Addr { return nil }

func (c *streamOutlet) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *streamOutlet) SetDeadline(t time.Time) error {
	if isClosedChan(c.localDone) || isClosedChan(c.remoteDone) {
		return io.ErrClosedPipe
	}
	c.rdDeadline.Set(t)
	c.wrDeadline.Set(t)
	return nil
}

func (c *streamOutlet) SetReadDeadline(t time.Time) error {
	if isClosedChan(c.localDone) || isClosedChan(c.remoteDone) {
		return io.ErrClosedPipe
	}
	c.rdDeadline.Set(t)
	return nil
}

func (c *streamOutlet) SetWriteDeadline(t time.Time) error {
	if isClosedChan(c.localDone) || isClosedChan(c.remoteDone) {
		return io.ErrClosedPipe
	}
	c.wrDeadline.Set(t)
	return nil
}

// Defaults
const (
	packetOutletNetwork = "packet_outlet"
	streamOutletNetwork = "stream_outlet"
)

// Interface guards
var (
	_ StreamConn = (*streamOutlet)(nil)
	_ MixedConn  = (*inletConn)(nil)
)
