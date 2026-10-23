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
	"net/netip"
)

// splitMode is how a splitter reads from and writes to its underlying connection, as Split picks it once
// for the interfaces the connection implements, so that the read loop and the write loop needn't tell them apart.
type splitMode interface {
	// read reads a packet into b, and fills in p: the key of its flow, its interface index, if any,
	// and its addresses, if read as net.Addr. It returns the number of bytes read and an error,
	// which matters only if no bytes have been read.
	read(b []byte, p *splitPacketInfo) (int, error)

	// write writes a packet to the underlying connection, with as many of the addresses and the interface
	// index it has been queued with as the connection takes.
	write(pkt *splitPacket) error
}

// newSplitMode returns the mode of a splitter over pc, enabling packet info on a UDPConn if packetInfo is set,
// and reports whether the mode reports destination addresses, so that they identify flows too.
func newSplitMode(pc PacketConn, packetInfo bool) (splitMode, bool) {
	switch c := pc.(type) {
	case FromToUDPConn:
		return splitFromToUDPMode{c}, true
	case FromToPacketConn:
		return splitFromToPacketMode{c}, true
	case UDPConn:
		if packetInfo {
			if u, err := PacketInfoUDPConn(c); err == nil {
				return splitFromToUDPMode{u}, true
			}
		}
		return splitUDPMode{c}, false
	}
	return splitPacketMode{pc}, false
}

// splitPacketMode reads the source address of each packet from a PacketConn, and writes packets to their
// destination addresses only.
type splitPacketMode struct{ pc PacketConn }

func (m splitPacketMode) read(b []byte, p *splitPacketInfo) (n int, err error) {
	n, p.src, err = m.pc.ReadFrom(b)
	if p.src != nil {
		p.key.src = addrKeyOf(p.src)
	}
	return n, err
}

func (m splitPacketMode) write(pkt *splitPacket) error {
	_, err := m.pc.WriteTo(pkt.buf, pkt.dst)
	return err
}

// splitFromToPacketMode reads the source and destination addresses and the interface index of each packet
// from a FromToPacketConn, and writes packets with them.
type splitFromToPacketMode struct{ pc FromToPacketConn }

func (m splitFromToPacketMode) read(b []byte, p *splitPacketInfo) (n int, err error) {
	n, p.src, p.dst, p.idx, err = m.pc.ReadFromTo(b)
	if p.src != nil {
		p.key.src = addrKeyOf(p.src)
	}
	if p.dst != nil {
		p.key.dst = addrKeyOf(p.dst)
	}
	return n, err
}

func (m splitFromToPacketMode) write(pkt *splitPacket) error {
	_, err := m.pc.WriteToFrom(pkt.buf, pkt.dst, pkt.src, pkt.idx)
	return err
}

// splitUDPMode reads the source address of each packet from a UDPConn as a netip.AddrPort, which allocates
// nothing, unmapped as packet info reports it, and writes packets to their destination addresses only.
type splitUDPMode struct{ pc UDPConn }

func (m splitUDPMode) read(b []byte, p *splitPacketInfo) (int, error) {
	n, src, err := m.pc.ReadFromUDPAddrPort(b)
	p.key.src.ap = netip.AddrPortFrom(src.Addr().Unmap(), src.Port()) // Zero if src is
	return n, err
}

func (m splitUDPMode) write(pkt *splitPacket) error {
	_, err := m.pc.WriteTo(pkt.buf, pkt.dst)
	return err
}

// splitFromToUDPMode reads the source and destination addresses of each packet from a FromToUDPConn
// as netip.AddrPort values, which allocates nothing, along with its interface index, and writes packets
// with them.
type splitFromToUDPMode struct{ pc FromToUDPConn }

func (m splitFromToUDPMode) read(b []byte, p *splitPacketInfo) (n int, err error) {
	n, p.key.src.ap, p.key.dst.ap, p.idx, err = m.pc.ReadFromToUDPAddrPort(b)
	return n, err
}

func (m splitFromToUDPMode) write(pkt *splitPacket) error {
	_, err := m.pc.WriteToFrom(pkt.buf, pkt.dst, pkt.src, pkt.idx)
	return err
}
