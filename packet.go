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

import "net"

type dataPacketWithOutletPtr struct {
	DataPacket
	outlet *splitConn // Pointer to the outlet this packet goes to / comes from
}

func (p *dataPacketWithOutletPtr) Data() DataPacket { return p.DataPacket }

func (p *dataPacketWithOutletPtr) LocalIfIdx() int { return p.outlet.LocalIfIdx() }

func (p *dataPacketWithOutletPtr) LocalAddr() net.Addr { return p.outlet.localAddr }

func (p *dataPacketWithOutletPtr) RemoteAddr() net.Addr { return p.outlet.remoteAddr }

func (p *dataPacketWithOutletPtr) Reset() {
	p.DataPacket = nil
	p.outlet = nil
}

type dataPacketWithRemoteAddr struct {
	DataPacket
	remoteAddr net.Addr
}

func (p *dataPacketWithRemoteAddr) Data() DataPacket { return p.DataPacket }

func (p *dataPacketWithRemoteAddr) LocalIfIdx() int { return 0 }

func (p *dataPacketWithRemoteAddr) LocalAddr() net.Addr { return nil }

func (p *dataPacketWithRemoteAddr) RemoteAddr() net.Addr { return p.remoteAddr }

func (p *dataPacketWithRemoteAddr) Reset() {
	p.DataPacket = nil
	p.remoteAddr = nil
}

type dataPacketWithSocketInfo struct {
	DataPacket
	remoteAddr net.Addr
	localAddr  net.Addr
	localIfIdx int
}

func (p *dataPacketWithSocketInfo) Data() DataPacket { return p.DataPacket }

func (p *dataPacketWithSocketInfo) LocalIfIdx() int { return p.localIfIdx }

func (p *dataPacketWithSocketInfo) LocalAddr() net.Addr { return p.localAddr }

func (p *dataPacketWithSocketInfo) RemoteAddr() net.Addr { return p.remoteAddr }

func (p *dataPacketWithSocketInfo) Reset() {
	p.DataPacket = nil
	p.remoteAddr = nil
	p.localAddr = nil
	p.localIfIdx = 0
}

type emptyDataPacket struct{}

func (p *emptyDataPacket) Cap() int { return 0 }

func (p *emptyDataPacket) Len() int { return 0 }

func (p *emptyDataPacket) ReadableBuffer() []byte { return emptyBuffer }

func (p *emptyDataPacket) WritableBuffer() []byte { return emptyBuffer }

func (p *emptyDataPacket) Reset() {
	return
}

type sliceDataPacket []byte

func (p *sliceDataPacket) Cap() int { return len(*p) }

func (p *sliceDataPacket) Len() int { return len(*p) }

func (p *sliceDataPacket) ReadableBuffer() []byte { return *p }

func (p *sliceDataPacket) WritableBuffer() []byte { return *p }

func (p *sliceDataPacket) Reset() {
	return
}

type uniformDataPacket struct {
	b []byte // Contains some bytes to read / write
	n int    // Total readable bytes; 0 <= n <= len(b)
}

func (p *uniformDataPacket) Cap() int { return len(p.b) }

func (p *uniformDataPacket) Len() int { return p.n }

func (p *uniformDataPacket) ReadableBuffer() []byte { return p.b[:p.n] }

func (p *uniformDataPacket) WritableBuffer() []byte { return p.b }

func (p *uniformDataPacket) Reset() {
	p.b, p.n = []byte{}, 0
}

// Shared variables
var (
	emptyBuffer []byte
	emptyPacket = &emptyDataPacket{}
)

// Interface guards
var (
	_ DataPacket = (*emptyDataPacket)(nil)
	_ DataPacket = (*sliceDataPacket)(nil)
	_ DataPacket = (*uniformDataPacket)(nil)

	_ NetworkPacket = (*dataPacketWithOutletPtr)(nil)
	_ NetworkPacket = (*dataPacketWithRemoteAddr)(nil)
	_ NetworkPacket = (*dataPacketWithSocketInfo)(nil)
)
