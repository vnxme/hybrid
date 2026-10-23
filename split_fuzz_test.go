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
	"encoding/binary"
	"net"
	"sync"
	"testing"
)

// FuzzSplit writes a packet to a splitter over a trunk for each byte of the input, from one of 8 clients the byte
// picks, and ensures each packet is read once, by the split connection of its flow or by the fallback connection,
// in the order of writing, or counted as dropped. The handler of each flow serves it as the first packet it reads
// tells: it reads every packet, releases the flow with the packet or without it, or closes the connection, so
// that the next packet starts a new flow. The queues and the flow table are small, so as to fill up. Once all
// the packets are written, the client closes the trunk, so that the splitter reads them all, then closes.
func FuzzSplit(f *testing.F) {
	f.Add([]byte{0x00, 0x01, 0x00, 0x01})                   // Two flows read in full
	f.Add([]byte{0x08, 0x08, 0x08, 0x10, 0x10, 0x10})       // Flows released with the packet read and without it
	f.Add([]byte{0x18, 0x18, 0x18, 0x00, 0x00})             // A flow closed, then started anew
	f.Add([]byte{0x00, 0x09, 0x12, 0x1b, 0x04, 0x05, 0x0e}) // More flows than the table and the accept queue take
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<10 {
			t.Skip()
		}

		server := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}
		client, pc := BufferedTrunk(len(data)+1, nil, server)
		ln := Split(pc, &SplitConfig{
			AcceptQueueSize:   2,
			ReadQueueSize:     2,
			FallbackQueueSize: 2,
			IdleTimeout:       -1,
			MaxFlows:          4,
			FallbackConn:      true,
		})
		defer func() { _ = ln.Close() }()

		var mu sync.Mutex
		read := make(map[uint16]bool, len(data)) // The packets read for good, i.e. not released
		last := make(map[string]int)             // The latest packet read of each split connection or client
		check := func(who string, b []byte) {
			if len(b) != 3 {
				t.Errorf("%s read %x, want 3 bytes", who, b)
				return
			}
			seq := binary.BigEndian.Uint16(b)
			mu.Lock()
			defer mu.Unlock()
			if read[seq] {
				t.Errorf("%s read packet %d again", who, seq)
			}
			read[seq] = true
			if prev, ok := last[who]; ok && int(seq) <= prev {
				t.Errorf("%s read packet %d after packet %d", who, seq, prev)
			}
			last[who] = int(seq)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			fb := ln.Fallback()
			defer func() { _ = fb.Close() }() // Counts the packets released after the splitter has closed
			b := make([]byte, 16)
			for {
				n, addr, err := fb.ReadFrom(b)
				if err != nil {
					return
				}
				if want := clientAddr(b[2]); addr.String() != want.String() {
					t.Errorf("fallback read packet %x from %v, want %v", b[:n], addr, want)
				}
				check("fallback from "+addr.String(), b[:n])
			}
		})
		wg.Go(func() {
			for {
				c, err := ln.AcceptSplit()
				if err != nil {
					return
				}
				wg.Go(func() {
					defer func() { _ = c.Close() }()
					who := "split connection " + c.RemoteAddr().String()
					b := make([]byte, 16)
					for first := true; ; first = false {
						n, err := c.Read(b)
						if err != nil {
							return
						}
						if want := clientAddr(b[2]); c.RemoteAddr().String() != want.String() {
							t.Errorf("%s read packet %x of %v", who, b[:n], want)
						}
						if !first {
							check(who, b[:n])
							continue
						}
						switch b[2] >> 3 & 3 {
						case 1: // Hands the packet over too
							_ = c.Release(b[:n])
							return
						case 2:
							check(who, b[:n])
							_ = c.Release()
							return
						case 3:
							check(who, b[:n])
							return
						default:
							check(who, b[:n])
						}
					}
				})
			}
		})

		for i, x := range data {
			pkt := append(binary.BigEndian.AppendUint16(nil, uint16(i)), x) // Unique, with what the handler does
			_, err := client.WriteToFrom(pkt, server, clientAddr(x), 0)
			if err != nil {
				t.Fatal(err)
			}
		}
		_ = client.Close()
		wg.Wait()
		_ = ln.Close() // Waits until the splitter, closing as it has read the closed trunk, counts the packets left

		s := ln.Stats()
		dropped := s.Truncated + s.NoDestination + s.TableFull + s.AcceptQueueFull + s.ConnQueueFull + s.ConnClosed +
			s.NoFallback + s.FallbackQueueFull + s.FallbackClosed
		if got := uint64(len(read)) + dropped; got != uint64(len(data)) {
			t.Errorf("read %d packets and dropped %d (%+v), want %d in all", len(read), dropped, s, len(data))
		}
	})
}

// clientAddr returns the address of the client that x picks, out of 8.
func clientAddr(x byte) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1+x&7), Port: 1000 + int(x&7)}
}
