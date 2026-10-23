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

package hybrid_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/vnxme/hybrid"
)

// A splitter serves each flow recognized by a handler with a split connection, and the flows released
// by their handlers with the fallback connection. A trunk stands in for a UDP socket here.
func ExampleSplit() {
	server := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 5353}
	client, pc := hybrid.BufferedTrunk(8, nil, server)
	ln := hybrid.Split(pc, &hybrid.SplitConfig{FallbackConn: true})
	defer func() { _ = ln.Close() }() // Closes pc too

	// The fallback connection serves the released flows together, e.g. as the PacketConn of a QUIC server
	go func() {
		fb := ln.Fallback()
		b := make([]byte, 1500)
		for {
			n, addr, err := fb.ReadFrom(b)
			if err != nil {
				return // net.ErrClosed once the splitter is closed
			}
			_, _ = fb.WriteTo(fmt.Appendf(nil, "fallback: %s", b[:n]), addr)
		}
	}()

	// Each split connection serves a single flow, as an accepted TCP connection does
	go func() {
		for {
			c, err := ln.AcceptSplit()
			if err != nil {
				return // net.ErrClosed once the splitter is closed
			}
			go func() {
				defer func() { _ = c.Close() }()
				b := make([]byte, 1500)
				n, err := c.Read(b)
				if err != nil {
					return
				}
				if string(b[:n]) != "ping" {
					_ = c.Release(b[:n]) // Hands the flow over, the packet read first
					return
				}
				_, _ = c.Write([]byte("pong")) // Goes to the client, from the address it sent to
			}()
		}
	}()

	b := make([]byte, 1500)
	for _, pkt := range []struct {
		from *net.UDPAddr
		data string
	}{
		{&net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 1111}, "ping"},
		{&net.UDPAddr{IP: net.IPv4(198, 51, 100, 2), Port: 2222}, "hello"},
	} {
		if _, err := client.WriteToFrom([]byte(pkt.data), server, pkt.from, 0); err != nil {
			log.Fatal(err)
		}
		n, src, dst, _, err := client.ReadFromTo(b)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v -> %v: %s\n", src, dst, b[:n])
	}
	// Output:
	// 192.0.2.1:5353 -> 198.51.100.1:1111: pong
	// 192.0.2.1:5353 -> 198.51.100.2:2222: fallback: hello
}

// A trunk keeps packet boundaries and addresses, as UDP does, so that several flows may share it,
// told apart by addresses.
func ExampleBufferedTrunk() {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1111}
	b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2222}
	c1, c2 := hybrid.BufferedTrunk(8, a, b)
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	other := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 3333}
	_, _ = c1.WriteTo([]byte("hello"), b)               // From the local address of c1
	_, _ = c1.WriteToFrom([]byte("world"), b, other, 0) // From another address
	_, _ = c1.WriteTo(nil, b)                           // An empty packet
	_, _ = c1.WriteTo([]byte("truncated"), b)           // Longer than the read buffer

	buf := make([]byte, 5)
	for range 4 {
		n, src, dst, _, err := c2.ReadFromTo(buf)
		fmt.Printf("%v -> %v: %q, truncated: %t\n", src, dst, buf[:n], errors.Is(err, io.ErrShortBuffer))
	}
	// Output:
	// 192.0.2.1:1111 -> 192.0.2.2:2222: "hello", truncated: false
	// 198.51.100.1:3333 -> 192.0.2.2:2222: "world", truncated: false
	// 192.0.2.1:1111 -> 192.0.2.2:2222: "", truncated: false
	// 192.0.2.1:1111 -> 192.0.2.2:2222: "trunc", truncated: true
}

// An unbuffered trunk behaves like UDP, while its writes return once read, so that a write with no reader
// blocks until its deadline.
func ExampleUnbufferedTrunk() {
	client := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 1111}
	server := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 5353}
	c1, c2 := hybrid.UnbufferedTrunk(client, server)
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c1.WriteTo([]byte("ping"), server) // Returns once c2 has read it
		b := make([]byte, 1500)
		n, src, err := c1.ReadFrom(b)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("client: %s from %v\n", b[:n], src)
	}()

	b := make([]byte, 1500)
	n, src, err := c2.ReadFrom(b)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("server: %s from %v\n", b[:n], src)
	_, _ = c2.WriteTo([]byte("pong"), src) // Returns once c1 has read it
	<-done

	_ = c1.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	_, err = c1.WriteTo([]byte("lost"), server) // Nothing reads it
	fmt.Println("timed out:", errors.Is(err, os.ErrDeadlineExceeded))
	// Output:
	// server: ping from 198.51.100.1:1111
	// client: pong from 192.0.2.1:5353
	// timed out: true
}

// A buffered pipe behaves like TCP, while its writes return once queued, so that a single goroutine may write
// and read.
func ExampleBufferedPipe() {
	c1, c2 := hybrid.BufferedPipe(8)
	defer func() { _ = c2.Close() }()

	_, _ = c1.Write([]byte("hello, "))
	_, _ = c1.Write([]byte("world"))
	_ = c1.Close() // c2 reads io.EOF once it has read all the bytes queued

	b, err := io.ReadAll(c2)
	fmt.Printf("%q, %v\n", b, err)
	// Output:
	// "hello, world", <nil>
}

// An unbuffered pipe behaves like TCP, while its writes return once read, as with [net.Pipe].
func ExampleUnbufferedPipe() {
	c1, c2 := hybrid.UnbufferedPipe(false)
	defer func() { _ = c2.Close() }()

	go func() {
		defer func() { _ = c1.Close() }()
		_, _ = fmt.Fprint(c1, "first line\nsecond ")
		_, _ = fmt.Fprint(c1, "line\n")
	}()

	s := bufio.NewScanner(c2) // Stream reads ignore write boundaries
	for s.Scan() {
		fmt.Println(s.Text())
	}
	// Output:
	// first line
	// second line
}

// PacketToStreamConn serves a single remote address of a PacketConn as a StreamConn, e.g. for code written
// for [net.Conn], and drops the packets from any other address if filter is set, as a dialed UDP socket does.
func ExamplePacketToStreamConn() {
	server := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 5353}
	client := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 1111}
	c1, c2 := hybrid.BufferedTrunk(8, client, server)
	conn := hybrid.PacketToStreamConn(c1, server, true)
	defer func() { _ = conn.Close() }()
	defer func() { _ = c2.Close() }()

	_, _ = fmt.Fprint(conn, "hello") // A packet to the remote address
	b := make([]byte, 1500)
	n, src, err := c2.ReadFrom(b)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v: %s\n", src, b[:n])

	other := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 1), Port: 4444}
	_, _ = c2.WriteToFrom([]byte("spoofed"), client, other, 0) // Dropped
	_, _ = c2.WriteTo([]byte("world"), client)
	n, err = conn.Read(b)
	fmt.Printf("%v: %s, %v\n", conn.RemoteAddr(), b[:n], err)
	// Output:
	// 198.51.100.1:1111: hello
	// 192.0.2.1:5353: world, <nil>
}

// StreamToPacketConn serves a StreamConn as a PacketConn, whose packets keep their boundaries if the StreamConn
// keeps those of writes, as a pipe does, while TCP doesn't.
func ExampleStreamToPacketConn() {
	s1, s2 := hybrid.BufferedPipe(8)
	c1, c2 := hybrid.StreamToPacketConn(s1), hybrid.StreamToPacketConn(s2)
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	_, _ = c1.WriteTo([]byte("hello"), nil) // The address is ignored, as a StreamConn has a single remote one
	_, _ = c1.WriteTo([]byte("world"), nil)

	b := make([]byte, 1500)
	for range 2 {
		n, _, err := c2.ReadFrom(b)
		fmt.Printf("%s, %v\n", b[:n], err)
	}
	// Output:
	// hello, <nil>
	// world, <nil>
}

// A UDP socket bound to an unspecified address replies from the address each client sent to, once wrapped
// with PacketInfoUDPConn.
func ExamplePacketInfoUDPConn() {
	lc := net.ListenConfig{Control: hybrid.ControlUDPPacketInfo} // Optional: PacketInfoUDPConn enables it too
	pc, err := lc.ListenPacket(context.Background(), "udp", ":5353")
	if err != nil {
		log.Fatal(err)
	}
	conn, err := hybrid.PacketInfoUDPConn(pc.(*net.UDPConn))
	if err != nil {
		log.Fatal(err) // E.g. on a platform without packet info
	}
	defer func() { _ = conn.Close() }()

	b := make([]byte, 64<<10)
	for {
		n, src, dst, idx, err := conn.ReadFromToUDPAddrPort(b)
		if err != nil {
			return
		}
		// Reply from the address the client sent to, and through the interface the packet came through
		_, _ = conn.WriteToFromUDPAddrPort(b[:n], src, dst, idx)
	}
}
