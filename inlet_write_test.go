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
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// TestInletDefaultWrite ensures the default connection writes to a remote address it has seen from the local
// address and through the interface of its latest flow, and lets the system choose them for another one.
func TestInletDefaultWrite(t *testing.T) {
	a, u := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 9), Port: 9}
	local := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}
	client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true})
	if _, err := client.WriteToFrom([]byte("a1"), local, a, 3); err != nil {
		t.Fatal(err)
	}
	def := ln.Default()
	_ = def.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 8)
	if _, addr, err := def.ReadFrom(b); err != nil || addr.String() != a.String() {
		t.Fatalf("ReadFrom() = %v, %v, want %s, nil", addr, err, a)
	}

	read := func() (string, net.Addr, net.Addr, int) {
		t.Helper()
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, src, dst, idx, err := client.ReadFromTo(b)
		if err != nil {
			t.Fatalf("ReadFromTo() = %v, want nil", err)
		}
		return string(b[:n]), src, dst, idx
	}

	if _, err := def.WriteTo([]byte("x"), a); err != nil {
		t.Fatalf("WriteTo(%s) = %v, want nil", a, err)
	}
	if data, src, dst, idx := read(); data != "x" || src.String() != local.String() || dst.String() != a.String() || idx != 3 {
		t.Errorf("ReadFromTo() = %q from %s to %s through %d, want %q from %s to %s through 3", data, src, dst, idx, "x", local, a)
	}

	if _, err := def.WriteTo([]byte("y"), u); err != nil {
		t.Fatalf("WriteTo(%s) = %v, want nil", u, err)
	}
	if data, src, dst, idx := read(); data != "y" || src.String() == local.String() || dst.String() != u.String() || idx != 0 {
		t.Errorf("ReadFromTo() = %q from %s to %s through %d, want %q from the system's choice to %s through 0", data, src, dst, idx, "y", u)
	}

	if _, err := def.WriteTo([]byte("z"), nil); err == nil {
		t.Errorf("WriteTo(nil) = nil, want an error")
	}
}

// TestInletWriteCopies ensures a write returns once its packet is queued, with a copy of the bytes given.
func TestInletWriteCopies(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, nil)
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)

	b := []byte("abc")
	if n, err := conn.Write(b); n != len(b) || err != nil {
		t.Fatalf("Write() = %d, %v, want %d, nil", n, err, len(b))
	}
	copy(b, "xyz") // As the caller may reuse the buffer once Write returns

	rb := make([]byte, 8)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := client.ReadFrom(rb); err != nil || string(rb[:n]) != "abc" {
		t.Errorf("ReadFrom() = %q, %v, want %q, nil", rb[:n], err, "abc")
	}
}

// TestInletWriteQueueFull ensures a write waits for room in a full write queue until its write deadline.
func TestInletWriteQueueFull(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(1, nil, nil) // Holds one packet, the next one blocks the write loop
	t.Cleanup(func() { _ = client.Close() })
	ln := Inlet(server, &InletConfig{WriteQueueSize: 1, WriteTimeout: -1})
	t.Cleanup(func() { _ = ln.Close() })
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)

	for _, data := range []string{"w1", "w2", "w3"} { // Into the trunk, then blocking the write loop, then queued
		if _, err := conn.Write([]byte(data)); err != nil {
			t.Fatalf("Write(%q) = %v, want nil", data, err)
		}
	}
	_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := conn.Write([]byte("w4")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("Write() to a full queue = %v, want os.ErrDeadlineExceeded", err)
	}

	var got []string
	rb := make([]byte, 8)
	for range 3 {
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := client.ReadFrom(rb)
		if err != nil {
			t.Fatalf("ReadFrom() = %v, want nil", err)
		}
		got = append(got, string(rb[:n]))
	}
	if strings.Join(got, " ") != "w1 w2 w3" {
		t.Errorf("packets written = %q, want those queued", got)
	}
}

// TestInletWriteTimeout ensures the write loop drops and counts a packet it can't write within the write timeout,
// and goes on writing.
func TestInletWriteTimeout(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(1, nil, nil)
	t.Cleanup(func() { _ = client.Close() })
	ln := Inlet(server, &InletConfig{WriteTimeout: 20 * time.Millisecond})
	t.Cleanup(func() { _ = ln.Close() })
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)

	for _, data := range []string{"w1", "w2", "w3"} { // Only the first one fits into the trunk
		if _, err := conn.Write([]byte(data)); err != nil {
			t.Fatalf("Write(%q) = %v, want nil", data, err)
		}
	}
	waitInletStats(t, ln, InletStats{WriteTimedOut: 2})

	if _, err := conn.Write([]byte("w4")); err != nil {
		t.Fatal(err)
	}
	var got []string
	rb := make([]byte, 8)
	for range 2 {
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := client.ReadFrom(rb)
		if err != nil {
			t.Fatalf("ReadFrom() = %v, want nil", err)
		}
		got = append(got, string(rb[:n]))
	}
	if strings.Join(got, " ") != "w1 w4" {
		t.Errorf("packets written = %q, want those not timed out", got)
	}
}

// failingWrites is a FromToPacketConn whose writes to the given address fail with the given error.
type failingWrites struct {
	FromToPacketConn
	to  net.Addr
	err error
}

func (c *failingWrites) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
	if dst != nil && dst.String() == c.to.String() {
		return 0, c.err
	}
	return c.FromToPacketConn.WriteToFrom(b, dst, src, idx)
}

// TestInletWriteFailed ensures the write loop drops and counts a packet it fails to write, and goes on
// writing, unless the underlying connection is closed, which closes the inlet.
func TestInletWriteFailed(t *testing.T) {
	a, bad := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 66), Port: 66}
	setup := func(t *testing.T, err error) (FromToPacketConn, InletListener) {
		client, server := BufferedTrunk(16, nil, nil)
		t.Cleanup(func() { _ = client.Close() })
		ln := Inlet(&failingWrites{server, bad, err}, &InletConfig{DefaultOnly: true})
		t.Cleanup(func() { _ = ln.Close() })
		return client, ln
	}

	t.Run("Failed", func(t *testing.T) {
		client, ln := setup(t, errors.New("unreachable"))
		if _, err := ln.Default().WriteTo([]byte("x"), bad); err != nil {
			t.Fatalf("WriteTo(%s) = %v, want nil, as the write fails later", bad, err)
		}
		waitInletStats(t, ln, InletStats{WriteFailed: 1})

		if _, err := ln.Default().WriteTo([]byte("y"), a); err != nil {
			t.Fatal(err)
		}
		rb := make([]byte, 8)
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		if n, _, err := client.ReadFrom(rb); err != nil || string(rb[:n]) != "y" {
			t.Errorf("ReadFrom() = %q, %v, want %q, nil", rb[:n], err, "y")
		}
	})

	t.Run("Closed", func(t *testing.T) {
		_, ln := setup(t, net.ErrClosed)
		if _, err := ln.Default().WriteTo([]byte("x"), bad); err != nil {
			t.Fatal(err)
		}
		accepted := make(chan error, 1)
		go func() {
			_, err := ln.Accept()
			accepted <- err
		}()
		select {
		case err := <-accepted:
			if !errors.Is(err, net.ErrClosed) {
				t.Errorf("Accept() = %v, want net.ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Errorf("Accept() hasn't returned, as the inlet hasn't closed")
		}
	})
}

// TestInletDefaultWriteOther ensures the default connection finds the flow of a remote address of a network
// other than UDP, and writes from its local address.
func TestInletDefaultWriteOther(t *testing.T) {
	c, local := testAddr("c"), testAddr("local")
	client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true})
	if _, err := client.WriteToFrom([]byte("c1"), local, c, 0); err != nil {
		t.Fatal(err)
	}
	_ = readDefault(t, ln.Default())
	if _, err := ln.Default().WriteTo([]byte("x"), c); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, src, dst, _, err := client.ReadFromTo(b); err != nil || src != local || dst != c {
		t.Errorf("ReadFromTo() = from %v to %v, %v, want from %s to %s, nil", src, dst, err, local, c)
	}
}
