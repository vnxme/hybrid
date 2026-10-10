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

// TestInletRelease ensures releasing an inlet connection hands the packets queued and the later packets
// of its flow over to the fallback connection, in order, while the other flows go on as they were.
func TestInletRelease(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	conn := acceptInletConn(t, ln)
	if err := conn.(interface{ Release() error }).Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	writeFrom(t, client, a, "a3")
	writeFrom(t, client, b, "b1")

	if got, want := strings.Join(readFallback(t, ln.Fallback()), " "), a.String()+":a1 "+a.String()+":a2 "+a.String()+":a3"; got != want {
		t.Errorf("packets of the fallback connection = %q, want %q", got, want)
	}
	if other := acceptInletConn(t, ln); other.RemoteAddr().String() != b.String() {
		t.Errorf("RemoteAddr() = %s, want %s", other.RemoteAddr(), b)
	}
	if _, err := conn.Read(make([]byte, 8)); err == nil {
		t.Errorf("Read() after Release() = nil, want an error")
	}
	if err := conn.(interface{ Release() error }).Release(); err == nil {
		t.Errorf("Release() again = nil, want an error")
	}
	if err := conn.Close(); err == nil {
		t.Errorf("Close() after Release() = nil, want an error")
	}
	waitInletStats(t, ln, InletStats{})
}

// TestInletReleaseWithoutFallback ensures releasing an inlet connection without a fallback connection drops
// the packets of its flow, without starting a new flow.
func TestInletReleaseWithoutFallback(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, nil)
	if ln.Fallback() != nil {
		t.Fatalf("Fallback() = %v, want nil", ln.Fallback())
	}
	writeFrom(t, client, a, "a1")
	if err := acceptInletConn(t, ln).(interface{ Release() error }).Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	writeFrom(t, client, a, "a2")
	writeFrom(t, client, b, "b1")
	waitInletStats(t, ln, InletStats{NoFallback: 2})
	if conn := acceptInletConn(t, ln); conn.RemoteAddr().String() != b.String() { // Not a new flow of a
		t.Errorf("RemoteAddr() = %s, want %s", conn.RemoteAddr(), b)
	}
}

// TestInletDeadlines ensures the read and write deadlines of inlet connections and of the fallback connection.
func TestInletDeadlines(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)
	_ = readInletConn(t, conn)

	past := time.Now().Add(-time.Second)
	b := make([]byte, 8)
	for name, err := range map[string]error{
		"Read":             func() error { _ = conn.SetDeadline(past); _, err := conn.Read(b); return err }(),
		"Write":            func() error { _, err := conn.Write(b); return err }(),
		"FallbackReadFrom": func() error { _ = ln.Fallback().SetDeadline(past); _, _, err := ln.Fallback().ReadFrom(b); return err }(),
		"FallbackWriteTo": func() error {
			_ = ln.Fallback().SetWriteDeadline(past)
			_, err := ln.Fallback().WriteTo(b, a)
			return err
		}(),
	} {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("%s() after the deadline = %v, want os.ErrDeadlineExceeded", name, err)
		}
	}

	// A deadline in the future lets a waiting read time out
	_ = ln.Fallback().SetDeadline(time.Now().Add(20 * time.Millisecond))
	if _, _, err := ln.Fallback().ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("ReadFrom() of the fallback connection = %v, want os.ErrDeadlineExceeded", err)
	}
	if err := ln.Fallback().Close(); err != nil {
		t.Fatal(err)
	}
	if err := ln.Fallback().Close(); err == nil {
		t.Errorf("Close() of the fallback connection again = nil, want an error")
	}
	if _, _, err := ln.Fallback().ReadFrom(b); err == nil {
		t.Errorf("ReadFrom() of the closed fallback connection = nil, want an error")
	}
}
