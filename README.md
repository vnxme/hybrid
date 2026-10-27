# hybrid

[![CI](https://github.com/vnxme/hybrid/actions/workflows/ci.yml/badge.svg)](https://github.com/vnxme/hybrid/actions/workflows/ci.yml)
[![Vulnerabilities](https://github.com/vnxme/hybrid/actions/workflows/vuln.yml/badge.svg)](https://github.com/vnxme/hybrid/actions/workflows/vuln.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/vnxme/hybrid/badges/coverage.json)](https://github.com/vnxme/hybrid/actions/workflows/ci.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/vnxme/hybrid/badge)](https://scorecard.dev/viewer/?uri=github.com/vnxme/hybrid)
[![Go Reference](https://pkg.go.dev/badge/github.com/vnxme/hybrid.svg)](https://pkg.go.dev/github.com/vnxme/hybrid)
[![Go Version](https://img.shields.io/github/go-mod/go-version/vnxme/hybrid)](go.mod)
[![License](https://img.shields.io/github/license/vnxme/hybrid)](LICENSE)

Package `hybrid` provides versatile network types for building TCP- and UDP-based applications, proxies in
particular, and for testing them without sockets:

- **Splitter**: serves a single UDP socket, or any other packet connection, to many handlers at once,
  the way a TCP listener serves a port to many connections, so that UDP can be proxied the same way as TCP.
  It is designed to be integrated into [caddy-l4](https://github.com/mholt/caddy-l4), the layer 4 proxy module
  of [Caddy](https://caddyserver.com).
- **Packet info**: makes a UDP socket bound to an unspecified address reply from the address each client
  sent to, on every major platform.
- **Pipes** and **trunks**: in-memory stream and packet connections that behave like TCP and UDP respectively,
  for testing network code deterministically.
- **Wrappers**: small adapters changing one aspect of a connection, e.g. its addresses, its lifetime or its
  orientation (packet to stream, and back).
- **Deadlines**: reusable, allocation-free deadline primitives with optional hooks.

The name reflects the core idea: a `Conn` is a *hybrid* of a packet connection and a stream connection,
which keeps packet boundaries while serving a single remote address, so that a UDP flow can be handled by
code written for `net.Conn`.

## Contents

- [Installation](#installation)
- [Overview](#overview)
- [Interfaces](#interfaces)
- [Splitter](#splitter)
- [Packet info](#packet-info)
- [Pipes](#pipes)
- [Trunks](#trunks)
- [Wrappers](#wrappers)
- [Deadlines](#deadlines)
- [Error semantics](#error-semantics)
- [Platform support](#platform-support)
- [Testing](#testing)
- [Status](#status)
- [Contributing](#contributing)
- [Security](#security)
- [Credits](#credits)
- [License](#license)

## Installation

```sh
go get github.com/vnxme/hybrid
```

Go 1.26 or newer is required.

## Overview

| Type or function                                                                                       | Kind        | What it is                                                               |
|--------------------------------------------------------------------------------------------------------|-------------|--------------------------------------------------------------------------|
| [`Split`](#splitter)                                                                                    | constructor | A listener of split connections over a packet connection                 |
| [`SplitListener`](#splitter), [`SplitConn`](#splitter), [`SplitConfig`](#configuration), [`SplitStats`](#statistics) | types       | The listener, its connections, configuration and drop counters           |
| [`PacketInfoUDPConn`](#packet-info)                                                                     | constructor | A UDP connection that reads and writes local addresses and interfaces    |
| [`NeedPacketInfoUDPConn`](#packet-info), [`ControlUDPPacketInfo`](#packet-info)                         | functions   | Whether a socket needs packet info, and enabling it at socket creation   |
| [`UnbufferedPipe`](#pipes), [`BufferedPipe`](#pipes)                                                    | constructors| In-memory `StreamConn` pairs behaving like TCP                           |
| [`UnbufferedTrunk`](#trunks), [`BufferedTrunk`](#trunks)                                                | constructors| In-memory `FromToPacketConn` pairs behaving like UDP                     |
| [`FakeAddressPacketConn`](#addresses), [`FakeAddressStreamConn`](#addresses)                            | wrappers    | Shadow local and remote addresses                                        |
| [`FakeClosePacketConn`](#close), [`FakeCloseStreamConn`](#close)                                        | wrappers    | Shadow `Close`                                                           |
| [`IdleTimeoutPacketConn`](#idle-timeout), [`IdleTimeoutStreamConn`](#idle-timeout)                      | wrappers    | Close a connection once idle                                             |
| [`PacketToStreamConn`](#adapters), [`StreamToPacketConn`](#adapters), [`PacketToFromToPacketConn`](#adapters) | adapters    | Change the orientation or capabilities of a connection                   |
| [`SimpleDeadline`](#deadlines), [`HookDeadline`](#deadlines)                                            | constructors| Channel-based deadlines, without or with a hook                          |

## Interfaces

The package builds on the standard library rather than replacing it: `PacketConn`, `StreamConn`,
`StreamListener` and `PacketListener` are aliases of `net` types, so any `net` connection or listener fits,
and every type of the package can be used wherever a `net` type is expected.

| Interface              | Definition                                    | Satisfied by                                                        |
|------------------------|-----------------------------------------------|---------------------------------------------------------------------|
| `PacketConn`           | alias of `net.PacketConn`                     | `*net.UDPConn`, trunks, the fallback connection of a splitter       |
| `StreamConn`           | alias of `net.Conn`                           | `*net.TCPConn`, pipes, split connections                            |
| `StreamListener`       | alias of `net.Listener`                       | `*net.TCPListener`                                                  |
| `PacketListener`       | alias of `net.Listener`                       | `SplitListener`                                                     |
| `Conn`                 | `PacketConn` + `Read`, `Write`, `RemoteAddr`  | dialed `*net.UDPConn`, split connections                            |
| `FromToReader`         | `ReadFromTo`                                  | trunks, `FromToUDPConn`, split and fallback connections             |
| `FromToWriter`         | `WriteToFrom`                                 | trunks, `FromToUDPConn`, split and fallback connections             |
| `FromToPacketConn`     | `PacketConn` + `FromToReader` + `FromToWriter`| trunks, split connections, the fallback connection                  |
| `UDPConn`              | the methods of `*net.UDPConn`                 | `*net.UDPConn`                                                      |
| `FromToUDPConn`        | `UDPConn` + `FromTo*` + UDP-specific variants | `PacketInfoUDPConn`                                                 |
| `Wrapper[T]`           | `Unwrap() T`                                  | wrappers, adapters, `PacketInfoUDPConn`, `SplitListener`            |
| `WrappingPacketConn`   | `PacketConn` + `Wrapper[PacketConn]`          | the `PacketConn` wrappers                                           |
| `WrappingStreamConn`   | `StreamConn` + `Wrapper[StreamConn]`          | the `StreamConn` wrappers                                           |
| `Stopper`              | `Stop() error`                                | idle timeout wrappers, `SplitListener`                              |
| `StoppablePacketConn`  | `WrappingPacketConn` + `Stopper`              | `IdleTimeoutPacketConn`                                             |
| `StoppableStreamConn`  | `WrappingStreamConn` + `Stopper`              | `IdleTimeoutStreamConn`                                             |
| `StreamOverPacketConn` | `StreamConn` + `Wrapper[PacketConn]`          | `PacketToStreamConn`                                                |
| `PacketOverStreamConn` | `PacketConn` + `Wrapper[StreamConn]`          | `StreamToPacketConn`                                                |
| `Deadline`             | `Set(time.Time)`, `Wait() <-chan struct{}`    | `SimpleDeadline`, `HookDeadline`                                    |

### Conn

`Conn` is a hybrid of a `PacketConn` and a `StreamConn`: a packet-oriented connection serving a single remote
address, which it reads packets from with `Read` and writes packets to with `Write`. So it's a `StreamConn`
as well, but one keeping packet boundaries. A packet longer than the buffer of `Read` is truncated, silently or
with an error, e.g. `io.ErrShortBuffer`, depending on the connection, and the rest of it is lost, so reading
a packet in parts, e.g. with `io.ReadFull`, fails. Any `*net.UDPConn`, `*net.IPConn` or `*net.UnixConn`
satisfies `Conn`, but serves a single remote address only if dialed.

### FromToReader and FromToWriter

A plain `PacketConn` tells only the remote address of a packet. The from-to interfaces add the local address
and the local interface index, which matter when a socket is bound to an unspecified address:

```go
ReadFromTo(p []byte) (n int, src, dst net.Addr, idx int, err error)
WriteToFrom(p []byte, dst, src net.Addr, idx int) (n int, err error)
```

`ReadFromTo` returns the address a packet came from (`src`), the local address it came to (`dst`) and the index
of the interface it came through (`idx`). The local address is nil and the index is zero if the connection
doesn't know them, e.g. a UDP connection without packet info. `WriteToFrom` writes to `dst` from `src` through
`idx`, where a nil `src` and a zero `idx` leave them to the system. `FromToUDPConn` adds the variants taking
`*net.UDPAddr` and `netip.AddrPort`, the way `*net.UDPConn` does.

### Listeners

`PacketListener` is a `net.Listener` whose `Accept` returns a `Conn`, e.g. a `SplitConn`, as a `StreamConn`.
A listener may also have a method returning the very type, e.g. `AcceptSplit` of a `SplitListener`, as
`*net.TCPListener` has `AcceptTCP`. So a splitter plugs into any server loop written for `net.Listener`.

### Wrapper and Stopper

`Wrapper[T]` unwraps the underlying value of type `T`. `T` must be the very type `Unwrap` returns, e.g. a wrapper
returning a `FromToPacketConn` isn't a `Wrapper[PacketConn]`. The wrappers that keep the type of a connection
return a `WrappingPacketConn` or a `WrappingStreamConn`, so `Unwrap` needs no type assertion. So do the adapters
changing the orientation of a connection, which return a `StreamOverPacketConn` or a `PacketOverStreamConn`,
whose `Unwrap` returns the connection of the other orientation.

`Stopper` stops what a wrapper or a splitter does on its own, e.g. closing an idle connection or reading packets
from it, while leaving the underlying connection open, typically for the caller to take it over with `Unwrap`.
The wrappers that do something on their own return a `StoppablePacketConn` or a `StoppableStreamConn`, so neither
`Stop` nor `Unwrap` needs a type assertion.

## Splitter

A splitter serves a packet connection, typically a UDP socket, to many handlers at once, the way a TCP
listener serves a socket to many connections. Its read loop reads every packet, tells the flows apart, and
passes each flow either to a *split connection*, which a handler serves like an accepted TCP connection, or to
the *fallback connection*, which serves all the other flows together, e.g. as the packet connection of a QUIC
server, which tells QUIC connections apart itself. Its write loop writes the packets of all those connections.

```text
                                   ┌──────────────── splitter ───────────────┐
                                   │                                         │  AcceptSplit
 client A ─┐                       │            ┌─▶ split connection A ──────┼──▶ handler A
 client B ─┼──▶ PacketConn ──▶ read loop ──flows┼─▶ split connection B ──────┼──▶ handler B
 client C ─┘    (e.g. UDP)         │            └─▶ fallback connection ─────┼──▶ e.g. QUIC server
                     ▲             │                  (released flows)       │
                     └──────── write loop ◀── writes of all connections ─────┤
                                   └─────────────────────────────────────────┘
```

### Usage

```go
pc, err := net.ListenPacket("udp", ":5353")
if err != nil {
	log.Fatal(err)
}
ln := hybrid.Split(pc, &hybrid.SplitConfig{
	FallbackConn: true, // Serve the flows released by their handlers
	PacketInfo:   true, // Reply from the address each client sent to
})
defer ln.Close() // Closes pc too

go serveFallback(ln.Fallback()) // E.g. a QUIC server taking a net.PacketConn

for {
	c, err := ln.AcceptSplit()
	if err != nil {
		break // net.ErrClosed once the splitter is closed
	}
	go func() {
		defer c.Close() // Ends the flow, so that its next packet starts a new one
		b := make([]byte, 64<<10)
		n, err := c.Read(b)
		if err != nil {
			return
		}
		if !recognized(b[:n]) {
			_ = c.Release(b[:n]) // Hand the flow over to the fallback connection, the packet read first
			return
		}
		_, _ = c.Write(reply(b[:n])) // Goes to the client, from the address it sent to
	}()
}
```

`Accept` returns the same split connections as `StreamConn`s, so a splitter can be passed wherever
a `net.Listener` is expected, e.g. to a generic proxy server loop.

### Key methods

| Method                         | Description                                                                                         |
|--------------------------------|-----------------------------------------------------------------------------------------------------|
| `Split(pc, config)`            | Returns a `SplitListener` over `pc` and starts its loops. It takes `pc` over, clearing its deadlines |
| `SplitListener.AcceptSplit()`  | Returns the split connection of the next new flow                                                   |
| `SplitListener.Accept()`       | The same as `AcceptSplit`, returning a `StreamConn`                                                 |
| `SplitListener.Fallback()`     | Returns the fallback connection, or nil if `SplitConfig.FallbackConn` isn't set                     |
| `SplitListener.Stats()`        | Returns the numbers of packets dropped so far, by reason                                            |
| `SplitListener.Close()`        | Stops the splitter and closes `pc`                                                                  |
| `SplitListener.Stop()`         | Stops the splitter, but leaves `pc` open, with no deadlines, for the caller to take over            |
| `SplitListener.Unwrap()`       | Returns `pc` as given to `Split`                                                                    |
| `SplitConn.Release(pkts...)`   | Hands the flow over to the fallback connection, the packets given first, and closes the connection  |
| `SplitConn.Read`, `ReadFrom`, `ReadFromTo` | Read a packet of the flow; a short buffer gives `io.ErrShortBuffer`                     |
| `SplitConn.Write`, `WriteTo`, `WriteToFrom` | Write a packet to the client; another address fails with `net.ErrWriteToConnected`     |

### Flows

A flow of a connection that reports the destination address of each packet, e.g. of a UDP connection with packet
info or of a trunk, is identified by its source and destination addresses, and its split connection replies from
that destination address. A flow of any other connection, e.g. of a UDP connection without packet info, is
identified by its source address alone. Which addresses identify a flow is decided once per splitter, not per
packet, so that a flow stays whole: a splitter that uses destination addresses drops a packet without one. To split
a connection that reports destination addresses by source address alone, e.g. a trunk emulating a UDP socket
without packet info, wrap it so as to hide them, e.g. with `FakeAddressPacketConn(pc, nil, nil)`. The interface
index isn't part of a flow, as it may change, e.g. with asymmetric routing: the latest one is kept and used for
replies.

How to read from and write to the connection is decided once too, by the interfaces it implements: a
`FromToUDPConn`, any other `FromToPacketConn`, a `UDPConn`, with packet info enabled on it if
`SplitConfig.PacketInfo` is set and possible, or any other `PacketConn`. A UDP connection is read with
`netip.AddrPort` values, so that its packets are read and their flows looked up without allocating. IPv4 addresses
read from a dual-stack socket are reported unmapped, with packet info or without, as flows are keyed by them: e.g.
the `RemoteAddr` of a split connection is `127.0.0.1:1234` rather than `[::ffff:127.0.0.1]:1234`.

### Lifecycle

- The first packet of an unknown flow creates a split connection, queues it for `Accept`, and queues the packet
  on it.
- A handler accepting the connection either serves it or releases it, e.g. as no matcher recognizes the flow:
  then the packets it has read and hands back to `Release`, the packets queued on the connection and all the later
  packets of the flow go to the fallback connection, in this order, if there is one, or are dropped if there isn't.
- Once the fallback connection is closed, a released flow ends instead, so that its next packet is offered
  to `Accept` again, e.g. as the server behind the fallback connection has stopped.
- A flow ends when its connection is closed, by its handler or by its idle timeout, and its next packet starts
  a new flow straight away: UDP has nothing like TCP's TIME_WAIT and RST to tell a late packet of an old flow
  from the first packet of a new one.
- Once the splitter stops, `Accept` fails, and the split connections it hasn't returned are closed, while the others
  and the fallback connection return the packets queued, then `net.ErrClosed`. A packet left unread is counted
  as `ConnClosed` or `FallbackClosed` once its connection is closed.
- Only packets read start flows: a write keeps the flow of its remote address going, but a reply to a remote
  address without a flow, e.g. written by the fallback connection, makes its response start a new flow, queued
  for `Accept` like any other.

### Deadlines and timeouts

Split connections and the fallback connection have read and write deadlines of their own, like any `net.Conn`
or `net.PacketConn`. Each flow has an idle timeout, which every packet read or written refreshes; a periodic
sweeper ends idle flows within one and a quarter idle timeouts of their last packet. Only the read loop sets the
read deadline of the underlying connection, and only the write loop sets its write deadline: as the deadlines of
a socket are shared by everything using it, each has a single user.

### Reading and writing

The read loop changes the flow table alone, so a packet takes no lock on its way. It reads into a buffer as
large as the largest datagram, and copies each packet into a pooled buffer of its size before queueing it.

A write copies its packet into a pooled buffer and returns once the packet is queued for the write loop, the way
a UDP write returns once the system has copied it: if the write queue is full, it waits for room until the write
deadline of its connection. So a write can't report an error of the underlying connection: the write loop counts
it, and closes the splitter if the underlying connection fails for good. The write timeout of the splitter limits
how long the write loop waits on the underlying connection for one packet, so that a stuck write can't hold every
flow up.

The splitter leaves the socket options as they are. Notably, macOS and the BSDs refuse to send a datagram larger
than the send buffer of a socket, which is 9216 bytes by default, so a larger packet is counted as `WriteFailed`,
unless the send buffer has been raised, e.g. with `SetWriteBuffer`.

The fallback connection is a plain `PacketConn`, whose `WriteTo` knows only the remote address. So it looks
the remote address up in an index of the latest flow of each remote address, to write from the local address
and through the interface that flow came to. It deliberately gives no access to the socket, e.g. no
`SyscallConn`, so that a QUIC server can't change socket options or read from the socket past the splitter.

### Dropping

Like a UDP socket whose buffer is full, the splitter never blocks its read loop on a slow reader. It drops
a packet if the queue of its connection is full, if the accept queue is full (without creating a flow, so that
a retry by the client is a new flow), if the flow table is full, or if the packet has no destination address
while the splitter uses destination addresses. The write loop drops a packet whose write times out or fails.
Every drop is counted by its reason.

### Configuration

`SplitConfig` configures a splitter. The zero value of a field gives its default, as does a nil `*SplitConfig`.

| Field               | Default  | Description                                                                                      |
|---------------------|----------|--------------------------------------------------------------------------------------------------|
| `AcceptQueueSize`   | 8        | Split connections waiting for `Accept`; the first packet of a new flow is dropped beyond it      |
| `ReadQueueSize`     | 32       | Packets waiting to be read from each split connection                                            |
| `FallbackQueueSize` | 1024     | Packets waiting to be read from the fallback connection, which may serve many flows              |
| `IdleTimeout`       | 30s      | How long a flow may go without a packet; negative keeps flows until closed                       |
| `MaxFlows`          | 8192     | The maximum number of flows; the first packet of a new flow is dropped beyond it                 |
| `ReadBufferSize`    | 65527    | The maximum size of a packet read, at least 512; a larger packet is dropped                      |
| `WriteQueueSize`    | 64       | Packets waiting to be written; a write waits for room until its write deadline                   |
| `WriteTimeout`      | 500ms    | How long the write loop waits for one packet to be written; negative waits as long as it takes   |
| `FallbackConn`      | false    | Gives the splitter a fallback connection; without one, packets of released flows are dropped     |
| `PacketInfo`        | false    | Enables [packet info](#packet-info) on a `UDPConn`, so that flows are told apart by destination too |

As the next packet of an expired released flow goes to `Accept` again, `IdleTimeout` should be no shorter than
that of the application serving the fallback connection. `PacketInfo` matters for a socket listening on
an unspecified address: without it, replies may go from an address other than the one a client sent to.

### Statistics

`SplitStats`, returned by `Stats`, counts the packets a splitter has dropped, by reason:

| Stage     | Counter             | The packet was dropped because                                                     |
|-----------|---------------------|------------------------------------------------------------------------------------|
| Reading   | `Truncated`         | it was larger than `ReadBufferSize`                                                |
|           | `NoDestination`     | it had no destination address, while the splitter uses them                       |
|           | `TableFull`         | the flow table was full when it started a new flow                                 |
|           | `AcceptQueueFull`   | the accept queue was full when it started a new flow                               |
|           | `ConnQueueFull`     | the queue of its split connection was full                                         |
|           | `ConnClosed`        | its split connection was closed before reading it                                  |
| Releasing | `NoFallback`        | its flow was released, and there was no open fallback connection                   |
|           | `FallbackQueueFull` | the queue of the fallback connection was full                                      |
|           | `FallbackClosed`    | the fallback connection was closed before reading it                               |
| Writing   | `WriteTimedOut`     | the write loop timed out writing it                                                |
|           | `WriteFailed`       | the write loop failed to write it, e.g. to an unreachable address                  |
|           | `WriteUnsent`       | the splitter, or its underlying connection, closed before writing it               |

## Packet info

A UDP socket bound to an unspecified address, i.e. `0.0.0.0` or `::`, receives the packets sent to any address
of the host, while `ReadFrom` tells only the address a packet comes from, and `WriteTo` leaves the address
a reply goes from to the system. So a reply may go from an address other than the one its client sent to, e.g.
on a host with several addresses, and the client drops it. Packet info closes the gap: once it's enabled on
a socket, each packet read comes with a control message carrying the local address it came to and the index of
the interface it came through, and a packet written with such a message goes from the address and through
the interface given.

| Function                      | Description                                                                                          |
|-------------------------------|------------------------------------------------------------------------------------------------------|
| `PacketInfoUDPConn(conn)`     | Enables packet info on `conn` and wraps it as a `FromToUDPConn`; returns `conn` as is if it's one     |
| `NeedPacketInfoUDPConn(conn)` | Reports whether `conn` is bound to an unspecified address, so that it needs packet info              |
| `ControlUDPPacketInfo`        | Enables packet info as a socket is created, as `net.ListenConfig.Control`                            |

```go
lc := net.ListenConfig{Control: hybrid.ControlUDPPacketInfo} // Optional: PacketInfoUDPConn enables it too
pc, err := lc.ListenPacket(context.Background(), "udp", ":5353")
if err != nil {
	log.Fatal(err)
}
conn, err := hybrid.PacketInfoUDPConn(pc.(*net.UDPConn))
if err != nil {
	log.Fatal(err) // E.g. on a platform without packet info
}
defer conn.Close()

b := make([]byte, 64<<10)
for {
	n, src, dst, idx, err := conn.ReadFromTo(b)
	if err != nil {
		return
	}
	// Reply from the address the client sent to, and through the interface the packet came through
	_, _ = conn.WriteToFrom(b[:n], src, dst, idx)
}
```

Notable behavior:

- **Sockets.** IPv4-only, IPv6-only and dual-stack sockets are told apart once, from the local address and,
  for an IPv6 socket bound to `::`, from `IPV6_V6ONLY`, so as to enable packet info for each IP version
  the socket accepts, and to reject a write of a version it doesn't.
- **Addresses.** As a dual-stack socket reports IPv4 sources as IPv4-mapped IPv6 addresses, reads return IPv4
  addresses unmapped, so that source and destination addresses are in the same form on every platform, while
  writes take either form. Packet info carries no port: the local port of every packet read is that of
  the socket, which must have one.
- **Reading.** A read takes a pooled buffer for control messages. Other socket options may add messages of their
  own, which truncate packet info: then the read returns no local address and index (or fails on Windows),
  and the buffers grow for the reads after it, up to 64 KiB. A read without packet info returns no local
  address and index rather than failing.
- **Writing.** A write without a source address and an index goes as by `WriteTo`, so that the system chooses
  both, and no control message is needed. With an index alone, the system chooses the source address on
  the interface, except on Windows, which would send from the unspecified address: there, a write without
  a source address always goes as by `WriteTo`.

## Pipes

A pipe is a pair of in-memory `StreamConn` ends, connected to each other, for testing stream-oriented code
without sockets. It's deliberately based on `net.Pipe`, as a reference implementation: its code is derived from
that of `net.Pipe`, and its tests run against `net.Pipe` as well, so that a pipe differs from `net.Pipe` only where
documented. It comes in three modes:

| Constructor                | Mode       | A write returns once                                       | Write buffer reusable after return |
|----------------------------|------------|------------------------------------------------------------|------------------------------------|
| `UnbufferedPipe(false)`    | sync       | all its bytes have been read, as with `net.Pipe`           | yes                                |
| `UnbufferedPipe(true)`     | semi-sync  | a read has received it, retaining what didn't fit          | yes                                |
| `BufferedPipe(queueSize)`  | async      | it has been queued, unless `queueSize` writes are queued   | yes                                |

- **Sync.** A read is matched with a write, copies the bytes it can straight from the write buffer, and
  acknowledges them, so that the write sends the rest again, and returns once all of its bytes have been read.
- **Semi-sync.** A read is matched with a write as well, but retains the bytes it can't copy, and acknowledges
  the whole write, so that the write returns at once, and the next write waits until the bytes retained have
  been read.
- **Async.** A write queues a copy of its bytes and returns, waiting only while the queue is full; a read takes
  the bytes of a write left by the previous read first.

Writes are serialized, so that the bytes of a write are read together, and so are reads, as they share
the bytes retained. A copy is held in a pooled buffer, returned once all its bytes are read.

### Differences from net.Pipe

Where `net.Pipe` departs from TCP, a pipe behaves the way TCP does:

| Situation                                 | `net.Pipe`                | `hybrid` pipe                                    |
|-------------------------------------------|---------------------------|--------------------------------------------------|
| Using a closed end, or closing it again   | `io.ErrClosedPipe`        | `net.ErrClosed`                                  |
| Writing after the other end is closed     | `io.ErrClosedPipe`        | `EPIPE` (`io.ErrClosedPipe` on Plan 9)           |
| Reading after the other end is closed     | `io.EOF`                  | the bytes retained or queued, then `io.EOF`      |
| Setting a deadline after remote closure   | fails                     | succeeds until the local end is closed           |
| Exceeded read deadline vs. `io.EOF`       | `io.EOF` prevails         | the deadline prevails                            |
| A zero-length write                       | a read returns 0, nil     | sends nothing, as TCP does                       |
| Errors other than `io.EOF`                | bare                      | wrapped in `net.OpError`, as by TCP              |

```go
func TestEcho(t *testing.T) {
	client, server := hybrid.BufferedPipe(16)
	defer client.Close()
	go func() {
		defer server.Close()
		_, _ = io.Copy(server, server) // Echo until the client closes
	}()

	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 5)
	if _, err := io.ReadFull(client, b); err != nil || string(b) != "hello" {
		t.Fatalf("read %q, %v", b, err)
	}
}
```

## Trunks

A trunk is a pair of in-memory `FromToPacketConn` ends, connected to each other, for testing packet-oriented
code, e.g. a splitter, without sockets. It's based on pipes, and so on `net.Pipe` in turn: it has the same
channels, deadlines and closing, but carries whole packets with their addresses, the way UDP does, rather than
a stream of bytes.

| Constructor                              | Mode  | A write returns once                                     |
|------------------------------------------|-------|----------------------------------------------------------|
| `UnbufferedTrunk(addr1, addr2)`          | sync  | its packet has been copied by a read                     |
| `BufferedTrunk(queueSize, addr1, addr2)` | async | it has been queued, unless `queueSize` packets are queued |

There is no semi-sync trunk, as a read never leaves bytes of a packet for the next one.

- **Packets.** A trunk keeps the boundaries of writes: a read takes a single whole packet, and if its buffer is
  shorter, it returns the bytes copied with `io.ErrShortBuffer`, and drops the rest. An empty packet is carried
  like any other, as by UDP. Reads needn't be serialized, while writes still are.
- **Addresses.** The ends have local addresses `addr1` and `addr2`, or a generic trunk address if nil. A packet
  carries the source and destination addresses and the interface index its writer gives, cloned, as the writer
  and the reader may change them. A packet goes to the other end whatever its destination address is, so that
  many flows can share a trunk, told apart by their addresses, as by a splitter. An end writing a packet without
  a source address gives its own local address, and an end reading a packet without a destination address gives
  its own too.
- **Errors.** A trunk reports errors the way a UDP connection does, rather than TCP: using it once either end is
  closed fails with `net.ErrClosed`, except that a read returns the packets queued first; a deadline may be set
  until the local end is closed, and an exceeded read deadline prevails over the packets queued. Every error is
  wrapped in `net.OpError`, with the local address as the source and the remote address, if any, as the address.

```go
server := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}
c1, c2 := hybrid.BufferedTrunk(16, nil, server)
defer c1.Close()
defer c2.Close()

// Simulate two clients sharing one trunk, e.g. to test a splitter over c2
for _, port := range []int{1001, 1002} {
	src := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: port}
	_, _ = c1.WriteToFrom([]byte("query"), server, src, 0)
}

b := make([]byte, 1500)
n, src, dst, idx, err := c2.ReadFromTo(b) // 5 ("query"), 198.51.100.1:1001, 192.0.2.1:53, 0, nil
```

## Wrappers

A wrapper changes a single aspect of a connection, e.g. its addresses or its lifetime, or adapts it to another
interface, e.g. a `PacketConn` to a `StreamConn`, and `Unwrap` returns the connection as given. A wrapper embeds
the interface it wraps, so it has the methods of that interface only, and hides any other methods of
the connection, e.g. those of `FromToPacketConn` or `UDPConn`, as forwarding them would bypass the aspect it
changes. Hence, a wrapper goes on top: anything that needs such methods, e.g. to enable packet info on
a `UDPConn`, should be given the connection before it's wrapped, and a method unrelated to the aspect changed,
e.g. `SyscallConn`, may still be reached through `Unwrap`.

### Addresses

`FakeAddressPacketConn(pc, local, remote)` and `FakeAddressStreamConn(sc, local, remote)` shadow the local and
remote addresses. Unless the local address is nil, it is returned by `LocalAddr` instead of the native one.
Unless the remote address is nil, it is returned by `RemoteAddr` (stream) or by `ReadFrom` instead of the address
a packet comes from (packet), and the packet variant's `WriteTo` sends every packet to it, whatever address it is
given.

### Close

`FakeClosePacketConn(pc, closeFunc)` and `FakeCloseStreamConn(sc, closeFunc)` shadow `Close`. Unless `closeFunc`
is nil, `Close` calls it instead of closing the underlying connection, and returns its error; otherwise, `Close`
does nothing and returns nil. Either way, the underlying connection stays open: reads and writes go on, and
`Close` neither unblocks them nor makes them fail, so whoever owns the connection closes it, e.g. after `Unwrap`.
This is useful to hand a shared connection to code that closes what it's given.

### Idle timeout

`IdleTimeoutPacketConn(pc, timeout)` and `IdleTimeoutStreamConn(sc, timeout)` close the underlying connection
asynchronously once it has been idle for the timeout. The timeout is ignored if it's zero or negative. The idle
time restarts on each successful read and write operation, i.e. when `n > 0` or `err == nil`. It starts when
the connection is wrapped, and the timer stops for good once `Stop` is called, leaving the underlying connection
open, or once the connection is closed, by `Close` or by the timer.

```go
c := hybrid.IdleTimeoutStreamConn(conn, time.Minute)
// ... serve c ...

// Take the connection back, open, without the timer
if err := c.Stop(); err == nil {
	conn = c.Unwrap()
}
```

### Adapters

| Function                                         | Description                                                                                         |
|--------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `PacketToStreamConn(pc, remote, filter)`         | A `StreamConn` over a `PacketConn`, writing to `remote`, and reading only from it if `filter`       |
| `StreamToPacketConn(sc)`                         | A `PacketConn` over a `StreamConn`, reading and writing packets as single reads and writes          |
| `PacketToFromToPacketConn(pc, tryPacketInfoUDP)` | A `FromToPacketConn` over any `PacketConn`, with packet info for a `UDPConn` if asked and possible  |

- `PacketToStreamConn`: `RemoteAddr` returns the remote address given, and `Write` sends every packet to it.
  If `filter` is set and the remote address isn't nil, `Read` accepts a packet from the remote address only,
  as a dialed UDP socket does, and drops a packet from any other address, along with any error of its read,
  e.g. of its truncation if the connection reports the source of a truncated packet, and waits for the next one.
  A failed read, i.e. one reading nothing, returns its error whatever address it reports, e.g. once
  the connection closes or its read deadline passes. A UDP or IP address matches regardless of IPv4 mapping,
  e.g. `192.0.2.1:3` matches `[::ffff:192.0.2.1]:3`. Without the filter, `Read` accepts a packet from any
  address. Each `Read` reads a single packet, which is truncated if longer than the buffer, silently or with
  an error, depending on the underlying connection, so that reading a packet in parts, e.g. with `io.ReadFull`,
  fails.
- `StreamToPacketConn`: `ReadFrom` returns the remote address of the `StreamConn`, while `WriteTo` writes to it
  whatever address it's given, as the address argument is ignored. A packet read is what a single `Read`
  returns, so packets keep their boundaries only if the `StreamConn` keeps those of writes, as a pipe does with
  a buffer as long as any write, and TCP doesn't.
- `PacketToFromToPacketConn`: if the `PacketConn` satisfies `FromToPacketConn`, it's returned as is. If
  `tryPacketInfoUDP` is true, the `PacketConn` implements `UDPConn`, and system calls succeed, a `FromToUDPConn`
  made by `PacketInfoUDPConn` is returned. Otherwise, a simple wrapper returns zero values for the destination
  address and the local interface index on read, and ignores them on write.

## Deadlines

A `Deadline` signals that a point in time has passed by closing a channel, which a `select` may wait on along
with its other cases. It's what pipes, trunks and split connections use for their read and write deadlines,
exported for building custom connections.

```go
d := hybrid.HookDeadline(func() { log.Print("timed out") })
d.Set(time.Now().Add(time.Second))

select {
case v := <-work:
	handle(v)
case <-d.Wait(): // The hook has run by now
	return os.ErrDeadlineExceeded
}
```

- `Set(t)` sets the point in time when the deadline times out. Once a timeout has occurred, the deadline can be
  refreshed by a time in the future; a zero time prevents timeout. So a deadline may be exceeded, extended and
  cleared any number of times.
- `Wait()` returns a channel closed when the deadline is exceeded. As `Set` replaces a closed channel with a new
  one, `Wait` should be called again after `Set`, rather than its channel kept. `Wait` takes no lock: checking
  a deadline costs a single atomic load.
- `HookDeadline(hook)` runs `hook` on its own goroutine once the time set in the future has come, before closing
  the channel, so its effects are observed by any goroutine having received from the channel. It isn't run for
  a zero time or a time in the past. `Set` waits for a running hook to return, so the hook must not call `Set`
  on the same deadline, as it always deadlocks, and it should return promptly. `Wait` never blocks, so the hook
  may call it.
- `SimpleDeadline()` returns a deadline without a hook.

## Error semantics

The types of the package report errors the way the protocol they emulate does, so that code tested over them
behaves the same over sockets:

| Situation                        | Pipe (like TCP)                          | Trunk, split connection (like UDP)               |
|----------------------------------|------------------------------------------|--------------------------------------------------|
| Read after remote closure        | queued bytes, then `io.EOF`              | queued packets, then `net.ErrClosed` (trunk)     |
| Write after remote closure       | `EPIPE`                                  | `net.ErrClosed` (trunk)                          |
| Use or `Close` after local closure | `net.ErrClosed`                        | `net.ErrClosed`                                  |
| Deadline exceeded                | `os.ErrDeadlineExceeded`                 | `os.ErrDeadlineExceeded`                         |
| Short read buffer                | the rest is read next                    | `io.ErrShortBuffer`, the rest is dropped         |
| Error type                       | `*net.OpError`, except `io.EOF`          | `*net.OpError`                                   |

All errors can be checked with `errors.Is`, e.g. `errors.Is(err, net.ErrClosed)`.

## Platform support

Pipes, trunks, wrappers, deadlines and the splitter are pure Go and work everywhere Go does, Plan 9 included.
UDP sockets aren't available on `js`, `plan9` and `wasip1`. Packet info is supported as follows:

| Platforms                                 | IPv4 packet info turned on with      | IPv4 sent from                   |
|-------------------------------------------|--------------------------------------|----------------------------------|
| linux                                     | `IP_PKTINFO`                         | `ipi_spec_dst`, with an index    |
| darwin, illumos, solaris                  | `IP_RECVPKTINFO`                     | `ipi_spec_dst`, with an index    |
| zos                                       | `IP_RECVPKTINFO`                     | `ipi_addr`, with an index        |
| windows                                   | `IP_PKTINFO`                         | `ipi_addr`, with an index (*)    |
| dragonfly, freebsd, netbsd, openbsd       | `IP_RECVDSTADDR`, `IP_RECVIF`        | `IP_SENDSRCADDR`, no index       |
| aix                                       | `IP_RECVDSTADDR`, `IP_RECVINTERFACE` | `IP_SENDSRCADDR`, no index       |
| others, e.g. js, plan9, wasip1            | unsupported                          |                                  |

(*) Windows sends packet info without a source address from `0.0.0.0` or `::`, so a write without a source
address goes as by `WriteTo` there.

IPv6 packet info is the same everywhere: `IPV6_RECVPKTINFO` turns on `IPV6_PKTINFO` messages carrying
`in6_pktinfo`. Where packet info isn't supported, `PacketInfoUDPConn` fails with an error wrapping
`errors.ErrUnsupported`.

## Testing

[CI](.github/workflows/ci.yml) tests the package on Linux and macOS, with the race detector, and on Windows,
on amd64 and arm64 (macOS on arm64 only), with the minimum Go version of `go.mod` and the latest one, on big-endian
Linux, i.e. s390x under QEMU, lints it with
[golangci-lint](.golangci.yml), and vets it for the other platforms with code of their own, e.g. the BSDs, AIX,
Solaris, Plan 9 and WebAssembly. The coverage it reports counts a statement as covered if a test runs it
on any of the platforms tested, as each of them has code of its own:

```sh
go test -race -count=1 -coverpkg=./... -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
golangci-lint run ./...
GOOS=freebsd go vet ./...
```

Benchmarks measure the time and allocations of pipes, trunks, packet info and splitters, e.g. a round trip
through a split connection and through the fallback connection over each kind of underlying connection:

```sh
go test -run '^$' -bench . -benchmem ./...
```

The tests of pipes run against `net.Pipe` too, so that every difference is deliberate, and the tests of
the splitter run over pipes, trunks and UDP sockets alike, with payloads from empty to the largest UDP datagram
over IPv4 and IPv6. Pooled buffers can be poisoned in tests, to detect any use of a buffer after its return.

Fuzz tests check that the splitter reads each packet once or counts it as dropped, whatever its handlers do,
and that addresses and packet info are handled safely whatever they hold. Tests run only their seeds, while
[a weekly job](.github/workflows/fuzz.yml) fuzzes each target for 5 minutes, keeping the corpus between runs:

```sh
go test -run '^$' -fuzz '^FuzzSplit$' -fuzztime 1m .
```

## Status

The module is under active development, ahead of its integration into caddy-l4. Until v1.0.0, minor versions
may change the API, as allowed by [semantic versioning](https://semver.org).
[A check](.github/workflows/api.yml) of every change lists the API changes since the latest release
with [apidiff](https://pkg.go.dev/golang.org/x/exp/cmd/apidiff), warning of incompatible ones until v1.0.0,
and failing on them from then on.

## Contributing

Bug reports, fixes, tests and documentation are welcome, as [the contributing guide](CONTRIBUTING.md) describes.

## Security

Please report vulnerabilities privately, as [the security policy](SECURITY.md) describes.
Weekly and on every change, [`govulncheck`](https://go.dev/doc/security/vuln) checks the dependencies and
the standard library ([workflow](.github/workflows/vuln.yml)), [CodeQL](https://codeql.github.com) analyzes
the code ([workflow](.github/workflows/codeql.yml)), and [OpenSSF Scorecard](https://scorecard.dev) grades
the security practices of the repository, publishing the results ([workflow](.github/workflows/scorecard.yml)).
The workflows pin each action to a commit, which Dependabot updates.

## Credits

Parts of `pipe.go`, `trunk.go` and `deadline.go` are derived from
[`net/pipe.go`](https://github.com/golang/go/blob/go1.25.5/src/net/pipe.go) of Go 1.25.5, copyright 2010
The Go Authors, under a [BSD-style license](https://github.com/golang/go/blob/go1.25.5/LICENSE),
which is preserved in those files.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
