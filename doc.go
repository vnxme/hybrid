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

// Package hybrid provides versatile network types for building TCP- and UDP-based applications, proxies
// in particular, and for testing them without sockets:
//
//   - Splitter: [Split] serves a single UDP socket, or any other packet connection, to many handlers at once,
//     the way a TCP listener serves a port to many connections, so that UDP can be proxied the same way as TCP.
//     Each flow is served by a [SplitConn], and the flows released by their handlers by a fallback connection.
//   - Packet info: [PacketInfoUDPConn] makes a UDP socket bound to an unspecified address reply from
//     the address each client sent to, on every major platform.
//   - Pipes and trunks: [UnbufferedPipe] and [BufferedPipe] return in-memory stream connections behaving
//     like TCP, and [UnbufferedTrunk] and [BufferedTrunk] in-memory packet connections behaving like UDP,
//     for testing network code deterministically.
//   - Wrappers: small adapters changing one aspect of a connection, e.g. its addresses with
//     [FakeAddressPacketConn], its lifetime with [IdleTimeoutPacketConn], or its orientation with
//     [PacketToStreamConn] and [StreamToPacketConn].
//   - Deadlines: [SimpleDeadline] and [HookDeadline] return reusable deadlines, which allocate nothing
//     once set up, with an optional hook.
//
// The name reflects the core idea: a [Conn] is a hybrid of a packet connection and a stream connection,
// which keeps packet boundaries while serving a single remote address, so that a UDP flow can be handled
// by code written for [net.Conn].
package hybrid
