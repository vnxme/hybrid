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
	"math/bits"
	"sync"
	"sync/atomic"
)

// bufPoolMaxBits limits the capacity of pooled buffers to 1<<bufPoolMaxBits, i.e. 64 KiB,
// which covers the maximum UDP payload. Longer buffers aren't pooled.
const bufPoolMaxBits = 16

var (
	// bufPools hold reusable buffers by size class, see bufClass.
	bufPools [2*bufPoolMaxBits + 1]sync.Pool

	// bufPoison makes putBuf clear buffers, so that tests detect any use of a buffer after its return.
	bufPoison atomic.Bool
)

// bufClass returns the index and the capacity of the smallest size class fitting n > 0.
// Classes alternate between 1<<k and 3<<(k-1), i.e. 1, 2, 3, 4, 6, 8, 12, 16, ...,
// so that a buffer wastes at most a third of its capacity, the same way as Go's allocator.
func bufClass(n int) (int, int) {
	k := bits.Len(uint(n - 1)) // 1<<k is the smallest power of two >= n
	if k >= 2 && n <= 3<<(k-2) {
		return 2*k - 1, 3 << (k - 2)
	}
	return 2 * k, 1 << k
}

// getBuf returns a buffer of length n, reused if possible, along with its reference for putBuf,
// which is nil unless the buffer is pooled. The contents of the buffer are unspecified.
func getBuf(n int) ([]byte, *[]byte) {
	if n == 0 || n > 1<<bufPoolMaxBits {
		return make([]byte, n), nil
	}
	i, size := bufClass(n)
	if ref, ok := bufPools[i].Get().(*[]byte); ok {
		return (*ref)[:n], ref
	}
	buf := make([]byte, size)
	return buf[:n], &buf
}

// putBuf makes a buffer obtained from getBuf reusable. The buffer must not be used afterwards.
func putBuf(ref *[]byte) {
	if ref != nil {
		if bufPoison.Load() {
			clear(*ref)
		}
		i, _ := bufClass(cap(*ref))
		bufPools[i].Put(ref)
	}
}
