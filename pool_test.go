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

import "testing"

func TestBufClass(t *testing.T) {
	tests := []struct{ n, idx, size int }{
		{1, 0, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 6}, {6, 5, 6}, {7, 6, 8}, {8, 6, 8},
		{9, 7, 12}, {1024, 20, 1024}, {1025, 21, 1536}, {1536, 21, 1536}, {1537, 22, 2048},
		{49152, 31, 49152}, {49153, 32, 65536}, {65536, 32, 65536},
	}
	for _, test := range tests {
		if idx, size := bufClass(test.n); idx != test.idx || size != test.size {
			t.Errorf("bufClass(%d) = %d, %d, want %d, %d", test.n, idx, size, test.idx, test.size)
		}
	}

	// Every length gets the smallest class fitting it, wasting at most a third of its capacity
	prevIdx, prevSize := -1, 0
	for n := 1; n <= 1<<bufPoolMaxBits; n++ {
		idx, size := bufClass(n)
		switch {
		case idx < 0 || idx >= len(bufPools):
			t.Fatalf("bufClass(%d) = %d, want an index in [0, %d)", n, idx, len(bufPools))
		case size < n || (size-n)*3 > size:
			t.Fatalf("bufClass(%d) = %d, want a size in [%d, %d]", n, size, n, n*3/2)
		case idx == prevIdx && size != prevSize:
			t.Fatalf("bufClass(%d) = %d, %d, want the class of %d, i.e. %d, %d", n, idx, size, n-1, prevIdx, prevSize)
		case idx != prevIdx && (idx <= prevIdx || prevSize != n-1):
			t.Fatalf("bufClass(%d) = %d, %d, want the class following %d, %d once %d is exceeded", n, idx, size, prevIdx, prevSize, prevSize)
		}
		prevIdx, prevSize = idx, size
	}
}

func TestGetBuf(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 5, 1472, 1536, 1537, 1 << bufPoolMaxBits, 1<<bufPoolMaxBits + 1} {
		buf, ref := getBuf(n)
		pooled := n > 0 && n <= 1<<bufPoolMaxBits
		switch {
		case buf == nil || len(buf) != n:
			t.Errorf("getBuf(%d) = %d bytes, want %d", n, len(buf), n)
		case pooled != (ref != nil):
			t.Errorf("getBuf(%d) reference = %v, want non-nil = %v", n, ref, pooled)
		case pooled && (&(*ref)[:1][0] != &buf[:1][0] || cap(*ref) != cap(buf)):
			t.Errorf("getBuf(%d) reference doesn't refer to the buffer", n)
		case pooled:
			if _, size := bufClass(n); cap(buf) != size {
				t.Errorf("getBuf(%d) = %d capacity, want %d", n, cap(buf), size)
			}
		}

		// A buffer put back is reusable for any length of its class, whatever it contains
		for i := range buf {
			buf[i] = 0xff
		}
		putBuf(ref)
		if again, _ := getBuf(n); len(again) != n {
			t.Errorf("getBuf(%d) after putBuf = %d bytes, want %d", n, len(again), n)
		}
	}
	putBuf(nil) // Doesn't panic
}
