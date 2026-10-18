package disk

import (
	"math/bits"
	"math/rand/v2"
)

// Ratio is the probability Num/Den.
// It uses integers only, so a seed makes the same decisions on every platform.
// The zero Ratio never happens.
type Ratio struct {
	Num, Den uint64
}

// Never is the zero Ratio.
var Never = Ratio{}

// Always is the Ratio 1/1.
var Always = Ratio{Num: 1, Den: 1}

// OneIn returns the Ratio 1/n.
func OneIn(n uint64) Ratio {
	return Ratio{Num: 1, Den: n}
}

// Rand is a deterministic random source with integer-only helpers.
// It implements its own range reduction on PCG, so a change in the math/rand/v2 helpers cannot change a replay.
type Rand struct {
	pcg rand.PCG
}

// NewRand returns a source that the seed fully selects.
func NewRand(seed uint64) *Rand {
	r := &Rand{}
	r.pcg.Seed(seed, seed^0x9e3779b97f4a7c15)
	return r
}

// Uint64 returns 64 random bits.
func (r *Rand) Uint64() uint64 {
	return r.pcg.Uint64()
}

// Uint64N returns a uniform value in [0, n).
// It uses Lemire's method without bias.
// n must be positive.
func (r *Rand) Uint64N(n uint64) uint64 {
	if n == 0 {
		panic("disk: Uint64N with zero bound")
	}
	hi, lo := bits.Mul64(r.Uint64(), n)
	if lo < n {
		threshold := -n % n
		for lo < threshold {
			hi, lo = bits.Mul64(r.Uint64(), n)
		}
	}
	return hi
}

// IntN returns a uniform value in [0, n).
// n must be positive.
func (r *Rand) IntN(n int) int {
	return int(r.Uint64N(uint64(n)))
}

// Range returns a uniform value in [low, high].
// low must not be greater than high.
func (r *Rand) Range(low, high int) int {
	return low + r.IntN(high-low+1)
}

// Bool returns true for one half of the values.
func (r *Rand) Bool() bool {
	return r.Uint64()&1 == 1
}

// Chance returns true with the probability of ratio.
func (r *Rand) Chance(ratio Ratio) bool {
	if ratio.Num == 0 || ratio.Den == 0 {
		return false
	}
	if ratio.Num >= ratio.Den {
		return true
	}
	return r.Uint64N(ratio.Den) < ratio.Num
}

// Fill writes random bytes into data.
func (r *Rand) Fill(data []byte) {
	for len(data) >= 8 {
		value := r.Uint64()
		for index := range 8 {
			data[index] = byte(value >> (8 * index))
		}
		data = data[8:]
	}
	if len(data) > 0 {
		value := r.Uint64()
		for index := range data {
			data[index] = byte(value >> (8 * index))
		}
	}
}

// Weighted returns an index of weights.
// The probability of index i is weights[i] divided by the sum of the weights.
// The sum must be positive.
func (r *Rand) Weighted(weights []uint64) int {
	var total uint64
	for _, weight := range weights {
		total += weight
	}
	pick := r.Uint64N(total)
	for index, weight := range weights {
		if pick < weight {
			return index
		}
		pick -= weight
	}
	panic("disk: unreachable weighted choice")
}
