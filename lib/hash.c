#include "eddy-private.h"

// xxHash Copyright (c) 2012-2015, Yann Collet
// https://github.com/Cyan4973/xxHash

#define XX64_PRIME_1 UINT64_C(11400714785074694791)
#define XX64_PRIME_2 UINT64_C(14029467366897019727)
#define XX64_PRIME_3 UINT64_C( 1609587929392839161)
#define XX64_PRIME_4 UINT64_C( 9650029242287828579)
#define XX64_PRIME_5 UINT64_C( 2870177450012600261)

static inline uint64_t
rotl(uint64_t v, unsigned k)
{
    return (v << k) | (v >> (64 - k));
}

static inline uint64_t
xx64_round(uint64_t acc, uint64_t input)
{
	acc += input * XX64_PRIME_2;
	acc  = rotl(acc, 31);
	acc *= XX64_PRIME_1;
	return acc;
}

static inline uint64_t
xx64_merge(uint64_t acc, uint64_t val)
{
	val  = xx64_round(0, val);
	acc ^= val;
	acc  = acc * XX64_PRIME_1 + XX64_PRIME_4;
	return acc;
}

uint64_t
ed_hash(const uint8_t *p, size_t len, uint64_t seed)
{
	const uint8_t *pe = p + len;
	uint64_t h;

	if (len >= 32) {
		const uint8_t *const limit = pe - 32;
		uint64_t v1 = seed + XX64_PRIME_1 + XX64_PRIME_2;
		uint64_t v2 = seed + XX64_PRIME_2;
		uint64_t v3 = seed + 0;
		uint64_t v4 = seed - XX64_PRIME_1;

		do {
			v1 = xx64_round(v1, ed_l64(ed_fetch64(p))); p+=8;
			v2 = xx64_round(v2, ed_l64(ed_fetch64(p))); p+=8;
			v3 = xx64_round(v3, ed_l64(ed_fetch64(p))); p+=8;
			v4 = xx64_round(v4, ed_l64(ed_fetch64(p))); p+=8;
		} while (p<=limit);

		h = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18);
		h = xx64_merge(h, v1);
		h = xx64_merge(h, v2);
		h = xx64_merge(h, v3);
		h = xx64_merge(h, v4);
	}
	else {
		h = seed + XX64_PRIME_5;
	}

	h += (uint64_t)len;

	while (p+8 <= pe) {
		uint64_t const k1 = xx64_round(0, ed_l64(ed_fetch64(p)));
		h ^= k1;
		h  = rotl(h,27) * XX64_PRIME_1 + XX64_PRIME_4;
		p+=8;
	}

	if (p+4 <= pe) {
		h ^= ed_l64(ed_fetch32(p)) * XX64_PRIME_1;
		h  = rotl(h, 23) * XX64_PRIME_2 + XX64_PRIME_3;
		p+=4;
	}

	while (p < pe) {
		h ^= (*p) * XX64_PRIME_5;
		h  = rotl(h, 11) * XX64_PRIME_1;
		p++;
	}

	h ^= h >> 33;
	h *= XX64_PRIME_2;
	h ^= h >> 29;
	h *= XX64_PRIME_3;
	h ^= h >> 32;

	return h;
}

