int
get_random(unsigned int *ctx)
{
	long hi, lo, seed = (long)*ctx;

	/*
	 * Compute x = (7^5 * x) mod (2^31 - 1)
	 * wihout overflowing 31 bits:
	 *      (2^31 - 1) = 127773 * (7^5) + 2836
	 * From "Random number generators: good ones are hard to find",
	 * Park and Miller, Communications of the ACM, vol. 31, no. 10,
	 * October 1988, p. 1195.
	 */
	if (seed == 0) { seed = 123459876; }
	hi = seed / 127773;
	lo = seed % 127773;
	seed = 16807 * lo - 2836 * hi;
	if (seed < 0) { seed += 0x7fffffff; }
	*ctx = (unsigned int)seed;
	return (seed % ((unsigned long)RAND_MAX + 1));
}

