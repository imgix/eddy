#include "eddy-private.h"
#include "mu.h"

static void
test_basic(void)
{
}

int
main(void)
{
	mu_init("btree");
	mu_run(test_basic);
	return 0;
}

