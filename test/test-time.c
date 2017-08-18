#include "../lib/eddy-private.h"
#include "mu.h"

static const EdTimeUnix epoch = 1500000000;

static void
test_expired(void)
{
	EdTimeUnix now = ed_now_unix();
	EdTime exp;

	exp = ed_expiry_at(epoch, 100, now);
	mu_assert(!ed_expired_at(epoch, exp, now));

	now += 150;
	mu_assert(ed_expired_at(epoch, exp, now));

	exp = ed_expiry_at(epoch, -1, now);
	mu_assert(!ed_expired_at(epoch, exp, now));

	now += 1000000000000;
	mu_assert(!ed_expired_at(epoch, exp, now));
}

static void
test_ttl(void)
{
	EdTimeUnix now = ed_now_unix();
	EdTime exp;

	exp = ed_expiry_at(epoch, 100, now);
	mu_assert_int_eq(ed_ttl_at(epoch, exp, now), 100);

	now += 150;
	mu_assert_int_eq(ed_ttl_at(epoch, exp, now), 0);

	exp = ed_expiry_at(epoch, -1, now);
	mu_assert_int_eq(ed_ttl_at(epoch, exp, now), -1);

	now += 1000000000000;
	mu_assert_int_eq(ed_ttl_at(epoch, exp, now), -1);
}

static void
test_unix(void)
{
	EdTimeUnix now = ed_now_unix();
	EdTime exp;

	exp = ed_expiry_at(epoch, 100, now);
	mu_assert_int_eq(ed_time_to_unix(epoch, exp), now+100);
}

int
main(void)
{
	mu_init("time");
	mu_run(test_expired);
	mu_run(test_ttl);
	mu_run(test_unix);
	return 0;
}

