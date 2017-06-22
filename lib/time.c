#include "eddy-private.h"

int64_t
ed_now(int64_t epoch)
{
	return (int64_t)time(NULL) - epoch;
}

uint32_t
ed_expire(int64_t epoch, time_t ttlsec)
{
	if (ttlsec < 0) { return ED_TIME_INF; }
	int64_t tlater = ed_now(epoch) + (int64_t)ttlsec;
	if (tlater >= (int64_t)ED_TIME_INF) { return ED_TIME_INF-1; }
	if (tlater <= 0) { return ED_TIME_DELETE; }
	return (uint32_t)tlater;
}

time_t
ed_ttl_at(int64_t epoch, uint32_t exp, time_t t)
{
	if (exp == ED_TIME_INF) { return -1; };
	return (time_t)((int64_t)exp - ((int64_t)t - epoch));
}

time_t
ed_ttl_now(int64_t epoch, uint32_t exp)
{
	return ed_ttl_at(epoch, exp, time(NULL));
}

bool
ed_expired_at(int64_t epoch, uint32_t exp, time_t t)
{
	return exp != ED_TIME_INF && exp < (int64_t)t - epoch;
}

bool
ed_expired_now(int64_t epoch, uint32_t exp)
{
	return ed_expired_at(epoch, exp, time(NULL));
}

