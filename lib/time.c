#include "eddy-private.h"

EdTime
ed_time_from_unix(EdTimeUnix epoch, EdTimeUnix at)
{
	if (at > (EdTimeUnix)ED_TIME_MAX) { return ED_TIME_MAX; }
	if (at < 0) { return ED_TIME_INF; }
	if (at <= epoch) { return ED_TIME_DELETE; }
	return (EdTime)(at - epoch);
}

EdTimeUnix
ed_time_to_unix(EdTimeUnix epoch, EdTime at)
{
	if (at == ED_TIME_INF) { return -1; };
	if (at == ED_TIME_DELETE) { return 0; }
	return (EdTimeUnix)at + epoch;
}

EdTimeUnix
ed_now_unix(void)
{
	return (EdTimeUnix)time(NULL);
}

EdTime
ed_expiry_at(EdTimeUnix epoch, EdTimeTTL ttl, EdTimeUnix at)
{
	if (ttl < 0) { return ED_TIME_INF; }
	return ed_time_from_unix(epoch, at + ttl);
}

EdTimeTTL
ed_ttl_at(EdTimeUnix epoch, EdTime exp, EdTimeUnix at)
{
	if (exp == ED_TIME_INF) { return -1; };
	if (exp == ED_TIME_DELETE) { return 0; }
	EdTimeUnix ttl = ed_time_to_unix(epoch, exp) - at;
	return ttl < 0 ? 0 : ttl;
}

EdTimeUnix
ed_unix_from_ttl_at(EdTimeTTL ttl, EdTimeUnix at)
{
	return ttl < 0 ? -1 : ttl + at;
}

EdTimeUnix
ed_unix_from_ttl(EdTimeTTL ttl)
{
	return ed_unix_from_ttl_at(ttl, ed_now_unix());
}

bool
ed_expired_at(EdTimeUnix epoch, EdTime exp, EdTimeUnix at)
{
	if (exp == ED_TIME_INF) { return false; }
	if (exp == ED_TIME_DELETE) { return true; }
	EdTimeUnix cmp = ed_time_to_unix(epoch, exp);
	return cmp <= at;
}

