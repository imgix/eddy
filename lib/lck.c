#include "eddy-private.h"

void
ed_lock_init(EdLck *lock, off_t start, off_t len)
{
	struct flock f = {
		.l_type = (int)ED_LCK_UN,
		.l_whence = SEEK_SET,
		.l_start = start,
		.l_len = len,
	};
	lock->f = f;
	pthread_rwlock_init(&lock->rw, NULL);
}

void
ed_lock_final(EdLck *lock)
{
	pthread_rwlock_destroy(&lock->rw);
}

int
ed_lock(EdLck *lock, int fd, EdLckType type, bool wait, uint64_t flags)
{
	int rc = 0;
	if (!(flags & ED_FNOTLOCK)) {
		if (type == ED_LCK_EX) {
			rc = wait ?
				pthread_rwlock_wrlock(&lock->rw) :
				pthread_rwlock_trywrlock(&lock->rw);
		}
		else if (type == ED_LCK_SH) {
			rc = wait ?
				pthread_rwlock_rdlock(&lock->rw) :
				pthread_rwlock_tryrdlock(&lock->rw);
		}
		if (rc != 0) { return ed_esys(rc); }
	}

	if (!(flags & ED_FNOFLOCK)) {
		rc = ed_flock(lock, fd, type, wait);
	}

	if ((rc != 0 || type == ED_LCK_UN) && !(flags & ED_FNOTLOCK)) {
		pthread_rwlock_unlock(&lock->rw);
	}
	return rc;
}

int
ed_flock(EdLck *lock, int fd, EdLckType type, bool wait)
{
	struct flock f = lock->f;
	f.l_type = (int)type;
	int rc, op = wait ? F_SETLKW : F_SETLK;
	while ((rc = fcntl(fd, op, &f)) < 0 &&
			(rc = ED_ERRNO) == ed_esys(EINTR)) {}
	if (rc >= 0) { lock->f = f; }
	return rc;
}

