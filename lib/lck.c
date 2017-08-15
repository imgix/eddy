#include "eddy-private.h"

#define ed_lck_thread(flags) (!((flags) & ED_FNOTLCK))
#define ed_lck_file(flags) (!((flags) & ED_FNOFLCK))
#define ed_lck_wait(type, flags) ((type) == ED_LCK_UN || !((flags) & ED_FNOBLOCK))

void
ed_lck_init(EdLck *lock, off_t start, off_t len)
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
ed_lck_final(EdLck *lock)
{
	pthread_rwlock_destroy(&lock->rw);
}

int
ed_lck(EdLck *lock, int fd, EdLckType type, uint64_t flags)
{
	int rc = 0;
	if (ed_lck_thread(flags)) {
		if (type == ED_LCK_EX) {
			rc = ed_lck_wait(type, flags) ?
				pthread_rwlock_wrlock(&lock->rw) :
				pthread_rwlock_trywrlock(&lock->rw);
		}
		else if (type == ED_LCK_SH) {
			rc = ed_lck_wait(type, flags) ?
				pthread_rwlock_rdlock(&lock->rw) :
				pthread_rwlock_tryrdlock(&lock->rw);
		}
		if (rc != 0) { return ed_esys(rc); }
	}

	if (ed_lck_file(flags)) {
		struct flock f = lock->f;
		f.l_type = (int)type;
		int rc, op = ed_lck_wait(type, flags) ? F_SETLKW : F_SETLK;
		while ((rc = fcntl(fd, op, &f)) < 0 && (rc = ED_ERRNO) == ed_esys(EINTR)) {}
		if (rc >= 0) { lock->f = f; }
	}

	if (ed_lck_thread(flags) && (rc != 0 || type == ED_LCK_UN)) {
		pthread_rwlock_unlock(&lock->rw);
	}
	return rc;
}

