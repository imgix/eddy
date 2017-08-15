#include "eddy-private.h"

#define ed_lck_thread(flags) (!((flags) & ED_FNOTLCK))
#define ed_lck_file(flags) (!((flags) & ED_FNOFLCK))
#define ed_lck_wait(type, flags) ((type) == ED_LCK_UN || !((flags) & ED_FNOBLOCK))

void
ed_lck_init(EdLck *lck, off_t start, off_t len)
{
	struct flock f = {
		.l_type = (int)ED_LCK_UN,
		.l_whence = SEEK_SET,
		.l_start = start,
		.l_len = len,
	};
	lck->f = f;
	pthread_rwlock_init(&lck->rw, NULL);
}

void
ed_lck_final(EdLck *lck)
{
	pthread_rwlock_destroy(&lck->rw);
}

int
ed_lck(EdLck *lck, int fd, EdLckType type, uint64_t flags)
{
	int rc = 0;
	if (ed_lck_thread(flags)) {
		if (type == ED_LCK_EX) {
			rc = ed_lck_wait(type, flags) ?
				pthread_rwlock_wrlock(&lck->rw) :
				pthread_rwlock_trywrlock(&lck->rw);
		}
		else if (type == ED_LCK_SH) {
			rc = ed_lck_wait(type, flags) ?
				pthread_rwlock_rdlock(&lck->rw) :
				pthread_rwlock_tryrdlock(&lck->rw);
		}
		if (rc != 0) { return ed_esys(rc); }
	}

	if (ed_lck_file(flags)) {
		struct flock f = lck->f;
		f.l_type = (int)type;
		int rc, op = ed_lck_wait(type, flags) ? F_SETLKW : F_SETLK;
		while ((rc = fcntl(fd, op, &f)) < 0 && (rc = ED_ERRNO) == ed_esys(EINTR)) {}
		if (rc >= 0) { lck->f = f; }
	}

	if (ed_lck_thread(flags) && (rc != 0 || type == ED_LCK_UN)) {
		pthread_rwlock_unlock(&lck->rw);
	}
	return rc;
}

