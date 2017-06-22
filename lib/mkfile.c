#include "eddy-private.h"

int
ed_mkfile(int fd, off_t size)
{
#ifdef __APPLE__
	fstore_t store = {
		.fst_flags = F_ALLOCATECONTIG,
		.fst_posmode = F_PEOFPOSMODE,
		.fst_offset = 0,
		.fst_length = size,
	};
	if (fcntl(fd, F_PREALLOCATE, &store) < 0) {
		store.fst_flags = F_ALLOCATEALL;
		if (fcntl(fd, F_PREALLOCATE, &store)) { return ED_ERRNO; }
	}
#else
	if (posix_fallocate(fd, 0, size) < 0) { return ED_ERRNO; }
#endif
	if (ftruncate(fd, size) < 0) { return ED_ERRNO; }
	return 0;
}

