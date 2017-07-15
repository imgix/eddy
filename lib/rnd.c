#include "eddy-private.h"

#ifdef S_ISNAM
# define IS_RAND_MODE(mode) (S_ISNAM(mode) || S_ISCHR(mode))
#else
# define IS_RAND_MODE(mode) (S_ISCHR(mode))
#endif

#if defined(__linux__)
# define IS_RAND_DEVICE(dev) ((dev) == makedev(1, 9))
#elif defined(__APPLE__)
# define IS_RAND_DEVICE(dev) ((dev) == makedev(14, 1))
#elif defined(__FreeBSD__)
# define IS_RAND_DEVICE(dev) ((dev) == makedev(0, 10))
#elif defined(__DragonFly__)
# define IS_RAND_DEVICE(dev) ((dev) == makedev(8, 4))
#elif defined(__NetBSD__)
# define IS_RAND_DEVICE(dev) ((dev) == makedev(46, 1))
#elif defined(__OpenBSD__)
# define IS_RAND_DEVICE(dev) ((dev) == makedev(45, 2))
#else
# define IS_RAND_DEVICE(dev) 1
#endif

#define IS_RAND(st) (IS_RAND_MODE((st).st_mode) && IS_RAND_DEVICE((st).st_rdev))

static int
ed_rnd_check(int fd)
{
	struct stat st;
	if (fstat(fd, &st) < 0) { return ED_ERRNO; }
	if (!IS_RAND(st)) { return ed_esys(EBADF); }
	return 0;
}

int
ed_rnd_open(void)
{
	int fd, err;
	do {
		fd = open("/dev/urandom", O_RDONLY|O_CLOEXEC);
		if (fd >= 0) {
			err = ed_rnd_check(fd);
			if (err == 0) { return fd; }
			close(fd);
		}
		else {
			err = ED_ERRNO;
		}
		if (err != ed_esys(EINTR)) {
			return err;
		}
	} while (1);
}

ssize_t
ed_rnd_buffer(int fd, void *buf, size_t len)
{
	bool new = false;
	if (fd < 0) {
		fd = ed_rnd_open();
		if (fd < 0) { return fd; }
		new = true;
	}

	uint8_t *p = buf;
	ssize_t rc = 0;
	do {
		ssize_t n = read(fd, p, len-rc);
		if (n < 0 && errno != EINTR) { rc = ED_ERRNO; break; }
		else if (n == 0) { rc = 0; break; }
		else {
			if ((rc += n) >= (ssize_t)len) { break; }
			p += n;
		}
	} while(1);

	if (new) { close(fd); }
	return rc;
}

int
ed_rnd_u64(int fd, uint64_t *val)
{
	return (int)ed_rnd_buffer(fd, val, 8);
}

