#include "eddy-private.h"

#ifdef S_ISNAM
# define IS_RAND_MODE(mode) (S_ISNAM(mode) || S_ISCHR(mode))
#else
# define IS_RAND_MODE(mode) (S_ISCHR(mode))
#endif

#ifdef __linux__
# define IS_RAND_DEVICE(dev) ((dev) == makedev(1, 8) || (dev) == makedev(1, 9))
#else
# define IS_RAND_DEVICE(dev) 1
#endif

#define IS_RAND(st) (IS_RAND_MODE((st).st_mode) && IS_RAND_DEVICE((st).st_dev))

static atomic_int ed_rnd_fd = -1;

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

int
ed_rnd_global(void)
{
	if (ed_rnd_fd == -1) {
		int old = -1, new = ed_rnd_open();
		if (new < 0) { return new; }
		if (!atomic_compare_exchange_strong(&ed_rnd_fd, &old, new)) {
			close(new);
		}
	}
	return ed_rnd_fd;
}

void
ed_rnd_close(void)
{
	do {
		int old = atomic_load(&ed_rnd_fd);
		if (old == -1) { return; }
		if (atomic_compare_exchange_strong(&ed_rnd_fd, &old, -1)) {
			close(old);
			return;
		}
	} while(1);
}

ssize_t
ed_rnd_buffer(void *buf, size_t len)
{
	int fd = ed_rnd_global();
	if (fd < 0) { return fd; }
	do {
		ssize_t n = read(fd, buf, len);
		if (n < 0 && errno != EAGAIN) { return ED_ERRNO; }
		else if (n == 0) {
			if (atomic_compare_exchange_strong(&ed_rnd_fd, &fd, -1)) {
				close(fd);
			}
			return 0;
		}
		else if ((size_t)n == len) {
			return n;
		}
	} while(1);
}

int
ed_rnd_u64(uint64_t *val)
{
	return (int)ed_rnd_buffer(val, 8);
}

