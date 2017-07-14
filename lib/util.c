#include "eddy-util.h"

bool
ed_parse_size(const char *val, long long *out)
{
	char *end;
	long long size = strtoll(val, &end, 10);
	if (size < 0) { return false; }
	switch(*end) {
	case 'k': case 'K': size *= ED_KiB; end++; break;
	case 'm': case 'M': size *= ED_MiB; end++; break;
	case 'g': case 'G': size *= ED_GiB; end++; break;
	case 't': case 'T': size *= ED_TiB; end++; break;
	case 'p': case 'P': size *= PAGESIZE; end++; break;
	}
	if (*end != '\0') { return false; }
	*out = size;
	return true;
}

int
ed_input_read(EdInput *in, int fd, off_t max)
{
	struct stat sbuf;
	if (fstat(fd, &sbuf) < 0) { return ED_ERRNO; }

	if (S_ISREG (sbuf.st_mode)) {
		if (sbuf.st_size > max) { return ed_esys(EFBIG); }
		void *m = mmap(NULL, sbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
		if (m == MAP_FAILED) { return ED_ERRNO; }
		in->data = m;
		in->length = sbuf.st_size;
		in->mapped = true;
		return 0;
	}

	size_t len = 0, cap = 0, next = 4096;
	uint8_t *p = NULL;
	int rc;
	do {
		if (len == cap) {
			uint8_t *new = realloc(p, next);
			if (new == NULL) { goto done_errno; }
			p = new;
			cap = next;
			next *= 2;
		}
		ssize_t n = read(fd, p + len, cap - len);
		if (n < 0) { goto done_errno; }
		if (n == 0) { break; }
		len += n;
		if (max > 0 && len > (size_t)max) {
			rc = ed_esys(EFBIG);
			goto done_rc;
		}
	} while (1);
	in->data = p;
	in->length = len;
	in->mapped = false;
	return 0;

done_errno:
	rc = ED_ERRNO;
done_rc:
	free(p);
	return rc;
}

int
ed_input_fread(EdInput *in, const char *path, off_t max)
{
	if (path == NULL || strcmp(path, "-") == 0) {
		return ed_input_read(in, STDIN_FILENO, max);
	}
	int fd = open(path, O_RDONLY);
	if (fd < 0) { return ED_ERRNO; }
	int rc = ed_input_read(in, fd, max);
	close(fd);
	return rc;
}

void
ed_input_final(EdInput *in)
{
	if (in->mapped) { munmap(in->data, in->length); }
	else { free(in->data); }
	in->data = NULL;
	in->length = 0;
	in->mapped = false;
}

