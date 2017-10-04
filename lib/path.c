#include "eddy-private.h"
// Copywright https://github.com/imgix/siphon

ssize_t
ed_path_join(char *out, size_t len,
		const char *a, size_t alen,
		const char *b, size_t blen)
{
	ssize_t olen = ed_esys(ENOBUFS);

	// ignore b if it is empty
	if (blen == 0) {
		if (alen <= len) {
			olen = alen;
			memmove(out, a, olen);
		}
		goto done;
	}

	// ignore a if it is empty or b is absolute
	if (alen == 0 || *b == '/') {
		if (blen <= len) {
			olen = blen;
			memmove(out, b, olen);
		}
		goto done;
	}

	if (a[alen-1] == '/') {
		alen--;
	}
	olen = alen + blen + 1;
	if ((size_t)olen > len) {
		olen = ed_esys(ENOBUFS);
		goto done;
	}
	memmove(out, a, alen);
	out[alen] = '/';
	memmove(out+alen+1, b, blen);

done:
	if (olen >= 0 && len > (size_t)olen) {
		out[olen] = '\0';
	}
	return olen;
}

size_t
ed_path_clean(char *path, size_t len)
{
	bool rooted = path[0] == '/';
	char *p, *w, *up, *pe;

	p = up = path;
	pe = p + len;
	w = rooted ? p + 1 : p;

	while (p < pe) {
		switch (*p) {
			case '/':
				// empty path element
				p++;
				break;
			case '.':
				if (p+1 == pe || *(p+1) == '/') {
					// . element
					p++;
					break;
				}
				if (*(p+1) == '.' && (p+2 == pe || *(p+2) == '/')) {
					// .. element: remove to last /
					p += 2;
					if (w > up) {
						// can backtrack
						for (w--; w > up && *w != '/'; w--);
					}
					else if (!rooted) {
						// cannot backtrack, but not rooted, so append .. element.
						if (w > path) {
							*(w++) = '/';
						}
						*(w++) = '.';
						*(w++) = '.';
						up = w;
					}
					break;
				}
				// fallthrough
			default:
				// real path element.
				// add slash if needed
				if ((rooted && w != path+1) || (!rooted && w != path)) {
					*(w++) = '/';
				}
				// copy element
				for (; p < pe && *p != '/'; p++) {
					*(w++) = *p;
				}
				break;
		}
	}
	if (w == path) { *(w++) = '.'; }
	if (w < pe) { *w = '\0'; }
	return w - path;
}

ssize_t
ed_path_abs(char *out, size_t len,
		const char *path, size_t plen)
{
	if (getcwd(out, len) == NULL) { return ED_ERRNO; }
	size_t clen = strnlen(out, len);
	ssize_t jlen = ed_path_join(out, len, out, clen, path, plen);
	if (jlen < 0) { return jlen; }
	return ed_path_clean(out, jlen);
}

