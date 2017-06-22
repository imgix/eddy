#ifndef INCLUDED_EDDY_MIME_H
#define INCLUDED_EDDY_MIME_H

#include "eddy.h"

#define ED_FMIME_NOVERIFY (1 << 0) /* don't verify the mime database */
#define ED_FMIME_MLOCK    (1 << 1) /* mlock the full mime database */

typedef struct EdMime EdMime;
typedef struct EdMimeMatch EdMimeMatch;

ED_EXPORT int
ed_mime_load(EdMime **, const void *data, size_t size, int flags);

ED_EXPORT int
ed_mime_open(EdMime **, const char *path, int flags);

ED_EXPORT void
ed_mime_close(EdMime **);

ED_EXPORT const EdMimeMatch *
ed_mime_get_match(const EdMime *, const char *mime);

ED_EXPORT uint32_t
ed_mime_test_match(const EdMime *, const EdMimeMatch *, const void *data, size_t len);

ED_EXPORT const char *
ed_mime_type(const EdMime *, const void *data, size_t len, bool fallback);

ED_EXPORT const char *
ed_mime_file_type(const EdMime *, const char *path, bool fallback);

ED_EXPORT const char *
ed_mime_fallback(const EdMime *, const void *data, size_t len);

ED_EXPORT const char *
ed_mime_alias(const EdMime *, const char *mime);

ED_EXPORT size_t
ed_mime_parents(const EdMime *, const char *mime, const char **parents, size_t n);

ED_EXPORT size_t
ed_mime_max_extent(const EdMime *);

ED_EXPORT void
ed_mime_list(const EdMime *, void (*)(const char *, void *), void *);

#endif

