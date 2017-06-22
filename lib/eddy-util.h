#ifndef INCLUDED_EDDY_UTIL_H
#define INCLUDED_EDDY_UTIL_H

#include "eddy-private.h"

#define ED_KiB 1024ll
#define ED_MiB (ED_KiB*ED_KiB)
#define ED_GiB (ED_KiB*ED_MiB)
#define ED_TiB (ED_KiB*ED_GiB)

typedef struct EdInput EdInput;

struct EdInput {
	uint8_t *data;
	size_t length;
	bool mapped;
};

#define ed_input_make() ((EdInput){ NULL, 0, false })

ED_LOCAL bool ed_parse_size(const char *val, long long *out);

ED_LOCAL  int ed_input_read(EdInput *in, int fd, off_t max);
ED_LOCAL  int ed_input_fread(EdInput *in, const char *path, off_t max);
ED_LOCAL void ed_input_final(EdInput *in);

#endif

