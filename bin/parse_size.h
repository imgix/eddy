#define ED_KiB 1024ll
#define ED_MiB (ED_KiB*ED_KiB)
#define ED_GiB (ED_KiB*ED_MiB)
#define ED_TiB (ED_KiB*ED_GiB)

static bool
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

