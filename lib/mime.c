#include "eddy-private.h"
#include "eddy-mime.h"

#include <ctype.h>

#define ed_mime_ptr(T, db, off) ed_ptr_b32(T, (db)->data, off)

typedef enum EdCharset {
	ED_CHARSET_BINARY,
	ED_CHARSET_ASCII,
	ED_CHARSET_UTF8,
	ED_CHARSET_UTF16BE,
	ED_CHARSET_UTF16LE,
	ED_CHARSET_UTF32BE,
	ED_CHARSET_UTF32LE,
} EdCharset;

typedef struct EdMimeHdr EdMimeHdr;
typedef struct EdMimeAlias EdMimeAlias;
typedef struct EdMimeAliasList EdMimeAliasList;
typedef struct EdMimeParents EdMimeParents;
typedef struct EdMimeParent EdMimeParent;
typedef struct EdMimeParentList EdMimeParentList;
typedef struct EdMimeMagic EdMimeMagic;
typedef struct EdMimeMatchlet EdMimeMatchlet;
typedef struct EdMimeIcon EdMimeIcon;
typedef struct EdMimeIconList EdMimeIconList;

struct EdMimeHdr {
	uint16_t major_version;
	uint16_t minor_version;
	uint32_t alias_list_offset;
	uint32_t parent_list_offset;
	uint32_t literal_list_offset;
	uint32_t reverse_suffix_tree_offset;
	uint32_t glob_list_offset;
	uint32_t magic_list_offset;
	uint32_t namespace_list_offset;
	uint32_t icons_list_offset;
	uint32_t generic_icons_list_offset;
};

struct EdMime {
	union {
		const uint8_t *data;
		const EdMimeHdr *hdr;
	};
	size_t size;
	uint32_t max_extent;
	bool mapped;
	uint16_t major_version;
	uint16_t minor_version;
	const EdMimeAliasList *alias_list;
	const EdMimeParentList *parent_list;
	const EdMimeMagic *magic_list;
};

struct EdMimeAlias {
	uint32_t alias_offset;
	uint32_t mime_type_offset;
};

struct EdMimeAliasList {
	uint32_t n_aliases;
	EdMimeAlias alias_list[];
};

struct EdMimeParents {
	uint32_t n_parents;
	uint32_t mime_type_offsets[];
};

struct EdMimeParent {
	uint32_t mime_type_offset;
	uint32_t parents_offset;
};

struct EdMimeParentList {
	uint32_t n_entries;
	EdMimeParent parent_list[];
};

struct EdMimeMagic {
	uint32_t n_matches;
	uint32_t max_extent;
	uint32_t first_match_offset;
};

struct EdMimeMatch {
	uint32_t priority;
	uint32_t mime_type_offset;
	uint32_t n_matchlets;
	uint32_t first_matchlet_offset;
};

struct EdMimeMatchlet {
	uint32_t range_start;
	uint32_t range_length;
	uint32_t word_size;
	uint32_t value_length;
	uint32_t value;
	uint32_t mask;
	uint32_t n_children;
	uint32_t first_child_offset;
};

struct EdMimeIcon {
	uint32_t mime_type_offset;
	uint32_t icon_name_offset;
};

struct EdMimeIconList {
	uint32_t n_icons;
	EdMimeIcon icon_list[];
};

static inline uint32_t
fetch32(const uint8_t *restrict p)
{
	uint32_t result;
	memcpy(&result, p, sizeof(result));
	return ed_b32(result);
}

static inline bool
maskeq(const uint8_t *a, const uint8_t *b, const uint8_t *m, size_t len)
{
	for (size_t i = 0; i < len; i++) {
		if ((a[i] & m[i]) != (b[i] & m[i])) { return false; }
	}
	return true;
}

static uint32_t
max_extent_matchlet(const EdMime *db, const EdMimeMatchlet *m, const uint8_t *end)
{
	uint32_t rng_off  = ed_b32(m->range_start);
	uint32_t rng_end  = ed_b32(m->range_length) + rng_off;
	uint32_t data_len = ed_b32(m->value_length);
	uint32_t max = rng_end + data_len;
	uint32_t nel = ed_b32(m->n_children);
	if (nel > 0) {
		const EdMimeMatchlet *base = ed_mime_ptr(EdMimeMatchlet, db, m->first_child_offset);
		for (uint32_t i = 0; i < nel; i++) {
			uint32_t ext = max_extent_matchlet(db, &base[i], end);
			if (ext == 0) { return 0; }
			if (ext > max) { max = ext; }
		}
	}
	return max;
}

static uint32_t
max_extent_match(const EdMime *db, const EdMimeMatch *m, const void *end)
{
	uint32_t nel = ed_b32(m->n_matchlets);
	const EdMimeMatchlet *base = ed_mime_ptr(EdMimeMatchlet, db, m->first_matchlet_offset);
	uint32_t i;
	uint32_t max = 0;

	for (i = 0; i < nel; i++) {
		uint32_t ext = max_extent_matchlet(db, &base[i], end);
		if (ext == 0) { return 0; }
		if (ext > max) { max = ext; }
	}
	return max;
}

static uint32_t
check_max_extent(const EdMime *db, const void *end)
{
	uint32_t nel = ed_b32(db->magic_list->n_matches);
	const EdMimeMatch *base = ed_mime_ptr(EdMimeMatch, db, db->magic_list->first_match_offset);
	uint32_t max = 0;

	if ((void *)base > end) { return 0; }

	for (uint32_t i = 0; i < nel; i++) {
		uint32_t ext = max_extent_match(db, &base[i], end);
		if (ext == 0) { return 0; }
		if (ext > max) { max = ext; }
	}
	return max;
}

int
ed_mime_load(EdMime **dbp, const void *data, size_t size, int flags)
{
	if (size < sizeof(EdMimeHdr)) { return ED_EMIME_FILE; }

	const EdMimeHdr *hdr = data;
	uint32_t generic_icons_list_offset = ed_b32(hdr->generic_icons_list_offset);

	if (generic_icons_list_offset + sizeof(EdMimeIconList) > size ||
			generic_icons_list_offset < ed_b32(hdr->alias_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->alias_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->parent_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->literal_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->reverse_suffix_tree_offset) ||
			generic_icons_list_offset < ed_b32(hdr->glob_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->magic_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->namespace_list_offset) ||
			generic_icons_list_offset < ed_b32(hdr->icons_list_offset)
		) {
		return ED_EMIME_FILE;
	}

	EdMime db = {
		.data = data,
		.size = size,
		.mapped = false,
		.major_version = ed_b16(hdr->major_version),
		.minor_version = ed_b16(hdr->minor_version),
		.alias_list = ed_ptr_b32(EdMimeAliasList, data, hdr->alias_list_offset),
		.parent_list = ed_ptr_b32(EdMimeParentList, data, hdr->parent_list_offset),
		.magic_list = ed_ptr_b32(EdMimeMagic, data, hdr->magic_list_offset),
	};
	db.max_extent = ed_b32(db.magic_list->max_extent);

	if (!(flags & ED_FMIME_NOVERIFY)) {
		uint32_t generic_icons_list_count = fetch32((uint8_t *)data + generic_icons_list_offset);
		size_t max = generic_icons_list_offset + sizeof(EdMimeIconList)
			+ generic_icons_list_count * sizeof(EdMimeIcon);
		if (max > size) {
			return ED_EMIME_FILE;
		}

		uint32_t ext = check_max_extent(&db, (const uint8_t *)data + size);
		if (db.max_extent != ext) {
			return ED_EMIME_FILE;
		}
	}

	if ((*dbp = malloc(sizeof(db))) == NULL) { return ED_ERRNO; }
	memcpy(*dbp, &db, sizeof(db));
	return 0;
}

int
ed_mime_open(EdMime **dbp, const char *path, int flags)
{
	(void)flags;

	int fd = open(path, O_RDONLY);
	if (fd < 0) { return -1; }

	struct stat stat;
	void *data;
	bool ok =
		fstat(fd, &stat) == 0 &&
		(data = mmap(NULL, stat.st_size, PROT_READ, MAP_SHARED, fd, 0)) != MAP_FAILED;
	close(fd);
	if (!ok) { return -1; }

	madvise(data, stat.st_size, MADV_RANDOM|MADV_WILLNEED);
	if (flags & ED_FMIME_MLOCK) {
		mlock(data, stat.st_size);
	}

	int rc = ed_mime_load(dbp, data, stat.st_size, flags);
	if (rc == 0) {
		if (*dbp == NULL) { return ed_esys(EFAULT); }
		(*dbp)->mapped = true;
	}
	return rc;
}

void
ed_mime_close(EdMime **dbp)
{
	EdMime *db = *dbp;
	if (db == NULL) { return; }

	*dbp = NULL;
	if (db->mapped) {
		munmap((void *)db->data, db->size);
	}
	free(db);
}

const EdMimeMatch *
ed_mime_get_match(const EdMime *db, const char *mime)
{
	assert(db != NULL);
	assert(mime != NULL);

	uint32_t nel = ed_b32(db->magic_list->n_matches);
	const EdMimeMatch *base = ed_mime_ptr(EdMimeMatch, db, db->magic_list->first_match_offset);
	for (uint32_t i = 0; i < nel; i++) {
		if (strcmp(ed_mime_ptr(char, db, base[i].mime_type_offset), mime) == 0) {
			return &base[i];
		}
	}
	return NULL;
}

static bool
is_matchlet(const EdMime *db, const EdMimeMatchlet *m, const uint8_t *data, size_t len)
{
	uint32_t rng_off  = ed_b32(m->range_start);
	uint32_t rng_end  = ed_b32(m->range_length) + rng_off;
	uint32_t data_len = ed_b32(m->value_length);
	uint32_t data_off = ed_b32(m->value);
	uint32_t mask_off = ed_b32(m->mask);
	uint32_t nel, i;

	for (i = rng_off; i < rng_end; i++) {
		// verify that the matchlet won't exceed the data length
		if (i + data_len > len) { return false; }

		if (mask_off) {
			// compare using a mask of each byte
			if (maskeq(db->data+data_off, data+i, db->data+mask_off, data_len)) {
				goto sub;
			}
		}
		else {
			if (memcmp(db->data+data_off, data+i, data_len) == 0) {
				goto sub;
			}
		}
	}
	return false;

sub:
	nel = ed_b32(m->n_children);
	if (nel == 0) {
		return true;
	}

	const EdMimeMatchlet *base = ed_mime_ptr(EdMimeMatchlet, db, m->first_child_offset);
	for (i = 0; i < nel; i++) {
		if (!is_matchlet(db, &base[i], data, len)) {
			return false;
		}
	}
	return true;
}

static bool
is_match(const EdMime *db, const EdMimeMatch *m, const void *data, size_t len)
{
	uint32_t nel = ed_b32(m->n_matchlets);
	const EdMimeMatchlet *base = ed_mime_ptr(EdMimeMatchlet, db, m->first_matchlet_offset);
	uint32_t i;

	for (i = 0; i < nel; i++) {
		if (is_matchlet(db, &base[i], data, len)) {
			return true;
		}
	}
	return false;
}

uint32_t
ed_mime_test_match(const EdMime *db, const EdMimeMatch *m, const void *data, size_t len)
{
	return is_match(db, m, data, len) ? ed_b32(m->priority) : 0;
}

const char *
ed_mime_type(const EdMime *db, const void *data, size_t len, bool fallback)
{
	assert(db != NULL);
	assert(data != NULL);
	assert(len > 0);

	uint32_t nel = ed_b32(db->magic_list->n_matches);
	const EdMimeMatch *base = ed_mime_ptr(EdMimeMatch, db, db->magic_list->first_match_offset);
	const char *mime = NULL;
	uint32_t max = 0;

	for (uint32_t i = 0; i < nel; i++) {
		uint32_t p = ed_mime_test_match(db, &base[i], data, len);
		if (p > max) {
			max = p;
			mime = ed_mime_ptr(char, db, base[i].mime_type_offset);
		}
	}

	if (mime == NULL && fallback) {
		mime = ed_mime_fallback(db, data, len);
	}

	return mime;
}

static bool
is_utf32(uint32_t ch)
{
	return 0 < ch && ch <= UINT32_C(0x10FFFF);
}

static EdCharset
maybe_utf32_le(const uint8_t *p, size_t len)
{
	const uint32_t *a = (const uint32_t *)p;
	size_t n = len / 4;
	for (size_t i = 0; i < n; i++) {
		if (!is_utf32(ed_l32(a[i]))) { return ED_CHARSET_BINARY; }
	}
	return ED_CHARSET_UTF32LE;
}

static EdCharset
maybe_utf32_be(const uint8_t *p, size_t len)
{
	const uint32_t *a = (const uint32_t *)p;
	size_t n = len / 4;
	for (size_t i = 0; i < n; i++) {
		if (!is_utf32(ed_b32(a[i]))) { return ED_CHARSET_BINARY; }
	}
	return ED_CHARSET_UTF32BE;
}

static size_t
is_utf16(uint16_t ch1, uint16_t ch2)
{
	if (ch1 == 0) { return 0; }
	if (ch1 < 0xd800 || ch1 > 0xdfff) { return 2; }
	if (ch1 >= 0xd800 && ch1 <= 0xdbff && ch2 >= 0xdc00 && ch2 <= 0xdfff) { return 4; }
	return 0;
}

static EdCharset
maybe_utf16_le(const uint8_t *p, size_t len)
{
	const uint16_t *a = (const uint16_t *)p;
	size_t n = (len / 4) * 2;
	for (size_t i = 0; i < n; ) {
		size_t c = is_utf16(ed_l16(a[i]), ed_l16(a[i+1]));
		if (c == 0) { return ED_CHARSET_BINARY; }
		i += c;
	}
	return ED_CHARSET_UTF16LE;
}

static EdCharset
maybe_utf16_be(const uint8_t *p, size_t len)
{
	const uint16_t *a = (const uint16_t *)p;
	size_t n = (len / 4) * 2;
	for (size_t i = 0; i < n; ) {
		size_t c = is_utf16(ed_b16(a[i]), ed_b16(a[i+1]));
		if (c == 0) { return ED_CHARSET_BINARY; }
		i += c;
	}
	return ED_CHARSET_UTF16BE;
}

static EdCharset
maybe_text(const uint8_t *p, size_t len)
{
	const uint8_t *pe = p + len;

	static const uint8_t u32_le[] = { 0xFF, 0xFE, 0x00, 0x00 };
	static const uint8_t u32_be[] = { 0x00, 0x00, 0xFE, 0xFF };
	if (len >= sizeof(u32_le)) {
		if (memcmp(p, u32_le, sizeof(u32_le)) == 0) {
			return maybe_utf32_le(p + sizeof(u32_le), len - sizeof(u32_le));
		}
		if (memcmp(p, u32_be, sizeof(u32_be)) == 0) {
			return maybe_utf32_be(p + sizeof(u32_be), len - sizeof(u32_be));
		}
	}

	static const uint8_t u16_le[] = { 0xFF, 0xFE };
	static const uint8_t u16_be[] = { 0xFE, 0xFF };
	if (len >= sizeof(u16_le)) {
		if (memcmp(p, u16_le, sizeof(u16_le)) == 0) {
			return maybe_utf16_le(p + sizeof(u16_le), len - sizeof(u16_le));
		}
		if (memcmp(p, u16_be, sizeof(u16_be)) == 0) {
			return maybe_utf16_be(p + sizeof(u16_be), len - sizeof(u16_be));
		}
	}

	EdCharset cs = ED_CHARSET_ASCII;

	// check for UTF-8/ASCII
	while (p < pe) {
		if (*p == 0) { return ED_CHARSET_BINARY; }
		if (*p < 0x80) {
			/* 0xxxxxxx */
			p++;
		}
		else if ((p[0] & 0xe0) == 0xc0) {
			if (pe - p < 2) { break; }
			/* 110XXXXx 10xxxxxx */
			if ((p[1] & 0xc0) != 0x80 || (p[0] & 0xfe) == 0xc0) {
				return ED_CHARSET_BINARY;
			}
			p += 2;
			cs = ED_CHARSET_UTF8;
		}
		else if ((p[0] & 0xf0) == 0xe0) {
			if (pe - p < 3) { break; }
			/* 1110XXXX 10Xxxxxx 10xxxxxx */
			if ((p[1] & 0xc0) != 0x80 ||
				(p[2] & 0xc0) != 0x80 ||
				(p[0] == 0xe0 && (p[1] & 0xe0) == 0x80) ||
				(p[0] == 0xed && (p[1] & 0xe0) == 0xa0) ||
				(p[0] == 0xef && p[1] == 0xbf && (p[2] & 0xfe) == 0xbe)) {
				return ED_CHARSET_BINARY;
			}
			p += 3;
			cs = ED_CHARSET_UTF8;
		}
		else if ((p[0] & 0xf8) == 0xf0) {
			if (pe - p < 4) { break; }
			/* 11110XXX 10XXxxxx 10xxxxxx 10xxxxxx */
			if ((p[1] & 0xc0) != 0x80 ||
				(p[2] & 0xc0) != 0x80 ||
				(p[3] & 0xc0) != 0x80 ||
				(p[0] == 0xf0 && (p[1] & 0xf0) == 0x80) ||
				(p[0] == 0xf4 && p[1] > 0x8f) || p[0] > 0xf4) {
				return ED_CHARSET_BINARY;
			}
			p += 4;
			cs = ED_CHARSET_UTF8;
		}
		else {
			return ED_CHARSET_BINARY;
		}
	}
	return cs;
}

const char *
ed_mime_fallback(const EdMime *db, const void *data, size_t len)
{
	static const char *fallback[] = {
		[ED_CHARSET_BINARY] = "application/octet-stream",
		[ED_CHARSET_ASCII] = "text/plain",
		[ED_CHARSET_UTF8] = "text/plain; charset=utf-8",
		[ED_CHARSET_UTF16BE] = "text/plain; charset=utf-16be",
		[ED_CHARSET_UTF16LE] = "text/plain; charset=utf-16le",
		[ED_CHARSET_UTF32BE] = "text/plain; charset=utf-32be",
		[ED_CHARSET_UTF32LE] = "text/plain; charset=utf-32le",
	};

	(void)db;
	if (len > 2048) { len = 2048; }
	return fallback[maybe_text(data, len)];
}

const char *
ed_mime_file_type(const EdMime *db, const char *path, bool fallback)
{
	assert(db != NULL);
	assert(path != NULL);

	int fd = -1;
	struct stat st;
	void *data = MAP_FAILED;
	const char *mime = NULL;

	fd = open(path, O_RDONLY, 0);
	if (fd < 0) {
		return NULL;
	}

	if (fstat(fd, &st) < 0) {
		goto out;
	}

	switch (st.st_mode & S_IFMT) {
	case S_IFIFO:  mime = "inode/fifo"; goto out;
	case S_IFCHR:  mime = "inode/chardevice"; goto out;
	case S_IFDIR:  mime = "inode/directory"; goto out;
	case S_IFBLK:  mime = "inode/blockdevice"; goto out;
	case S_IFLNK:  mime = "inode/symlink"; goto out;
	case S_IFSOCK: mime = "inode/socket"; goto out;
	}

	data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (data == MAP_FAILED) { goto out; }
	madvise(data, st.st_size, MADV_SEQUENTIAL|MADV_WILLNEED);

	mime = ed_mime_type(db, data, st.st_size, false);
	// TODO: glob check
	if (mime == NULL && fallback) {
		mime = ed_mime_fallback(db, data, st.st_size);
	}

	munmap((void *)data, st.st_size);
out:
	close(fd);
	return mime;
}

const char *
ed_mime_alias(const EdMime *db, const char *mime)
{
	assert(db != NULL);
	assert(mime != NULL);

	uint32_t nel = ed_b32(db->alias_list->n_aliases);
	const EdMimeAlias *try, *base = db->alias_list->alias_list;
	int cmp;

	while (nel > 0) {
		try = base + nel/2;
		cmp = strcmp(mime, ed_mime_ptr(char, db, try->alias_offset));
		if (cmp == 0) {
			return ed_mime_ptr(char, db, try->mime_type_offset);
		}
		if (nel == 1) { break; }
		if (cmp < 0) {
			nel /= 2;
		}
		else {
			base = try;
			nel -= nel/2;
		}
	}
	return mime;
}

size_t
ed_mime_parents(const EdMime *db, const char *mime, const char **parents, size_t n)
{
	assert(db != NULL);
	assert(mime != NULL);
	assert(parents != NULL);

	uint32_t nel = ed_b32(db->parent_list->n_entries);
	const EdMimeParent *try, *base = db->parent_list->parent_list;
	int cmp;

	while (nel > 0) {
		try = base + nel/2;
		cmp = strcmp(mime, ed_mime_ptr(char, db, try->mime_type_offset));
		if (cmp == 0) {
			const EdMimeParents *par = ed_mime_ptr(EdMimeParents, db, try->parents_offset);
			uint32_t npar = ed_b32(par->n_parents);
			if ((size_t)npar < n) { n = npar; }
			for (size_t i = 0; i < n; i++) {
				parents[i] = ed_mime_ptr(char, db, par->mime_type_offsets[i]);
			}
			return n;
		}
		if (nel == 1) { break; }
		if (cmp < 0) {
			nel /= 2;
		}
		else {
			base = try;
			nel -= nel/2;
		}
	}
	return 0;
}

size_t
ed_mime_max_extent(const EdMime *db)
{
	return db->max_extent;
}

void
ed_mime_list(const EdMime *db, void (*cb)(const char *, void *), void *data)
{
	assert(db != NULL);
	assert(cb != NULL);

	uint32_t nel = ed_b32(db->magic_list->n_matches);
	const EdMimeMatch *base = ed_mime_ptr(EdMimeMatch, db, db->magic_list->first_match_offset);
	for (uint32_t i = 0; i < nel; i++) {
		cb(ed_mime_ptr(char, db, base[i].mime_type_offset), data);
	}
}

