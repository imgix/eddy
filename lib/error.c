#include "eddy-private.h"

static const char *const econfig[] = {
	[ed_ecode(ED_ECONFIG_SLAB_NAME)]    = "slab name is too long",
	[ed_ecode(ED_ECONFIG_INDEX_NAME)]    = "index name is too long",
};

static const char *const eindex[] = {
	[ed_ecode(ED_EINDEX_MODE)]           = "index file mode invalid",
	[ed_ecode(ED_EINDEX_SIZE)]           = "index size is invalid",
	[ed_ecode(ED_EINDEX_MAGIC)]          = "index has invalid magic value",
	[ed_ecode(ED_EINDEX_ENDIAN)]         = "index has incorrect endianness",
	[ed_ecode(ED_EINDEX_MARK)]           = "index mark is invalid",
	[ed_ecode(ED_EINDEX_VERSION)]        = "index version is unsupported",
	[ed_ecode(ED_EINDEX_FLAGS)]          = "index flags mismatched",
	[ed_ecode(ED_EINDEX_MAX_ALIGN)]      = "index max alignment differs",
	[ed_ecode(ED_EINDEX_PAGE_SIZE)]      = "index page size differs",
	[ed_ecode(ED_EINDEX_ALLOC_COUNT)]    = "index allocation count differs",
	[ed_ecode(ED_EINDEX_PAGE_REF)]       = "index page has multiple references",
	[ed_ecode(ED_EINDEX_PAGE_LOST)]      = "index page has been lost",
	[ed_ecode(ED_EINDEX_DEPTH)]          = "index btree depth limit exceeded",
	[ed_ecode(ED_EINDEX_KEY_MATCH)]      = "index btree entry does not match key",
	[ed_ecode(ED_EINDEX_RANDOM)]         = "failed to produce seed from /dev/urandom",
	[ed_ecode(ED_EINDEX_RDONLY)]         = "the search cursor is read-only",
};

static const char *const ekey[] = {
	[ed_ecode(ED_EKEY_LENGTH)]           = "key length too long",
};

static const char *const eslab[] = {
	[ed_ecode(ED_ESLAB_MODE)]            = "slab file mode invalid",
	[ed_ecode(ED_ESLAB_SIZE)]            = "slab file size too large",
	[ed_ecode(ED_ESLAB_BLOCK_SIZE)]      = "slab file block/sector size is not supported",
	[ed_ecode(ED_ESLAB_BLOCK_COUNT)]     = "slab file block/sector count has changed",
	[ed_ecode(ED_ESLAB_INODE)]           = "slab inode reference invalid",
};

static const char *const emime[] = {
	[ed_ecode(ED_EMIME_FILE)]            = "invalid mime.cache file",
};

#define EGETMSG(arr, idx) \
	(idx >= 0 && idx < (int)(sizeof(arr) / sizeof(arr[0])) ? arr[idx] : NULL)

const char *
ed_strerror(int code)
{
	const char *msg = NULL;
	if (code < 0) {
		int ec = ed_ecode(code);
		switch (ed_etype(code)) {
		case ED_ESYS:    msg = strerror(ec); break;
		case ED_ECONFIG: msg = EGETMSG(econfig, ec); break;
		case ED_EINDEX:  msg = EGETMSG(eindex, ec); break;
		case ED_EKEY:    msg = EGETMSG(ekey, ec); break;
		case ED_ESLAB:   msg = EGETMSG(eslab, ec); break;
		case ED_EMIME:   msg = EGETMSG(emime, ec); break;
		}
	}
	return msg ? msg : "Undefined error";
}

bool ed_eissys(int code)    { return ed_etype(code) == ED_ESYS; }
bool ed_eisconfig(int code) { return ed_etype(code) == ED_ECONFIG; }
bool ed_eisindex(int code)  { return ed_etype(code) == ED_EINDEX; }
bool ed_eiskey(int code)    { return ed_etype(code) == ED_EKEY; }
bool ed_eisslab(int code)   { return ed_etype(code) == ED_ESLAB; }
bool ed_eismime(int code)   { return ed_etype(code) == ED_EMIME; }

