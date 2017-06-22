#ifndef INCLUDED_EDDY_H
#define INCLUDED_EDDY_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>
#include <errno.h>

#define ED_EXPORT extern __attribute__((visibility ("default")))

// EdConfig and ed_cache_open flags
#define ED_FCHECKSUM     UINT32_C(        0x00000001) /* Calculate checksums for entries. */
#define ED_FNOPAGEALIGN  UINT32_C(        0x00000002) /* Don't force file data to a page boundary. */
#define ED_FREBUILD      UINT64_C(0x0000000800000000) /* Rebuild a new index if invalid. */
#define ED_FCREATE       UINT64_C(0x0000001000000000) /* Always create a new index. */
#define ED_FNOMLOCK      UINT64_C(0x0000002000000000) /* Disable mlocking the index. */
#define ED_FNOBLOCK      UINT64_C(0x0000004000000000) /* May return EAGAIN for open or create. */
#define ED_FNOSYNC       UINT64_C(0x0000008000000000) /* Don't perform file syncing. */
#define ED_FOPTIMIZE     UINT64_C(0x0000010000000000) /* Attempt to optimize when opening. */

// ed_cache_stat flags
#define ED_FSTAT_EXTEND  (1<<0)

typedef struct EdConfig EdConfig;
typedef struct EdCache EdCache;
typedef struct EdObject EdObject;
typedef struct EdObjectAttr EdObjectAttr;

struct EdConfig {
	const char *cache_path;
	const char *index_path;
	uint64_t flags;
};

struct EdObjectAttr {
	uint32_t object_size;
	uint16_t key_size, meta_size;
	const void *key, *meta;
	time_t expiry;
};

#define ed_config_make() ((EdConfig){ .flags = 0 })



ED_EXPORT int
ed_cache_open(EdCache **cachep, const EdConfig *cfg);

ED_EXPORT void
ed_cache_close(EdCache **cachep);

ED_EXPORT EdCache *
ed_cache_ref(EdCache *cache);

ED_EXPORT int
ed_cache_stat(EdCache *cache, FILE *out, int flags);



ED_EXPORT int
ed_open(EdCache *cache, EdObject **objp, const void *key, size_t len);

ED_EXPORT int
ed_create(EdCache *cache, EdObject **objp, EdObjectAttr *attr);

ED_EXPORT int
ed_unlink(EdCache *cache, const void *key, size_t len);

ED_EXPORT int
ed_set_ttl(EdObject *obj, time_t ttl);

ED_EXPORT time_t
ed_tll(const EdObject *obj);

ED_EXPORT time_t
ed_expiry(const EdObject *obj);

ED_EXPORT int64_t
ed_write(EdObject *obj, const void *buf, size_t len);

ED_EXPORT ssize_t
ed_splice(EdObject *obj, int s, size_t len);

ED_EXPORT uint32_t
ed_size(const EdObject *obj);

ED_EXPORT ssize_t
ed_sendfile(EdObject *obj, int s, size_t len);

ED_EXPORT const void *
ed_value(EdObject *obj);

ED_EXPORT int
ed_close(EdObject *obj);

ED_EXPORT int
ed_discard(EdObject *obj);


#define ED_ESYS    1
#define ED_ECONFIG 2
#define ED_EINDEX  3
#define ED_EKEY    4
#define ED_ECACHE  5
#define ED_EMIME   6

#define ed_emake(t, n) (-(((t) << 16) | (((n)+1) & 0xffff)))
#define ed_etype(n) (-(n) >> 16)
#define ed_ecode(n) ((-(n) & 0xffff)-1)

#define ed_esys(n)    ed_emake(ED_ESYS, n)
#define ed_econfig(n) ed_emake(ED_ECONFIG, n)
#define ed_eindex(n)  ed_emake(ED_EINDEX, n)
#define ed_ekey(n)    ed_emake(ED_EKEY, n)
#define ed_ecache(n)  ed_emake(ED_ECACHE, n)
#define ed_emime(n)   ed_emake(ED_EMIME, n)

#define ED_ERRNO ed_esys(errno)

#define ED_ECONFIG_CACHE_NAME    ed_econfig(0)
#define ED_ECONFIG_INDEX_NAME    ed_econfig(1)

#define ED_EINDEX_MODE           ed_eindex(0)
#define ED_EINDEX_SIZE           ed_eindex(1)
#define ED_EINDEX_MAGIC          ed_eindex(2)
#define ED_EINDEX_ENDIAN         ed_eindex(3)
#define ED_EINDEX_MARK           ed_eindex(4)
#define ED_EINDEX_VERSION        ed_eindex(5)
#define ED_EINDEX_FLAGS          ed_eindex(6)
#define ED_EINDEX_MAX_ALIGN      ed_eindex(7)
#define ED_EINDEX_PAGE_SIZE      ed_eindex(8)
#define ED_EINDEX_PAGE_COUNT     ed_eindex(9)
#define ED_EINDEX_ALLOC_COUNT    ed_eindex(10)
#define ED_EINDEX_PAGE_REF       ed_eindex(11)
#define ED_EINDEX_PAGE_LOST      ed_eindex(12)
#define ED_EINDEX_INODE          ed_eindex(13)
#define ED_EINDEX_DEPTH          ed_eindex(14)
#define ED_EINDEX_KEY_MATCH      ed_eindex(15)
#define ED_EINDEX_RANDOM         ed_eindex(16)

#define ED_ECACHE_MODE           ed_ecache(0)
#define ED_ECACHE_SIZE           ed_ecache(1)
#define ED_ECACHE_BLOCK_SIZE     ed_ecache(2)

#define ED_EKEY_LENGTH           ed_ekey(0)

#define ED_EMIME_FILE            ed_emime(0)

ED_EXPORT const char *
ed_strerror(int code);

ED_EXPORT bool ed_eissys(int code);
ED_EXPORT bool ed_eisconfig(int code);
ED_EXPORT bool ed_eisindex(int code);
ED_EXPORT bool ed_eiskey(int code);
ED_EXPORT bool ed_eiscache(int code);
ED_EXPORT bool ed_eismime(int code);

#endif

