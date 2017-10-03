#ifndef INCLUDED_EDDY_H
#define INCLUDED_EDDY_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>
#include <errno.h>

#define ED_EXPORT extern __attribute__((visibility ("default")))

#define ED_MAX_KEY 4032

/** @defgroup  flags  EdConfig and ed_cache_open flags
 * @{
 */
#define ED_FCHECKSUM     UINT32_C(        0x00000001) /** Calculate checksums for entries. */
#define ED_FPAGEALIGN    UINT32_C(        0x00000002) /** Force file data to a page boundary. */
#define ED_FVERBOSE      UINT64_C(0x0000000800000000) /** Print informational messages to stderr. */
#define ED_FCREATE       UINT64_C(0x0000001000000000) /** Create a new index if missing. */
#define ED_FALLOCATE     UINT64_C(0x0000002000000000) /** Allocate slab space when opening. */
#define ED_FREPLACE      UINT64_C(0x0000004000000000) /** Replace an existing index. */
#define ED_FREPAIR       UINT64_C(0x0000008000000000) /** Attempt to repair problems in the index (not yet supported). */
#define ED_FMLOCK        UINT64_C(0x0000010000000000) /** Hint for mlocking the index. */
#define ED_FNOSYNC       UINT64_C(0x0000020000000000) /** Don't perform file syncing. */
#define ED_FASYNC        UINT64_C(0x0000040000000000) /** Use asynchronous syncing. */
#define ED_FNOTLCK       UINT64_C(0x0000080000000000) /** Disable thread locking. */
#define ED_FNOBLOCK      UINT64_C(0x0000100000000000) /** May return EAGAIN for open or create. */
#define ED_FRDONLY       UINT64_C(0x0000200000000000) /** The operation does not need to write. */
#define ED_FRESET        UINT64_C(0x8000000000000000) /** Reset the transaction when closing. */
/** @} */

/** @brief  Seconds in UNIX time */
typedef time_t EdTimeUnix;

/** @brief  Relative time-to-live in seconds */
typedef time_t EdTimeTTL;

typedef struct EdConfig EdConfig;
typedef struct EdCache EdCache;
typedef struct EdObject EdObject;
typedef struct EdObjectAttr EdObjectAttr;

struct EdConfig {
	const char * index_path;
	const char * slab_path;
	unsigned     max_conns;
	long long    slab_size;
	uint64_t     flags;
};

struct EdObjectAttr {
	const void * key;
	const void * meta;
	uint16_t     keylen;
	uint16_t     metalen;
	uint32_t     datalen;
	uint16_t     tag;
};

#define ed_config_make() ((EdConfig){ .flags = 0 })
#define ed_object_attr_make() ((EdObjectAttr){ .tag = 0 })



ED_EXPORT int
ed_cache_open(EdCache **cachep, const EdConfig *cfg);

ED_EXPORT void
ed_cache_close(EdCache **cachep);

ED_EXPORT EdCache *
ed_cache_ref(EdCache *cache);

ED_EXPORT int
ed_cache_stat(EdCache *cache, FILE *out, uint64_t flags);



ED_EXPORT int
ed_open(EdCache *cache, EdObject **objp, const void *key, size_t len);

ED_EXPORT int
ed_create(EdCache *cache, EdObject **objp, const EdObjectAttr *attr);

ED_EXPORT int
ed_update_ttl(EdCache *cache, const void *key, size_t len, EdTimeTTL ttl);

ED_EXPORT int
ed_update_expiry(EdCache *cache, const void *key, size_t len, EdTimeUnix expiry);

ED_EXPORT int
ed_unlink(EdCache *cache, const void *key, size_t len);

ED_EXPORT int64_t
ed_write(EdObject *obj, const void *buf, size_t len);

ED_EXPORT ssize_t
ed_splice(EdObject *obj, int s, size_t len);

ED_EXPORT ssize_t
ed_sendfile(EdObject *obj, int s, size_t len);

ED_EXPORT const void *
ed_value(EdObject *obj, size_t *len);

ED_EXPORT const void *
ed_meta(EdObject *obj, size_t *len);

ED_EXPORT int
ed_close(EdObject **objp);

ED_EXPORT void
ed_discard(EdObject **objp);

ED_EXPORT int
ed_set_ttl(EdObject *obj, EdTimeTTL ttl);

ED_EXPORT int
ed_set_expiry(EdObject *obj, EdTimeUnix expiry);

ED_EXPORT EdTimeTTL
ed_ttl(const EdObject *obj, EdTimeUnix from);

ED_EXPORT EdTimeUnix
ed_expiry(const EdObject *obj);

ED_EXPORT EdTimeUnix
ed_created_at(const EdObject *obj);

ED_EXPORT uint16_t
ed_tag(const EdObject *obj);



/** @defgroup  ed_errors  Error System
 * @{
 */

#define ED_ESYS    1 /** Error group for system errors. */
#define ED_ECONFIG 2 /** Error group for configuration errors. */
#define ED_EINDEX  3 /** Error group for index errors. */
#define ED_EKEY    4 /** Error group for key errors. */
#define ED_ESLAB   5 /** Error group for slab errors. */
#define ED_EOBJECT 6 /** Error group for object errors. */
#define ED_EMIME   7 /** Error group for mime errors. */

/** 
 * @brief  Produces an error code
 * @param  t  The error group type
 * @param  n  The error number within the group
 * @return  Combined error code <0
 */
#define ed_emake(t, n) (-(((t) << 16) | (((n)+1) & 0xffff)))

/**
 * @brief  Extracts the error group
 * @param  n  The error code
 * @return  Error group number
 */
#define ed_etype(n) (-(n) >> 16)

/**
 * @brief  Extracts the group's error number
 * @param  n  The error code
 * @return  Error code for the group
 */
#define ed_ecode(n) ((-(n) & 0xffff)-1)

/**
 * @brief  Produces a system error code
 * @param  n  The system error code
 * @return  Combined error code <0
 */
#define ed_esys(n)    ed_emake(ED_ESYS, n)

/**
 * @brief  Produces a configuration error code
 * @param  n  The configuration error code
 * @return  Combined error code <0
 */
#define ed_econfig(n) ed_emake(ED_ECONFIG, n)

/**
 * @brief  Produces an index error code
 * @param  n  The index error code
 * @return  Combined error code <0
 */
#define ed_eindex(n)  ed_emake(ED_EINDEX, n)

/**
 * @brief  Produces a key error code
 * @param  n  The key error code
 * @return  Combined error code <0
 */
#define ed_ekey(n)    ed_emake(ED_EKEY, n)

/**
 * @brief  Produces a slab error code
 * @param  n  The slab error code
 * @return  Combined error code <0
 */
#define ed_eslab(n)   ed_emake(ED_ESLAB, n)

/**
 * @brief  Produces an object error code
 * @param  n  The object error code
 * @return  Combined error code <0
 */
#define ed_eobject(n) ed_emake(ED_EOBJECT, n)

/**
 * @brief  Produces a mime error code
 * @param  n  The mime error code
 * @return  Combined error code <0
 */
#define ed_emime(n)   ed_emake(ED_EMIME, n)

#define ED_ERRNO ed_esys(errno)                /** Error code for the current global system error. */

#define ED_ECONFIG_SLAB_NAME     ed_econfig(0) /** Error code for an invalid slab path. */
#define ED_ECONFIG_INDEX_NAME    ed_econfig(1) /** Error code for an invalid index path. */

#define ED_EINDEX_MODE           ed_eindex(0)  /** Error code when the index file mode is invalid. */
#define ED_EINDEX_SIZE           ed_eindex(1)  /** Error code when the index size requested is invalid. */
#define ED_EINDEX_MAGIC          ed_eindex(2)  /** Error code if the index magic marker is invalid. */
#define ED_EINDEX_ENDIAN         ed_eindex(3)  /** Error code if the index endian marker is invalid. */
#define ED_EINDEX_MARK           ed_eindex(4)  /** Error code if the index byte marker is invalid. */
#define ED_EINDEX_VERSION        ed_eindex(5)  /** Error code if the index version is invalid. */
#define ED_EINDEX_FLAGS          ed_eindex(6)  /** Error code if the index flags don't match. */
#define ED_EINDEX_PAGE_SIZE      ed_eindex(7)  /** Error code if the index page size changed. */
#define ED_EINDEX_PAGE_REF       ed_eindex(8)  /** Error code if a page is double referenced. */
#define ED_EINDEX_PAGE_LOST      ed_eindex(9)  /** Error code if a page has been lost. */
#define ED_EINDEX_DEPTH          ed_eindex(10) /** Error code if a btree depth exceeds maximum. */
#define ED_EINDEX_KEY_MATCH      ed_eindex(11) /** Error code if an entry key doesn't match the tree key. */
#define ED_EINDEX_RANDOM         ed_eindex(12) /** Error code if /dev/urandom seed failed. */
#define ED_EINDEX_RDONLY         ed_eindex(13) /** Error code if the search cursor is read-only. */
#define ED_EINDEX_BUSY           ed_eindex(14) /** Error code if connection cannot be acquired. */
#define ED_EINDEX_DUPKEY         ed_eindex(15) /** Error code if too many duplicate keys are added */
#define ED_EINDEX_FORK           ed_eindex(16) /** Error code if the index is used across a fork */
#define ED_EINDEX_TXN_CLOSED     ed_eindex(17) /** Error code if the transaction is closed */

#define ED_ESLAB_MODE            ed_eslab(0)   /** Error code when the slab file mode is invalid. */
#define ED_ESLAB_SIZE            ed_eslab(1)   /** Error code when the slab size requested is invalid. */
#define ED_ESLAB_BLOCK_SIZE      ed_eslab(2)   /** Error code when the slab sector size is not supported. */
#define ED_ESLAB_BLOCK_COUNT     ed_eslab(3)   /** Error code when the slab block count changed. */
#define ED_ESLAB_INODE           ed_eslab(4)   /** Error code when the slab inode changed. */

#define ED_EKEY_LENGTH           ed_ekey(0)    /** Error code when the key is too long. */

#define ED_EOBJECT_TOOBIG        ed_eobject(0) /** Error code when too many bytes are written to an object. */
#define ED_EOBJECT_TOOSMALL      ed_eobject(1) /** Error code when too few bytes are written to an object. */
#define ED_EOBJECT_RDONLY        ed_eobject(2) /** Error code when attempting to modify a read-only object. */

#define ED_EMIME_FILE            ed_emime(0)   /** Error code when the mime.cache file can't be loaded. */

/**
 * @brief  Gets the error string for the error code.
 * @param  code  Error code <0
 * @return  C-string error message
 */
ED_EXPORT const char *
ed_strerror(int code);

/**
 * @brief  Tests if the error code is a system error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eissys(int code);

/**
 * @brief  Tests if the error code is a configuration error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eisconfig(int code);

/**
 * @brief  Tests if the error code is an index error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eisindex(int code);

/**
 * @brief  Tests if the error code is a key error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eiskey(int code);

/**
 * @brief  Tests if the error code is a slab error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eisslab(int code);

/**
 * @brief  Tests if the error code is a slab error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eisobject(int code);

/**
 * @brief  Tests if the error code is a mime error.
 * @param  code  Error code <0
 */
ED_EXPORT bool ed_eismime(int code);

/** @} */

#endif

