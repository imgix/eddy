#ifndef INCLUDED_EDDY_PRIVATE_H
#define INCLUDED_EDDY_PRIVATE_H

#include "eddy.h"

#include <stdatomic.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <assert.h>

#if defined(__linux__)
# include <linux/fs.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
# include <sys/disk.h>
#else
# error Platform not supported
#endif

#define ED_FSAVE(f) ((uint32_t)((f) & UINT64_C(0x00000000FFFFFFFF)))
#define ED_FOPEN(f) ((f) & UINT64_C(0xFFFFFFFF00000000))

#define ED_LOCAL __attribute__((visibility ("hidden")))

#ifndef ED_ALLOC_COUNT
# define ED_ALLOC_COUNT 16
#endif

#ifndef ED_MAX_ALIGN
# ifdef __BIGGEST_ALIGNMENT__
#  define ED_MAX_ALIGN __BIGGEST_ALIGNMENT__
# else
#  define ED_MAX_ALIGN 16
# endif
#endif

#define ed_len(arr) (sizeof(arr) / sizeof((arr)[0]))

#define ED_INLINE inline __attribute__((always_inline))

#if BYTE_ORDER == LITTLE_ENDIAN
# define ed_b16(v) __builtin_bswap16(v)
# define ed_l16(v) (v)
# define ed_b32(v) __builtin_bswap32(v)
# define ed_l32(v) (v)
# define ed_b64(v) __builtin_bswap64(v)
# define ed_l64(v) (v)
#elif BYTE_ORDER == BIG_ENDIAN
# define ed_b16(v) (v)
# define ed_l16(v) __builtin_bswap16(v)
# define ed_b32(v) (v)
# define ed_l32(v) __builtin_bswap32(v)
# define ed_b64(v) (v)
# define ed_l64(v) __builtin_bswap64(v)
#endif

#define ed_ptr_b32(T, b, off) ((const T *)((const uint8_t *)(b) + ed_b32(off)))

#define ED_PGINDEX     UINT32_C(0x998ddcb0)
#define ED_PGFREE_HEAD UINT32_C(0xc3e873b2)
#define ED_PGFREE_CHLD UINT32_C(0xea104f71)
#define ED_PGBRANCH    UINT32_C(0x2c17687a)
#define ED_PGLEAF      UINT32_C(0x2dd39a85)

#define ED_NODE_PAGE_COUNT ((PAGESIZE - sizeof(EdBTree)) / sizeof(EdNodePage))
#define ED_NODE_KEY_COUNT ((PAGESIZE - sizeof(EdBTree)) / sizeof(EdNodeKey))

#define ED_PAGE_NONE UINT32_MAX
#define ED_BLK_NONE UINT64_MAX
#define ED_TIME_DELETE 0
#define ED_TIME_INF UINT32_MAX

typedef uint32_t EdPgno;
typedef uint64_t EdBlkno;
typedef uint64_t EdHash;

typedef struct EdPg EdPg;
typedef struct EdPgfree EdPgfree;
typedef struct EdPgtail EdPgtail;
typedef struct EdPgalloc EdPgalloc;
typedef struct EdPgallocHdr EdPgallocHdr;

typedef struct EdBTree EdBTree;
typedef struct EdBSearch EdBSearch;
typedef struct EdBNode EdBNode;
typedef struct EdNodePage EdNodePage;
typedef struct EdNodeKey EdNodeKey;
typedef struct EdIndex EdIndex;
typedef struct EdIndexHdr EdIndexHdr;
typedef struct EdObjectHdr EdObjectHdr;

typedef enum EdLock {
	ED_LOCK_SH = F_RDLCK,
	ED_LOCK_EX = F_WRLCK,
	ED_LOCK_UN = F_UNLCK,
} EdLock;

struct EdPgalloc {
	EdPgallocHdr *hdr;
	void *pg;
	EdPgfree *free;
	uint64_t flags;
	int fd;
	uint8_t dirty, free_dirty;
	bool from_new;
};

struct EdIndex {
	EdPgalloc alloc;
	uint64_t flags;
	uint64_t seed;
	int64_t epoch;
	EdIndexHdr *hdr;
	EdBTree *blocks;
	EdBTree *keys;
	pthread_rwlock_t rw;
};

struct EdCache {
	EdIndex index;
	atomic_int ref;
	int fd;
	size_t bytes_used;
	size_t pages_used;
};

struct EdObject {
	EdCache *cache;
	time_t expiry;
	const void *data;
	const void *key;
	const void *meta;
	size_t datalen;
	uint16_t keylen;
	uint16_t metalen;
	EdObjectHdr *hdr;
};

struct EdBSearch {
	EdBTree **root;       // indirect reference to the root node
	struct EdBNode {
		EdBTree *tree, *parent;
		bool dirty;
		// TODO add index and change dirty to uint8_t
	} nodes[24];          // node path to the leaf
	uint64_t key;         // key searched for
	void *entry;          // pointer to the entry in the leaf
	size_t entry_size;    // size in bytes of the entry
	uint32_t entry_index; // index of the entry in the leaf
	int fd;               // file descriptor for mapping pages
	int nnodes;           // number of nodes in the path list
	int nsplits;          // number of nodes requiring splits for an insert
	int nextra;           // number of extra nodes stashed in the node array
	int match;            // return code of the search
};

#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wpadded"

struct EdPg {
	EdPgno no;
	uint32_t type;
};

struct EdPgfree {
	EdPg base;
	EdPgno count;
#define ED_PGFREE_COUNT ((PAGESIZE - sizeof(EdPg) - sizeof(EdPgno)) / sizeof(EdPgno))
	EdPgno pages[ED_PGFREE_COUNT];
};

struct EdPgallocHdr {
	uint16_t size_page;
	uint16_t size_block;
	EdPgno free_list;
	_Atomic struct EdPgtail {
		EdPgno start;
		EdPgno off;
	} tail;
};

struct EdIndexHdr {
	EdPg base;
	char magic[4];
	char endian;
	uint8_t mark;
	uint16_t version;
	uint32_t flags;
	uint32_t pos;
	EdPgno key_tree;
	EdPgno block_tree;
	uint64_t seed;
	int64_t epoch;
	EdPgallocHdr alloc;
	uint64_t slab_ino;
	EdPgno slab_page_count;
	uint8_t size_align;
	uint8_t alloc_count;
	uint8_t _pad[2];
};

struct EdObjectHdr {
	uint64_t hash;
	uint32_t expiry;
	uint32_t datalen;
	uint16_t keylen;
	uint16_t metalen;
	uint32_t _pad;
};

struct EdBTree {
	EdPg base;
	EdPgno right;
	uint16_t vers;
	uint16_t nkeys;
	uint8_t data[PAGESIZE - sizeof(EdPg) - sizeof(EdPgno) - 4];
};

struct EdNodePage {
	EdBlkno block; // XXX last block of the entry?
	uint32_t exp;
	EdPgno meta;
};

struct EdNodeKey {
	EdHash hash;
	uint32_t exp;
	EdPgno meta;
	EdBlkno slab;
};

#pragma GCC diagnostic pop


ED_LOCAL uint64_t ed_hash(const uint8_t *val, size_t len, uint64_t seed);


/* Page Module */
ED_LOCAL   void * ed_pgmap(int fd, EdPgno no, EdPgno count);
ED_LOCAL      int ed_pgunmap(void *p, EdPgno count);
ED_LOCAL      int ed_pgsync(void *p, EdPgno count, int flags, uint8_t lvl);
ED_LOCAL   void * ed_pgload(int fd, EdPg **pgp, EdPgno no);
ED_LOCAL     void ed_pgmark(EdPg *pg, EdPgno *no, uint8_t *dirty);
ED_LOCAL      int ed_pgalloc_new(EdPgalloc *, const char *, size_t meta);
ED_LOCAL     void ed_pgalloc_init(EdPgalloc *, EdPgallocHdr *, int fd, uint64_t flags);
ED_LOCAL     void ed_pgalloc_close(EdPgalloc *);
ED_LOCAL     void ed_pgalloc_sync(EdPgalloc *);
ED_LOCAL   void * ed_pgalloc_meta(EdPgalloc *alloc);
ED_LOCAL      int ed_pgalloc(EdPgalloc *, EdPg **, EdPgno n, bool exclusive);
ED_LOCAL     void ed_pgfree(EdPgalloc *, EdPg **, EdPgno n);
ED_LOCAL   void * ed_pgfree_list(EdPgalloc *);
#if ED_MMAP_DEBUG
ED_LOCAL     void ed_pgtrack(EdPgno no, uint8_t *pg, EdPgno count);
ED_LOCAL     void ed_pguntrack(uint8_t *pg, EdPgno count);
ED_LOCAL      int ed_pgcheck(void);
#endif


#if ED_BACKTRACE
typedef struct EdBacktrace EdBacktrace;

ED_LOCAL      int ed_backtrace_new(EdBacktrace **);
ED_LOCAL     void ed_backtrace_free(EdBacktrace **);
ED_LOCAL     void ed_backtrace_print(EdBacktrace *, int skip, FILE *);
ED_LOCAL      int ed_backtrace_index(EdBacktrace *, const char *name);
#endif


/* B+Tree Module */

ED_LOCAL     void ed_btree_init(EdBTree *);
ED_LOCAL      int ed_btree_search(EdBTree **, int fd, uint64_t key, size_t entry_size, EdBSearch *);
ED_LOCAL      int ed_bsearch_next(EdBSearch *);
ED_LOCAL      int ed_bsearch_ins(EdBSearch *, const void *entry, EdPgalloc *);
ED_LOCAL      int ed_bsearch_set(EdBSearch *, const void *entry);
ED_LOCAL      int ed_bsearch_del(EdBSearch *);
ED_LOCAL     void ed_bsearch_final(EdBSearch *);


/* Index Module */
ED_LOCAL      int ed_index_open(EdIndex *, const char *path, int64_t slabsize, uint64_t flags, uint64_t ino);
ED_LOCAL     void ed_index_close(EdIndex *);
ED_LOCAL      int ed_index_load_trees(EdIndex *);
ED_LOCAL      int ed_index_save_trees(EdIndex *);
ED_LOCAL      int ed_index_lock(EdIndex *, EdLock type, bool wait);
ED_LOCAL      int ed_index_stat(EdIndex *, FILE *, int flags);


/* Random Module */
ED_LOCAL      int ed_rnd_open(void);
ED_LOCAL      int ed_rnd_global(void);
ED_LOCAL     void ed_rnd_close(void);
ED_LOCAL  ssize_t ed_rnd_buffer(void *buf, size_t len);
ED_LOCAL      int ed_rnd_u64(uint64_t *);


/* Time Module */
ED_LOCAL  int64_t ed_now(int64_t epoch);
ED_LOCAL uint32_t ed_expire(int64_t epoch, time_t ttlsec);
ED_LOCAL   time_t ed_ttl_at(int64_t epoch, uint32_t exp, time_t t);
ED_LOCAL   time_t ed_ttl_now(int64_t epoch, uint32_t exp);
ED_LOCAL     bool ed_expired_at(int64_t epoch, uint32_t exp, time_t t);
ED_LOCAL     bool ed_expired_now(int64_t epoch, uint32_t exp);


/* File Module */
ED_LOCAL      int ed_mkfile(int fd, off_t size);


static inline uint32_t __attribute__((unused))
ed_fetch32(const void *p)
{
	uint32_t val;
	memcpy(&val, p, sizeof(val));
	return val;
}

static inline uint64_t __attribute__((unused))
ed_fetch64(const void *p)
{
	uint64_t val;
	memcpy(&val, p, sizeof(val));
	return val;
}

#endif

