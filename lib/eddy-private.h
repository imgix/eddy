#ifndef INCLUDED_EDDY_PRIVATE_H
#define INCLUDED_EDDY_PRIVATE_H

#include "eddy.h"

#include <stdatomic.h>
#include <time.h>
#include <math.h>
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

#define ED_ALIGN(n) ((((n) + (ED_MAX_ALIGN-1)) / ED_MAX_ALIGN) * ED_MAX_ALIGN)

#define ed_len(arr) (sizeof(arr) / sizeof((arr)[0]))

#define ED_INLINE static inline __attribute__((always_inline))

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

#define ed_verbose(f, ...) do { \
	if ((f) & ED_FVERBOSE) { fprintf(stderr, __VA_ARGS__); fflush(stderr); } \
} while (0)

#define ED_PGINDEX     UINT32_C(0x998ddcb0)
#define ED_PGFREE_HEAD UINT32_C(0xc3e873b2)
#define ED_PGFREE_CHLD UINT32_C(0xea104f71)
#define ED_PGBRANCH    UINT32_C(0x2c17687a)
#define ED_PGLEAF      UINT32_C(0x2dd39a85)
#define ED_PGOVERFLOW  UINT32_C(0x09c2fd2f)

#define ED_NODE_PAGE_COUNT ((PAGESIZE - sizeof(EdBTree)) / sizeof(EdNodePage))
#define ED_NODE_KEY_COUNT ((PAGESIZE - sizeof(EdBTree)) / sizeof(EdNodeKey))

#define ED_PAGE_NONE UINT32_MAX
#define ED_BLK_NONE UINT64_MAX
#define ED_TIME_DELETE 0
#define ED_TIME_INF UINT32_MAX

#define ED_IS_FILE(mode) (S_ISREG(mode))
#define ED_IS_DEVICE(mode) (S_ISCHR(mode) || S_ISBLK(mode))
#define ED_IS_MODE(mode) (ED_IS_FILE(mode) || ED_IS_DEVICE(mode))

typedef uint32_t EdPgno;
typedef uint64_t EdBlkno;
typedef uint64_t EdHash;

typedef struct EdPg EdPg;
typedef struct EdPgFree EdPgFree;
typedef struct EdPgTail EdPgTail;
typedef struct EdPgAlloc EdPgAlloc;
typedef struct EdPgAllocHdr EdPgAllocHdr;
typedef struct EdPgMap EdPgMap;
typedef struct EdPgNode EdPgNode;

typedef struct EdBTree EdBTree;
typedef enum EdBTreeApply {
	ED_BT_NONE,
	ED_BT_INSERT,
	ED_BT_REPLACE,
	ED_BT_DELETE
} EdBTreeApply;

typedef struct EdTx EdTx;
typedef struct EdTxType EdTxType;
typedef struct EdTxSearch EdTxSearch;

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

struct EdPgAlloc {
	EdPgAllocHdr *hdr;
	void *pg;
	EdPgFree *free;
	uint64_t flags;
	int fd;
	uint8_t dirty, free_dirty;
	bool from_new;
};

struct EdIndex {
	EdPgAlloc alloc;
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

struct EdTx {
	EdPgAlloc *alloc;     // page allocator
	EdPg **pg;            // array to hold allocated pages
	unsigned npg;         // number of pages allocated
	unsigned npgused;     // number of pages used

	struct EdPgNode {
		union {
			EdPg *page;       // mapped page
			EdBTree *tree;    // mapped page as a tree
		};
		EdPgNode *parent;     // parent node
		uint16_t pindex;      // index of page in the parent
		uint8_t dirty;        // dirty state of the page
	} *nodes;             // array of node wrapped pages
	unsigned nnodes;      // length of node array
	unsigned nnodesused;  // number of nodes used

	int state;

	unsigned ndb;             // number of search objects
	struct EdTxSearch {
		EdPgNode *head;       // first node searched
		EdPgNode *tail;       // current tail node
		EdPgno *root;         // pointer to page number of root node
		uint64_t key;         // key searched for
		void *entry;          // pointer to the entry in the leaf
		size_t entry_size;    // size in bytes of the entry
		uint32_t entry_index; // index of the entry in the leaf
		int nsplits;          // number of nodes requiring splits for an insert
		int match;            // return code of the search
		int nmatches;         // number of matched keys so far
		void *scratch;        // new entry content
		EdBTreeApply apply;   // replace, insert, or delete entry
	} db[1];
};

struct EdTxType {
	EdPgno *no;
	size_t entry_size;
};

#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wpadded"

struct EdPg {
	EdPgno no;
	uint32_t type;
};

struct EdPgFree {
	EdPg base;
	EdPgno count;
#define ED_PGFREE_COUNT ((PAGESIZE - sizeof(EdPg) - sizeof(EdPgno)) / sizeof(EdPgno))
	EdPgno pages[ED_PGFREE_COUNT];
};

struct EdPgAllocHdr {
	uint16_t size_page;
	uint16_t size_block;
	EdPgno free_list;
	_Atomic struct EdPgTail {
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
	EdPgAllocHdr alloc;
	uint8_t size_align;
	uint8_t alloc_count;
	uint8_t _pad[2];
	EdPgno slab_page_count;
	uint64_t slab_ino;
	char slab_path[1024];
};

struct EdObjectHdr {
	uint64_t hash;
	uint32_t datalen;
	uint16_t keylen;
	uint16_t metalen;
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
ED_LOCAL      int ed_pgsync(void *p, EdPgno count, uint64_t flags, uint8_t lvl);
ED_LOCAL   void * ed_pgload(int fd, EdPg **pgp, EdPgno no);
ED_LOCAL     void ed_pgunload(EdPg **pgp);
ED_LOCAL     void ed_pgmark(EdPg *pg, EdPgno *no, uint8_t *dirty);
ED_LOCAL      int ed_pgalloc_new(EdPgAlloc *, const char *, size_t meta);
ED_LOCAL     void ed_pgalloc_init(EdPgAlloc *, EdPgAllocHdr *, int fd, uint64_t flags);
ED_LOCAL     void ed_pgalloc_close(EdPgAlloc *);
ED_LOCAL     void ed_pgalloc_sync(EdPgAlloc *);
ED_LOCAL   void * ed_pgalloc_meta(EdPgAlloc *alloc);
ED_LOCAL      int ed_pgalloc(EdPgAlloc *, EdPg **, EdPgno n, bool exclusive);
ED_LOCAL     void ed_pgfree(EdPgAlloc *, EdPg **, EdPgno n);
ED_LOCAL EdPgFree * ed_pgfree_list(EdPgAlloc *);
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


/* Transaction Module */
ED_LOCAL      int ed_txopen(EdTx **, EdPgAlloc *alloc, EdTxType *type, unsigned ntype);
ED_LOCAL     void ed_txclose(EdTx **, uint64_t flags);
ED_LOCAL      int ed_txcommit(EdTx *, bool exclusive);
ED_LOCAL      int ed_txmap(EdTx *, EdPgno, EdPgNode *par, uint16_t pidx, EdPgNode **out);
ED_LOCAL EdPgNode * ed_txalloc(EdTx *tx, EdPgNode *par, uint16_t pidx);
ED_LOCAL EdTxSearch * ed_txsearch(EdTx *tx, unsigned db, bool reset);


/* B+Tree Module */
typedef int (*EdBTreePrint)(const void *, char *buf, size_t len);
ED_LOCAL   size_t ed_btcapacity(size_t esize, size_t depth);
ED_LOCAL     void ed_btinit(EdBTree *bt);
ED_LOCAL      int ed_btfind(EdTx *tx, unsigned db, uint64_t key, void **ent);
ED_LOCAL      int ed_btnext(EdTx *tx, unsigned db, void **ent);
ED_LOCAL      int ed_btset(EdTx *tx, unsigned db, const void *ent, bool replace);
ED_LOCAL      int ed_btdel(EdTx *tx, unsigned db);
ED_LOCAL     void ed_btapply(EdTx *tx, unsigned db, const void *ent, EdBTreeApply);
ED_LOCAL     void ed_btprint(EdBTree *, int fd, size_t esize, FILE *, EdBTreePrint);
ED_LOCAL      int ed_btverify(EdBTree *, int fd, size_t esize, FILE *);


/* Index Module */
ED_LOCAL      int ed_index_open(EdIndex *, const EdConfig *cfg, int *slab_fd);
ED_LOCAL     void ed_index_close(EdIndex *);
ED_LOCAL      int ed_index_load_trees(EdIndex *);
ED_LOCAL      int ed_index_save_trees(EdIndex *);
ED_LOCAL      int ed_index_lock(EdIndex *, EdLock type, bool wait);
ED_LOCAL      int ed_index_stat(EdIndex *, FILE *, int flags);


/* Random Module */
ED_LOCAL      int ed_rnd_open(void);
ED_LOCAL  ssize_t ed_rnd_buf(int fd, void *buf, size_t len);
ED_LOCAL      int ed_rnd_u64(int fd, uint64_t *);


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

