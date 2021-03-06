#include <map>
#include <set>
#include <memory>

#undef ED_MMAP_DEBUG
#define ED_MMAP_DEBUG 1

#include "eddy-backtrace.h"

static void
PrintStack(EdBacktrace *bt)
{
	int idx = bt->Find("ed_pg_untrack");
	if (idx < 0) { idx = bt->Find("ed_pg_track"); }
	bt->Print(idx < 0 ? 0 : idx + 1, stderr);
}

struct EdPgState {
	EdPgno no;
	bool active;
	std::shared_ptr<EdBacktrace> stack;

	void Print() { PrintStack(stack.get()); }
};

typedef std::map<uintptr_t, EdPgState> EdPgtrack;

static pthread_rwlock_t track_lock = PTHREAD_RWLOCK_INITIALIZER;
static EdPgtrack *track = NULL;
static int track_errors = 0;
static int track_pid = 0;

void 
ed_pg_track(EdPgno no, uint8_t *pg, EdPgno count)
{
	if (pg == NULL) { return; }

	if (pthread_rwlock_wrlock(&track_lock) < 0) {
		fprintf(stderr, "*** failed to lock: %s\n", strerror(errno));
		abort();
	}

	try {
		if (track_pid != getpid()) {
			track = new EdPgtrack();
			track_pid = getpid();
		}

		auto stack = std::make_shared<EdBacktrace>();
		stack->Load();

		uintptr_t k = (uintptr_t)pg, ke = k+(count*PAGESIZE);
		auto start = track->lower_bound(k);
		auto end = track->upper_bound(ke-PAGESIZE);

		for (auto &it = start; it != end; ++it) {
			if (it->second.active) {
				fprintf(stderr, "*** page address mapped multiple times: 0x%012" PRIxPTR "/%u\n",
						k, it->second.no);
				fprintf(stderr, "*** allocation stack:\n");
				it->second.Print();
				fprintf(stderr, "*** current stack:\n");
				PrintStack(stack.get());
				fprintf(stderr, "\n");
				track_errors++;
			}
		}

		for (; k < ke; k += PAGESIZE, no++) {
			EdPgState state = { no, true, stack };
			auto result = track->emplace(k, state);
			if (!result.second) {
				result.first->second = state;
			}
		}
	}
	catch (...) {
	}

	if (pthread_rwlock_unlock(&track_lock) < 0) {
		fprintf(stderr, "*** failed to unlock: %s\n", strerror(errno));
		abort();
	}
}

void 
ed_pg_untrack(uint8_t *pg, EdPgno count)
{
	if (pg == NULL) {
		fprintf(stderr, "*** attempting to unmap NULL\n");
		EdBacktrace bt;
		if (bt.Load() > 0) {
			PrintStack(&bt);
			fprintf(stderr, "\n");
		}
		track_errors++;
		return;
	}

	if (pthread_rwlock_wrlock(&track_lock) < 0) {
		fprintf(stderr, "*** failed to lock: %s\n", strerror(errno));
		abort();
	}

	try {
		auto stack = std::make_shared<EdBacktrace>();
		uintptr_t k = (uintptr_t)pg, ke = k+(count*PAGESIZE);
		if (track == NULL) {
			fprintf(stderr, "*** uninitialized page address unmapped: 0x%012" PRIxPTR "/%u\n",
					k, *(EdPgno *)pg);
			PrintStack(stack.get());
			fprintf(stderr, "\n");
			track_errors++;
		}
		else {
			auto start = track->lower_bound(k);
			auto end = track->upper_bound(ke-PAGESIZE);
			if (start == track->end()) {
				fprintf(stderr, "*** page address unmapped not tracked: 0x%012" PRIxPTR "u\n", k);
				fprintf(stderr, "*** current stack:\n");
				PrintStack(stack.get());
				fprintf(stderr, "\n");
				track_errors++;
			}
			else {
				EdPgno no = start->second.no;
				std::set<uintptr_t> skip = {};
				for (auto &it = start; it != end; ++it) {
					if (!it->second.active) {
						fprintf(stderr, "*** page address unmapped multiple times: 0x%012" PRIxPTR "/%u\n",
								it->first, it->second.no);
						fprintf(stderr, "*** deallocation stack:\n");
						it->second.Print();
						fprintf(stderr, "*** current stack:\n");
						PrintStack(stack.get());
						fprintf(stderr, "\n");
						track_errors++;
						skip.emplace(it->first);
					}
				}

				for (; k < ke; k += PAGESIZE, no++) {
					if (skip.find(k) != skip.end()) { continue; }
					EdPgState state = { no, false, stack };
					auto result = track->emplace(k, state);
					if (!result.second) {
						result.first->second = state;
					}
				}
			}
		}
	}
	catch (...) {
	}

	if (pthread_rwlock_unlock(&track_lock) < 0) {
		fprintf(stderr, "*** failed to unlock: %s\n", strerror(errno));
		abort();
	}
}

int
ed_pg_check(void)
{
	if (pthread_rwlock_rdlock(&track_lock) < 0) {
		fprintf(stderr, "*** failed to lock: %s\n", strerror(errno));
		abort();
	}

	int rc = track_errors;
	if (track != NULL) {
		for (auto it = track->begin(); it != track->end(); it++) {
			if (it->second.active) {
				fprintf(stderr, "*** page address left mapped: 0x%012" PRIxPTR "/%u\n",
						it->first, it->second.no);
				fprintf(stderr, "*** allocation stack:\n");
				it->second.Print();
				fprintf(stderr, "\n");
				rc++;
			}
		}
	}

	if (pthread_rwlock_unlock(&track_lock) < 0) {
		fprintf(stderr, "*** failed to unlock: %s\n", strerror(errno));
		abort();
	}

	return rc;
}

