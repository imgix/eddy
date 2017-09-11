#include "eddy-private.h"

typedef union {
	uint64_t value;
	struct {
		uint16_t fault;
		uint16_t flags;
		uint32_t count;
	};
} EdFaultValue;

_Static_assert(sizeof(EdFaultValue) == sizeof(uint64_t),
		"EdFaultValue size invalid");

static EdFaultValue ed_fault = { .fault = ED_FAULT_NONE, .count = 0 };

static const char *
fault_name(EdFault f)
{
	const char *name = "UNKNOWN";
	switch (f) {
#define XX(f) case ED_FAULT_##f: name = #f; break;
	ED_FAULT_MAP(XX)
#undef XX
	}
	return name;
}

void
ed_fault__enable(EdFault f, uint32_t count, uint16_t flags, const char *file, int line)
{
	EdFaultValue next = { .fault = (uint16_t)f, .flags = flags, .count = count }, old;
	do {
		old = ed_fault;
	} while(!__sync_bool_compare_and_swap(&ed_fault.value, old.value, next.value));
	if (!(flags & ED_FAULT_NOPRINT)) {
		fprintf(stderr, "*** %s fault enabled (%s:%d)\n", fault_name(f), file, line);
	}
}

void
ed_fault__trigger(EdFault f, const char *file, int line)
{
	EdFaultValue next, old;
	do {
		old = ed_fault;
		if (old.fault != f || old.count == 0) { return; }
		next = old;
		next.count--;
	} while(!__sync_bool_compare_and_swap(&ed_fault.value, old.value, next.value));

	if (next.count > 0) { return; }
	if (!(next.flags & ED_FAULT_NOPRINT)) {
		fprintf(stderr, "*** %s fault triggered (%s:%d)\n", fault_name(f), file, line);
		fflush(stderr);
	}
	abort();
}

