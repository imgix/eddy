#include "eddy-private.h"

typedef union {
	uint64_t value;
	struct {
		uint32_t fault;
		uint32_t count;
	};
} EdFaultValue;

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
ed_fault__enable(EdFault f, uint32_t count, const char *file, int line)
{
	EdFaultValue next = { .fault = f, .count = count }, old;
	do {
		old = ed_fault;
	} while(!__sync_bool_compare_and_swap(&ed_fault.value, old.value, next.value));
	fprintf(stderr, "*** %s fault enabled (%s:%d)\n", fault_name(f), file, line);
}

void
ed_fault__trigger(EdFault f, const char *file, int line)
{
	EdFaultValue next, old;
	do {
		old = ed_fault;
		if (old.fault != f || old.count == 0) { return; }
		next = (EdFaultValue){ .fault = f, .count = old.count - 1 };
	} while(!__sync_bool_compare_and_swap(&ed_fault.value, old.value, next.value));

	if (next.count > 0) { return; }
	
	fprintf(stderr, "*** %s fault triggered (%s:%d)\n", fault_name(f), file, line);
	fflush(stderr);
	abort();
}

