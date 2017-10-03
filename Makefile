all: bin lib

# Optionally include custom build settings
-include Build.mk

VMAJ:= 0
VMIN:= 2
VBLD:= $(shell git rev-parse HEAD)

# Select default options.
LIBNAME?= eddy
BINNAME?= eddy
BUILD?= release
PREFIX?= /usr/local
UNAME?=$(shell uname -s)
BUILD_MIME?= yes
BUILD_DUMP?= yes
PAGESIZE?=$(shell getconf PAGESIZE)
ifeq ($(BUILD),release)
  OPT?= 3
  LTO?= yes
else
  DEBUG_MMAP?= yes
  DEBUG_FAULT?= yes
  LTO?= no
  SANITIZE?= address
endif
ifdef OPT
  CFLAGS?= -O$(OPT) -DNDEBUG
  DEBUG?= no
  STRIP:= strip
else
  CFLAGS?= -Wall -Wextra -pedantic -Werror
  DEBUG?= yes
  STRIP:= :
endif
ifndef $(GDB)
  GDB:=$(shell which lldb)
  ifeq ($(GDB),)
    GDB:=$(shell which gdb)
  endif
endif

# Select source files.
LIBSRC:= lib/cache.c \
	lib/error.c \
	lib/rnd.c \
	lib/hash.c \
	lib/lck.c \
	lib/pg.c \
	lib/txn.c \
	lib/bpt.c \
	lib/idx.c \
	lib/stat.c \
	lib/mkfile.c \
	lib/time.c
BINSRC:= bin/eddy.c
ifeq ($(BUILD_MIME),yes)
  LIBSRC+= lib/mime.c
  CFLAGS+= -DED_MIME=1
  ifeq ($(BUILD_MIMEDB),yes)
    LIBSRC+= lib/mimedb.c
    CFLAGS+= -DED_MIMEDB=1
  endif
endif
ifeq ($(DEBUG_MMAP),yes)
  LIBSRC+= lib/pgtrack.cc lib/backtrace.cc
  CFLAGS+= -DED_MMAP_DEBUG=1 -DED_BACKTRACE=1
  ifneq ($(UNAME),Darwin)
    LDFLAGS+= -lexecinfo -ldl
  endif
endif
ifeq ($(DEBUG_FAULT),yes)
  LIBSRC+= lib/fault.c
  CFLAGS+= -DED_FAULT=1
endif
ifeq ($(BUILD_DUMP),yes)
  CFLAGS+= -DED_DUMP=1
endif
TESTSRC:= $(wildcard test/test-*.c)

# Select compiler and linker.
ifeq ($(findstring .cc,$(suffix $(LIBSRC))),.cc)
  LD:=$(CXX)
else
  LD:=$(CC)
endif
CC:= $(CC) -std=gnu11
CXX:= $(CXX) -std=gnu++11

# Append required flags.
ifeq ($(DEBUG),yes)
  CFLAGS+= -g -DED_DEBUG=1
  LDFLAGS+= -g
endif
CFLAGS+= -Ilib -march=native -fvisibility=hidden -pthread -D_GNU_SOURCE -D_BSD_SOURCE -DPAGESIZE=$(PAGESIZE)
ifeq ($(LTO),yes)
  LDFLAGS+= -flto
endif

ifneq ($(SANITIZE),)
  CFLAGS+= -fsanitize=$(SANITIZE)
  LDFLAGS+= -fsanitize=$(SANITIZE)
endif

# Append version information.
CFLAGS += -DVERSION_MAJOR=$(VMAJ) -DVERSION_MINOR=$(VMIN) -DVERSION_BUILD=$(VBLD) -DBUILD=$(BUILD)

# Define build-specific directories.
TMP:= build/$(BUILD)/tmp
LIB:= build/$(BUILD)/lib
BIN:= build/$(BUILD)/bin
TEST:= build/$(BUILD)/test

# Build object file lists. When LTO is enabled, keep the OBJA list referencing
# non-lto object files. For amalgamated builds, there is a single object file.
ifeq ($(LTO),yes)
  OBJEXT:=lto.o
  OBJ:= $(LIBSRC:lib/%=$(TMP)/%.$(OBJEXT))
  OBJA:= $(TMP)/$(LIBNAME)-amalg.c.o
else
  OBJEXT:=o
  ifeq ($(LTO),amalg)
    OBJ:= $(TMP)/$(LIBNAME)-amalg.c.$(OBJEXT)
  else
    OBJ:= $(LIBSRC:lib/%=$(TMP)/%.$(OBJEXT))
  endif
  OBJA:= $(OBJ)
endif

# Setup library names and additional flags.
A:= lib$(LIBNAME).a
ifeq ($(UNAME),Darwin)
  SOEXT:=dylib
  SOMAJ:=lib$(LIBNAME).$(VMAJ).$(SOEXT)
  SOMIN:=lib$(LIBNAME).$(VMAJ).$(VMIN).$(SOEXT)
  SOFLAGS:= -dynamiclib -install_name $(PREFIX)/lib/$(SOMIN)
else
  SOEXT:=so
  SOMAJ:=lib$(LIBNAME).$(SOEXT).$(VMAJ)
  SOMIN:=lib$(LIBNAME).$(SOEXT).$(VMAJ).$(VMIN)
  SOFLAGS:= -shared -Wl,-soname,$(SOMIN),-rpath,$(PREFIX)/lib/$(SOMIN)
endif
SO:=lib$(LIBNAME).$(SOEXT)

PRODUCTS:= \
	$(DESTDIR)$(PREFIX)/bin/$(BINNAME) \
	$(DESTDIR)$(PREFIX)/include/eddy.h \
	$(DESTDIR)$(PREFIX)/lib/$(A) \
	$(DESTDIR)$(PREFIX)/lib/$(SOMIN) \
	$(DESTDIR)$(PREFIX)/lib/$(SOMAJ) \
	$(DESTDIR)$(PREFIX)/lib/$(SO)

ifndef VERBOSE
  COMPILE_PREFIX = @echo "$(word 1,$(1))\t$(2)" && 
  LINK_PREFIX    = @echo "bin\t$(2)" && 
  STATIC_PREFIX  = @echo "a\t$(2)" && 
  DYNAMIC_PREFIX = @echo "$(SOEXT)\t$(2)" && 
  SYMLINK_PREFIX = @echo "ln\t$(2) -> $(1)" && 
  INSTALL_PREFIX = @echo "install\t$(2)" && 
endif

ifeq ($(UNAME),Darwin)
  OPEN:= @open
else
  OPEN:= @echo open 
endif



# Build only the executable tools.
bin: $(BIN)/$(BINNAME)

# Build the static and shared libraries.
lib: static dynamic

# Build only the static library.
static: $(LIB)/$(A)

# Build the shared library and version symlinks.
dynamic: $(LIB)/$(SOMIN) $(LIB)/$(SOMAJ) $(LIB)/$(SO)

analyze:
	@which scan-build >/dev/null || (echo 'scan-build required: pip install typing scan-build' && exit 1)
	rm -rf build/analyze
	scan-build -o build/analyze/tmp $(MAKE) BUILD=analyze
	$(OPEN) build/analyze/tmp/scan-build*/index.html

# Build and run tests.
test: $(TESTSRC:test/test-%.c=test-%)

# Build and run a single test.
test-%: $(TEST)/test-% | ./test/tmp
	@./$<

./test/tmp:
	./test/setup.sh $@

# Build and run a single test in a debugger.
debug-%: $(TEST)/test-%
	MU_NOFORK=1 $(GDB) ./$<

# Copy files into destination
install: $(PRODUCTS)

# Remove files from destination
uninstall:
	@rm -f $(PRODUCTS)
	@for f in $(PRODUCTS); do echo "remove\t$$f"; done

# Remove all build files.
clean:
	rm -rf build



# Executable linking template.
ifeq ($(DEBUG)-$(UNAME),yes-Darwin)
  LINK= $(LINK_PREFIX)$(LD) $(LDFLAGS) $(1) -o $(2) && dsymutil $(2)
else
  LINK= $(LINK_PREFIX)$(LD) $(LDFLAGS) $(1) -o $(2)
endif

# Generate and strip statically linked executable.
$(BIN)/$(BINNAME): $(TMP)/eddy.c.$(OBJEXT) $(OBJ) | $(BIN)
	$(call LINK,$^,$@)
	@$(STRIP) $@

# Generate statically linked test executable.
$(TEST)/test-%: $(TMP)/test-%.c.$(OBJEXT) $(OBJ) | $(TEST)
	$(call LINK,$^,$@)



# Static library archiving template.
STATIC= $(STATIC_PREFIX)$(AR) rcus $(2) $(1)

# Generate static library from non-lto object(s).
$(LIB)/$(A): $(OBJA) | $(LIB)
	$(call STATIC,$^,$@)



# Shared library linking template.
DYNAMIC= $(DYNAMIC_PREFIX)$(LD) $(LDFLAGS) $(SOFLAGS) $(1) -o $(2)

# Generate shared library.
$(LIB)/$(SOMIN): $(OBJ) | $(LIB)
	$(call DYNAMIC,$^,$@)



# Compiler template.
COMPILE= $(COMPILE_PREFIX)$(1) $(CFLAGS) -fPIC -MMD -c \
	$(2) -o $(3) $(subst .lto.o,-flto,$(findstring .lto.o,$(3)))

# Build C source files in lib.
$(TMP)/%.c.$(OBJEXT): lib/%.c | $(TMP)
	$(call COMPILE,$(CC),$<,$@)

# Build C++ source files in lib.
$(TMP)/%.cc.$(OBJEXT): lib/%.cc | $(TMP)
	$(call COMPILE,$(CXX),$<,$@)

# Build C source files in bin.
$(TMP)/%.c.$(OBJEXT): bin/%.c | $(TMP)
	$(call COMPILE,$(CC),$<,$@)

# Build C source files in test.
$(TMP)/test-%.c.$(OBJEXT): test/test-%.c | $(TMP)
	$(call COMPILE,$(CC),$<,$@)

# Build intermediate C source files.
$(TMP)/%.c.o: $(TMP)/%.c | $(TMP)
	$(call COMPILE,$(CC),$<,$@)

# Produce amalgamated intermediate source file.
$(TMP)/$(LIBNAME)-amalg.c: $(LIBSRC) | $(TMP)
	@echo "amalg\t$@"
	@date +"/*  $(LIBNAME): %Y-%m-%d %T %Z */" > $@
	@for f in $^; do cat $$f >> $@; done



# Create build directories.
$(TMP) $(LIB) $(BIN) $(TEST) $(DESTDIR)$(PREFIX)/bin $(DESTDIR)$(PREFIX)/lib $(DESTDIR)$(PREFIX)/include:
	@mkdir -p $@



# Symlink template.
SYMLINK= $(SYMLINK_PREFIX)cd $(dir $(2)) && ln -s $(1) $(notdir $(2))

# Create versioned symlink for shared library.
$(LIB)/$(SOMAJ) $(LIB)/$(SO):
	$(call SYMLINK,$(SOMIN),$@)

# Install versioned symlink for shared library.
$(DESTDIR)$(PREFIX)/lib/$(SOMAJ) $(DESTDIR)$(PREFIX)/lib/$(SO):
	$(call SYMLINK,$(SOMIN),$@)



# Install template.
INSTALL= $(INSTALL_PREFIX)cp $(1) $(2)

# Install executable tools.
$(DESTDIR)$(PREFIX)/bin/%: $(BIN)/% | $(DESTDIR)$(PREFIX)/bin
	$(call INSTALL,$<,$@)

# Install libraries.
$(DESTDIR)$(PREFIX)/lib/%: $(LIB)/% | $(DESTDIR)$(PREFIX)/lib
	$(call INSTALL,$<,$@)

# Install headers.
$(DESTDIR)$(PREFIX)/include/%.h: lib/%.h | $(DESTDIR)$(PREFIX)/include
	$(call INSTALL,$<,$@)



.PHONY: all bin lib static dynamic test install uninstall clean
.SECONDARY:

-include $(OBJ:%.o=%.d) $(BINSRC:bin/%=$(TMP)/%.d) $(TESTSRC:test/%.c=$(TMP)/%.c.d)

$(TMP)/mimedb.c.$(OBJEXT): lib/mimedb.c test/mime.cache
