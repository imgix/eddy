all: bin lib

# Optionally include custom build settings
-include Build.mk

VMAJ:= 0
VMIN:= 2

# Select default options.
LIBNAME?= eddy
BINNAME?= ed-
BUILD?= release
PREFIX?= /usr/local
BUILD_MIME?= yes
PAGESIZE?=$(shell getconf PAGESIZE)
ifneq ($(BUILD),debug)
  OPT?= 3
  LTO?= yes
else
  BUILD_DEV?= yes
  LTO?= no
endif
ifdef OPT
  CFLAGS?= -O$(OPT) -DNDEBUG
  DEBUG?= no
  STRIP:= strip
else
  CFLAGS?= -Wall -Wextra -pedantic -Werror
  DEBUG?= yes
  STRIP:= @:
endif
ifndef $(GDB)
  GDB:=$(shell which lldb)
  ifeq ($(GDB),)
    GDB:=$(shell which gdb)
  endif
endif

# Append required flags.
ifeq ($(DEBUG),yes)
  CFLAGS+= -g
endif
CFLAGS+= -std=c11 -Ilib -fvisibility=hidden -pthread -D_GNU_SOURCE -D_BSD_SOURCE -DPAGESIZE=$(PAGESIZE)
ifeq ($(LTO),yes)
  LDFLAGS+=-flto
endif

# Select source files.
LIBSRC:= lib/cache.c lib/error.c lib/rnd.c lib/hash.c lib/pg.c lib/btree.c lib/index.c lib/mkfile.c lib/time.c
BINSRC:= bin/ed-new.c bin/ed-stat.c bin/ed-get.c bin/ed-set.c
ifeq ($(BUILD_MIME),yes)
  LIBSRC+= lib/mime.c
  BINSRC+= bin/ed-mime.c
endif
ifeq ($(BUILD_DEV),yes)
  BINSRC+= bin/ed-alloc.c bin/ed-exec.c
endif
TESTSRC:= $(wildcard test/*.c)

# Define build-specific directories.
TMP:= build/$(BUILD)/tmp
LIB:= build/$(BUILD)/lib
BIN:= build/$(BUILD)/bin
TEST:= build/$(BUILD)/test

# Build object file lists. When LTO is enabled, keep the OBJA list referencing
# non-lto object files. For amalgamated builds, there is a single object file.
ifeq ($(LTO),yes)
  OBJ:= $(LIBSRC:lib/%.c=$(TMP)/%-lto.o)
  OBJA:= $(TMP)/$(LIBNAME).o
  OBJBIN:= $(OBJ) $(TMP)/util-lto.o
else
  ifeq ($(LTO),amalg)
    OBJ:= $(TMP)/$(LIBNAME).o
  else
    OBJ:= $(LIBSRC:lib/%.c=$(TMP)/%.o)
  endif
  OBJA:= $(OBJ)
  OBJBIN:= $(OBJ) $(TMP)/util.o
endif

# Setup library names and additional flags.
A:= lib$(LIBNAME).a
ifeq ($(shell uname -s),Darwin)
  SO:=lib$(LIBNAME).dylib
  SOMAJ:=lib$(LIBNAME).$(VMAJ).dylib
  SOMIN:=lib$(LIBNAME).$(VMAJ).$(VMIN).dylib
  SOFLAGS:= -dynamiclib -install_name $(PREFIX)/lib/$(SOMIN)
else
  SO:=lib$(LIBNAME).so
  SOMAJ:=lib$(LIBNAME).so.$(VMAJ)
  SOMIN:=lib$(LIBNAME).so.$(VMAJ).$(VMIN)
  SOFLAGS:= -shared -Wl,-soname,$(SOMIN),-rpath,$(PREFIX)/lib/$(SOMIN)
endif

INSTALL:= \
	$(BINSRC:bin/ed-%.c=$(DESTDIR)$(PREFIX)/bin/$(BINNAME)%) \
	$(DESTDIR)$(PREFIX)/lib/$(A) \
	$(DESTDIR)$(PREFIX)/lib/$(SO) \
	$(DESTDIR)$(PREFIX)/lib/$(SOMAJ) \
	$(DESTDIR)$(PREFIX)/lib/$(SOMIN) \
	$(DESTDIR)$(PREFIX)/include/eddy.h

# Build only the executable tools.
bin: $(BINSRC:bin/ed-%.c=$(BIN)/$(BINNAME)%)

# Build the static and shared libraries.
lib: static dynamic

# Build only the static library.
static: $(LIB)/$(A)

# Build the shared library and version symlinks.
dynamic: $(LIB)/$(SOMIN) $(LIB)/$(SOMAJ) $(LIB)/$(SO)

# Build and run tests.
test: $(TESTSRC:test/%.c=$(TEST)/%)
	@for f in $^; do ./$$f; done

# Build and run a single test.
test-%: $(TEST)/%
	@./$<

# Build and run a single test.
debug-%: $(TEST)/%
	MU_NOFORK=1 $(GDB) ./$<

# Copy files into destination
install: $(INSTALL)

# Remove files from destination
uninstall:
	rm -f $(INSTALL)

# Remove all build files.
clean:
	rm -rf build

# Generate and strip statically linked executable.
$(BIN)/$(BINNAME)%: bin/ed-%.c $(OBJBIN) | $(BIN)
	$(CC) $< $(OBJBIN) $(CFLAGS) $(LDFLAGS) -MMD -MF $(TMP)/ed-$*.d -o $@
	$(STRIP) $@

# Generate static library from non-lto object(s).
$(LIB)/$(A): $(OBJA) | $(LIB)
	$(AR) rcus $@ $^

# Generate shared library.
$(LIB)/$(SOMIN): $(OBJ) | $(LIB)
	$(CC) $(LDFLAGS) $(SOFLAGS) $^ -o $@

# Generate statically linked test executable.
$(TEST)/%: test/%.c $(OBJ) | $(TEST)
	$(CC) $< $(OBJ) $(CFLAGS) $(LDFLAGS) -MMD -MF $(TMP)/test-$*.d -o $@

# Create versioned symlink for shared library.
$(LIB)/$(SOMAJ) $(LIB)/$(SO): $(LIB)/$(SOMIN)
	cd $(LIB) && ln -s $(SOMIN) $(notdir $@)

# Build source files in lib without lto.
$(TMP)/%.o: lib/%.c Makefile | $(TMP)
	$(CC) $(CFLAGS) -fPIC -MMD -c -o $@ $<

# Build intermediate source files without lto.
$(TMP)/%.o: $(TMP)/%.c Makefile | $(TMP)
	$(CC) $(CFLAGS) -fPIC -MMD -c -o $@ $<

# Build source files in lib with lto.
$(TMP)/%-lto.o: lib/%.c Makefile | $(TMP)
	$(CC) $(CFLAGS) -flto -fPIC -MMD -c -o $@ $<

# Build intermediate source files with lto.
$(TMP)/%-lto.o: $(TMP)/%.c Makefile | $(TMP)
	$(CC) $(CFLAGS) -flto -fPIC -MMD -c -o $@ $<

# Produce amalgamated intermediate source file.
$(TMP)/$(LIBNAME).c: $(LIBSRC) | $(TMP)
	@date +"/*  $(LIBNAME): %Y-%m-%d %T %Z */" > $@
	@for f in $^; do cat $$f >> $@; done

# Create build directories.
$(TMP) $(LIB) $(BIN) $(TEST) $(DESTDIR)$(PREFIX)/bin $(DESTDIR)$(PREFIX)/lib $(DESTDIR)$(PREFIX)/include:
	mkdir -p $@

# Install executable tools.
$(DESTDIR)$(PREFIX)/bin/%: $(BIN)/% | $(DESTDIR)$(PREFIX)/bin
	cp $< $@

# Install versioned symlink for shared library.
$(DESTDIR)$(PREFIX)/lib/$(SOMAJ) $(DESTDIR)$(PREFIX)/lib/$(SO): $(DESTDIR)$(PREFIX)/lib/$(SOMIN)
	cd $(DESTDIR)$(PREFIX)/lib && ln -s $(SOMIN) $(notdir $@)

# Install libraries.
$(DESTDIR)$(PREFIX)/lib/%: $(LIB)/% | $(DESTDIR)$(PREFIX)/lib
	cp $< $@

# Install headers.
$(DESTDIR)$(PREFIX)/include/%.h: lib/%.h | $(DESTDIR)$(PREFIX)/include
	cp $< $@

.PHONY: all bin lib static dynamic test install uninstall clean

-include $(OBJ:%.o=%.d) $(BINSRC:bin/%.c=$(TMP)/%.d) $(TESTSRC:test/%.c=$(TMP)/test-%.d)
