##############
# RocksDB Build Configuration
##############
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
    ROCKSDB_PREFIX := $(shell brew --prefix rocksdb 2>/dev/null || echo /usr/local)
    SNAPPY_PREFIX := $(shell brew --prefix snappy 2>/dev/null || echo /usr/local)
    LZ4_PREFIX := $(shell brew --prefix lz4 2>/dev/null || echo /usr/local)
    ZSTD_PREFIX := $(shell brew --prefix zstd 2>/dev/null || echo /usr/local)

    ROCKSDB_CGO_CFLAGS := -I$(ROCKSDB_PREFIX)/include
    ROCKSDB_CGO_LDFLAGS := -L$(ROCKSDB_PREFIX)/lib -L$(SNAPPY_PREFIX)/lib -L$(LZ4_PREFIX)/lib -L$(ZSTD_PREFIX)/lib \
                          -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd \
                          -Wl,-rpath,$(ROCKSDB_PREFIX)/lib
    ROCKSDB_LIB_PATH := DYLD_LIBRARY_PATH=$(ROCKSDB_PREFIX)/lib
endif

ifeq ($(UNAME_S),Linux)
    ROCKSDB_PREFIX := $(or $(ROCKSDB_HOME),/usr/local)

    ROCKSDB_CGO_CFLAGS := -I$(ROCKSDB_PREFIX)/include
    ROCKSDB_CGO_LDFLAGS := -L$(ROCKSDB_PREFIX)/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd
    ROCKSDB_LIB_PATH := LD_LIBRARY_PATH=$(ROCKSDB_PREFIX)/lib:$(LD_LIBRARY_PATH)
endif

# Common CGO environment for all build/test targets
CGO_ENV := CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS) $(ROCKSDB_CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(ROCKSDB_CGO_LDFLAGS)"

##############
# Targets
##############

.PHONY: all build clean check-rocksdb-env

all: build

build: build-backfill

# Verify RocksDB dependencies are installed
check-rocksdb-env:
	@echo "RocksDB Environment Check ($(UNAME_S)):"
	@echo "  ROCKSDB_PREFIX: $(ROCKSDB_PREFIX)"
	@echo "  CGO_CFLAGS: $(ROCKSDB_CGO_CFLAGS)"
	@echo "  CGO_LDFLAGS: $(ROCKSDB_CGO_LDFLAGS)"
ifeq ($(UNAME_S),Darwin)
	@brew list rocksdb >/dev/null 2>&1 && echo "  rocksdb installed" || echo "  rocksdb NOT installed (brew install rocksdb)"
	@brew list snappy >/dev/null 2>&1 && echo "  snappy installed" || echo "  snappy NOT installed"
	@brew list lz4 >/dev/null 2>&1 && echo "  lz4 installed" || echo "  lz4 NOT installed"
	@brew list zstd >/dev/null 2>&1 && echo "  zstd installed" || echo "  zstd NOT installed"
endif
ifeq ($(UNAME_S),Linux)
	@test -f $(ROCKSDB_PREFIX)/lib/librocksdb.so && echo "  librocksdb.so found" || echo "  librocksdb.so NOT found"
	@test -f $(ROCKSDB_PREFIX)/include/rocksdb/db.h && echo "  rocksdb headers found" || echo "  rocksdb headers NOT found"
endif

# Build backfill binary
build-backfill:
	@echo "Building backfill..."
	@mkdir -p bin
	$(CGO_ENV) go build -o bin/backfill ./backfill/cmd
	@echo "Built bin/backfill"

# Remove all built binaries
clean:
	@echo "Removing bin/..."
	@rm -rf bin
	@echo "Clean complete"
