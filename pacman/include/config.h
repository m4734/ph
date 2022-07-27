/* config.h is generated from config.h.in by CMake */

#pragma once

#include <cstddef>

static constexpr char PMEM_DIR[] = "/mnt/pmem0/";
static constexpr size_t IDX_POOL_SIZE = 40ul << 30;

static constexpr int USE_NUMA_NODE = 0;
// CPU cores information
static constexpr int CORES_PER_SOCKET = 24;//18;
static constexpr int NUM_SOCKETS = 2//4;
static constexpr int NUMA_SPAN = CORES_PER_SOCKET * NUM_SOCKETS;
static constexpr int CPU_BIND_BEGIN_CORE = USE_NUMA_NODE * CORES_PER_SOCKET;

#define CACHE_LINE_SIZE 64

#define IDX_PERSISTENT
#define LOG_PERSISTENT
/* #undef USE_PMDK */

#define INDEX_TYPE 1

#ifdef LOG_PERSISTENT
#define LOG_BATCHING  // simulate FlatStore's batching
static constexpr size_t LOG_BATCHING_SIZE = 512;
#endif

/* #undef GC_SHORTCUT */
/* #undef BATCH_COMPACTION */
/* #undef REDUCE_PM_ACCESS */
/* #undef HOT_COLD_SEPARATE */

#ifdef GC_SHORTCUT
#define PREFETCH_ENTRY
#endif

#ifdef BATCH_COMPACTION
#define BATCH_FLUSH_INDEX_ENTRY
#endif

#if defined(REDUCE_PM_ACCESS) || !defined(LOG_PERSISTENT)
#define WRITE_TOMBSTONE
#endif
