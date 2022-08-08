cmake_minimum_required(VERSION 3.14)
project(ph_test)
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

set(BUILD_TARGET "Test" CACHE STRING "Build test")

if (${BUILD_TARGET} STREQUAL "Viper")
	message(STATUS "Viper")
	add_executable(test test/test.cc)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBUILD_Viper")
	target_include_directories(test PUBLIC viper/)
#	target_link_libraries(ph_test PUBLIC viper/concurrentqueue)
	target_include_directories(test PUBLIC viper/concurrentqueue-src)
	target_compile_options(test PUBLIC -march=native -mclwb)
	target_compile_options(test PUBLIC -O3 -Ofast -mtune=native)
	target_link_libraries(test PUBLIC -lpmem)
endif()

if (${BUILD_TARGET} STREQUAL "PH")
	message(STATUS "PH")
#	add_executable(test test/test.cc ph/global.cpp ph/data.cpp ph/hash.cpp ph/query.cpp ph/thread.cpp ph/cceh.cpp)
	aux_source_directory(ph/ PH_FILES)
	add_executable(test test/test.cc ${PH_FILES})
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBUILD_PH")
	target_include_directories(test PUBLIC ph/)
	target_compile_options(test PUBLIC -march=native)
	target_compile_options(test PUBLIC -O3 -Ofast -mtune=native)
#	target_compile_options(test PUBLIC -g)
	target_link_libraries(test PUBLIC -lpmem)
endif()

if (${BUILD_TARGET} STREQUAL "PMEM-ROCKSDB")
	message(STATUS "PMEM_ROCKSDB")
	add_executable(test test/test.cc)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBUILD_PMEMROCKSDB -DON_DCPMM")
	target_include_directories(test PUBLIC pmem-rocksdb/)
	target_compile_options(test PUBLIC -march=native)
	target_compile_options(test PUBLIC -O3 -Ofast -mtune=native)
	target_link_libraries(test PUBLIC -lrocksdb -lpmem -lpmemobj -ldl -lsnappy -lz -lzstd -lbz2 -llz4)
endif()

if (${BUILD_TARGET} STREQUAL "PACMAN")
	message(STATUS "PACMAN")

#pacman flatstore-h

	set(USE_NUMA_NODE 0 CACHE STRING "NUMANUMA")
	option(IDX_PERSISTENT "IDX_PERSISTENT" OFF)
	option(LOG_PERSISTENT "LOG_PERSISTENT" ON)
	set(INDEX_TYPE 1 CACHE STRING "INDEX TYPE")
	option(LOG_BATCHING "LOG_BATCHING" ON)
	option(REDUCE_PM_ACCESS ON)
	set(HOT_COLD_SEPARATE ON)
	set(BATCH_COMPACTION ON)
	set(GC_SHORTCUT OFF)

	configure_file(
			"pacman/include/config.h.in"
			"${PROJECT_SOURCE_DIR}/pacman/config.h"
		      )

	aux_source_directory(pacman/db DB_FILES)
	aux_source_directory(pacman/util UTIL_FILES)

	add_executable(test test/test.cc ${DB_FILES} ${UTIL_FILES})
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBUILD_PACMAN")
	target_include_directories(test PUBLIC pacman)
	target_include_directories(test PUBLIC pacman/include)
	target_compile_options(test PUBLIC -march=native)
	target_compile_options(test PUBLIC -O3 -Ofast -mtune=native)

	include_directories(
			"pacman/include"
			"pacman"
			)
	add_library(cceh pacman/db/index/CCEH/CCEH_MSB.cpp)
	target_link_libraries(test cceh -lpmem -lpmemobj)

endif()
