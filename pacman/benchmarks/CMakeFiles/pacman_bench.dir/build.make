# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/cgmin/20220321/pacman

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/cgmin/20220321/pacman

# Include any dependencies generated for this target.
include benchmarks/CMakeFiles/pacman_bench.dir/depend.make

# Include the progress variables for this target.
include benchmarks/CMakeFiles/pacman_bench.dir/progress.make

# Include the compile flags for this target's objects.
include benchmarks/CMakeFiles/pacman_bench.dir/flags.make

benchmarks/CMakeFiles/pacman_bench.dir/benchmark.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/benchmark.cpp.o: benchmarks/benchmark.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/benchmark.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/benchmark.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/benchmark.cpp

benchmarks/CMakeFiles/pacman_bench.dir/benchmark.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/benchmark.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/benchmark.cpp > CMakeFiles/pacman_bench.dir/benchmark.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/benchmark.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/benchmark.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/benchmark.cpp -o CMakeFiles/pacman_bench.dir/benchmark.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/histogram.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/histogram.cpp.o: benchmarks/histogram.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/histogram.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/histogram.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/histogram.cpp

benchmarks/CMakeFiles/pacman_bench.dir/histogram.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/histogram.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/histogram.cpp > CMakeFiles/pacman_bench.dir/histogram.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/histogram.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/histogram.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/histogram.cpp -o CMakeFiles/pacman_bench.dir/histogram.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/__/db/db.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/__/db/db.cpp.o: db/db.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/__/db/db.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/__/db/db.cpp.o -c /home/cgmin/20220321/pacman/db/db.cpp

benchmarks/CMakeFiles/pacman_bench.dir/__/db/db.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/__/db/db.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/db/db.cpp > CMakeFiles/pacman_bench.dir/__/db/db.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/__/db/db.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/__/db/db.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/db/db.cpp -o CMakeFiles/pacman_bench.dir/__/db/db.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.o: db/hotkeyset.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.o -c /home/cgmin/20220321/pacman/db/hotkeyset.cpp

benchmarks/CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/db/hotkeyset.cpp > CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/db/hotkeyset.cpp -o CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.o: db/log_cleaner.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.o -c /home/cgmin/20220321/pacman/db/log_cleaner.cpp

benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/db/log_cleaner.cpp > CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/db/log_cleaner.cpp -o CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.o: db/log_structured.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.o -c /home/cgmin/20220321/pacman/db/log_structured.cpp

benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/db/log_structured.cpp > CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/db/log_structured.cpp -o CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.o: db/recovery.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.o -c /home/cgmin/20220321/pacman/db/recovery.cpp

benchmarks/CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/db/recovery.cpp > CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/db/recovery.cpp -o CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.s

benchmarks/CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.o: benchmarks/CMakeFiles/pacman_bench.dir/flags.make
benchmarks/CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.o: util/index_arena.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_8) "Building CXX object benchmarks/CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.o -c /home/cgmin/20220321/pacman/util/index_arena.cpp

benchmarks/CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/util/index_arena.cpp > CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.i

benchmarks/CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/util/index_arena.cpp -o CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.s

# Object files for target pacman_bench
pacman_bench_OBJECTS = \
"CMakeFiles/pacman_bench.dir/benchmark.cpp.o" \
"CMakeFiles/pacman_bench.dir/histogram.cpp.o" \
"CMakeFiles/pacman_bench.dir/__/db/db.cpp.o" \
"CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.o" \
"CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.o" \
"CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.o" \
"CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.o" \
"CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.o"

# External object files for target pacman_bench
pacman_bench_EXTERNAL_OBJECTS =

benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/benchmark.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/histogram.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/__/db/db.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/__/db/hotkeyset.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_cleaner.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/__/db/log_structured.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/__/db/recovery.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/__/util/index_arena.cpp.o
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/build.make
benchmarks/pacman_bench: _deps/benchmark-build/src/libbenchmark.a
benchmarks/pacman_bench: /usr/local/lib/libpmem.so
benchmarks/pacman_bench: /usr/local/lib/libpmemobj.so
benchmarks/pacman_bench: libcceh.a
benchmarks/pacman_bench: /usr/lib/x86_64-linux-gnu/librt.so
benchmarks/pacman_bench: benchmarks/CMakeFiles/pacman_bench.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_9) "Linking CXX executable pacman_bench"
	cd /home/cgmin/20220321/pacman/benchmarks && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/pacman_bench.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
benchmarks/CMakeFiles/pacman_bench.dir/build: benchmarks/pacman_bench

.PHONY : benchmarks/CMakeFiles/pacman_bench.dir/build

benchmarks/CMakeFiles/pacman_bench.dir/clean:
	cd /home/cgmin/20220321/pacman/benchmarks && $(CMAKE_COMMAND) -P CMakeFiles/pacman_bench.dir/cmake_clean.cmake
.PHONY : benchmarks/CMakeFiles/pacman_bench.dir/clean

benchmarks/CMakeFiles/pacman_bench.dir/depend:
	cd /home/cgmin/20220321/pacman && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/cgmin/20220321/pacman /home/cgmin/20220321/pacman/benchmarks /home/cgmin/20220321/pacman /home/cgmin/20220321/pacman/benchmarks /home/cgmin/20220321/pacman/benchmarks/CMakeFiles/pacman_bench.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : benchmarks/CMakeFiles/pacman_bench.dir/depend

