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
include benchmarks/other/CMakeFiles/chameleondb_bench.dir/depend.make

# Include the progress variables for this target.
include benchmarks/other/CMakeFiles/chameleondb_bench.dir/progress.make

# Include the compile flags for this target's objects.
include benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make

benchmarks/other/CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.o: benchmarks/other/chameleondb_bench.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/other/chameleondb_bench.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/other/chameleondb_bench.cpp > CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/other/chameleondb_bench.cpp -o CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.s

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.o: benchmarks/other/ChameleonDB/ChameleonIndex.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/ChameleonIndex.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/ChameleonIndex.cpp > CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/ChameleonIndex.cpp -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.s

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.o: benchmarks/other/ChameleonDB/hotkeyset.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/hotkeyset.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/hotkeyset.cpp > CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/hotkeyset.cpp -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.s

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.o: benchmarks/other/ChameleonDB/log.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/log.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/log.cpp > CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/log.cpp -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.s

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.o: benchmarks/other/ChameleonDB/log_gc.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/log_gc.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/log_gc.cpp > CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/other/ChameleonDB/log_gc.cpp -o CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.s

benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.o: benchmarks/histogram.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.o -c /home/cgmin/20220321/pacman/benchmarks/histogram.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/benchmarks/histogram.cpp > CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/benchmarks/histogram.cpp -o CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.s

benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.o: benchmarks/other/CMakeFiles/chameleondb_bench.dir/flags.make
benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.o: util/index_arena.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "Building CXX object benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.o"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.o -c /home/cgmin/20220321/pacman/util/index_arena.cpp

benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.i"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cgmin/20220321/pacman/util/index_arena.cpp > CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.i

benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.s"
	cd /home/cgmin/20220321/pacman/benchmarks/other && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cgmin/20220321/pacman/util/index_arena.cpp -o CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.s

# Object files for target chameleondb_bench
chameleondb_bench_OBJECTS = \
"CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.o" \
"CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.o" \
"CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.o" \
"CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.o" \
"CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.o" \
"CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.o" \
"CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.o"

# External object files for target chameleondb_bench
chameleondb_bench_EXTERNAL_OBJECTS =

benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/chameleondb_bench.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/ChameleonIndex.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/hotkeyset.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/ChameleonDB/log_gc.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/histogram.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/__/__/util/index_arena.cpp.o
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/build.make
benchmarks/other/chameleondb_bench: _deps/benchmark-build/src/libbenchmark.a
benchmarks/other/chameleondb_bench: /usr/local/lib/libpmem.so
benchmarks/other/chameleondb_bench: /usr/local/lib/libpmemobj.so
benchmarks/other/chameleondb_bench: /usr/lib/x86_64-linux-gnu/librt.so
benchmarks/other/chameleondb_bench: benchmarks/other/CMakeFiles/chameleondb_bench.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/cgmin/20220321/pacman/CMakeFiles --progress-num=$(CMAKE_PROGRESS_8) "Linking CXX executable chameleondb_bench"
	cd /home/cgmin/20220321/pacman/benchmarks/other && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/chameleondb_bench.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
benchmarks/other/CMakeFiles/chameleondb_bench.dir/build: benchmarks/other/chameleondb_bench

.PHONY : benchmarks/other/CMakeFiles/chameleondb_bench.dir/build

benchmarks/other/CMakeFiles/chameleondb_bench.dir/clean:
	cd /home/cgmin/20220321/pacman/benchmarks/other && $(CMAKE_COMMAND) -P CMakeFiles/chameleondb_bench.dir/cmake_clean.cmake
.PHONY : benchmarks/other/CMakeFiles/chameleondb_bench.dir/clean

benchmarks/other/CMakeFiles/chameleondb_bench.dir/depend:
	cd /home/cgmin/20220321/pacman && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/cgmin/20220321/pacman /home/cgmin/20220321/pacman/benchmarks/other /home/cgmin/20220321/pacman /home/cgmin/20220321/pacman/benchmarks/other /home/cgmin/20220321/pacman/benchmarks/other/CMakeFiles/chameleondb_bench.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : benchmarks/other/CMakeFiles/chameleondb_bench.dir/depend

