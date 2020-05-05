#!/bin/bash
set -e

files=`find . -name "*.cpp" -or -name "*.hpp" -or -name ".h" | grep -v "./tools/*" | grep -v "./cmake-build-debug/CMakeFiles/3.16.5/CompilerIdCXX/CMakeCXXCompilerId.cpp" | grep -v "./tests" | grep -v "./include/1/" | grep -v "./cmake-build-debug/_3rdParty/Hunter/toolchain/_builds/CMakeFiles/3.16.5/CompilerIdCXX/CMakeCXXCompilerId.cpp"`
filter=-build/c++11,-runtime/references,-whitespace/braces,-whitespace/indent,-whitespace/comments,-build/include_order
echo $files | xargs cpplint --filter=$filter

export CTEST_OUTPUT_ON_FAILURE=true
# address sanitizer
#CMAKE_LINKER_OPTS="-DCMAKE_EXE_LINKER='-fuse-ld=gold'"
CMAKE_CONFIG_OPTS="-DHUNTER_CONFIGURATION_TYPES=Debug -DCMAKE_BUILD_TYPE=Debug"
CMAKE_TOOLCHAIN_OPTS="-DCMAKE_TOOLCHAIN_FILE='`pwd`/tools/polly/sanitize-address-cxx17-pic.cmake'"
CMAKE_OPTS="$CMAKE_LINKER_OPTS $CMAKE_CONFIG_OPTS $CMAKE_TOOLCHAIN_OPTS"
cmake -H. -B_builds/sanitize-address-cxx17 $CMAKE_OPTS
cmake --build _builds/sanitize-address-cxx17
./_builds/sanitize-address-cxx17/tests
# thread sanitizer
CMAKE_TOOLCHAIN_OPTS="-DCMAKE_TOOLCHAIN_FILE='`pwd`/tools/polly/sanitize-thread-cxx17-pic.cmake'"
CMAKE_OPTS="$CMAKE_LINKER_OPTS $CMAKE_CONFIG_OPTS $CMAKE_TOOLCHAIN_OPTS"
cmake -H. -B_builds/sanitize-thread-cxx17 $CMAKE_OPTS
cmake --build _builds/sanitize-thread-cxx17
./_builds/sanitize-thread-cxx17/tests
