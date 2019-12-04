cmake_minimum_required(VERSION 3.15)
project(Block5Test)

set(CMAKE_C_COMPILER clang)
add_executable(peer peer.c uthash.h hashing.c hashing.h dht.c dht.h queue.c queue.h)


