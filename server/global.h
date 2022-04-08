#pragma once

// network thread

#include <stdint.h>

//#ifndef GLOBAL
//#define GLOBAL

/*
typedef uint8_t u_int8_t
typedef uint16_t u_int16_t
typedef uint64_t u_int64_t
*/
/*
typedef u_int8_t uint8_t
typedef u_int16_t uint16_t
typedef u_int64_t uint64_t
*/
int num_of_thread;
int connection_per_thread;
int total_connection;
int port;


// hash

int point_hash_table_size;
int range_hash_table_size;

#define NODE_BUFFER 1024
int node_size;

int key_size;
int len_size;
int value_size;
int entry_size;

//data

unsigned long long int pmem_size;
char pmem_file[100] = "/mnt/pmem0/file";

void temp_static_conf()
{
	num_of_thread = 1;
	connection_per_thread = 1;
	port = 5516;
	total_connection = num_of_thread * connection_per_thread;

	node_size = 1024; // ??
	key_size = 8; // ???
	len_size = 2;
	value_size = 100; //YCSB?
	entry_size = key_size + len_size + value_size; //no meta?

	point_hash_table_size = 20*1000*1000; // 1 entry = 32byte 10M = 320MB
	range_hash_table_size = 32*1024; // total range = point / per node * 2

	pmem_size = 1024*1024*1024;
}


//#endif
