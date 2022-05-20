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
extern int num_of_thread;
extern int connection_per_thread;
extern int total_connection;
extern int port;

#define USE_DRAM 0

// hash

extern int point_hash_table_size;
extern int range_hash_table_size;

#define NODE_BUFFER 1024-64+1024+56
//#define NODE_BUFFER 256 // test
extern int node_size;

extern int key_size;
extern int len_size;
extern int value_size;
extern int entry_size;

//data

extern uint64_t pmem_size;
extern char pmem_file[100];// = "/mnt/pmem0/ph_data";

//query
extern unsigned char empty[10];
extern int empty_len;


void temp_static_conf(int tn,int ks,int vs);

// multiple definition
/*
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
*/

//#endif
