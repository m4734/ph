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
/*
#define SMALL_NODE
#ifdef SMALL_NODE
	#define NODE_BUFFER 1024-4-4-8 // test
#else
	#define NODE_BUFFER 1024*4-4-4-8
#endif
*/
#define NSK 1
#define NODE_BUFFER 1024*NSK-4-4-8

//extern int node_size;
#define PART_MAX 4
//#define PART_MAX 16
//#define PART_MAX 2 // test

#if PART_MAX == 1
	#define PM_N 1
#else
	#define PM_N 4
#endif

#define PAGE_SIZE 4096

/*
#if PART_MAX > 1
	#define split_thread
#endif
*/

//#define split_thread
//#define try_recover

#ifdef split_thread
#define SPLIT_NUM 4
#else
#define SPLIT_NUM 0
#endif

namespace PH
{

struct Node_offset
{
	uint16_t file;
	uint16_t offset;
	bool operator==(const Node_offset &v) { return (file == v.file) && (offset == v.offset); }
	bool operator!=(const Node_offset &v) { return (file != v.file) || (offset != v.offset); }

	/*
	Node_offset& operator=(Node_offset &v) { 
		(file = v.file);
		(offset = v.offset);
       return *this;
	}
	*/
};

	const Node_offset INIT_OFFSET={0,0};
	const Node_offset SPLIT_OFFSET={0,1};
const Node_offset HEAD_OFFSET={0,2};
const Node_offset TAIL_OFFSET={0,3};

struct Node_offset_u
{
	union
	{
		Node_offset no;
		uint32_t no_32;
	};
};

const Node_offset_u TAIL_OFFSET_u = {0,3};
/*
struct ValueEntry2
{
	uint32_t kv_offset; // offset in file
	uint16_t file;
	uint16_t len;
};
*/
struct ValueEntry
//class ValueEntry
{
	Node_offset node_offset;
	uint16_t kv_offset;
//	uint16_t len;
	uint8_t index;
	uint8_t ts;
	
	/*
	volatile uint32_t node_offset;
	volatile uint16_t kv_offset;
	volatile uint16_t len;
*/
//	public:
/*	
	uint32_t node_offset;
	uint16_t kv_offset;
	uint16_t len;
	
	volatile ValueEntry& operator=(const ValueEntry& ve) volatile { 
		node_offset = ve.node_offset;
		kv_offset = ve.kv_offset;
		len = ve.len;
		return *this;
	}
	
	volatile ValueEntry& operator=(const volatile ValueEntry& ve) volatile { 
		node_offset = ve.node_offset;
		kv_offset = ve.kv_offset;
		len = ve.len;
		return *this;
	}

	
	ValueEntry& operator=(const volatile ValueEntry& ve) { 
		node_offset = ve.node_offset;
		kv_offset = ve.kv_offset;
		len = ve.len;
		return *this;
	}
	
	ValueEntry& operator=(const ValueEntry& ve) { 
		node_offset = ve.node_offset;
		kv_offset = ve.kv_offset;
		len = ve.len;
		return *this;
	}
*/
};

struct ValueEntry_u
{
	union
	{
		ValueEntry ve;
		uint64_t ve_64;
	};
};

extern int num_of_thread;
//extern int connection_per_thread;
//extern int total_connection;
//extern int port;

extern int num_of_split;


#define USE_DRAM 0

// hash

extern int point_hash_table_size;
extern int* range_hash_table_size;

#define PH_KEY_SIZE 8
#define PH_LEN_SIZE 2
#define PH_TS_SIZE 2
#define PH_LTK_SIZE PH_KEY_SIZE+PH_LEN_SIZE+PH_TS_SIZE

//extern int key_size;
//extern int len_size;
extern int value_size;
extern int entry_size;
extern int key_bit;

/*
#define key_size 8
#define len_size 2
#define value_size 200
#define entry_size 210
#define key_bit 64
*/
//data

extern uint64_t pmem_size;
extern char pmem_file[100];// = "/mnt/pmem0/ph_data";

//query
extern unsigned char empty[10];
extern int empty_len;

void temp_static_conf(int tn,int ks,int vs);
void clean();
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
//
}
