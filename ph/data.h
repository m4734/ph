// Node needs alignment to(?) data find lock

#include <pthread.h>
#include <atomic>
//#include <mutex>
#include <sys/mman.h>

#include "global.h"
//#include "query.h"

//#define split_bit (1<<15)
//#define free_bit (1<14)

#define HEAD_OFFSET 1
#define TAIL_OFFSET 2

//#define dtt

namespace PH
{

struct Scan_list
{
	void* query; //need offset and mutex
	struct Scan_list* next;
};

struct Node
//class Node
{
//	public:
//	atomic int8_t s_lock;
//	atomic int8_t e_lock;
//	char* next_p;	
//	volatile unsigned int meta; // ???? 2bit - lock? 6bit - size	2^6 64-1
//	uint8_t lock; // s e lock 4bit 4bit atomic uint32 uint32
//	std::atomic<uint8_t> state; // 0-init	1-split	2-free + ref
//	std::atomic<uint8_t> ref;
//	std::mutex m;		
//	volatile uint16_t size; //size // needed cas but replaced to double check...
	unsigned int next_offset; //	2^32

	unsigned char buffer[NODE_BUFFER]; // node size? 256 * n 1024-8-8
}; // size must be ...

// 8byte atomic --- 8byte align

struct Node_meta
{
	volatile unsigned int next_offset;
	volatile unsigned int prev_offset;
	std::atomic<uint8_t> state;	
	/*volatile */uint16_t size; //size // needed cas but replaced to double check...
	uint16_t invalidated_size;
	uint16_t continue_len;
//	std::atomic<uint16_t> size;
//	unsigned int next_offset; //	2^32

//	pthread_mutex_t mutex;	
//	std::atomic<int> lock;		
//	uint8_t state;
//	std::atomic<uint8_t> ref;
//	uint8_t ref;	

	/*
	volatile uint16_t size; //size // needed cas but replaced to double check...
	unsigned int next_offset; //	2^32
	*/


	Scan_list* scan_list;

//	uint16_t ic;
//	uint16_t inv_kv[100];//NODE_BUFFER/216]; // need to be list
	uint16_t* inv_kv;
	uint16_t inv_cnt;
	uint16_t inv_max;
};

extern unsigned char* meta_addr;
extern Node_meta* meta_array;
extern unsigned char* pmem_addr;
extern Node* node_data_array;

int init_data();
void clean_data();

//void s_unlock(unsigned int offset);
//void e_unlock(unsigned int offset);
//int try_s_lock(unsigned int offset); // it will s lock??? // when e lock fail
//int try_e_lock(unsigned int offset); // when e lock fail
int inc_ref(unsigned int offset);
void dec_ref(unsigned int offset);
int try_hard_lock(unsigned int offset);
void hard_unlock(unsigned int offset);
void soft_lock(unsigned int offset);

inline Node_meta* offset_to_node(unsigned int offset) // it will be .. use macro
{
	return &meta_array[offset];
}
inline Node* offset_to_node_data(unsigned int offset)
{
	return &node_data_array[offset];
}
inline unsigned int point_to_offset(unsigned char* kv_p)
{
	return (kv_p - meta_addr)/sizeof(Node_meta);
}
inline unsigned int data_point_to_offset(unsigned char* kv_p)
{
	return (kv_p - pmem_addr)/sizeof(Node);
}
inline unsigned int calc_offset_data(void* node) // it will be optimized with define
{
	return ((unsigned char*)node-pmem_addr)/sizeof(Node);
}


void delete_kv(unsigned char* kv_p); // e lock needed

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length,int old_size);
int split(unsigned int offset, unsigned char* prefix, int continue_len);

int compact(unsigned int offset,int continue_len);//, struct range_hash_entry* range_entry);//,unsigned char* prefix, int continue_len)
void print_kv(unsigned char* kv_p);
int check_size(unsigned int offset,int value_length);

//int advance(unsigned char** kv_pp,int* offset,Node* node_p);
int advance_offset(void* query);
//void copy_node(Node* node1,Node* node2);
void sort_node(Node* node,int* sorted_index,int* max);
void insert_scan_list(Node_meta* node,void* query);
void delete_scan_entry(unsigned int scan_offset,void* query);

void at_lock(std::atomic<uint8_t> &lock);
inline void at_unlock(std::atomic<uint8_t> &lock)
{
	lock = 0;
}
inline int try_at_lock(std::atomic<uint8_t> &lock)
{
	uint8_t z = 0;
	return lock.compare_exchange_strong(z,1);
}

void invalidate_kv(unsigned int node_offset, unsigned int kv_offset,unsigned int kv_len);
int split_or_compact(unsigned int node_offset);
inline int get_continue_len(unsigned int node_offset)
{
	return offset_to_node(node_offset)->continue_len;
}




}
