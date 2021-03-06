// Node needs alignment to(?) data find lock

#include <pthread.h>
#include <atomic>
//#include <mutex>
#include <sys/mman.h>

#include "global.h"
#include "query.h"

//#define split_bit (1<<15)
//#define free_bit (1<14)

//#define HEAD_OFFSET 1
//#define TAIL_OFFSET 2

//const Node_offset HEAD_OFFSET={0,1};
//const Node_offset TAIL_OFFSET={0,2};

//#define dtt

namespace PH
{
	/*
	const Node_offset INIT_OFFSET={0,0};
	const Node_offset SPLIT_OFFSET={0,1};
const Node_offset HEAD_OFFSET={0,2};
const Node_offset TAIL_OFFSET={0,3};
*/
/*
struct Node_offset
{
	uint16_t file;
	uint16_t offset;
};
*/
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
//	unsigned int next_offset; //	2^32
	Node_offset next_offset;
	unsigned int part;	

	unsigned char buffer[NODE_BUFFER]; // node size? 256 * n 1024-8-8
}; // size must be ...

// 8byte atomic --- 8byte align

struct Node_meta
{
//	volatile unsigned int next_offset;
//	volatile unsigned int prev_offset;
		
//	volatile Node_offset next_offset;
	volatile uint32_t next_offset;	
	unsigned int part;
//	volatile Node_offset prev_offset;
	volatile uint32_t prev_offset;	
//	Node_offset start_offset;
	Node_offset start_offset;	
	/*volatile */Node_offset end_offset;
	std::atomic<uint8_t> state;	
	uint8_t continue_len;
	/*volatile */uint16_t size; //size // needed cas but replaced to double check...
	uint16_t invalidated_size;
	uint16_t group_size;
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

	unsigned char padding[8];
};

extern unsigned char** meta_addr;
extern Node_meta** meta_array;
extern unsigned char** pmem_addr;
extern Node** node_data_array;

int init_data();
void clean_data();

//void s_unlock(unsigned int offset);
//void e_unlock(unsigned int offset);
//int try_s_lock(unsigned int offset); // it will s lock??? // when e lock fail
//int try_e_lock(unsigned int offset); // when e lock fail
int inc_ref(Node_offset offset);
void dec_ref(Node_offset offset);
int try_hard_lock(Node_offset offset);
void hard_unlock(Node_offset offset);
void soft_lock(Node_offset offset);

//#define offset_to_node(offset) ((Node_meta*)&meta_array[offset.file][offset.offset])
//#define offset_to_node_data(offset) ((Node*)&node_data_array[offset.file][offset.offset])

inline Node_meta* offset_to_node(Node_offset &offset) // it will be .. use macro
{
	return &meta_array[offset.file][offset.offset];
//	return &(Node_meta*)(meta_array[offset.file] + offset.offset);
//	return &meta_array[offset];
}
inline Node* offset_to_node_data(Node_offset &offset)
{
	return &node_data_array[offset.file][offset.offset];
//	return &node_data_array[offset];
}


/*
inline unsigned int point_to_offset(unsigned char* kv_p)
{
	return (kv_p - meta_addr)/sizeof(Node_meta);
}
inline unsigned int data_point_to_offset(unsigned char* kv_p)
{
	return (kv_p - pmem_addr)/sizeof(Node);
}
*/
/*
inline unsigned int calc_offset_data(void* node) // it will be optimized with define
{
	return ((unsigned char*)node-pmem_addr)/sizeof(Node);
}
*/

void delete_kv(unsigned char* kv_p); // e lock needed

unsigned char* insert_kv(Node_offset& offset,unsigned char* key,unsigned char* value,int value_length);
int split(Node_offset offset, unsigned char* prefix, int continue_len);

int compact(Node_offset offset,int continue_len);//, struct range_hash_entry* range_entry);//,unsigned char* prefix, int continue_len)
void print_kv(unsigned char* kv_p);
//int check_size(unsigned int offset,int value_length);

//int advance(unsigned char** kv_pp,int* offset,Node* node_p);
int advance_offset(Query *query);
//void copy_node(unsigned char* node_data,Node_offset node_offset);
//void sort_node(Node* node,int* sorted_index,int* max,const int node_size);
//void sort_node(Query *query);
void copy_and_sort_node(Query *query);
void insert_scan_list(Node_offset &node,void* query);
void delete_scan_entry(Node_offset &scan_offset,void* query);

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

//void invalidate_kv(Node_offset node_offset, unsigned int kv_offset,unsigned int kv_len);
void invalidate_kv(ValueEntry& ve);
int split_or_compact(Node_offset node_offset);
inline int get_continue_len(Node_offset node_offset)
{
	return offset_to_node(node_offset)->continue_len;
}

inline Node_offset get_start_offset(Node_offset& node_offset)
{
	return offset_to_node(node_offset)->start_offset;
}
inline void move_to_end_offset(Node_offset& node_offset)
{
	node_offset = offset_to_node(node_offset)->end_offset;
}


}
