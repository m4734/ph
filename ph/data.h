// Node needs alignment to(?) data find lock

#include <pthread.h>
#include <atomic>
//#include <mutex>
#include <sys/mman.h>
//#include <vector>

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

extern volatile int file_num;

#define INV_BIT ((uint16_t)1<<15) // should be removed
//#define LOG_BIT ((uint16_t)1<<15)


#define SPLIT_QUEUE_LEN 500
#define SPLIT_MAX SPLIT_QUEUE_LEN-10

#define NODE_SPLIT_BIT (1<<7)
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
//	unsigned int part;	
	Node_offset next_offset_ig; // in group // cas first from dram
//	uint8_t continue_len;
//	uint8_t part;
	uint16_t continue_len;
	unsigned char padding[8-2];

	unsigned char buffer[NODE_BUFFER]; // node size? 256 * n 1024-8-8
}; // size must be ...








struct LENGTH_LIST
{
	std::atomic<uint16_t> value[12];
	std::atomic<void*> next;
};

uint16_t inv_ll(void* ll,int index);
void set_ll(void* ll,int index,uint16_t value);
uint16_t get_ll(void* ll,int index);
void new_ll(std::atomic<void*>* next);

// 8byte atomic --- 8byte align

//#define NODE_SPLIT_BIT 1 << 7
#define ARRAY_INIT_SIZE 16

struct Node_meta
{

	volatile uint32_t next_offset; // 4

	std::atomic<uint32_t> next_offset_ig;//4 // need cas

	volatile uint32_t prev_offset; // 4

	Node_offset start_offset; // 4

	//16--------------------------------------------------------

//	/*volatile */Node_offset end_offset1; // 4
//	Node_offset end_offset2; // 4
	std::atomic<uint32_t> end_offset;
	std::atomic<uint32_t> end_offset1;
	std::atomic<uint32_t> end_offset2;

	//28------------------------------------------------------

	std::atomic<uint8_t> state;	// 1 // split or not
	std::atomic<uint8_t> ts; // 1

	uint8_t part; // 1
	uint8_t continue_len; // 1

	//32---------------------------------------------------------

	std::atomic<uint16_t> size_l;
	std::atomic<uint16_t> size_r; //size // needed cas but replaced to double check... // 2

	//36----------------------------------------------------------

	// used only start node
	uint32_t invalidated_size; // 4
	uint32_t group_size; // 4

	//44---------------------------------------------------------------------

//	unsigned char padding[64-38-24]; // 64-38-24 = 2


//	std::atomic<uint16_t> la_lock; //length array lock may not be used // only for max

	//


	//20

//	uint16_t inv_cnt; // 2
	
	std::atomic<uint16_t> ll_cnt; // 2
//	std::atomic<uint16_t> la_max; // 2
	std::atomic<void*> ll;//length_list; // 8

	//16

//	std::atomic<std::atomic<uint16_t>*> length_array; // 8
	

//	std::vector<std::atomic<uint16_t>> length_array;

	Scan_list* scan_list; // 8 // when will we use it?

};

extern unsigned char** meta_addr;
extern Node_meta** meta_array;
extern unsigned char** pmem_addr;
extern Node** node_data_array;

#define FILE_SIZE sizeof(Node)*(1<<16)
#define META_SIZE sizeof(Node_meta)*(1<<16)
#define MAX_FILE_NUM (1 << 16)
#define MAX_OFFSET (1 << 16)

void init_file();
void init_data();
void clean_data();

//void s_unlock(unsigned int offset);
//void e_unlock(unsigned int offset);
//int try_s_lock(unsigned int offset); // it will s lock??? // when e lock fail
//int try_e_lock(unsigned int offset); // when e lock fail
/*
inline int inc_ref(Node_offset offset)
{
	return try_at_lock(offset_to_node(offset)->state);	
}
inline void dec_ref(Node_offset offset)
{
	at_unlock(offset_to_node(offset)->state);

}
*/

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
	return (Node*)(pmem_addr[offset.file] + sizeof(Node) * offset.offset);
//	return &node_data_array[offset.file][offset.offset];
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
ValueEntry insert_kv2(Node_offset start_offset,unsigned char* key,unsigned char*value,int value_len);

int split(Node_offset offset);//,unsigned char* prefix);//, unsigned char* prefix, int continue_len);
int split2p(Node_offset offset);//,unsigned char* prefix);//, unsigned char* prefix, int continue_len);


int compact(Node_offset offset);//,int continue_len);//, struct range_hash_entry* range_entry);//,unsigned char* prefix, int continue_len)
int compact2p(Node_offset offset);//,int continue_len);//, struct range_hash_entry* range_entry);//,unsigned char* prefix, int continue_len)

int flush(Node_offset offset);
Node_offset append_node(Node_offset& offset);


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
//	lock = 0;
	lock--;
}
inline int try_at_lock(std::atomic<uint8_t> &lock)
{
	/*
	uint8_t z = 0;
	return lock.compare_exchange_strong(z,1);
	*/
	if (lock & (1<<7))
		return 0;
	lock++;
	return 1;
}

//void invalidate_kv(Node_offset node_offset, unsigned int kv_offset,unsigned int kv_len);
void invalidate_kv(ValueEntry& ve);
void invalidate_kv2(ValueEntry& ve);

int split_or_compact(Node_offset node_offset);
inline int get_continue_len(Node_offset node_offset)
{
	return offset_to_node(node_offset)->continue_len;
}

inline Node_offset get_start_offset(Node_offset& node_offset)
{
	return offset_to_node(node_offset)->start_offset;
}
/*
inline void move_to_end_offset(Node_offset& node_offset)
{
	node_offset = offset_to_node(node_offset)->end_offset;
}
*/
inline void inc_ref(Node_offset offset)
{
	offset_to_node(offset)->state++;
//	return try_at_lock(offset_to_node(offset)->state);	
}
inline void dec_ref(Node_offset offset)
{
	offset_to_node(offset)->state--;
//	at_unlock(offset_to_node(offset)->state);
}

inline int check_ref(Node_offset offset)
{
	return offset_to_node(offset)->state;
}

uint16_t get_length_from_ve(ValueEntry& ve);
void split3(Node_offset offset);
void compact3(Node_offset offset);
int need_split(Node_offset &offset);

#ifdef split_thread
inline int try_split(Node_offset offset)
{
	uint8_t t=2;
	Node_meta* meta = offset_to_node(offset);
	return (meta->state == 2) && meta->state.compare_exchange_strong(t,1);
}
void* split_work(void* id);
int add_split(Node_offset node_offset);
inline int need_split(Node_offset node_offset,int value_len)
{
	Node_meta* meta = offset_to_node(node_offset);
	return (meta->part == PART_MAX-1);// && (meta->size + len_size + key_size + value_len + len_size > NODE_BUFFER); // %2
}

void init_split();
void clean_split();
#endif

int scan_node(Node_offset offset,unsigned char* key,int result_req,std::string* scan_result);

void init_data_local();
void clean_thread_local();
}
