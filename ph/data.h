// Node needs alignment to(?) data find lock

#include <pthread.h>
//#include <atomic>
//#include <mutex>
#include <sys/mman.h>

#include "global.h"
//#include "query.h"

//#define split_bit (1<<15)
//#define free_bit (1<14)

#define HEAD_OFFSET 1
#define TAIL_OFFSET 2

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
	volatile uint16_t size; //size // needed cas but replaced to double check...
	unsigned int next_offset; //	2^32

	unsigned char buffer[NODE_BUFFER]; // node size? 256 * n 1024-8-8
}; // size must be ...

// 8byte atomic --- 8byte align

struct Node_meta
{
	volatile uint16_t size; //size // needed cas but replaced to double check...
	unsigned int next_offset; //	2^32

	pthread_mutex_t mutex;	
	uint8_t state;
	uint8_t ref;	

	/*
	volatile uint16_t size; //size // needed cas but replaced to double check...
	unsigned int next_offset; //	2^32
	*/

	unsigned int prev_offset; // should be removed

	Scan_list* scan_list;
};



/*

   (key_len?)
   (value_len?)
   key
   value

*/
/*
struct Data
{

};
*/
int init_data();
void clean_data();

void s_unlock(unsigned int offset);
void e_unlock(unsigned int offset);
int try_s_lock(unsigned int offset); // it will s lock??? // when e lock fail
int try_e_lock(unsigned int offset); // when e lock fail
int inc_ref(unsigned int offset,uint16_t limit);
void dec_ref(unsigned int offset);
int try_hard_lock(unsigned int offset);
void hard_unlock(unsigned int offset);
void soft_lock(unsigned int offset);

Node_meta* offset_to_node(unsigned int offset); // it will be .. use macro
Node* offset_to_node_data(unsigned int offset);
unsigned int point_to_offset(unsigned char* kv_p);
unsigned int data_point_to_offset(unsigned char* kv_p);

void delete_kv(unsigned char* kv_p); // e lock needed

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length,int old_size);
int split(unsigned int offset, unsigned char* prefix, int continue_len);

int compact(unsigned int offset);//, struct range_hash_entry* range_entry);//,unsigned char* prefix, int continue_len)
void print_kv(unsigned char* kv_p);
int check_size(unsigned int offset,int value_length);

//int advance(unsigned char** kv_pp,int* offset,Node* node_p);
int advance_offset(void* query);
//void copy_node(Node* node1,Node* node2);
void sort_node(Node* node,int* sorted_index,int* max);
void insert_scan_list(Node_meta* node,void* query);
void delete_scan_entry(unsigned int scan_offset,void* query);

