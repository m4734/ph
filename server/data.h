// Node needs alignment to(?) data find lock

#include <pthread.h>
//#include <atomic>
//#include <mutex>

#include "global.h"

#define split_bit (1<<15)
#define free_bit (1<14)

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
	pthread_mutex_t mutex;	
	volatile uint8_t state;
	volatile uint8_t ref;	
	volatile uint16_t size; //size // needed cas but replaced to double check...
	unsigned int prev_offset; // should be removed
	unsigned int next_offset; //	2^32
	unsigned char buffer[NODE_BUFFER]; // node size? 256 * n 1024-8-8
}; // size must be ...

// 8byte atomic --- 8byte align


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
int data_init();
void data_clean();

void s_unlock(unsigned int offset);
void e_unlock(unsigned int offset);
int try_s_lock(unsigned int offset); // it will s lock??? // when e lock fail
int try_e_lock(unsigned int offset); // when e lock fail
int inc_ref(unsigned int offset,uint16_t limit);
void dec_ref(unsigned int offset);


unsigned int point_to_offset(unsigned char* kv_p);

void delete_kv(unsigned char* kv_p); // e lock needed

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length);
int split(unsigned int offset, unsigned char* prefix, int continue_len);

int compact(unsigned int offset, struct range_hash_entry* range_entry);//,unsigned char* prefix, int continue_len)
void print_kv(unsigned char* kv_p);

