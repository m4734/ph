#include <pthread.h>

#include "global.h"

#define INIT_OFFSET 0 // it is not found from now
#define SPLIT_OFFSET 1

//#define htt

namespace PH
{

//OP need entry allocator

//struct point_hash_entry* find_point_entry(char* key_p,int key_len);
//struct range_hash_entry* find_range_entry(char* key_p,int key_len);

//struct point_hash_entry* find_or_insert_point_entry(unsigned char* key_p/*,int key_len*/,int insert);
//struct range_hash_entry* find_or_insert_range_entry(char* key_p,int key_len,char* p,int update);

ValueEntry find_point_entry(unsigned char* &key_p);
ValueEntry* find_or_insert_point_entry(unsigned char* &key_p,void* unlock);
void insert_point_entry(unsigned char* key_p,ValueEntry ve);
void unlock_entry(void* unlock);


//struct range_hash_entry*
unsigned int find_range_entry(unsigned char* key_p,int* continue_len);
//struct range_hash_entry*
unsigned int find_range_entry2(unsigned char* &key_p,int* continue_len); // binary

void insert_range_entry(unsigned char* key_p,int len,unsigned int offset);
void remove_point_entry(unsigned char* &key_p);

void init_hash();
void clean_hash();

}
