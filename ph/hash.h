#include <pthread.h>

#include <atomic>

#include "global.h"

//#define INIT_OFFSET 0 // it is not found from now
//#define SPLIT_OFFSET 1

//#define htt

namespace PH
{

//OP need entry allocator

//struct point_hash_entry* find_point_entry(char* key_p,int key_len);
//struct range_hash_entry* find_range_entry(char* key_p,int key_len);

//struct point_hash_entry* find_or_insert_point_entry(unsigned char* key_p/*,int key_len*/,int insert);
//struct range_hash_entry* find_or_insert_range_entry(char* key_p,int key_len,char* p,int update);

ValueEntry find_point_entry(unsigned char* &key_p);
//volatile uint64_t* find_or_insert_point_entry(unsigned char* &key_p,void* unlock);
std::atomic<uint64_t>* find_or_insert_point_entry(unsigned char* &key_p,void* &unlock);
void insert_point_entry(unsigned char* key_p,ValueEntry& ve);
void unlock_entry(void* &unlock);


//struct range_hash_entry*
Node_offset find_range_entry(unsigned char* key_p,int* continue_len);
//struct range_hash_entry*
Node_offset find_range_entry2(unsigned char* &key_p,int* continue_len); // binary

void insert_range_entry(unsigned char* key_p,int len,Node_offset offset);
void remove_point_entry(unsigned char* &key_p);

void init_hash();
void clean_hash();

void find_in_log(unsigned char* key_p,int len); // test

int find_hot(unsigned char* key_p,int continue_len);


/*
Node_offset test_read(unsigned char* key_p,int len);
void test_insert(unsigned char* key_p,int len,Node_offset offset);

void init_t();
*/

}
