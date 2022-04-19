#include <pthread.h>

struct point_hash_entry
{
	/*
	char* key;
	int8_t key_len;
	*/
	unsigned char key[8];
	volatile unsigned char* kv_p; // kv pointer
	struct point_hash_entry* next;
};
struct range_hash_entry
{
	/*
	char* key;
	int8_t key_len;
	*/
	unsigned char key[8];
//	volatile char* entry_p; // node pointer
	volatile unsigned int offset; // 0 - init / 1 - splited	
	struct range_hash_entry* next;
	//OP ancestor
	//OP local_depth
};

//OP need entry allocator

//struct point_hash_entry* find_point_entry(char* key_p,int key_len);
//struct range_hash_entry* find_range_entry(char* key_p,int key_len);

struct point_hash_entry* find_or_insert_point_entry(unsigned char* key_p/*,int key_len*/,int insert);
//struct range_hash_entry* find_or_insert_range_entry(char* key_p,int key_len,char* p,int update);

struct range_hash_entry* find_range_entry(unsigned char* key_p,int* continue_len);

void insert_range_entry(unsigned char* key_p,int len,unsigned int offset);

void init_hash();
void clean_hash();
