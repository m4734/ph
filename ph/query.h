#pragma once

#define QUERY_BUFFER 10000

#include <pthread.h>
#include <atomic>

#define qtt

namespace PH
{

struct Query
{
	// test will not use query buffer
	/*
	unsigned char buffer[QUERY_BUFFER+10];
	int length;
	int cur;
*/

//	int buffer_offset,cur,buffer_len; // buffer_offset + buffer_len // cur < len
//	int n32;	
//	char* key_p;
	// ----------------------------------------------	
//	unsigned char* buffer;	
//	int length;

	// -----------------------------------------------

//	unsigned char key_p[8];	
	unsigned char* key_p;	
	unsigned char* value_p;
	int op;
	int key_len,value_len;
	//------------------------------------------
	unsigned int ref_offset; // node offset - unlock
	//----------------------------------------- scan/next
	int scan_offset; // volatile???
	void* node_data;
//	unsigned char* kv_p;

	int sorted_index[100];
	int index_num,index_max;

//	pthread_mutex_t scan_mutex; // offset
	std::atomic<uint8_t> scan_lock;
};

//unsigned char empty[10];// = {"empty"};
//int empty_len;// = 5;

void init_query(Query* query);
void reset_query(Query* query);

void free_query(Query* query);

int lookup_query(unsigned char* key_p, unsigned char* result_p,int* result_len_p);
int delete_query(unsigned char* key_p);
int insert_query(unsigned char* key_p, unsigned char* value_p);
int scan_query(Query* query);
int next_query(Query* query,unsigned char* result_p,int* result_len_p);

}
