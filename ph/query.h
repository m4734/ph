#pragma once

#define QUERY_BUFFER 10000

#include <pthread.h>
#include <atomic>
#include <string>

#include "global.h"

//#define qtt

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
//	volatile Node_offset scan_offset; // volatile???
	volatile uint32_t scan_offset;
	void* node_data;
//	Node_data node_data[PART_MAX];
//	int* node_size;
//	int part;
//	unsigned char* kv_p;

//	int sorted_index[100*PART_NUM]; // 100?
//	int index_num,index_max;
	unsigned char* sorted_kv[100*PART_MAX];
	int sorted_kv_i,sorted_kv_max;

//	pthread_mutex_t scan_mutex; // offset
//	std::atomic<uint8_t> scan_lock;
};

//unsigned char empty[10];// = {"empty"};
//int empty_len;// = 5;

void init_query(Query* query);
void reset_query(Query* query);

void free_query(Query* query);

int lookup_query(unsigned char* &key_p, unsigned char* &result_p,int* result_len_p);
int lookup_query(unsigned char* &key_p, std::string *value);

int delete_query(unsigned char* key_p);

void insert_query(unsigned char* &key_p, unsigned char* &value_p);
void insert_query(unsigned char* &key_p, unsigned char* &value_p, int &value_len);

void insert_query_l(unsigned char* &key_p, unsigned char* &value_p);
void insert_query_l(unsigned char* &key_p, unsigned char* &value_p, int &value_len);

int scan_query(Query* query);
int next_query(Query* query,unsigned char* result_p,int* result_len_p);
int next_query(Query* query,std::string* result);

size_t scan_query2(unsigned char* key,int cnt,std::string* scan_result);

}
