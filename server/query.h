#pragma once

#define QUERY_BUFFER 10000

struct Query
{
	unsigned char buffer[QUERY_BUFFER+10];
	int length;
	int cur;


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
	int ref_offset; // node offset - unlock
	//----------------------------------------- scan/next
	int offset;
	void* node;
	unsigned char* kv_p;
};

//unsigned char empty[10];// = {"empty"};
//int empty_len;// = 5;

void init_query(Query* query);

int parse_query(Query* query);

int process_query(Query* query,unsigned char** result,int* result_len);

void complete_query(Query* query);

void free_query(Query* query);
