#include <string.h>

#include "global.h"

// init
#include "data.h"
#include "thread.h"
#include "hash.h"

//using namespace PH;
//
namespace PH
{

int num_of_thread;
//int connection_per_thread;
//int total_connection;
//int port;

int num_of_split;


// hash

int point_hash_table_size;
int* range_hash_table_size;

//#define NODE_BUFFER 1024
int node_size;

int key_size;
int len_size;
int value_size;
int entry_size;
int key_bit;

//data

uint64_t pmem_size;
char pmem_file[100];// = "/mnt/pmem0/file";

//query
unsigned char empty[10];
int empty_len;

void clean()
{
	printf("clean\n");
	clean_hash();
	clean_thread();
	clean_data(); //clean data after log unmap
}
void temp_static_conf(int tn, int ks,int vs)
{
#ifdef split_thread
	num_of_split = SPLIT_NUM;
#else
	num_of_split = 0; // temp
#endif
//	num_of_thread = 1;

	// use max tn
	tn = 24;

	num_of_thread = tn + num_of_split;	
//	connection_per_thread = 1;
//	port = 5516;
//	total_connection = num_of_thread * connection_per_thread;


//	node_size = sizeof(Node); // ?? data header
//	key_size = 8; // ???
	len_size = 2;
//	value_size = 100; //YCSB?
	key_size = ks;
	value_size = vs;
	entry_size = key_size + len_size + value_size; //no meta?
	key_bit = ks * 8;

	point_hash_table_size = 10*1000*1000; // 1 entry = 32byte 10M = 320MB
//	range_hash_table_size = 10*1000*1000; //32*1024; // total range = point / per node * 2

	pmem_size = (uint64_t)1024*1024*1024*80;

	/*
	empty[0] = 'e';
	empty[1] = 'm';
	empty[2] = 'p';
	empty[3] = 't';
	empty[4] = 'y';
	empty[5] = 0;
	*/
	empty_len = strlen("empty");
	memcpy(empty,"empty",empty_len);
	empty[empty_len] = 0;

	memcpy(pmem_file,"/mnt/pmem0/",strlen("/mnt/pmem0/"));
	pmem_file[strlen("/mnt/pmem0/")] = 0;

	init_file(); // before log
#ifdef DOUBLE_LOG
	init_log(); //before new log file
#endif
	init_thread(); // need alloc mutex later // what?
	init_hash();
	init_data();

//	exit_thread();
}

}
