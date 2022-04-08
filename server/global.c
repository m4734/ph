#include <string.h>

#include "global.h"

int num_of_thread;
int connection_per_thread;
int total_connection;
int port;


// hash

int point_hash_table_size;
int range_hash_table_size;

//#define NODE_BUFFER 1024
int node_size;

int key_size;
int len_size;
int value_size;
int entry_size;

//data

unsigned long long int pmem_size;
char pmem_file[100];// = "/mnt/pmem0/file";

//query
unsigned char empty[10];
int empty_len;


void temp_static_conf()
{
	num_of_thread = 1;
	connection_per_thread = 1;
	port = 5516;
	total_connection = num_of_thread * connection_per_thread;

	node_size = 1024; // ??
	key_size = 8; // ???
	len_size = 2;
	value_size = 100; //YCSB?
	entry_size = key_size + len_size + value_size; //no meta?

	point_hash_table_size = 20*1000*1000; // 1 entry = 32byte 10M = 320MB
	range_hash_table_size = 32*1024; // total range = point / per node * 2

	pmem_size = 1024*1024*1024;

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

	memcpy(pmem_file,"/mnt/pmem0/file",strlen("/mnt/pmem0/file"));
	pmem_file[strlen("/mnt/pmem0/file")] = 0;
}

