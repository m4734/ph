#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<pthread.h>

#include <time.h>

#include <stdint.h>

#include "test.h"

#ifdef BUILD_PH
#include "../ph/kvs.h"
#endif

#ifdef BUILD_Viper
#include "../viper/kvs.h"
#endif

#ifdef BUILD_PMEMKV
#include <libpmemkv.h>
#include "../pmemkv/kvs.h"
#endif

#ifdef BUILD_PMEMROCKSDB
#include "../pmem-rocksdb/kvs.h"
#endif

#ifdef BUILD_PACMAN
#include "../pacman/kvs.h"
#endif

struct Thread_parameter
{
	int tn;
};

int type;
//char path[1000];
int num_of_threads;
int workload_key_size;
int workload_value_size;
int ops,tops;

struct TestQuery** queries;

unsigned char* key_array;
//unsigned char* value_array;
unsigned char** value_array;

//unsigned char** result_array;
//unsigned char*** scan_result_array;

KVS* kvs;

unsigned char* dummy_ptr;

#define LINE_LENGTH 2000

void load_workload(char* path)
{
	FILE* in;
	char op[100];
	char line[LINE_LENGTH];
	int i,t,o,j;
	unsigned char* key_ptr;
	unsigned char* value_ptr;
	int key_type;
	int n32;

	in = fopen(path,"r");

	if (in == NULL)
		printf("file open fail\n");

//	fscanf(in,"%d %d\n",&ops,&key_type);
	fgets(line,LINE_LENGTH,in);
	sscanf(line,"%d %d\n",&ops,&key_type);

	tops = ops/num_of_threads+1;

	queries = (struct TestQuery**)malloc(sizeof(struct TestQuery*) * num_of_threads);
	for (i=0;i<num_of_threads;i++)
		queries[i] = (struct TestQuery*)malloc(sizeof(struct TestQuery) * tops);

	key_array = (unsigned char*)malloc(workload_key_size * ops);
//	value_array = (unsigned char*)malloc(workload_value_size * ops);
//	value_array = (unsigned char*) mmap(NULL,workload_value_size*ops,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);	
	value_array = (unsigned char**)malloc(sizeof(unsigned char*) * ops);
	for (i=0;i<ops;i++)
		value_array[i] = (unsigned char*)malloc(workload_value_size);

	dummy_ptr = (unsigned char*)malloc(workload_value_size);

	memset(key_array,0,workload_key_size * ops);
//	memset(value_array,0,workload_key_size * ops);

	for (i=0;i<workload_value_size;i++)
		dummy_ptr[i] = i%256;
/*
	result_array = (unsigned char**)malloc(sizeof(unsigned char*) * num_of_threads);
	scan_result_array = (unsigned char***)malloc(sizeof(unsigned char**) * num_of_threads);
	for (i=0;i<num_of_threads;i++)
	{
		result_array[i] = (unsigned char*)malloc(workload_value_size);
		scan_result_array[i] = (unsigned char**)malloc(100 * sizeof(unsigned char*));
	
		for (j=0;j<100;j++)
			scan_result_array[i][j] = (unsigned char*)malloc(workload_key_size + workload_value_size);
	}
*/
	t = 0;
	o = 0;
	key_ptr = key_array;
//	value_ptr = value_array;

	for (i=0;i<ops;i++)
	{
		fgets(line,LINE_LENGTH,in);
		if (key_type == 0) // key binary
		{
			sscanf(line,"%s %s",op,key_ptr);
		}
		else if (key_type == 1 || key_type == 2) // key int64
		{
			sscanf(line,"%s %ld",op,(uint64_t*)key_ptr);
			//test
//			printf("%ld\n",*((uint64_t*)key_ptr));
		}
		if (strcmp(op,"INSERT") == 0)
			queries[t][o].op = 1;
		else if (strcmp(op,"READ") == 0)
			queries[t][o].op = 2;
		else if (strcmp(op,"UPDATE") == 0)
			queries[t][o].op = 3;
		else if (strcmp(op,"DELETE") == 0)
			queries[t][o].op = 4;
		else if (strcmp(op,"SCAN") == 0)
		{
			queries[t][o].op = 5;
			queries[t][o].cnt = 100; //??
		}
		else
		{
			printf("op error\n");
			queries[t][o].op = 0;
		}

		queries[t][o].key = key_ptr;
		key_ptr+=workload_key_size;

		if (queries[t][o].op == 1 || queries[t][o].op == 3) // have value
		{
			if (key_type == 2)
			{
				queries[t][o].value = dummy_ptr;
			}
			else
			{
				n32 = 0;
				for (j=0;j<LINE_LENGTH;j++)
				{
					if (line[j] == ' ')
					{
						n32++;
						if (n32 == 2)
							break;
					}
				}
				j++;
				memcpy(value_array[i],line+j,workload_value_size);
//				memcpy(value_ptr,line+j,workload_value_size);				
//				queries[t][o].value = value_ptr;//value_array[i];
				queries[t][o].value = value_array[i];
//				value_ptr+=workload_value_size;
			}
		
		}
		else if (queries[t][o].op == 5) //scan
		{
				n32 = 0;
				for (j=0;j<LINE_LENGTH;j++)
				{
					if (line[j] == ' ')
					{
						n32++;
						if (n32 == 2)
							break;
					}
				}
				j++;

			sscanf(line+j,"%d",&queries[t][o].cnt);
		}

//		if (strlen((char*)value_ptr) < value_size)
//			printf("data short %ld\n",strlen((char*)value_ptr));

		t++;
		if (t == num_of_threads)
		{
			t=0;
			o++;
		}
	}


	for (;i<tops*num_of_threads;i++)
	{
		queries[t][o].op = 0;
		t++;
	}


	fclose(in);
}

void* worker_function(void* thread_parameter)
{
	int i,tn;
	Thread_parameter* tp;

	tp = (Thread_parameter*)thread_parameter;
	tn = tp->tn;
/*
	for (i=0;i<tops;i++)
	{
		if (queries[tn][i].op == 0)
			continue;
		if (queries[tn][i].op == 1)
			kvs->insert_op(queries[tn][i].key,queries[tn][i].value);
		if (queries[tn][i].op == 2)
			kvs->read_op(queries[tn][i].key,result_array[tn]);
		if (queries[tn][i].op == 3)
			kvs->update_op(queries[tn][i].key,queries[tn][i].value);
		if (queries[tn][i].op == 4)
			kvs->delete_op(queries[tn][i].key);
		if (queries[tn][i].op == 5)
			kvs->scan_op(queries[tn][i].key,queries[tn][i].cnt,scan_result_array[tn]);
	}
	*/
	kvs->run(queries[tn],tops);
	kvs->exit_thread();
	return NULL;
}

void run()
{
	int i;

	pthread_t* pthread_array;
	struct Thread_parameter* thread_parameter_array;

	struct timespec ts1,ts2;
	unsigned long long int t;

	pthread_array = (pthread_t*)malloc(sizeof(pthread_t) * num_of_threads);
	thread_parameter_array = (Thread_parameter*)malloc(sizeof(Thread_parameter) * num_of_threads);

	clock_gettime(CLOCK_MONOTONIC,&ts1);
	for (i=0;i<num_of_threads;i++)
	{
		thread_parameter_array[i].tn = i;
		pthread_create(&pthread_array[i],NULL,worker_function,(void*)&thread_parameter_array[i]);
	}

	for (i=0;i<num_of_threads;i++)
	{
		pthread_join(pthread_array[i],NULL);
	}
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	t = (ts2.tv_sec-ts1.tv_sec)*1000000000 + ts2.tv_nsec-ts1.tv_nsec;

	free(thread_parameter_array);
	free(pthread_array);

	printf("result\n");
	printf("---------------------------------\n");
	printf("thread %d\n",num_of_threads);
	printf("ops %d\n",ops);
	printf("run time %lld %lld\n",t/1000000000,t%1000000000);
	printf("---------------------------------\n");

}

void clean()
{

	// free workload
	int i,j;
	for (i=0;i<num_of_threads;i++)
		free(queries[i]);
	free(queries);

	free(key_array);
//	free(value_array);
//	munmap(value_array,workload_value_size*ops);	
	
	for (i=0;i<ops;i++)
		free(value_array[i]);
	free(value_array);	
	
/*
	for (i=9;i<num_of_threads;i++)
	{
		free(result_array[i]);
		for (j=0;j<100;j++)
			free(scan_result_array[i][j]);
		free(scan_result_array[i]);
	}
	free(result_array);
	free(scan_result_array);
*/
	// free thread???
}

int main()
{

	int type;
	char path[1000];
	int init;

	printf("num_of_threads\n");
	scanf("%d",&num_of_threads);

	printf("key_size\n");
	scanf("%d",&workload_key_size);

	printf("value_size\n");
	scanf("%d",&workload_value_size);

	// init here

/*
	// new KVS
	if (type == 0)
		kvs = new KVS();
	else if (type == 1)
	{
#ifdef BUILD_PH
		printf("kvs_ph\n");
		kvs = new KVS_ph();
#elif BUILD_Viper
		printf("kvs_viper\n");
		kvs = new KVS_viper<uint64_t,uint64_t>();
#else
		printf("ph?\n");
		kvs = new KVS();
#endif
	}
*/
	init = 0;
	while(1)
	{
		printf("workload file name or exit\n");
		scanf("%s",path);
		if (strcmp(path,"exit") == 0)
			break;

		printf("loading workload\n");
		load_workload(path);
		printf("loaded\n");

		if (init == 0)
		{
#ifdef BUILD_PH
		printf("kvs_ph\n");
		kvs = new KVS_ph();
#elif BUILD_Viper
		printf("kvs_viper\n");
		if (workload_value_size == 100)
			kvs = new KVS_viper<Byte_8,Byte_100>();
		else if(workload_value_size == 200)
			kvs = new KVS_viper<Byte_8,Byte_200>();
#elif BUILD_PMEMKV
		printf("kvs_pmemkv\n");
		kvs = new KVS_pmemkv();
#elif BUILD_PMEMROCKSDB
		printf("kvs_pmem-rocksdb\n");
		kvs = new KVS_pmemrocksdb();
#elif BUILD_PACMAN
		printf("kvs_pacman\n");
		kvs = new KVS_pacman();
#else
		printf("ph?\n");
		kvs = new KVS();
#endif
			kvs->init(num_of_threads,workload_key_size,workload_value_size,ops*2); // *2
			init = 1;
		}

//		kvs->reset();

		printf("run\n");
		run();

		printf("clean\n");	
		clean();

		printf("end\n");

	}

	printf("clean2\n");
	if (init)
	{
		kvs->clean();
		delete kvs;
	}

	return 0;
}
