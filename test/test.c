#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<pthread.h>

#include "test.h"

struct Query
{
	int op;
	int cnt;
	unsigned char* key;
	unsigned char* value;
};

struct Thread_parameter
{
	int tn;
};

int type;
char path[1000];
int num_of_threads;
int key_size;
int value_size;
int ops,tops;

struct Query** queries;

unsigned char* key_array;
unsigned char* value_array;

struct KVS* kvs;

void load_workload()
{
	FILE* in;
	char op[100];
	int i,t,o;
	unsigned char* key_ptr;
	unsigned char* value_ptr;

	in = fopen(path,"r");

	fscanf(in,"%d\n",&ops);

	tops = ops/num_of_threads+1;

	queries = (struct Query**)malloc(sizeof(struct Query*) * num_of_threads);
	for (i=0;i<num_of_threads;i++)
		queries[i] = (struct Query*)malloc(sizeof(struct Query) * tops);

	key_array = (unsigned char*)malloc(key_size * ops);
	value_array = (unsigned char*)malloc(value_size * ops);

	t = 0;
	o = 0;
	key_ptr = key_array;
	value_ptr = value_array;

	for (i=0;i<ops;i++)
	{
		fscanf(in,"%s",op);
		if (strcmp(op,"INSERT") == 0)
			queries[t][o].op = 1;
		else if (strcmp(op,"READ") == 0)
			queries[t][o].op = 2;
		else if (strcmp(op,"UPDATE") == 0)
			queries[t][o].op = 3;
		else if (strcmp(op,"DELETE") == 0)
			queries[t][o].op = 4;
		else if (strcmp(op,"SCAN") == 0)
			queries[t][o].op = 5;
		else
		{
			printf("op error\n");
			queries[t][o].op = 0;
		}

		// can be changed
		// i will use string this time

		fscanf(in,"%s",key_ptr);
		fscanf(in,"%s",value_ptr);

		queries[t][o].key = key_ptr;
		queries[t][o].value = value_ptr;

		key_ptr+=key_size;
		value_ptr+=value_size;

		t++;
		if (t == num_of_threads)
		{
			t=0;
			o++;
		}
	}


	for (;i<tops;i++)
	{
		queries[t][o].op = 0;
		o++;
	}


	fclose(in);
}

void worker_function()
{
	int i,tn;
	for (i=0;i<tops;i++)
	{
		if (queries[tn][i].op == 0)
			continue;
		if (queries[tn][i].op == 1)
			kvs->insert_op(queries[tn][i].key,queries[tn][i].value);
		if (queries[tn][i].op == 2)
			kvs->read_op(queries[tn][i].key);
		if (queries[tn][i].op == 3)
			kvs->update_op(queries[tn][i].key,queries[tn][i].value);
		if (queries[tn][i].op == 4)
			kvs->delete_op(queries[tn][i].key);
		if (queries[tn][i].op == 5)
			kvs->scan_op(queries[tn][i].key,queries[tn][i].cnt);
	}
}

void run()
{
	pthread_t* pthread_array;
	struct Thread_parameter* thread_parameter_array;
	int i;

	pthread_array = (pthread_t*)malloc(sizeof(pthread_t) * num_of_threads);
	thread_parameter_array = (Thread_parameter*)malloc(sizeof(Thread_parameter) * num_of_threads);

	for (i=0;i<num_of_threads;i++)
	{
		thread_parameter_array[i].tn = i;
		pthread_create(&pthread_array[i],NULL,worker_function,(void*)&thread_parameter_array[i]);
	}

	for (i=0;i<num_of_threads;i++)
	{
		pthread_join(pthread_array[i],NULL);
	}

	free(thread_parameter_array);
	free(pthread_array);
}

void clean()
{

	// free workload
	int i;
	for (i=0;i<num_of_threads;i++)
		free(queries[i]);
	free(queries);

	free(key_array);
	free(value_array);

	// free thread???
}

int main()
{

	int type;
	char path[1000];

	printf("type\n");
	printf("1.PH\n");
	scanf("%d",&type);

	printf("num_of_threads\n");
	scanf("%d",&num_of_threads);

	printf("key_size\n");
	scanf("%d",&key_size);

	printf("value_size\n");
	scanf("%d",&value_size);

	// init here


	while(1)
	{
		printf("workload file name or exit\n");
		scanf("%s",path);
		if (strcmp(path,"exit") == 0)
			break;

		printf("loading workload\n");
		load_workload();
		printf("loaded\n");

		printf("run\n");
		run();
		
		clean();

		printf("end\n");

	}



	return 0;
}
