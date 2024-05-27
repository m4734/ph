#include <stdio.h>
#include <time.h>
#include <pthread.h>

#include <stdlib.h>

#include "global2.h"


#define VALUE_SIZE 100
const size_t KEY_RANGE = 100*1000*1000; // 100M *100B = 10GB
const size_t TOTAL_OPS = 1000*1000*1000; // 1G ops
//#define KEY_RANGE 1000000000 //100M = 10G

//#define THREAD_NUM 16
//#define PMEM_NUM 4
//#define EVICT_NUM 8

#define THREAD_NUM 4
#define PMEM_NUM 1
#define EVICT_NUM 1

#define PRINT_OPS

enum OP_TYPE
{
	INSERT_OP,
	READ_OP,

};

struct Parameter
{
	PH::PH_Interface *phi;
	int ops;
	size_t time;
	size_t op_id;
	OP_TYPE op_type;
};

//#define VALIDATION

size_t key_gen()
{
	size_t v1,v2,v3;
	v1 = rand()%1000;
	v2 = rand()%1000;
	v3 = rand()%1000;
	return (v1 + v2 * 1000 + v3 * 1000000) % KEY_RANGE;
}

void *run(void *parameter)
{
	int i;
	Parameter *para = (Parameter*)parameter;

	uint64_t key;
	unsigned char value[VALUE_SIZE];

	struct timespec ts1,ts2,ts3,ts4;
	para->time = 0;

	clock_gettime(CLOCK_MONOTONIC,&ts1);
	if (para->op_type == INSERT_OP)
	{
#ifdef PRINT_OPS
		size_t old_ops=0;
		clock_gettime(CLOCK_MONOTONIC,&ts3);
#endif
	for (i=0;i<para->ops;i++)
	{
//		key = (para->op_id+i)%KEY_RANGE;
//		key = rand() % KEY_RANGE;
		key = key_gen();
		*(uint64_t*)value = para->op_id+i;
		para->phi->insert_op(key,value);
	//		printf("insert key %lu value %lu\n",key,(*(uint64_t*)value));
#ifdef PRINT_OPS
		clock_gettime(CLOCK_MONOTONIC,&ts4);
		if ((ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec > 1000000000)
		{
			printf("old ops %lu ops %d diff %lu\n",old_ops,i,i-old_ops);
			old_ops = i;
			ts3 = ts4;

		}
#endif

	}
	}
	else if (para->op_type == READ_OP)
	{
	for (i=0;i<para->ops;i++)
	{
		key = (para->op_id+i)%KEY_RANGE;
		if (para->phi->read_op(key,value) < 0)
			printf("not found!\n");
#ifdef VALIDATION
		if (*(uint64_t*)value != para->op_id+i)
			printf("validation fail key %lu value %lu\n",key,(*(uint64_t*)value));
#endif
	}

	}

	clock_gettime(CLOCK_MONOTONIC,&ts2);

	para->time = (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
	para->phi->end_op();
	return NULL;
}

//#define LOG_PER_PMEM 4
void work(PH::PH_Interface &phi, size_t ops, OP_TYPE op_type)
{
//--------------------------- init

	Parameter para[100];

	int i;
	for (i=0;i<THREAD_NUM;i++)
	{
		para[i].phi = &phi;
		para[i].ops = ops/THREAD_NUM;
		para[i].op_id = (ops/THREAD_NUM)*i;
		para[i].op_type = op_type;
	}

	printf("key range %lu\n",KEY_RANGE);
	printf("ops %lu\n",ops);

//---------------------------- run

	pthread_t pthread[100];
	for (i=0;i<THREAD_NUM;i++)
	{
		pthread_create(&pthread[i],NULL,run,(void*)&para[i]);
	}

//----------------------------- end

	size_t time=0;
	for (i=0;i<THREAD_NUM;i++)
	{
		pthread_join(pthread[i],NULL);
		time+=para[i].time;
	}
	time/=THREAD_NUM;

	double dops=ops;
	printf("time sum %lu ns\n",time);
	printf("lat avg %lu ns\n",time/(ops/THREAD_NUM));
	printf("Mops avg %lf\n",dops*1000/time);
	printf("GB/s avg %lf\n",dops*(8+8+VALUE_SIZE+4)*1000*1000*1000/1024/1024/1024/time);

}
int main()
{

	printf("THREAD_NUM %d\n",THREAD_NUM);
	printf("PMEM_NUM %d\n",PMEM_NUM);

	printf("ph_tset start\n");

	PH::PH_Interface phi;

	phi.global_init(THREAD_NUM,PMEM_NUM,EVICT_NUM);

#if 1
	phi.run_evict_thread();

	size_t ops=TOTAL_OPS;

	printf("insert\n");
	work(phi,ops,INSERT_OP);
	printf("read\n");
	work(phi,ops,READ_OP);
#endif
	phi.global_clean();
	printf("ph_test end\n");
	return 0;
}
