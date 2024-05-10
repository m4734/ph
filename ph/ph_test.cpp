#include <stdio.h>
#include <time.h>
#include <pthread.h>

#include "global2.h"


#define VALUE_SIZE 100
#define KEY_RANGE 1000000

struct Parameter
{
	PH::PH_Interface *phi;
	int ops;
	size_t time;
	size_t op_id;
};


void *run(void *parameter)
{
	int i;
	Parameter *para = (Parameter*)parameter;

	uint64_t key;
	unsigned char value[VALUE_SIZE];

	struct timespec ts1,ts2;

	clock_gettime(CLOCK_MONOTONIC,&ts1);

	for (i=0;i<para->ops;i++)
	{
		key = (para->op_id+i)%KEY_RANGE;
		*(uint64_t*)value = para->op_id+i;
		para->phi->insert_op(key,value);
	}

	clock_gettime(CLOCK_MONOTONIC,&ts2);

	para->time = (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
	return NULL;
}

#define THREAD_NUM 16
#define PMEM_NUM 4
//#define LOG_PER_PMEM 4

int main()
{

	printf("THREAD_NUM %d\n",THREAD_NUM);
	printf("PMEM_NUM %d\n",PMEM_NUM);

	printf("ph_tset start\n");

	PH::PH_Interface phi;

	phi.global_init(THREAD_NUM,PMEM_NUM);

//--------------------------- init

	size_t ops=1000000000;
	Parameter para[100];

	int i;
	for (i=0;i<THREAD_NUM;i++)
	{
		para[i].phi = &phi;
		para[i].ops = ops/THREAD_NUM;
		para[i].op_id = (ops/THREAD_NUM)*i;
	}



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
	printf("Mops avg %lf\n",dops/1000/1000/(time/1000/1000/1000));
	printf("GB/s avg %lf\n",dops*(116)/1024/1024/1024/(time/1000/1000/1000));



	phi.global_clean();
	printf("ph_test end\n");
	return 0;
}
