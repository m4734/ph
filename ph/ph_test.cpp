#include <stdio.h>
#include <time.h>

#include "global2.h"

int main()
{
	printf("ph_tset start\n");

	PH::PH_Interface phi;

	phi.global_init(1,1,1);


	struct timespec ts1,ts2;

	clock_gettime(CLOCK_MONOTONIC,&ts1);

	uint64_t key;
	unsigned char value[100];

	size_t ops=1000000;

	int i;
	for (i=1;i<=ops;i++)
		phi.insert_op(key,value);

	clock_gettime(CLOCK_MONOTONIC,&ts2);

	size_t time;
	time = (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
	printf("time sum %lu ns\n",time);
	printf("time avg %lu ns\n",time/ops);



	phi.global_clean();
	printf("ph_test end\n");
	return 0;
}
