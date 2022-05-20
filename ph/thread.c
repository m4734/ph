#include "thread.h"
#include "data.h" //free_cnt
#include "global.h" //num_of_thread

#include <stdlib.h>

PH_Thread* thread_list;

extern unsigned int free_cnt;
extern int num_of_thread;

mtx_t m;

void init_thread()
{
	int i;

	thread_list = (PH_Thread*)malloc(num_of_thread * sizeof(PH_Thread));
	for (i=0;i<num_of_thread;i++)
		thread_list[i].free_cnt = 999999999; // ignore until new
	mtx_init(&m,mtx_plain);
}

void clean_thread()
{
	mtx_destroy(&m);
	free(thread_list);
}

void new_thread()
{
	int i;
	thrd_t tid;
	tid = thrd_current();
	mtx_lock(&m);
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].free_cnt == 999999999)
		{
			thread_list[i].tid = tid;
			thread_list[i].free_cnt = free_cnt;
			break;
		}
	}
	mtx_unlock(&m);
}

void update_free_cnt()
{
	int i;
	thrd_t tid;

	tid = thrd_current();
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].tid == tid)
		{
			thread_list[i].free_cnt = free_cnt;
			return;
		}
	}
	new_thread();
}

unsigned int min_free_cnt()
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_of_thread;i++)
	{
		if (min > thread_list[i].free_cnt)
			min = thread_list[i].free_cnt;
	}
	return min;
}
