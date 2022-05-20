#include "thread.h"
#include "data.h" //free_cnt
#include "global.h" //num_of_thread

#include <stdlib.h>

#define FCCT 1000 // free cnt check threshold

PH_Thread* thread_list;

extern unsigned int free_cnt;
extern int num_of_thread;

pthread_mutex_t m;

void reset_thread()
{
	int i;
	for (i=0;i<num_of_thread;i++)
		thread_list[i].free_cnt = 999999999;
}

void init_thread()
{
	int i;

	thread_list = (PH_Thread*)malloc(num_of_thread * sizeof(PH_Thread) * 2); // temp
	for (i=0;i<num_of_thread;i++)
		thread_list[i].free_cnt = 999999999; // ignore until new
	pthread_mutex_init(&m,NULL);
}

void clean_thread()
{
	pthread_mutex_destroy(&m);
	free(thread_list);
}

void new_thread()
{
	int i;
	pthread_t pt;
	pt = pthread_self();
	pthread_mutex_lock(&m);
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].free_cnt == 999999999)
		{
			thread_list[i].tid = pt;
			thread_list[i].free_cnt = free_cnt;
			break;
		}
	}
	pthread_mutex_unlock(&m);
}

void update_free_cnt()
{
	int i;
	pthread_t pt;

	pt = pthread_self();
	for (i=0;i<num_of_thread;i++)
	{
		if (pthread_equal(thread_list[i].tid,pt))
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
