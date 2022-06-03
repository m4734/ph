#include "thread.h"
#include "data.h" //free_cnt
#include "global.h" //num_of_thread

#include <stdlib.h>

#define FCCT 1000 // free cnt check threshold

//using namespace PH;
namespace PH
{

PH_Thread* thread_list;

extern volatile unsigned int free_cnt;
extern int num_of_thread;

extern volatile unsigned int seg_free_cnt;

extern pthread_mutex_t alloc_mutex;
//pthread_mutex_t m;

void reset_thread()
{
	int i;
	for (i=0;i<num_of_thread;i++)
		thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999;
}

void exit_thread()
{
	int i;
	pthread_t pt;
	pt = pthread_self();
	for (i=0;i<num_of_thread;i++)
	{
		if (pthread_equal(thread_list[i].tid,pt))
//	if (thread_list[i].tid == pt)
		{
			thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999;
			break;
		}
	}
}

void init_thread()
{
	int i;

	thread_list = (PH_Thread*)malloc(num_of_thread * sizeof(PH_Thread) * 2); // temp
	for (i=0;i<num_of_thread;i++)
		thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999; // ignore until new
	pthread_mutex_init(&alloc_mutex,NULL);
}

void clean_thread()
{
	pthread_mutex_destroy(&alloc_mutex);
	free(thread_list);
}

void new_thread()
{
	int i;
	pthread_t pt;
	pt = pthread_self();
	pthread_mutex_lock(&alloc_mutex);
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].free_cnt == 999999999)
		{
			thread_list[i].tid = pt;
			thread_list[i].free_cnt = free_cnt;
			thread_list[i].seg_free_cnt = seg_free_cnt;
			break;
		}
	}
	pthread_mutex_unlock(&alloc_mutex);
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
			thread_list[i].seg_free_cnt = seg_free_cnt;
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
	if (min == 999999999)
		return free_cnt;
	return min;
}

unsigned int min_seg_free_cnt()
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_of_thread;i++)
	{
		if (min > thread_list[i].seg_free_cnt)
			min = thread_list[i].seg_free_cnt;
	}
	if (min == 999999999)
		return seg_free_cnt;
	return min;
}

}
