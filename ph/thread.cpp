#include "thread.h"
#include "data.h" //free_cnt
#include "global.h" //num_of_thread

#include <stdlib.h>

#include <stdio.h> //test

#define FCCT 1000 // free cnt check threshold

//using namespace PH;
namespace PH
{

#define INV9 999999999

PH_Thread* thread_list;

thread_local PH_Thread* my_thread = NULL;
//thread_local unsigned int op_cnt = 0;

extern volatile unsigned int free_cnt[PM_N];
extern int num_of_thread;

extern volatile unsigned int seg_free_cnt;

//extern pthread_mutex_t alloc_mutex;
//extern std::atomic<int> alloc_lock;
std::atomic<uint8_t> thread_lock;
//pthread_mutex_t m;
/*
inline void thread_run()
{
#ifdef idle_thread
	my_thread->running = 1;
#endif
}
inline void thread_idle()
{
#ifdef idle_thread
	my_thread->running = 0;
#endif
}
*/
/*
#ifdef idle_thread

#define THREAD_RUN my_thread->running=1;
#define THREAD_IDLE my_thread->running=0;

#else

#define THREAD_RUN
#define THREAD_IDLE

#endif
*/
void reset_thread()
{
	int i;
	for (i=0;i<num_of_thread;i++)
	{
		thread_list[i].clean();
//		thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999;
	}
}
/*
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
			thread_list[i].log->clean();
			delete thread_list[i].log;
//			printf("exit thread %d\n",i);
			break;
		}
	}
	my_thread = NULL;
//	op_cnt = 0;

	// have to reset locall alloc free queue
}
*/
void exit_thread()
{
	if (my_thread)
		my_thread->clean();
}
void PH_Thread::clean()
{
	int i;
	for (i=0;i<PM_N;i++)
		local_free_cnt[i] = INV9;
	local_seg_free_cnt = INV9;
//	log->clean();
	clean_thread_local();
}

#ifdef split_thread
pthread_t* split_threads;
int* si;
#endif

void init_thread()
{
	int i;

//	thread_list = (PH_Thread*)malloc(num_of_thread * sizeof(PH_Thread) * 2); // temp
	thread_list = new PH_Thread[num_of_thread+1];
	for (i=0;i<num_of_thread;i++)
	{
//		thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999; // ignore until new
		thread_list[i].init();
#ifdef idle_thread
//	thread_list[i].running=0;
#endif
	}
//	pthread_mutex_init(&alloc_mutex,NULL);
	thread_lock = 0;//
#ifdef split_thread
	init_split();
	split_threads = (pthread_t*)malloc(sizeof(pthread_t)*num_of_split);
	si = (int*)malloc(sizeof(int)*num_of_split);
	for(i=0;i<num_of_split;i++)
	{
		si[i] = i;
		pthread_create(&split_threads[i],NULL,split_work,(void*)&si[i]);
	}
#endif
}

void clean_thread()
{
//	pthread_mutex_destroy(&alloc_mutex);
//	free(thread_list);
#ifdef DOUBLE_LOG
	int i;
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].log)
			thread_list[i].log->clean();
	}
#endif
#ifdef split_thread
	clean_split();

	int i;
	for (i=0;i<num_of_split;i++)
		pthread_join(split_threads[i],NULL);

	free(split_threads);
	free(si);

#endif
	delete[] thread_list;
}

void PH_Thread::init()
{
	op_cnt = 0;

//	log = new LOG();
//	log->init();
#ifdef DOUBLE_LOG
	log = NULL;
#endif

#ifdef idle_thread
	running = 0;
#endif
	int i;
	for (i=0;i<PM_N;i++)
		local_free_cnt[i] = INV9;
	local_seg_free_cnt = INV9;
//	init_data_local(); // this function called by init thread
	// and init data local is about thread local
}

void new_thread()
{
	int i,j;
//	pthread_t pt;
//	pt = pthread_self();
//	pthread_mutex_lock(&alloc_mutex);
	at_lock(thread_lock);	
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].local_free_cnt[0] == INV9)
		{
			// alloc thread
//			thread_list[i].tid = pt;
			for (j=0;j<PM_N;j++)
				thread_list[i].local_free_cnt[j] = free_cnt[j];
			thread_list[i].local_seg_free_cnt = seg_free_cnt;
#ifdef DOUBLE_LOG
			if (thread_list[i].log == NULL)
			{
				thread_list[i].log = new LOG();
				thread_list[i].log->init();
			}
#endif
			init_data_local();
			my_thread = &thread_list[i];

		//	my_thread->log = new LOG();
		//	my_thread->log->init();
//			op_cnt = 0;
			break;
		}
	}
//	pthread_mutex_unlock(&alloc_mutex);
	at_unlock(thread_lock);	
}

#ifdef idle_thread

void update_idle()
{
	int i,j;
//	print_thread_info(); //test
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].local_free_cnt[0] != INV9)
		{
			if (thread_list[i].running == 0)
			{
				for (j=0;j<PM_N;j++)
				thread_list[i].local_free_cnt[j] = free_cnt[j];
				thread_list[i].local_seg_free_cnt = seg_free_cnt;
			}
		}
	}
}

#else
void update_idle()
{
}

#endif

void update_free_cnt()
{
	if (my_thread)
	{
		my_thread->op_cnt++;
		if (my_thread->op_cnt % 128 == 0)
		{
			int i;
			for (i=0;i<PM_N;i++)
				my_thread->local_free_cnt[i] = free_cnt[i];
			my_thread->local_seg_free_cnt = seg_free_cnt;
			
		}
	}
	else
		new_thread();
#if 0
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
#endif
}

unsigned int min_free_cnt(int part)
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].running && min > thread_list[i].local_free_cnt[part]) // ???
			min = thread_list[i].local_free_cnt[part];
	}
	if (min == 999999999)
		return free_cnt[part];
	return min;
}

void print_thread_info()
{
	int i,j;
	for (i=0;i<num_of_thread;i++)
	{
		for (j=0;j<PM_N;j++)
		{
		if (thread_list[i].local_free_cnt[j] != 999999999)
		{
			printf("thread %d part %d / %d\n",i,j,thread_list[i].local_free_cnt[j]);//,thread_list[i].local_seg_free_cnt,thread_list[i].running);
		}
		}
	}

}

unsigned int min_seg_free_cnt()
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_of_thread;i++)
	{
		if (min > thread_list[i].local_seg_free_cnt)
			min = thread_list[i].local_seg_free_cnt;
	}
	if (min == 999999999)
		return seg_free_cnt;
	return min;
}
/*
int check_slow()
{
	if (free_cnt-my_thread->free_cnt > 500)
		return 1;
	return 0;
}
*/
}
