#include "thread.h"
#include "data.h" //free_cnt
#include "global.h" //num_of_thread

#include <stdlib.h>

#include <stdio.h> //test

#include<unistd.h>

#define FCCT 1000 // free cnt check threshold

//using namespace PH;
namespace PH
{

#define INV9 999999999

PH_Thread* thread_list;

thread_local PH_Thread* my_thread = NULL;
//thread_local unsigned int op_cnt = 0;

extern volatile unsigned int free_cnt[PM_N]; // max
extern volatile unsigned int free_index[PM_N]; // min
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

	printf("op_cnt %d\n",op_cnt);

	for (i=0;i<PM_N;i++)
	{
		local_free_cnt[i] = INV9;
		local_free_index[i] = INV9;
	}
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
#endif
}

void start_split_thread()
{
#ifdef split_thread
int i;
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

	printf("clean thread\n");

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
			{
				thread_list[i].local_free_cnt[j] = free_cnt[j];
				thread_list[i].local_free_index[j] = free_index[j];
			}
			thread_list[i].local_seg_free_cnt = seg_free_cnt;
			
			init_data_local();
			my_thread = &thread_list[i];
			my_thread->ti = i;

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
				{
					thread_list[i].local_free_cnt[j] = free_cnt[j];
					thread_list[i].local_free_index[j] = free_index[j];
				}
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

std::atomic<int> fff=0;
void ff()
{
	printf("lkejfslkjf\n");
}

#define wait_for_slow

void update_free_cnt0()
{
			int i,retry=1,min,in=0;
			while(retry)
			{
				retry=0;
				for (i=0;i<PM_N;i++)
				{
					my_thread->local_free_cnt[i] = free_cnt[i];
					my_thread->local_free_index[i] = free_index[i];
				}
				my_thread->local_seg_free_cnt = seg_free_cnt;
#ifdef wait_for_slow
				for (i=0;i<PM_N;i++)
				{
					min = get_min_free_cnt(i);
					if (min + FREE_QUEUE_LEN/2 < my_thread->local_free_cnt[i])
					{
						retry = 1;
					}
					/*
					printf("in1\n");
					while(min + FREE_QUEUE_LEN/2 < my_thread->local_free_cnt[i])
						min = get_min_free_cnt(i);	
					printf("out1\n");
					*/
					min = get_min_free_index(i);
					if (min + FREE_QUEUE_LEN/2 < my_thread->local_free_index[i])
					{
						retry = 2;
					}
					/*
					printf("in3\n");
					while(min + FREE_QUEUE_LEN/2 < my_thread->local_free_index[i])
						min = get_min_free_index(i);	
					printf("out3\n");
					*/
				}
				int min = min_seg_free_cnt();
				if (min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
				{
					retry = 3;
					/*
				fff++;
				printf("in2\n");
				if (fff == 15)
					ff();
				while(min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
				{
					update_idle();
					min = min_seg_free_cnt();
				}
					printf("out2\n");
					fff--;
					*/
				}
				if (retry)
				{
					update_idle();
					
					if (min + FREE_QUEUE_LEN-100 < my_thread->local_free_cnt[i])
					{
						if (in == 0)
						{
							in = 1;
							printf("in 1\n");
						}	
//						printf("retry1\n");
					}
					if (min + FREE_QUEUE_LEN/-100 < my_thread->local_free_index[i])
					{
						if (in == 0)
						{
							in = 2;
							printf("in 2\n");
						}
//						printf("retry2\n");
					}
					if (min + FREE_SEG_LEN/-100 < my_thread->local_seg_free_cnt)
					{
						if (in == 0)
						{
							in = 3;
							printf("in 3\n");
						}
//						printf("retry3\n");
					}
						
					usleep(1);
				}
#endif
			}
			if (in)
				printf("out %d\n",in);

}

void update_free_cnt()
{
	if (my_thread)
	{
		my_thread->op_cnt++;
		if (my_thread->op_cnt % 128 == 0)
		{
			update_free_cnt0();	
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

unsigned int get_min_free_cnt(int part)
{
	int i;
	unsigned int min=999999999;
//	my_thread->local_free_cnt[part] = free_cnt[part]; // ?????
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].running && min > thread_list[i].local_free_cnt[part]) // ???
			min = thread_list[i].local_free_cnt[part];
	}
	if (min == 999999999)
		return free_cnt[part];
	return min;
}
unsigned int get_min_free_index(int part)
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].running && min > thread_list[i].local_free_index[part]) // ???
			min = thread_list[i].local_free_index[part];
	}
	if (min == 999999999)
		return free_index[part];
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
	for (i=0;i<num_of_thread;i++)
	{
		printf("%d local_seg_free %d\n",i,thread_list[i].local_seg_free_cnt);
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
