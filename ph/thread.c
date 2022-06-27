#include "thread.h"
#include "data.h" //free_cnt
#include "global.h" //num_of_thread

#include <stdlib.h>

#include <stdio.h> //test

#include <string.h> //log

#define FCCT 1000 // free cnt check threshold

//using namespace PH;
namespace PH
{

PH_Thread* thread_list;

thread_local PH_Thread* my_thread = NULL;
//thread_local unsigned int op_cnt = 0;

extern volatile unsigned int free_cnt;
extern int num_of_thread;

extern volatile unsigned int seg_free_cnt;

//extern pthread_mutex_t alloc_mutex;
//extern std::atomic<int> alloc_lock;
std::atomic<uint8_t> thread_lock;
//pthread_mutex_t m;

//unsigned char* pmem_log_addr;
//unsigned char* dram_log_addr;
/*
void init_log(int i)
{
	int len,is,file_len;
	file_len = strlen(log_file);
	log_file[file_len+2] = 0;
		log_file[file_len] = i/10;
		log_file[file_len+1] = i%10;
		thread_list[i].pmem_log_addr = (unsigned char*)mmap(log_file,log_size,PMEM_FILE_CREATE,0777,&len,&is);
		thread_list[i].dram_log_addr = (unsigned char*)mmap(NULL,log_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);

}

void clean_log(int i)
{
	munmap(thread_list[i].dram_log_addr,log_size);
	pmem_unmap(thread_list[i].pmem_log_addr,log_size);
}
*/


void reset_thread()
{
	int i;
	for (i=0;i<num_of_thread;i++)
	{
		thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999;
		if (thread_list[i].log != NULL)
		{
			thread_list[i].log->clean_log();
			delete thread_list[i].log;
		}
	}
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
//			clean_log(i);
			thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999;
			thread_list[i].log->clean_log();
			delete thread_list[i].log;
			break;
		}
	}
	my_thread = NULL;
//	op_cnt = 0;
}

void init_thread()
{
	int i;

	thread_list = (PH_Thread*)malloc(num_of_thread * sizeof(PH_Thread) * 2); // temp
	for (i=0;i<num_of_thread;i++)
	{
		thread_list[i].seg_free_cnt = thread_list[i].free_cnt = 999999999; // ignore until new
		thread_list[i].log = NULL;
	}
//	pthread_mutex_init(&alloc_mutex,NULL);
	thread_lock = 0;//

}

void clean_thread()
{
//	pthread_mutex_destroy(&alloc_mutex);
	free(thread_list);
}

void new_thread()
{
	int i;
	pthread_t pt;
	pt = pthread_self();
//	pthread_mutex_lock(&alloc_mutex);
	at_lock(thread_lock);	
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].free_cnt == 999999999)
		{
			thread_list[i].tid = pt;
			thread_list[i].free_cnt = free_cnt;
			thread_list[i].seg_free_cnt = seg_free_cnt;
			my_thread = &thread_list[i];
			if (use_log)
			{
				thread_list[i].log = new LOG();
				thread_list[i].log->set_num(i+1);
				thread_list[i].log->init_log();
			}

//			init_log(i);

//			op_cnt = 0;
			break;
		}
	}
//	pthread_mutex_unlock(&alloc_mutex);
	at_unlock(thread_lock);	
}

void update_free_cnt()
{
	if (my_thread)
	{
//		op_cnt++;
//		if (op_cnt & 1024)
//		{
			my_thread->free_cnt = free_cnt;
			my_thread->seg_free_cnt = seg_free_cnt;
//		}
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

void print_thread_info()
{
	int i;
	for (i=0;i<num_of_thread;i++)
	{
		if (thread_list[i].free_cnt != 999999999)
		{
			printf("thread %d %d\n",i,thread_list[i].free_cnt);
		}
	}

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

int check_slow()
{
	if (free_cnt-my_thread->free_cnt > 500)
		return 1;
	return 0;
}

}
