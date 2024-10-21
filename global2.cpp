#include <stdio.h>
#include <x86intrin.h> //fence


#include "global2.h"
#include "log.h"
#include "thread2.h"
#include "cceh.h"
#include "lock.h"
#include "skiplist.h"
#include "data2.h"
#include "large.h"

namespace PH
{

extern size_t HARD_EVICT_SPACE;
extern size_t SOFT_EVICT_SPACE;

extern PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];
extern PH_Evict_Thread evict_thread_list[EVICT_THREAD_MAX];

extern Skiplist* skiplist;
extern PH_List* list;
extern LargeAlloc* largeAlloc;

extern NodeAllocator* nodeAllocator;

thread_local PH_Query_Thread* my_query_thread = NULL;
thread_local PH_Evict_Thread* my_evict_thread = NULL;
thread_local PH_Thread* my_thread;

size_t TOTAL_DATA_SIZE;



// should be private....
int num_thread;
int num_pmem;
int num_log;
int num_query_thread;
int num_evict_thread;

extern int log_max;

	std::atomic<uint64_t> global_seq_num[COUNTER_MAX];

	void recover_counter(uint64_t key,uint64_t value) // single thread
	{
		if (global_seq_num[key%COUNTER_MAX] < value)
			global_seq_num[key%COUNTER_MAX] = value;
	}

	//check
	std::atomic<uint64_t> warm_to_warm_sum;
	std::atomic<uint64_t> warm_log_write_sum;
	std::atomic<uint64_t> log_write_sum;
	std::atomic<uint64_t> hot_to_warm_sum;
	std::atomic<uint64_t> warm_to_cold_sum;
	std::atomic<uint64_t> direct_to_cold_sum;
	std::atomic<uint64_t> hot_to_hot_sum;
	std::atomic<uint64_t> hot_to_cold_sum;

	std::atomic<uint64_t> soft_htw_sum;
	std::atomic<uint64_t> hard_htw_sum;

	std::atomic<uint64_t> htw_cnt_sum;
	std::atomic<uint64_t> wtc_cnt_sum;

	std::atomic<uint64_t> htw_time_sum;
	std::atomic<uint64_t> wtc_time_sum;

	std::atomic<uint64_t> dtc_time_sum;

	std::atomic<uint64_t> reduce_group_sum;
	std::atomic<uint64_t> list_merge_sum;

	std::atomic<uint64_t> data_sum_sum,ld_sum_sum,ld_cnt_sum;

#ifdef WARM_STAT
	std::atomic<uint64_t> warm_hit_sum;
	std::atomic<uint64_t> warm_miss_sum;
	std::atomic<uint64_t> warm_no_sum;
#endif


void PH_Interface::global_reset_test()
{
	int i;
	for (i=0;i<QUERY_THREAD_MAX;i++)
		query_thread_list[i].reset_test();
	for (i=0;i<EVICT_THREAD_MAX;i++)
		evict_thread_list[i].reset_test();
	for (i=0;i<log_max;i++)
		doubleLogList[i].block_cnt = 0;

	warm_log_write_sum = log_write_sum = hot_to_warm_sum = warm_to_cold_sum = direct_to_cold_sum = hot_to_hot_sum = hot_to_cold_sum = 0;
	warm_to_warm_sum = 0;
	soft_htw_sum = hard_htw_sum = 0;
	dtc_time_sum = htw_time_sum = wtc_time_sum = 0;
	htw_cnt_sum = wtc_cnt_sum = 0;

	reduce_group_sum = 0;
	list_merge_sum = 0;

#ifdef WARM_STAT
	warm_hit_sum = warm_miss_sum = warm_no_sum = 0;
#endif

}

void PH_Interface::new_query_thread()
{
	if (my_query_thread)
		printf("new query thread error\n");
	int i;
	for (i=0;i<QUERY_THREAD_MAX;i++)
	{
		while(query_thread_list[i].lock == 0)
		{
			if (try_at_lock2(query_thread_list[i].lock))
				my_query_thread = &query_thread_list[i];
		}
		if (my_query_thread)
			break;
	}

	// find new DoubleLog
	my_thread = my_query_thread;
//	my_query_thread->thread_id = i;
	my_query_thread->init();
}
void PH_Interface::clean_query_thread()
{
	my_query_thread->clean();

//	at_unlock2(my_query_thread->lock); // unlock twice??
	my_query_thread = NULL;
	my_thread = NULL;
}

void PH_Interface::new_evict_thread()
{
	if (my_evict_thread)
		printf("new evict thread error\n");
	int i;
	for (i=0;i<EVICT_THREAD_MAX;i++)
	{
		while(evict_thread_list[i].lock == 0)
		{
			if (try_at_lock2(evict_thread_list[i].lock))
				my_evict_thread = &evict_thread_list[i];
		}
		if (my_evict_thread)
			break;
	}

	my_thread = my_evict_thread;
//	my_evict_thread->thread_id = i;
//	my_evict_thread->init();
}

void PH_Interface::clean_evict_thread()
{
	my_evict_thread->clean();

	at_unlock2(my_evict_thread->lock);
	my_evict_thread=NULL;
	my_thread = NULL;
}

void PH_Interface::global_init(size_t max_data_size,int n_t,int n_p,int n_e,int recover)
{
	printf("global init MDS %lf thread %d pmem %d evict %d\n",double(max_data_size)/1024/1024/1024,n_t,n_p,n_e);

	if (recover)
		printf("recover\n");

	int i;
	for (i=0;i<COUNTER_MAX;i++)
		global_seq_num[i] = 1;

	TOTAL_DATA_SIZE = max_data_size;

	num_query_thread = n_t;
	num_pmem = n_p;

//	num_log = (n_t-1)/n_p+1; // num_log per pmem
	num_log = n_t;

	num_evict_thread = n_e;
	num_thread = num_query_thread + num_evict_thread;

//	log_max = num_pmem*num_log*2;

	init_log(num_pmem,num_log);

	init_cceh();

	nodeAllocator = new NodeAllocator;
	nodeAllocator->init();

//	init_evict();

// list fisrt
// alloc 0 list start
// alloc 1 list end
// alloc 2 skiplist start
// alloc 3 skiplist end
	list = new PH_List;
	if (recover)
		list->recover_init();
	else
		list->init();
	skiplist = new Skiplist;
	if (recover)
		skiplist->recover_init(TOTAL_DATA_SIZE);
	else
		skiplist->init(TOTAL_DATA_SIZE/1); // test
//	skiplist->init(TOTAL_DATA_SIZE/10); // warm 1/10...

	largeAlloc = new LargeAlloc();

	if (recover)
	{
		printf("recover start\n");

		struct timespec ts1,ts2;
		_mm_mfence();
		clock_gettime(CLOCK_MONOTONIC,&ts1);
		_mm_mfence();

		new_query_thread();

		list->recover();
		printf("cold list\n");
		skiplist->recover();
		printf("warm list\n");
		recover_log();
		printf("hot log\n");

		nodeAllocator->collect_free_node();

		clean_query_thread(); // clean recover thread

		printf("recover end\n");

		_mm_mfence();
		clock_gettime(CLOCK_MONOTONIC,&ts2);
		_mm_mfence();

//		skiplist->traverse_test();

		double recover_time;
		recover_time = (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
		printf("recover_time %lf\n",recover_time/1000000000);
	}

	global_reset_test();

	init_threads();

	printf("global init end\n");
}

void PH_Interface::init_threads()
{
	int i;
	for (i=0;i<num_query_thread;i++)
	{
		query_thread_list[i].thread_id = i;
//		query_thread_list[i].init();
	}
	for (i=0;i<num_evict_thread;i++)
	{
		evict_thread_list[i].thread_id = i;
		evict_thread_list[i].init();
	}
}

void PH_Interface::exit_threads()
{
	int i;
	for (i=0;i<num_query_thread;i++)
	{
		if (query_thread_list[i].lock == 0)
		{
//			printf("xxxx\n");
			continue;
		}
//		query_thread_list[i].exit = 1;
//		query_thread_list[i].clean();
		while(query_thread_list[i].run);
	}
//	printf("query clean\n");
	for (i=0;i<num_evict_thread;i++)
	{
		if (evict_thread_list[i].lock == 0)
		{
			printf("eceptioehtn\n");
			continue;
		}
		evict_thread_list[i].exit = 1;
//		evict_thread_list[i].clean();
	}
	_mm_mfence();
	// query join???
//	printf("start evict join\n");
	for (i=0;i<num_evict_thread;i++) // thread list and pthread are not synced!!!!
		pthread_join(evict_pthreads[i],NULL);
//	printf("joined\n");
	for (i=0;i<num_evict_thread;i++)
	{
		if (evict_thread_list[i].lock == 0)
			continue;
		{
//			evict_thread_list[i].exit = 0;
			evict_thread_list[i].clean();
		}
	}
}

void PH_Interface::global_clean()
{
	printf("global clean\n");

	exit_threads();
printf("cc\n");
	skiplist->clean();
	delete skiplist;
	list->clean();
	delete list;
printf("ccc\n");
	nodeAllocator->clean();

	delete largeAlloc;

	int i;
	size_t hbs=0;
	for (i=0;i<log_max;i++)
		hbs+=doubleLogList[i].block_cnt;
	
	printf("hot block cnt %lu\n",hbs);

	clean_cceh();
	clean_log();

	//check

#ifdef WARM_STAT
	printf("addr2 hit %ld miss %ld no %ld\n",warm_hit_sum.load(),warm_miss_sum.load(),warm_no_sum.load());
#endif

	printf("log_write %lu warm_log_write %lu hot_to_warm %lu warm_to cold %lu\n",log_write_sum.load(),warm_log_write_sum.load(),hot_to_warm_sum.load(),warm_to_cold_sum.load());
	printf("warm_to_warm %lu\n",warm_to_warm_sum.load());
	printf("direct to cold %lu\n",direct_to_cold_sum.load());
	printf("hot to hot %lu\n",hot_to_hot_sum.load());
	printf("hot to cold %lu\n",hot_to_cold_sum.load());

	printf("soft htw %lu hard htw %lu\n",soft_htw_sum.load(),hard_htw_sum.load());

	if (soft_htw_sum + hard_htw_sum > 0)
		printf("avg evict %lf\n",double(hot_to_warm_sum.load()+warm_to_warm_sum.load())/(soft_htw_sum.load()+hard_htw_sum.load()));

	if (htw_cnt_sum > 0 && wtc_cnt_sum > 0)
		printf("htw time avg %lu wtc time avg %lu\n",htw_time_sum/htw_cnt_sum,wtc_time_sum/wtc_cnt_sum);
	if (direct_to_cold_sum > 0)
		printf("dtc time avg %lu\n",dtc_time_sum/direct_to_cold_sum);

	if (hot_to_warm_sum > 0)
		printf("hot_to_warm avg %lu\n",htw_time_sum/hot_to_warm_sum);
	if (warm_to_cold_sum > 0)
		printf("warm_to_cold avg %lu\n",wtc_time_sum/warm_to_cold_sum);

	printf("reducd group sum %lu\n",reduce_group_sum.load());
	printf("list merge sum %lu\n",list_merge_sum.load());

	printf("data sum sum %lfGB large sum %lu large cnt %lu\n",double(data_sum_sum)/1024/1024/1024,ld_sum_sum.load(),ld_cnt_sum.load());
}

int PH_Interface::insert_op(uint64_t key,uint64_t value_size, unsigned char* value)
{
	if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->insert_op(key,value_size, value);
}
int PH_Interface::read_op(uint64_t key,unsigned char* buf)
{
		if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->read_op(key,buf,NULL);
}
int PH_Interface::read_op(uint64_t key,std::string *value)
{
	if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->read_op(key,NULL,value);
}

int PH_Interface::delete_op(uint64_t key)
{
		if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->delete_op(key);
}
//int PH_Interface::scan_op(uint64_t start_key,uint64_t end_key)
int PH_Interface::scan_op(uint64_t start_key,uint64_t length)
{
	if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->scan_op(start_key,length);
}
int PH_Interface::next_op(unsigned char* buf)
{
	if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->next_op(buf);
}

int PH_Interface::end_op()
{
	if (my_query_thread)
	{
		clean_query_thread();
//		my_query_thread->clean();
//		my_query_thread = NULL;
	}
	return 0;
}

void PH_Interface::run_evict_direct()
{
	if (my_evict_thread == NULL)
		new_evict_thread();
	my_evict_thread->evict_loop();
//	clean_evict_thread();
}

//void PH_Interface::*run_evict(void* p)
void *run_evict(void* p)
{
	PH_Interface* phi = (PH_Interface*)p;
	phi->run_evict_direct();
	return NULL;
}

void PH_Interface::run_evict_thread()
{
	printf("run evict thread\n");
	int i;
	for (i=0;i<num_evict_thread;i++)
	{
		pthread_create(&evict_pthreads[i],NULL,run_evict,this);
	}
}

}
