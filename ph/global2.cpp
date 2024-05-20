#include <stdio.h>

#include "global2.h"
#include "log.h"
#include "thread2.h"
#include "cceh.h"
#include "lock.h"
#include "skiplist.h"
#include "data2.h"

namespace PH
{

extern PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];
extern PH_Evict_Thread evict_thread_list[EVICT_THREAD_MAX];

extern Skiplist* skiplist;
extern PH_List* list;

extern NodeAllocator* nodeAllocator;

thread_local PH_Query_Thread* my_query_thread = NULL;
thread_local PH_Evict_Thread* my_evict_thread = NULL;

// should be private....
int num_thread;
int num_pmem;
int num_log;
int num_query_thread;
int num_evict_thread;

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

	my_query_thread->init();
}
void PH_Interface::clean_query_thread()
{
	my_query_thread->clean();

	at_unlock2(my_query_thread->lock);
	my_query_thread = NULL;
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

	my_evict_thread->init();
}

void PH_Interface::clean_evict_thread()
{
	my_evict_thread->clean();

	at_unlock2(my_evict_thread->lock);
	my_evict_thread=NULL;
}

void PH_Interface::global_init(int n_t,int n_p,int n_e)
{
	printf("global init\n");
	num_query_thread = n_t;
	num_pmem = n_p;
	num_log = (n_t-1)/n_p+1;
	num_evict_thread = n_e;
	num_thread = num_query_thread + num_evict_thread;

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
	list->init();
	skiplist = new Skiplist;
	skiplist->init();

}
void PH_Interface::global_clean()
{
	printf("global clean\n");

	list->clean();
	skiplist->clean();
	delete list;
	delete skiplist;

	nodeAllocator->clean();

	clean_cceh();
	clean_log();
}

int PH_Interface::insert_op(uint64_t key,unsigned char* value)
{
	if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->insert_op(key,value);
}
int PH_Interface::read_op(uint64_t key,unsigned char* buf)
{
		if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->read_op(key,buf);
}
int PH_Interface::delete_op(uint64_t key)
{
		if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->delete_op(key);
}
int PH_Interface::scan_op(uint64_t start_key,uint64_t end_key)
{
		if (my_query_thread == NULL)
		new_query_thread();
	return my_query_thread->scan_op(start_key,end_key);
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
		my_query_thread->clean();
	return 0;
}

}
