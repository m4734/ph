#include <stdio.h>
#include <cstring>
#include <x86intrin.h> //fence

#include "thread2.h"
#include "log.h"
#include "lock.h"
#include "cceh.h"

namespace PH
{

extern int num_thread;
extern int log_max;
extern DoubleLog* doubleLogList;
extern volatile unsigned int seg_free_cnt;
extern CCEH* hash_index;

PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];


//---------------------------------------------- seg

unsigned int min_seg_free_cnt()
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_thread;i++)
	{
		if (query_thread_list[i].run && min > query_thread_list[i].local_seg_free_cnt)
			min = query_thread_list[i].local_seg_free_cnt;
	}
	if (min == 999999999)
		return seg_free_cnt;
	return min;
}

void PH_Query_Thread::update_free_cnt()
{
		op_cnt++;
		if (op_cnt % 128 == 0)
		{
			local_seg_free_cnt = seg_free_cnt;
#ifdef wait_for_slow
			int min = min_seg_free_cnt();
			if (min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
			{
				printf("in2\n");
				while(min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
				{
					update_idle();
					min = min_seg_free_cnt();
				}
					printf("out2\n");
			}
#endif
			
		}
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

//-------------------------------------------------


void PH_Query_Thread::init()
{

	int i;

	my_log = 0;

	for (i=0;i<log_max;i++)
	{
		while (doubleLogList[i].use == 0)
		{
//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
			if (try_at_lock2(doubleLogList[i].use))
				my_log = &doubleLogList[i];
		}
		if (my_log)
			break;
	}

	if (my_log == 0)
		printf("new query thread no log!!!\n");
	else
		printf("log allocated\n");

//	local_seg_free_cnt = min_seg_free_cnt();
//	local_seg_free_cnt = INV9;
	local_seg_free_cnt = seg_free_cnt;

	read_lock = 0;

	run = 1;
}

void PH_Query_Thread::clean()
{
	my_log->use = 0;
	my_log = NULL;

	run = 0;
}

#define INDEX

int PH_Query_Thread::insert_op(uint64_t key,unsigned char* value)
{
	update_free_cnt();

	unsigned char* new_addr;
	unsigned char* old_addr;
	unsigned char* pmem_head_p = my_log->get_pmem_head_p();
	unsigned char* dram_head_p = my_log->get_dram_head_p();

#if 0 
	unsigned char* checksum_i = (unsigned char*)&ble;
	int i,cnt=0;
	for (i=0;i<8+8+VALUE_SIZE;i++)
	{
		if (*checksum_i == 0)
			cnt++;
	}
	if (cnt == -1)
		printf("xxx\n");
#endif
	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
//	my_log->insert_log(&ble);

	my_log->ready_log();
	my_log->insert_pmem_log(key,value);

//	my_log->ready_log();
//	head_p = my_log->get_head_p();
/*
	BaseLogEntry *ble = (BaseLogEntry*)head_p;
	ble->key = key;
	memcpy(ble->value,value,VALUE_SIZE);
*/


	// 2 lock index ------------------------------------- lock from here

#ifdef INDEX
	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;
	kvp_p = hash_index->insert(key,&seg_lock,read_lock);
//	kvp_p = hash_index->insert_with_fail(key,&seg_lock,read_lock);
	old_addr = (unsigned char*)kvp_p->value;
#endif

	// 3 get and write new version <- persist

	uint64_t old_version,new_version;
#ifdef INDEX
	old_version = kvp_p->version;
	new_version = old_version+1;
	remove_loc_mask(new_version);
	set_prev_loc(new_version,old_version);
	set_loc_hot(new_version);
#endif
	// 4 add dram list
#ifdef USE_DRAM_CACHE
	new_addr = dram_head_p;
	my_log->insert_dram_log(new_version,key,value);
#else
	new_addr = pmem_head_p; // PMEM
#endif

	//--------------------------------------------------- make version
//if (kvp_p)
	my_log->write_version(new_version);

	// 6 update index
#ifdef INDEX
	
//	kvp_p->value = (uint64_t)my_log->get_head_p();
	kvp_p->value = (uint64_t)new_addr;
	kvp_p->version = new_version;
	_mm_sfence(); // value first!
	if (kvp_p->key != key) // not so good
	{
		kvp_p->key = key;
		_mm_sfence();
	}
	
#endif

	// 7 unlock index -------------------------------------- lock to here
#ifdef INDEX
//	if (kvp_p)
	hash_index->unlock_entry2(seg_lock,read_lock);
#endif
	// 5 add to key list if new key

	// 8 remove old dram list
#ifdef USE_DRAM_CACHE
	if (is_loc_hot(old_version))
	{
//		printf("old\n");
//		dl = (Dram_List*)old_addr; 
//		my_log->remove_dram_list(dl);
		set_invalid((uint64_t*)old_addr);
	}
#endif
	// 9 check GC

	return 0;
}
int PH_Query_Thread::read_op(uint64_t key,unsigned char* buf)
{
	update_free_cnt();

	uint64_t ret;
	hash_index->read(key,&ret);
	if (ret == 0)
		return -1;
	memcpy(buf,((unsigned char*)ret)+VERSION_SIZE+KEY_SIZE,VALUE_SIZE);

	return 0;
}
int PH_Query_Thread::delete_op(uint64_t key)
{
	update_free_cnt();

	return 0;
}
int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t end_key)
{
	update_free_cnt();

	return 0;
}
int PH_Query_Thread::next_op(unsigned char* buf)
{
	update_free_cnt();

	return 0;
}


}
