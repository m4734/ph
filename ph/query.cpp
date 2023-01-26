#include "query.h"
#include "hash.h"
#include "data.h"
#include "thread.h"

#ifndef NULL
#define NULL 0
#endif

#include <stdio.h> //test
#include <stdlib.h>
#include <string.h>

#include <x86intrin.h>

//#include <mutex> // test

//#define print 0
#define print 0

//using namespace PH;

//#define LOCK_FAIL_STOP


namespace PH
{

#ifdef idle_thread

extern thread_local PH_Thread* my_thread;
#define THREAD_RUN my_thread->running=1;
#define THREAD_IDLE my_thread->running=0;

#else

#define THREAD_RUN
#define THREAD_IDLE

#endif



	extern uint64_t qtt1,qtt2,qtt3,qtt4,qtt5,qtt6,qtt7,qtt8;
void tf()
{
	printf("test function\n");
}
void print_query(Query* query)
{
	printf("print query\n");
	int i;
	printf("key %d ",query->key_len);
	for (i=0;i<8;i++)
		printf("[%d]",(int)query->key_p[i]);
	printf("\n");
	printf("value %d ",query->value_len);
	for (i=0;i<query->value_len;i++)
		printf("[%d]",(int)query->value_p[i]);
	printf("\n");
}
/*
void reset_query(Query* query)
{
	query->length = 0;
	query->op = 0;
	query->key_p = NULL;
	query->value_p = NULL;
	query->key_len = query->value_len=0;//query->offset = 0;
	query->cur = 0;
//	query->node = NULL:
//	query->offset = TAIL_OFFSET; // NOT HERE!!!
}
*/
void init_query(Query* query)
{
	query->node_data = NULL;
//	query->node_size = NULL;
	query->scan_offset = TAIL_OFFSET_u.no_32;
//	query->scan_lock = 0;	
}

#define LEN_VALID_BIT 1<<15

int lookup_query(unsigned char* &key_p, unsigned char* &result_p,int* result_len_p)
{
//	return 0;
#ifdef qtt
	timespec ts1,ts2,ts3,ts4;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
//		unsigned char* kv_p;
		ValueEntry ve;
//		int value_len;
//		const int kls = PH_KEY_SIZE+PH_LEN_SIZE;//key_size+len_size;
//		unsigned int offset;
		update_free_cnt();
	THREAD_RUN
#ifdef qtt
//		clock_gettime(CLOCK_MONOTONIC,&ts3);
//		_mm_mfence();
#endif

#ifdef read_lock
		while(1) // lock version
		{
#endif
		ve = find_point_entry(key_p); // don't create
#ifdef qtt
		_mm_mfence();
		clock_gettime(CLOCK_MONOTONIC,&ts4);
//		qtt6+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
		qtt6+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;
	
#endif
#ifdef qtt
		clock_gettime(CLOCK_MONOTONIC,&ts3);
		_mm_mfence();
#endif
//		return 0;

		uint16_t len = get_length_from_ve(ve);

		if (ve.node_offset == INIT_OFFSET || ve.kv_offset == 0 || (len & INV_BIT == 0))
		{
//			result_p = empty;
			memcpy(result_p,empty,empty_len);			
			*result_len_p = empty_len;
//			*result_len_p = 0;
//thread_idle();
			printf("not found?\n");
			THREAD_IDLE
//		update_free_cnt();

			return 0; // fail
		}
//		while(1)
//		{
//			if (kv_p == NULL) // deleted
//				break;
			// lock version
//			if (print)
//			printf("kv_p %p\n",kv_p);	
//			offset = data_point_to_offset(kv_p);
#ifdef read_lock
//			if (inc_ref(offset,1)) // split state ok
			if (inc_ref(ve.node_offset))
			{
#endif
				// it will not happen
 /*
				if (kv_p != (unsigned char*)entry->kv_p) // recycled?
				{
//					dec_ref(offset);
					continue;
				}
				*/
//				query->ref_offset = offset; // lock ref

//				print_kv(kv_p);	
//				*result_len_p = *((uint16_t*)(kv_p));

				*result_len_p = len-INV_BIT;

// len from PMEM
//				*result_len_p = *((uint16_t*)((unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset));

// len from hash entry		
//				*result_len_p = ve.len;				



//				*result_len_p = value_size;				
#if 0
				if ((*result_len_p & (1 << 15)) != 0) // deleted // it doesn't work now
				{
//					dec_ref(offset);
		memcpy(result_p,empty,empty_len);		
		*result_len_p = empty_len;
		*result_len_p = 0;		
		return 0;

//					break;
				}
#endif
//				*result = kv_p+key_size+len_size;
//				memcpy(result_p,kv_p+key_size+len_size,value_size);
				memcpy(result_p,(unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset+PH_LTK_SIZE,*result_len_p);
#ifdef read_lock
				dec_ref(ve.node_offset);
				break;
			}
		}
#endif
//				memcpy(result_p,(unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset+kls,200);
//				memcpy(result_p,kv_p+key_size+len_size,200);
			
//				s_unlock(offset); // it will be released after result
//				dec_ref(offset);				
//				break;
#ifdef qtt
		_mm_mfence();
		clock_gettime(CLOCK_MONOTONIC,&ts4);
		qtt7+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
		qtt8+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;

#endif
//thread_idle();
		THREAD_IDLE
//		update_free_cnt();

				return 1;//ok
//			}
			


//			if (kv_p == (unsigned char*)entry->kv_p)
//				break;
//		}
		//deleted
//		*result_p = empty;
/*		
		memcpy(result_p,empty,empty_len);		
		*result_len_p = empty_len;
//		*result_len_p = 0;		
		return 0;
		*/
}

#define INV_MASK 0x7fff

int lookup_query(unsigned char* &key_p, std::string *value)
{
//	return 0;
//	thread_run();
		ValueEntry ve;
//		const int kls = PH_KEY_SIZE+PH_LEN_SIZE;//key_size+len_size;
		update_free_cnt();
	THREAD_RUN
		ve = find_point_entry(key_p); // don't create
		if (ve.node_offset == INIT_OFFSET || ve.kv_offset == 0)
		{
			value->assign((char*)empty,empty_len);
//			thread_idle();
			THREAD_IDLE
//		update_free_cnt();
				printf("not found0\n");

			return 0; // failed
		}
#if 1

//		while(1)
		{
//			if (inc_ref(ve.node_offset))
//			{
				unsigned char* kv_p = (unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset;
//				uint16_t len = *((uint16_t*)(kv_p));// ve len disappear??
				uint16_t len = get_length_from_ve(ve);
#if 0
				if (len & INV_BIT) // moved during query?
				{
				value->assign((char*)kv_p+PH_LTK_SIZE,len-INV_BIT);
				break;
				}
				else
				{
					continue;
			value->assign((char*)empty,empty_len);
			printf("not found\n");
				}
#endif

				len&=INV_MASK;
#if 0
				if (len != 100)
					printf("error1\n");
				uint64_t k1,k2;
				k1 = *((uint64_t*)key_p);
				k2 = *((uint64_t*)(kv_p+PH_LEN_SIZE+PH_TS_SIZE));
				if (k1 != k2)
					printf("error1\n");

#endif
				value->assign((char*)kv_p+PH_LTK_SIZE,len);

//				dec_ref(ve.node_offset);
//				break;
//			}
//			ve = find_point_entry(key_p);
		}

#else
		value->assign((char*)offset_to_node_data(ve.node_offset)+ve.kv_offset+kls,ve.len);
#endif
//		thread_idle();
		THREAD_IDLE
//		update_free_cnt();

		return 1;//ok
}


// not now!!!
int delete_query(unsigned char* key_p)
{
return 0;
#if 0
	update_free_cnt();

//		point_hash_entry* entry;
		unsigned char* kv_p;
		ValueEntry ve;		
		unsigned int offset;
//		entry = find_or_insert_point_entry(key_p,0); // don't create
		while(1)
		{

//			kv_p = point_hash.find(key_p);
//			kv_p = (unsigned char*)entry->
			ve = find_point_entry(key_p);
			if (ve.node_offset == INIT_OFFSET) // deleted!!!
				return 0;
//			offset = data_point_to_offset(kv_p);
//			if (*((uint64_t*)query->key_p) != *((uint64_t*)kv_p)) // instead CAS
//				continue;
#ifdef split_thread
			if (inc_ref(ve.node_offset) || try_split(ve.node_offset)) //init state ok
#else
				if (inc_ref(ve.node_offset))
#endif
			{
				kv_p = (unsigned char*)offset_to_node_data(ve.node_offset) + ve.kv_offset;
				if (*((uint64_t*)kv_p) != *((uint64_t*)key_p)) // key is different -  it is moved - can it be happen after node lock?
				{
					dec_ref(ve.node_offset);
					continue;
				}
				/*
				if (kv_p != (unsigned char*)entry->kv_p)
				{
					dec_ref(offset);
					continue;
				}
				*/
//				query->offset = offset; // restore ref cnt

//				soft_lock(offset);
//				entry->kv_p = NULL; // what should be first?
				delete_kv(kv_p);
//				invalidate_kv(ve.node_offset,ve.kv_offset,ve.len);
				invalidate_kv2(ve);				
				remove_point_entry(key_p);
//				point_hash.remove(key_p); // hash twice??
//				hard_unlock(offset);

//				e_unlock(offset);
				dec_ref(ve.node_offset);				
			/*	
				*result = empty;
				*result_len = empty_len;
*/
//				break;
				return 0;				
			}
		}
#endif
}

	// insert
	// find point entry
	// calc node and e lock or fail
	// find range entry and e lock
	// insert kv
	// fail and split
	// delete old?
	// e unlock

#define keep_lock

void insert_query(unsigned char* &key_p, unsigned char* &value_p)
{
	insert_query(key_p,value_p,value_size);
}

// find node
// find location
// do cas

#define INV0 0xffffffffffffffff

extern int time_check;


void insert_query(unsigned char* &key_p, unsigned char* &value_p,int &value_len)
{





	unsigned char* kv_p;

	ValueEntry_u ve_u;
	std::atomic<uint64_t>* v64_p;
	void* unlock;

	int continue_len;
	continue_len = 0;
	int rv;

	ValueEntry rve,ove;
	uint64_t ove64,rve64;

	ValueEntry_u old_ve_u;
	Node_offset locked_offset;
	unsigned char* new_kv_p;

	Node_offset start_offset;
#ifdef qtt
	timespec ts1,ts2,ts3,ts4,ts5,ts6;
if (time_check)
{
//	clock_gettime(CLOCK_MONOTONIC,&ts1);
//_mm_mfence();
}
#endif
	update_free_cnt();

	THREAD_RUN

	while(1) // offset can be changed when retry
	{

//		if (*(uint64_t*)key_p == 0xd762346c3cc7f756)
//			printf("nfefsofudn!!\n");


#ifdef qtt
		if (time_check)
		{
		clock_gettime(CLOCK_MONOTONIC,&ts3);
		_mm_mfence();
		}
#endif

#if 0
		ve_u.ve.node_offset = {0,4};
	insert_kv2(ve_u.ve.node_offset,key_p,value_p,value_len);
	break;
#endif

//		ve_u.ve.node_offset = INIT_OFFSET; //init - not found yet
#ifdef keep_lock		
	v64_p = find_or_insert_point_entry(key_p,unlock);
//	old_ve_u.ve_64 = *v64_p;	
	ve_u.ve_64 = *v64_p;
#else		
	// it will not be compatible anymore
//		ve_u.ve = find_point_entry(key_p);		
#endif

		if (ve_u.ve_64 != INV0) // found the record
		{
		if (time_check == 0)
		{
			time_check = 1;
			printf("time check\n");

#ifdef qtt
		clock_gettime(CLOCK_MONOTONIC,&ts3);
		_mm_mfence();

#endif
		}
//			if (get_start_offset(ve_u.ve.node_offset) == INIT_OFFSET)
//				printf("inini\n");
			ve_u.ve.node_offset = get_start_offset(ve_u.ve.node_offset);
//			start_offset = get_start_offset(ve_u.ve.node_offset);
//			ve_u.ve.node_offset = start_offset;//??
		}
		else
		{

			if ((ve_u.ve.node_offset = find_range_entry2(key_p,&continue_len)) == INIT_OFFSET) // it should be split
			{
				unlock_entry(unlock); // escape!!! will continue
				continue;
			}

		}

#if 0
		while(old_ve_u.ve.node_offset != INIT_OFFSET)// found existing record	
		{
			start_offset = get_start_offset(old_ve_u.ve.node_offset);
#ifdef split_thread
			if (inc_ref(start_offset) || try_split(start_offset))
#else
//				if (inc_ref(start_offset))
//				if (check_ref(start_offset)) // split
#endif
			{
				ve_u.ve.node_offset = start_offset;
					break;
			}


			//else not found and try again ???? it is impossble now
#ifdef keep_lock			
			unlock_entry(unlock);
				v64_p = find_or_insert_point_entry(key_p,&unlock);
				old_ve_u.ve_64 = *v64_p;	
#else
				ve_u.ve = find_point_entry(key_p);			
#endif
		}
//		if (offset == 0) // the key doesn't exist
//		if (ve.node_offset == 0)		
		if (ve_u.ve.node_offset == INIT_OFFSET)// key doens't exist
//		if (kv_p == NULL)		
		{
//			while(1)
//			{
				if ((ve_u.ve.node_offset = find_range_entry2(key_p,&continue_len)) == INIT_OFFSET) // it should be split
//				if (range_entry == NULL) // spliting...
				{
//
//					printf("---------------split collision\n");
					unlock_entry(unlock); // escape!!! will continue

//					update_free_cnt(); // from  seg list it could be old cache


//					ec++;

					continue;
				}
//			}
				/*
#ifdef split_thread
				if (inc_ref(ve_u.ve.node_offset) || try_split(ve_u.ve.node_offset))
#else
					if(inc_ref(ve_u.ve.node_offset))
#endif
*/
				{
					old_ve_u.ve.node_offset = INIT_OFFSET; // there is no old record
				}
				/*
				else
				{
					unlock_entry(unlock); // escape!!! will be continued in next loop

					continue; // failed try again
				}
				*/

//			}
		}
//		else
//			ve = *vep;
		//e locked
//		if ((kv_p = insert_kv(offset,query->key_p,query->value_p,query->value_len)) == NULL)
#endif
		#ifdef qtt
if (time_check)
{
	_mm_mfence();		
	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt2+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
}
#endif
//	dec_ref(offset);
//	return 0;

#if 0 // remove this before  !!!!!!
	if (offset_to_node(ve_u.ve.node_offset)->state & NODE_SPLIT_BIT)
	{
		unlock_entry(unlock);
		continue;
	}
#endif 
//	inc_ref(ve_u.ve.node_offset);

	// location is found now
	// hash - v64_p / start_offset - ve_u / old_entry = old_ve


//		if (rv >= 0) // node is not spliting we will insert
//		locked_offset = ve_u.ve.node_offset;	

#ifdef qtt
if (time_check)
{
	clock_gettime(CLOCK_MONOTONIC,&ts3);
	_mm_mfence();
}
#endif
#if 0
	if (time_check)
	{
		test3(key_p,value_p,value_len);
		unlock_entry(unlock);
		THREAD_IDLE
		return;
	}
#endif

#if 0
	if (time_check)
{
	test3(key_p,value_p,value_len);
	
//	if (insert_kv3(/*ve_u.ve.node_offset,*/key_p,value_p,value_len) == 0)
//	{
//		unlock_entry(unlock);
//		continue;
//	}
}
else
#else
{
	if (insert_kv2(ve_u.ve.node_offset,key_p,value_p,value_len,v64_p) == 0)
	{
		unlock_entry(unlock);
		continue;
	}

}
#endif
//	rve = insert_kv2(ve_u.ve.node_offset,key_p,value_p,value_len,v64_p);

//	dec_ref(ve_u.ve.node_offset);
#if 0
	rve64 = *(uint64_t*)(&rve);

//	if (rv.node_offset.file == 0 && rv.node_offset.offset == 0)
//		printf("testestset\n");

	while(1)
	{
		ove64 = v64_p->load();
		ove = *(ValueEntry*)(&ove64);
//		if ((ove.ts <= rve.ts) || ((ove.ts & (1<<7)) != (rve.ts & (1<<7))))
		{
			if (v64_p->compare_exchange_strong(ove64,rve64))
			{
				if (ove64 != INV0)
					invalidate_kv2(ove);
				break;
			}
			//retry
		}
//		else // timeout?i
//			break;
	}
#endif

	unlock_entry(unlock);

#ifdef qtt
	if (time_check)
{
_mm_mfence();
//	clock_gettime(CLOCK_MONOTONIC,&ts6);
//	qtt4+=(ts6.tv_sec-ts5.tv_sec)*1000000000+ts6.tv_nsec-ts5.tv_nsec;
	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt3+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
}
#endif

//	dec_ref(ve_u.ve.node_offset);

//	break;

	// need split?

//	_mm_sfence();

//	int hot = find_hot(key_p,offset_to_node(ve_u.ve.node_offset)->continue_len);
//	int hot = 0;

	rv = need_split(ve_u.ve.node_offset);



#ifdef split_thread
	if (rv == 1)
	{
			split3(ve_u.ve.node_offset);
	}
	else if (rv == 2)
	{
//		add_split(ve_u.ve.node_offset);
//		while (compact3_lock(ve_u.ve.node_offset));


//		if (compact_check(ve_u.ve.node_offset,0))
//			while(compact4(ve_u.ve.node_offset,0));
//		else
//		compact_temp(ve_u.ve.node_offset);
//		if (need_split(ve_u.ve.node_offset))
			while (compact3(ve_u.ve.node_offset));
/*		
		else if (compact_check(ve_u.ve.node_offset,50))
			while(compact4(ve_u.ve.node_offset,50));
		else
			while(compact4(ve_u.ve.node_offset,99));
*/
	}
	else if (rv == 3)
	{
		while (compact3_lock(ve_u.ve.node_offset));
	}
#else
	/*
	if (rv)
	{
		if (split_or_compact(ve_u.ve.node_offset))
			split3(ve_u.ve.node_offset);
		else
			compact3(ve_u.ve.node_offset);
	}
	*/
	if (rv == 1)
	{
		/*
		if (file_num >= FILE_LIMIT || offset_to_node(ve_u.ve.node_offset)->ll_cnt <= 1)
			compact3(ve_u.ve.node_offset);
		else
		*/
			split3(ve_u.ve.node_offset);
	}
	else if (rv == 2)
		compact3(ve_u.ve.node_offset);
#endif

	break; // finish here
#if 0
		if (new_kv_p = insert_kv(ve_u.ve.node_offset,key_p,value_p,value_len))
		{
#ifdef qtt
	clock_gettime(CLOCK_MONOTONIC,&ts5);
#endif
	move_to_end_offset(ve_u.ve.node_offset); // move to end
			ve_u.ve.kv_offset = new_kv_p-(unsigned char*)offset_to_node_data(ve_u.ve.node_offset);
//			ve_u.ve.len = value_len;		
			// no space we should insert timestamp instead of value_len



#ifdef keep_lock
//			*vep = ve; // is it atomic?
			*v64_p = ve_u.ve_64;
			unlock_entry(unlock);
#else
//			insert_point_entry(key_p,ve);
			insert_point_entry(key_p,ve_u.ve);	
#endif
//			if (old_kv_offset)			
//				invalidate_kv(old_node_offset,old_kv_offset,old_len);

//			_mm_sfence();

			if (old_ve_u.ve.node_offset != INIT_OFFSET)
			{
				invalidate_kv(old_ve_u.ve);
			}
		// delete was here but i think pmem op is expensive and we can do this on dram with node meta but not yet	
			/*
			if (kv_p != NULL) // old key
			{
//				printf("delete_kv\n");
				delete_kv(kv_p);
			}
//			point_entry->kv_p = kv_p;
*/
//			dec_ref(ve_u.ve.node_offset);		

#ifdef qtt
_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts6);
	qtt4+=(ts6.tv_sec-ts5.tv_sec)*1000000000+ts6.tv_nsec-ts5.tv_nsec;
	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt3+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif
#ifdef split_thread
			if (need_split(ve_u.ve.node_offset,value_len))
			{
				if (add_split(locked_offset) == 1)
					break;
			}
#endif
//			else
				dec_ref(locked_offset);			


			break;
		}
		else // rv == -1 and it means we will split
		{
#ifdef qtt
	clock_gettime(CLOCK_MONOTONIC,&ts5);
#endif
#ifdef keep_lock
unlock_entry(unlock);
#endif
			//failed and need split
		/* //it may not happen because of free cnt
			if (range_entry == NULL)
			{
				range_entry = find_range_entry2(key_p,&continue_len);
//				if (range_entry != NULL && range_entry->offset != offset)
//					printf("flksjeflsekf %d %d\n",offset_to_node(offset)->state,offset_to_node(offset)->ref);
				// it could happen because split change range entry fisrt					
				if (range_entry == NULL || range_entry->offset != offset)
				{
//					printf("---------------split collision2\n");
					dec_ref(offset);
					continue;
				}
			}
		*/	
			if (continue_len == 0)
			{
				continue_len = get_continue_len(ve_u.ve.node_offset);
//				rv = find_range_entry2(key_p,&continue_len);
//				if (rv != offset)
//					printf("??? offset error\n");
//				if (rv == 0)
//					printf("rv == 0\n");

			}
//			if (continue_len < key_bit)//64) // split
//			if (continue_len == 63)
//				tf();
//			printf("split or compact\n");
			if (split_or_compact(ve_u.ve.node_offset))
			{	
				if (print)
				printf("split\n");
				// we need compaction continue len before get in here!!!!
				// point hash entry should have continue_len information
				/*
				if (range_entry == NULL)
				{
					range_entry = find_range_entry(query->key_p,&continue_len);
					if (range_entry == NULL)
					{
						printf("error!!!! %d\n",continue_len);
						scanf("%d\n",&test);
					}
				}
				*/
//				static std::mutex m;
//				m.lock();
				if ((rv = split2p(ve_u.ve.node_offset))<0)//,key_p,continue_len))<0)
				{

					dec_ref(locked_offset);

					/*
					printf("split lock failed %d/%d\n",ve_u.ve.node_offset.file,ve_u.ve.node_offset.offset);
	//				e_unlock(offset); //NEVER RELEASE E LOCK unless fail...
//					dec_ref(ve_u.ve.node_offset);
					test++;
					if (test > 1000)
					{
						int t;
						printf("???\n");
//						scanf("%d",&t);
					}
					*/
				}
				else
				{
//					range_entry->offset = SPLIT_OFFSET; //splited
//					insert_range_entry // in split function	
					// may need fence
// 					dec_ref(locked_offset);//ve.node_offset); // have to be after offset 1
					// why shoud we unlock
					continue_len++;		
				}
//				m.unlock();
//				continue; // try again after split
				if (print)
					printf("split end\n");				
			}
//			if (0)// compaction
			else
			{
			/*	
				if ((rv=compact(offset)) < 0)
					printf("compaction lock failed\n");
//				else
//					range_entry->offset = rv;
					*/
				if (compact2p(ve_u.ve.node_offset) < 0) // failed//,continue_len);
					dec_ref(locked_offset);				
				/*
				if ((rv=compact(offset)) >= 0)
				{
					if (range_entry == NULL)
					{
						continue_len = key_bit;//64;
						range_entry = find_range_entry2(key_p,&continue_len);
						if (range_entry == NULL)
						{
							printf("error!!!! %d\n",continue_len);
							scanf("%d\n",&test);
						}
					}
//					range_entry->offset = rv;// in split function
				}
				else
					printf("compaction lock failed\n");
					*/
//				e_unlock(offset);
//				dec_ref(ve.node_offset);			
			}
//int t;
//					scanf("%d",&t);// test
//			thread_idle();
#ifdef qtt
			/*
_mm_mfence();					
	clock_gettime(CLOCK_MONOTONIC,&ts6);
	qtt5+=(ts6.tv_sec-ts5.tv_sec)*1000000000+ts6.tv_nsec-ts5.tv_nsec;
	*/
#endif
		}
		if (print)
			printf("insert retry\n");	
#endif
#ifdef qtt
			/*
_mm_mfence();

	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt3+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
	*/
#endif
	}
	THREAD_IDLE
//		update_free_cnt();

#ifdef qtt
	/*
_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	qtt1+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
	*/
#endif
//	return 0;

}

void delete_query_scan_entry(Query* query)
{
#if 0
	Node_meta* node;
	Node_offset_u nou;
	while(1)
	{
//	while(query->scan_lock.compare_exchange_weak(z,1) == 0);		
//	at_lock(query->scan_lock);	
	if (query->scan_offset != TAIL_OFFSET_u.no_32)
	{
		nou.no_32 = query->scan_offset;
		node = offset_to_node(nou.no);
//		node = offset_to_node(query->scan_offset);
//		pthread_mutex_lock(&node->mutex);
//		while(node->lock.compare_exchange_weak(z,1) == 0);	
		if (try_at_lock(node->state) == 0)
		{
//			at_unlock(query->scan_lock);
			continue;
		}
		/*
		if (node->state > 0)
		{
			node->lock = 0;
			query->scan_lock = 0;
//			pthread_mutex_unlock(&node->mutex);
//			pthread_mutex_unlock(&query->scan_mutex);
			continue;
		}
		*/
//		delete_scan_entry(query->scan_offset,query);
		delete_scan_entry(nou.no,query);
		query->scan_offset = TAIL_OFFSET_u.no_32;
//		node->lock = 0;
		at_unlock(node->state);		
//		query->scan_lock = 0;
//		at_unlock(query->scan_lock);		
//		pthread_mutex_unlock(&node->mutex);
//		pthread_mutex_unlock(&query->scan_mutex);
		break;
	}
	else
	{
//		query->scan_lock = 0;
//		at_unlock(query->scan_lock);		
//		pthread_mutex_unlock(&query->scan_mutex);
		break;
	}
	}
#endif
}

int scan_query(Query* query)//,unsigned char** result,int* result_len)
{
	return 0;
#if 0
	/*
	*result = empty;
	*result_len = empty_len;
*/
//	point_hash_entry* entry;
//	range_hash_entry* range_entry;
//	unsigned char* kv_p;
//	unsigned int offset;
	ValueEntry ve;
	int continue_len;
	Node* node_data;
	Node_offset start_offset;
	int size;

	update_free_cnt();
#if 1
	if (query->node_data == NULL)
	{
		query->node_data = (Node*)malloc(sizeof(Node)*PART_MAX);
		if (query->node_data == NULL)
			printf("node data alloc fail\n");
//		query->node_size = (int*)malloc(sizeof(int)*PART_MAX);
	}
#endif
	delete_query_scan_entry(query);

	node_data = (Node*)query->node_data;

//	offset = 0;
//	entry = find_or_insert_point_entry(query->key_p,0); // don't create
//	
	{

			ve = find_point_entry(query->key_p);
		while(ve.node_offset != INIT_OFFSET)
		{
//			kv_p = (unsigned char*)entry->kv_p;
//			offset = data_point_to_offset(kv_p);

			start_offset = get_start_offset(ve.node_offset);
			if (inc_ref(start_offset))
			{
				ve.node_offset = start_offset;
				break;
#if 0
				// it will not happen
				/*
				if (kv_p != (unsigned char*)entry->kv_p)
				{
					hard_unlock(offset);
					continue;
				}
				*/

//			copy_node(node,offset_to_node(offset));
			*node_data = *offset_to_node_data(ve.node_offset); //copy node
			size = offset_to_node(ve.node_offset)->size;
			insert_scan_list(offset_to_node(ve.node_offset),(void*)query);
			hard_unlock(ve.node_offset);
			sort_node(node_data,(int*)(query->sorted_index),&(query->index_max),size);
		
//			query->kv_p = node->buffer;
			query->index_num = 0;			
//			pthread_mutex_lock(&query->scan_mutex);
//			whlie(query->scan_lock.compare_exchange_weak(z,1) == 0);
			at_lock(query->scan_lock);			
			query->scan_offset = ve.node_offset;
//			pthread_mutex_unlock(&query->scan_mutex);
			query->scan_lock = 0;
/*
			while (*((uint64_t*)query->kv_p) < *((uint64_t*)query->key_p))
			{
				if (advance(&(query->kv_p),&(query->offset),node) < 0)
					return 0;
			}
*/
			while (*((uint64_t*)(node_data->buffer+query->sorted_index[query->index_num])) < *((uint64_t*)query->key_p))
			{
				query->index_num++;
				if (query->index_num >= query->index_max)
				{
					if (advance_offset((void*)query) < 0)
					{
//						dec_ref(offset);
						return 0;
					}
//					copy_node(node,offset_to_node(query->scan_offset));
					int size = offset_to_node(query->scan_offset)->size; // i don't know
					sort_node(node_data,(int*)(query->sorted_index),&(query->index_max),size);
					query->index_num=0;
				}
			}
//			dec_ref(offset);
			return 0;
#endif
			}
			ve = find_point_entry(query->key_p);

		}
	}
	if (ve.node_offset == INIT_OFFSET)
	{
	continue_len = 0;
//	continue_len = -1;
	//unsigned int offset,offset2;	
//	Node_offset offset,offset2;
	while(1)
	{
//		range_entry = find_range_entry2(query->key_p,&continue_len);
		if ((ve.node_offset = find_range_entry2(query->key_p,&continue_len)) == INIT_OFFSET)
			continue;
		/*
		if (range_entry == NULL)
			continue;
		offset = range_entry->offset;
		*/
		if (inc_ref(ve.node_offset))
		{
			break;
#if 0
			offset2 = find_range_entry2(query->key_p,&continue_len);
			if (offset != offset2)
			{
				hard_unlock(offset);
				offset = INIT_OFFSET;
				continue;
			}

//			copy_node(node,offset_to_node(offset));
			*node_data = *offset_to_node_data(offset); //copy node
			size = offset_to_node(offset)->size;
			insert_scan_list(offset_to_node(offset),(void*)query);
			hard_unlock(offset);
			sort_node(node_data,(int*)(query->sorted_index),&(query->index_max),size);
		
//			query->kv_p = node->buffer;
			query->index_num = 0;			
//			pthread_mutex_lock(&query->scan_mutex);
//			while(query->scan_lock.compare_exchange_weak(z,1) == 0);
			at_lock(query->scan_lock);			
			query->scan_offset = offset;
//			pthread_mutex_unlock(&query->scan_mutex);
			query->scan_lock = 0;			
/*
			while (*((uint64_t*)query->kv_p) < *((uint64_t*)query->key_p))
			{
				if (advance(&(query->kv_p),&(query->offset),node) < 0)
					return 0;
			}
*/
			while (*((uint64_t*)(node_data->buffer+query->sorted_index[query->index_num])) < *((uint64_t*)query->key_p))
			{
				query->index_num++;
				if (query->index_num >= query->index_max)
				{
					if (advance_offset((void*)query) < 0)
					{
//						dec_ref(offset);
						return 0;
					}
//					copy_node(node,offset_to_node(query->scan_offset));
					sort_node(node_data,(int*)(query->sorted_index),&(query->index_max),size);
					query->index_num=0;
				}
			}


			return 0;
#endif
		}
//		dec_ref(offset);

	}
	}


	// here, we have ref and ve.node_offset and we should copy it and def it

	Node_offset_u nou;
	nou.no = ve.node_offset;
//	query->scan_offset = ve.node_offset; // scan lock??
	query->scan_offset = nou.no_32;

//	insert_scan_list(query->scan_offset,query);
	insert_scan_list(ve.node_offset,query);

	// lock scan mutex or move dec ref
//	at_lock(query->scan_lock); // scan lock ??? ??? ??? 

	//copy node
//	copy_node(query->node_data,query->scan_offset);

	copy_and_sort_node(query);
	//insert scan list
	dec_ref(ve.node_offset);
	//sort node
//	sort_node(query);

	//set scan index index num
//	query->sorted_kv_i = 0;
//	query->kv_p = ((Node_data*)query->node_data)[0];
	//unlock scan mutex

	//advance until key_p
	while(1)
	{
		while (query->sorted_kv_max > query->sorted_kv_i)
		{
			if(*((uint64_t*)(query->sorted_kv[query->sorted_kv_i]+PH_LEN_SIZE/*len_size*/)) >= *((uint64_t*)query->key_p))
				return 0;
			++query->sorted_kv_i;
		}

		if (advance_offset(query) < 0) // advance fail	
			return 0;
	}

//	printf("never should come here\b");
	return 0;
#endif
}

//int next_query(Query* query,unsigned char** result,int* result_len)
int next_query(Query* query,unsigned char* result_p,int* result_len_p)
{
	return 0;
	update_free_cnt();

	if (query->scan_offset == TAIL_OFFSET_u.no_32)
	{
//		*result = empty;
		memcpy(result_p,empty,empty_len);		
		*result_len_p = empty_len;
		return 0;
	}
//	Node* node_data = (Node*)query->node_data;
//	const unsigned char* offset0 = (unsigned char*)query->node_data;	

//	*result_len_p = *((uint16_t*)(node_data->buffer+query->sorted_index[query->index_num]+key_size));
	*result_len_p = *((uint16_t*)(query->sorted_kv[query->sorted_kv_i]));
	/*
	if ((*result_len & (1 << 15)) != 0) // deleted
		advance(&(query->kv_p),&(query->offset),(Node*)query->node);	

	if (query->kv_p == NULL)
	{
		*result = empty;
		*result_len = empty_len;
		return 0;
	}
	*/

	*result_len_p+=8+2;
//	memcpy(result_p,node_data->buffer+query->sorted_index[query->index_num],*result_len_p);
	memcpy(result_p,query->sorted_kv[query->sorted_kv_i],*result_len_p);
//	*result = query->kv_p; // we need all kv_p

//	advance(&(query->kv_p),&(query->offset),(Node*)query->node);
	query->sorted_kv_i++;//index_num++;
	while (query->sorted_kv_i >= query->sorted_kv_max)
	{
//					pthread_mutex_lock(&query->scan_mutex);
		if (advance_offset(query) < 0)
		{
//						pthread_mutex_unlock(&query->scan_mutex);
			return 0;
		}
//					copy_node(node,offset_to_node(query->scan_offset));
//					pthread_mutex_unlock(&query->scan_mutex);
//int size = offset_to_node(query->scan_offset)->size; // ??? i don't know					
//		sort_node(node_data,(int*)(query->sorted_index),&(query->index_max),size);
//		query->index_num=0;
	}

	return 0;
}
int next_query(Query* query,std::string* result)
{
	return 0;
	THREAD_RUN
	update_free_cnt();

	if (query->scan_offset == TAIL_OFFSET_u.no_32)
	{
		result->assign((char*)empty,empty_len);
		THREAD_IDLE
//		update_free_cnt();

		return 0;
	}
	int result_len_p;
	result_len_p = *((uint16_t*)(query->sorted_kv[query->sorted_kv_i]));
	result_len_p+=8+2;
//	memcpy(result_p,query->sorted_kv[query->sorted_kv_i],*result_len_p);
	result->assign((char*)query->sorted_kv[query->sorted_kv_i],result_len_p);
//	*result = query->kv_p; // we need all kv_p

//	advance(&(query->kv_p),&(query->offset),(Node*)query->node);
	query->sorted_kv_i++;//index_num++;
	while (query->sorted_kv_i >= query->sorted_kv_max)
	{
		if (advance_offset(query) < 0)
		{
			THREAD_IDLE
//		update_free_cnt();

			return 1;
		}
//					copy_node(node,offset_to_node(query->scan_offset));
//int size = offset_to_node(query->scan_offset)->size; // ??? i don't know					
//		sort_node(node_data,(int*)(query->sorted_index),&(query->index_max),size);
//		query->index_num=0;
	}
	THREAD_IDLE
//		update_free_cnt();

	return 1;
}

void free_query(Query* query)
{
//	if (query->offset > TAIL_OFFSET)
//		dec_ref(query->offset);
//
/*
		pthread_mutex_lock(&query->scan_mutex);
	if (query->scan_offset > TAIL_OFFSET)
	{
		pthread_mutex_lock(&(offset_to_node(query->scan_offset)->mutex));
		delete_scan_entry(query->scan_offset,(void*)query);
		pthread_mutex_unlock(&(offset_to_node(query->scan_offset)->mutex));
	}		
		pthread_mutex_unlock(&query->scan_mutex);
		*/
	delete_query_scan_entry(query);
	free(query->node_data);
//	free(query->node_size);
}

size_t scan_query2(unsigned char* key,int cnt,std::string* scan_result)
{
#if 1
//	printf("%d\n",cnt);
//	if (cnt == 334)
//		printf("334\n");
	ValueEntry ve;
	int continue_len;
	Node* node_data;
//	Node_offset start_offset;
	int size;
	Node_meta* node_meta;

	THREAD_RUN
	update_free_cnt();

	ve = find_point_entry(key); // no lock
//	while(ve.node_offset != INIT_OFFSET)
	if (ve.node_offset != INIT_OFFSET)
	{

		ve.node_offset = get_start_offset(ve.node_offset);
		/*
		if (inc_ref(start_offset))
		{
			ve.node_offset = start_offset;
			break;
		}
		ve = find_point_entry(key);
		*/
	}

	if (ve.node_offset == INIT_OFFSET)
	{
		continue_len = 0;
		while ((ve.node_offset = find_range_entry2(key,&continue_len)) == INIT_OFFSET);
/*
		while(1)
		{
			if ((ve.node_offset = find_range_entry2(key,&continue_len)) == INIT_OFFSET)
				continue;
			if (inc_ref(ve.node_offset))
			{
				break;
			}
		}
		*/
	}

	// found start node
	int result_num,result_sum=0;
	Node_offset_u node_offset;
	node_offset.no= ve.node_offset;
//	while(cnt > result_sum)
	while(node_offset.no != TAIL_OFFSET)
	{
		result_num = scan_node(node_offset.no,key,cnt-result_sum,scan_result);
		result_sum+=result_num;
		scan_result+=result_num;

		if (cnt <= result_sum)
		{
//			dec_ref(node_offset);
//			return result_sum;
			return cnt;
		}

		node_offset.no_32 = offset_to_node(node_offset.no)->next_offset;
		/*
//		while(1)
		{
		next_offset.no_32 = offset_to_node(node_offset)->next_offset;
		if (next_offset.no == TAIL_OFFSET)
		{
//			dec_ref(node_offset);
			return result_sum;
		}
//		if (inc_ref(next_offset.no))
//			break;
		}

//		dec_ref(node_offset);

		node_offset = next_offset.no;
		*/
	}
THREAD_IDLE
//		update_free_cnt();

	return result_sum;


	//--------------------------------------
#if 0
	Node_offset_u nou;
	nou.no = ve.node_offset;

	//copy node
//	copy_node(query->node_data,query->scan_offset);

	copy_and_sort_node(query);
	//insert scan list
	dec_ref(ve.node_offset);
	//sort node
//	sort_node(query);

	//set scan index index num
//	query->sorted_kv_i = 0;
//	query->kv_p = ((Node_data*)query->node_data)[0];
	//unlock scan mutex

	//advance until key_p
	while(1)
	{
		while (query->sorted_kv_max > query->sorted_kv_i)
		{
			if(*((uint64_t*)(query->sorted_kv[query->sorted_kv_i]+len_size)) >= *((uint64_t*)query->key_p))
				return 0;
			++query->sorted_kv_i;
		}

		if (advance_offset(query) < 0) // advance fail	
			return 0;
	}

//	printf("never should come here\b");
	return 0;
#endif
#endif
}

}
