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

#include <mutex> // test

//#define print 0
#define print 0

//using namespace PH;
namespace PH
{

	extern uint64_t qtt1,qtt2,qtt3,qtt4,qtt5,qtt6,qtt7,qtt8;

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
	query->scan_offset = TAIL_OFFSET;
//	pthread_mutex_init(&query->scan_mutex,NULL);
	query->scan_lock = 0;	
}

int lookup_query(unsigned char* &key_p, unsigned char* &result_p,int* result_len_p)
{
#ifdef qtt
	timespec ts1,ts2,ts3,ts4;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
//		unsigned char* kv_p;
		ValueEntry ve;
//		int value_len;
		const int kls = key_size+len_size;
//		unsigned int offset;
		update_free_cnt();
#ifdef qtt
//		clock_gettime(CLOCK_MONOTONIC,&ts3);
//		_mm_mfence();
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
		if (ve.node_offset == 0|| ve.kv_offset == 0)
		{
//			result_p = empty;
			memcpy(result_p,empty,empty_len);			
			*result_len_p = empty_len;
//			*result_len_p = 0;
			return 0;
		}
//		while(1)
//		{
//			if (kv_p == NULL) // deleted
//				break;
			// lock version
//			if (print)
//			printf("kv_p %p\n",kv_p);	
//			offset = data_point_to_offset(kv_p);
//			if (inc_ref(offset,1)) // split state ok
//			{
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
				*result_len_p = ve.len;				
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
				memcpy(result_p,(unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset+kls,*result_len_p);
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

				return 0;				
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


int delete_query(unsigned char* key_p)
{

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
			if (ve.node_offset == 0) // deleted!!!
				return 0;
//			offset = data_point_to_offset(kv_p);
//			if (*((uint64_t*)query->key_p) != *((uint64_t*)kv_p)) // instead CAS
//				continue;
			if (inc_ref(ve.node_offset)) //init state ok
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
				invalidate_kv(ve.node_offset,ve.kv_offset);
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

int insert_query(unsigned char* key_p, unsigned char* value_p)
{
	if (print)
		printf("insert\n");
	/*
	*result = empty;
	*result_len = empty_len;
*/

	unsigned char* kv_p;

	ValueEntry ve;
	ValueEntry* vep;
	void* unlock;

//	unsigned int offset;
	int continue_len;
	const int value_len = value_size; // temp fix
	continue_len = 0;
//	continue_len = -1;	
	int rv;

	int test=0,test2=0;
	int z = 0;
	int old_kv_offset;
#ifdef qtt
	timespec ts1,ts2,ts3,ts4,ts5,ts6;

	clock_gettime(CLOCK_MONOTONIC,&ts1);
_mm_mfence();
#endif
	update_free_cnt();


	while(1) // offset can be changed when retry
	{
		if (print)
			printf("insert loop\n");
#ifdef qtt
		clock_gettime(CLOCK_MONOTONIC,&ts3);
		_mm_mfence();
#endif
//		ve.node_offset = 0;
//		vep = NULL;

//		offset = 0;
//		kv_p = NULL;
#if 1
//		ve = find_point_entry(key_p);
#ifdef keep_lock		
		vep = find_or_insert_point_entry(key_p,&unlock);	
#else		
		vep = &ve;		
		*vep = find_point_entry(key_p);		
#endif
		old_kv_offset = 0;
//		while(ve.node_offset != 0)
		while(vep->node_offset != 0)		
		{
//			kv_p = point_hash.find(key_p); // find or create
/*			
			kv_p = find_point_entry(key_p);			
			if (kv_p == NULL) // deleted during !!! or doesn't exist...
			{
				offset = 0;
				break;
			}
			*/
//			offset = data_point_to_offset(kv_p);
			if (inc_ref(vep->node_offset))
			{
				//do we need this?
				/*
				if (*((uint64_t*)key_p) != *((uint64_t*)kv_p))
				{
					dec_ref(offset);
					printf("value is moved\n");
					continue;
				}
				*/
								
//				if ((rv = check_size(offset,value_size)) >= -1) // node is not spliting
				/*
				const int ns = offset_to_node(ve.node_offset)->size;
				const int es = key_size + len_size + value_size;
				if (ns + es > NODE_BUFFER)
	rv = -1;
				else
				{
					rv = ns;
					offset_to_node(vep->node_offset)->size += es;
				}
				*/
				rv = check_size(vep->node_offset,value_len);//value_size);
				old_kv_offset = vep->kv_offset;
//				{
					break;
//				}

//					dec_ref(offset); // node is spliting
			}
//			ve = find_point_entry(key_p);
#ifdef keep_lock			
			unlock_entry(unlock);
			vep = find_or_insert_point_entry(key_p,&unlock);
#else
			*vep = find_point_entry(key_p);			
#endif
//				if (print)
//				printf("node is spliting??\n");
		}
#else
		vep = &ve;
		vep->node_offset = 0;
#endif
//		if (offset == 0) // the key doesn't exist
//		if (ve.node_offset == 0)		
		if (vep->node_offset == 0)		
//		if (kv_p == NULL)		
		{
			if (print)
			printf("find node\n");
			while(1)
			{
				if ((ve.node_offset = find_range_entry2(key_p,&continue_len)) == 0)
//				if (range_entry == NULL) // spliting...
				{
//
					printf("---------------split collision\n");
					
					test++;
				if (test > 1000)
				{
				printf("too many fail %lu\n",*((uint64_t*)key_p));
				int t;
				scanf("%d",&t);
					}
					continue;
				}
				if (inc_ref(ve.node_offset))
				{
//				if (range_entry->offset == SPLIT_OFFSET)
				/* // it can't be now because of free cnt
				if (range_entry->offset != offset)				
				{
					dec_ref(offset);
					continue;
				}
				*/

//				if ((rv = check_size(offset,value_size)) >= -1) // node is not spliting
/*
				Node_meta* nm = offset_to_node(offset);			
//				const int ns = nm->size;
				const int es = key_size + len_size + value_size;
				if (nm->size + es > NODE_BUFFER)
	rv = -1;
				else
				{
					rv = nm->size;
					nm->size += es;
				}
*/			
				rv = check_size(ve.node_offset,value_len);//value_size);
//				{
					break;
//				}

//				dec_ref(offset); // node is spliting
				}
				test2++;
				if (test2 > 10000)
				{
					printf("fail\n");
					int t;
					scanf("%d",&t);
				}


			}
//			if (print)
//			printf("node found offset %d\n",offset);
		}
		else
			ve = *vep;
		//e locked
//		if ((kv_p = insert_kv(offset,query->key_p,query->value_p,query->value_len)) == NULL)
		#ifdef qtt
	_mm_mfence();		
	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt2+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif
//	dec_ref(offset);
//	return 0;
#ifdef qtt
	clock_gettime(CLOCK_MONOTONIC,&ts3);
	_mm_mfence();
#endif
		if (rv >= 0) // node is not spliting we will insert
		{
#ifdef qtt
	clock_gettime(CLOCK_MONOTONIC,&ts5);
#endif
			unsigned char* rp;
//			unsigned char* tp;
						// it should be ve later
			rp = insert_kv(ve.node_offset,key_p,value_p,value_len/*size*/,rv); // never fail
//			soft_lock(offset); // not this! use hash lock
//			tp = kv_p;
//			point_hash.insert(key_p,rp);
			ve.kv_offset = (unsigned char*)offset_to_node_data(ve.node_offset)-rp;
			ve.len = value_len;			

#ifdef keep_lock
			*vep = ve; // is it atomic?
			unlock_entry(unlock);
#else
			insert_point_entry(key_p,ve);
#endif
			if (old_kv_offset)
				invalidate_kv(ve.node_offset,old_kv_offset);
//			// use vep instead of insert point
//			hard_unlock(offset);

		// delete was here but i think pmem op is expensive and we can do this on dram with node meta but not yet	
			/*
			if (kv_p != NULL) // old key
			{
//				printf("delete_kv\n");
				delete_kv(kv_p);
			}
//			point_entry->kv_p = kv_p;
*/
			dec_ref(ve.node_offset);
#ifdef qtt
_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts6);
	qtt4+=(ts6.tv_sec-ts5.tv_sec)*1000000000+ts6.tv_nsec-ts5.tv_nsec;
	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt3+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif
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
				rv = find_range_entry2(key_p,&continue_len);
//				if (rv != offset)
//					printf("??? offset error\n");
				if (rv == 0)
					printf("rv == 0\n");

			}
			if (continue_len < key_bit)//64) // split
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
				if ((rv = split(ve.node_offset,key_p,continue_len))<0)
				{
					printf("split lock failed %d\n",ve.node_offset);
	//				e_unlock(offset); //NEVER RELEASE E LOCK unless fail...
					dec_ref(ve.node_offset);
					test++;
					if (test > 1000)
					{
						int t;
						printf("???\n");
//						scanf("%d",&t);
					}
				}
				else
				{
//					range_entry->offset = SPLIT_OFFSET; //splited
//					insert_range_entry // in split function	
					// may need fence
// 					dec_ref(ve.node_offset); // have to be after offset 1
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
				compact(ve.node_offset);
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
#ifdef qtt
_mm_mfence();					
	clock_gettime(CLOCK_MONOTONIC,&ts6);
	qtt5+=(ts6.tv_sec-ts5.tv_sec)*1000000000+ts6.tv_nsec-ts5.tv_nsec;
#endif
		}
		if (print)
			printf("insert retry\n");		
#ifdef qtt
_mm_mfence();

	clock_gettime(CLOCK_MONOTONIC,&ts4);
	qtt3+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif
	}
#ifdef qtt
_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	qtt1+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
	return 0;

}

void delete_query_scan_entry(Query* query)
{
	Node_meta* node;
	const int z = 0;
	while(1)
	{
//	pthread_mutex_lock(&query->scan_mutex);
//	while(query->scan_lock.compare_exchange_weak(z,1) == 0);		
	at_lock(query->scan_lock);	
	if (query->scan_offset != TAIL_OFFSET)
	{
		node = offset_to_node(query->scan_offset);
//		pthread_mutex_lock(&node->mutex);
//		while(node->lock.compare_exchange_weak(z,1) == 0);	
		if (try_at_lock(node->state) == 0)
		{
			at_unlock(query->scan_lock);
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
		delete_scan_entry(query->scan_offset,query);
		query->scan_offset = TAIL_OFFSET;
//		node->lock = 0;
		at_unlock(node->state);		
//		query->scan_lock = 0;
		at_unlock(query->scan_lock);		
//		pthread_mutex_unlock(&node->mutex);
//		pthread_mutex_unlock(&query->scan_mutex);
		break;
	}
	else
	{
//		query->scan_lock = 0;
		at_unlock(query->scan_lock);		
//		pthread_mutex_unlock(&query->scan_mutex);
		break;
	}
	}

}

int scan_query(Query* query)//,unsigned char** result,int* result_len)
{
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

	update_free_cnt();

	if (query->node_data == NULL)
		query->node_data = (Node*)malloc(sizeof(Node));

	delete_query_scan_entry(query);

	node_data = (Node*)query->node_data;

//	offset = 0;
//	entry = find_or_insert_point_entry(query->key_p,0); // don't create
//	
	{
		while(1)
		{
			ve = find_point_entry(query->key_p);
//			kv_p = (unsigned char*)entry->kv_p;
			if (ve.node_offset == 0)
				break;
//			offset = data_point_to_offset(kv_p);

			if (try_hard_lock(ve.node_offset))
			{
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
			insert_scan_list(offset_to_node(ve.node_offset),(void*)query);
			hard_unlock(ve.node_offset);
			sort_node(node_data,(int*)(query->sorted_index),&(query->index_max));
		
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
					sort_node(node_data,(int*)(query->sorted_index),&(query->index_max));
					query->index_num=0;
				}
			}
//			dec_ref(offset);
			return 0;
			}

		}
	}
	continue_len = 0;
//	continue_len = -1;
	unsigned int offset,offset2;	
	while(1)
	{
//		range_entry = find_range_entry2(query->key_p,&continue_len);
		offset = find_range_entry2(query->key_p,&continue_len);		
		/*
		if (range_entry == NULL)
			continue;
		offset = range_entry->offset;
		*/
		if (offset == 0) // spliting
			continue;
		if (try_hard_lock(offset))
		{
			offset2 = find_range_entry2(query->key_p,&continue_len);
			if (offset != offset2)
			{
				hard_unlock(offset);
				offset = 0;
				continue;
			}

//			copy_node(node,offset_to_node(offset));
			*node_data = *offset_to_node_data(offset); //copy node
			insert_scan_list(offset_to_node(offset),(void*)query);
			hard_unlock(offset);
			sort_node(node_data,(int*)(query->sorted_index),&(query->index_max));
		
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
					sort_node(node_data,(int*)(query->sorted_index),&(query->index_max));
					query->index_num=0;
				}
			}


			return 0;
		}
//		dec_ref(offset);

	}
	printf("never should come here\b");
	return 0;
}

//int next_query(Query* query,unsigned char** result,int* result_len)
int next_query(Query* query,unsigned char* result_p,int* result_len_p)
{
	update_free_cnt();

	if (query->scan_offset == TAIL_OFFSET)
	{
//		*result = empty;
		memcpy(result_p,empty,empty_len);		
		*result_len_p = empty_len;
		return 0;
	}
	Node* node_data = (Node*)query->node_data;

	*result_len_p = *((uint16_t*)(node_data->buffer+query->sorted_index[query->index_num]+key_size));
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
	memcpy(result_p,node_data->buffer+query->sorted_index[query->index_num],*result_len_p);
//	*result = query->kv_p; // we need all kv_p

//	advance(&(query->kv_p),&(query->offset),(Node*)query->node);
	query->index_num++;
	while (query->index_num >= query->index_max)
	{
//					pthread_mutex_lock(&query->scan_mutex);
		if (advance_offset((void*)query) < 0)
		{
//						pthread_mutex_unlock(&query->scan_mutex);
			return 0;
		}
//					copy_node(node,offset_to_node(query->scan_offset));
//					pthread_mutex_unlock(&query->scan_mutex);
		sort_node(node_data,(int*)(query->sorted_index),&(query->index_max));
		query->index_num=0;
	}

	return 0;
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
}

}
