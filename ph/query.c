#include "query.h"
#include "hash.h"
#include "data.h"

#ifndef NULL
#define NULL 0
#endif

#include <stdio.h> //test
#include <stdlib.h>
#include <string.h>

//#define print 0
#define print 0

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
	query->node = NULL;
	query->scan_offset = TAIL_OFFSET;
	pthread_mutex_init(&query->scan_mutex,NULL);
}

int lookup_query(unsigned char* key_p, unsigned char* result_p,int* result_len_p)
{
		point_hash_entry* entry;
		unsigned char* kv_p;
		unsigned int offset;
		entry = find_or_insert_point_entry(key_p,0); // don't create
		if (entry == NULL)
		{
//			result_p = empty;
			memcpy(result_p,empty,empty_len);			
			*result_len_p = empty_len;
//			*result_len_p = 0;
			return 0;
		}
		while(1)
		{
			kv_p = (unsigned char*)entry->kv_p;
			if (kv_p == NULL) // deleted
				break;
			// lock version
			if (print)
			printf("kv_p %p\n",kv_p);	
			offset = point_to_offset(kv_p);
			if (inc_ref(offset,1)) // split state ok
			{
				if (kv_p != (unsigned char*)entry->kv_p) // recycled?
				{
					dec_ref(offset);
					continue;
				}
//				query->ref_offset = offset; // lock ref

//				print_kv(kv_p);	

				*result_len_p = *((uint16_t*)(kv_p+key_size));
				if ((*result_len_p & (1 << 15)) != 0) // deleted
				{
//					dec_ref(offset);
					break;
				}
//				*result = kv_p+key_size+len_size;
				memcpy(result_p,kv_p+key_size+len_size,value_size);				
//				s_unlock(offset); // it will be released after result
				dec_ref(offset);				
//				break;
				return 0;				
			}
			


//			if (kv_p == (unsigned char*)entry->kv_p)
//				break;
		}
		//deleted
//		*result_p = empty;
		memcpy(result_p,empty,empty_len);		
		*result_len_p = empty_len;
//		*result_len_p = 0;		
		return 0;
}


int delete_query(unsigned char* key_p)
{
		point_hash_entry* entry;
		unsigned char* kv_p;
		unsigned int offset;
		entry = find_or_insert_point_entry(key_p,0); // don't create
		if (entry == NULL || entry->kv_p == NULL)
		{
			/*
//			*result = empty;
			memcpy(result_p,empty,empty_len);			
			*result_len = empty_len;
			*/
			return 0;
		}
		while(1)
		{
			kv_p = (unsigned char*)entry->kv_p;
			if (kv_p == NULL) // deleted!!!
				return 0;
			offset = point_to_offset(kv_p);
//			if (*((uint64_t*)query->key_p) != *((uint64_t*)kv_p)) // instead CAS
//				continue;
			if (inc_ref(offset,0)) //init state ok
			{
				if (kv_p != (unsigned char*)entry->kv_p)
				{
					dec_ref(offset);
					continue;
				}
//				query->offset = offset; // restore ref cnt

				soft_lock(offset);
				entry->kv_p = NULL; // what should be first?
				delete_kv(kv_p);
				hard_unlock(offset);

//				e_unlock(offset);
				dec_ref(offset);				
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

int insert_query(unsigned char* key_p, unsigned char* value_p)
{
	if (print)
		printf("insert\n");
	/*
	*result = empty;
	*result_len = empty_len;
*/

	point_hash_entry* point_entry;
	range_hash_entry* range_entry;
	unsigned char* kv_p;
	unsigned int offset;
	int continue_len;	
	continue_len = 0;
	int rv;

	int test=0;

	point_entry = find_or_insert_point_entry(key_p,1); // find or create
	while(1) // offset can be changed when retry
	{
		if (print)
			printf("insert loop\n");
		offset = 0;
		range_entry = NULL;
		if (point_entry) // always true
		{
			while(1)
			{
				kv_p = (unsigned char*)point_entry->kv_p;
				if (kv_p == NULL) // deleted during !!! or doesn't exist...
				{
					offset = 0;
					break;
				}
				offset = point_to_offset(kv_p);
				if (inc_ref(offset,0))
				{
				if (kv_p != (unsigned char*)point_entry->kv_p)
				{
					dec_ref(offset);
//					printf("value is moved\n");
					continue;
				}
								
				if ((rv = check_size(offset,value_size)) >= -1) // node is not spliting
				{
					break;
				}

					dec_ref(offset); // node is spliting
				}
//				if (print)
//				printf("node is spliting??\n");
			}
		}
		else
		{
			int t;
			printf("efefsefesf\n");
			scanf("%d",&t);
		}
		if (offset == 0) // the key doesn't exist
		{
			if (print)
			printf("find node\n");
			while(1)
			{
				if ((range_entry = find_range_entry(key_p,&continue_len)) == NULL)
//				if (range_entry == NULL) // spliting...
				{
//
					printf("---------------split collision\n");
					
					test++;
				if (test > 1000)
				{
				printf("too many fail %lu\n",*((uint64_t*)key_p));
					}
					continue;
				}
				offset = range_entry->offset;
				if (inc_ref(offset,0))
				{
//				if (range_entry->offset == SPLIT_OFFSET)
				if (range_entry->offset != offset)				
				{
					dec_ref(offset);
					continue;
				}

				if ((rv = check_size(offset,value_size)) >= -1) // node is not spliting
				{
					break;
				}

					dec_ref(offset); // node is spliting
				}


			}
			if (print)
			printf("node found offset %d\n",offset);
		}
		//e locked
//		if ((kv_p = insert_kv(offset,query->key_p,query->value_p,query->value_len)) == NULL)
		if (rv >= 0) // node is not spliting we will insert
		{
			unsigned char* rp;
			unsigned char* tp;
			rp = insert_kv(offset,key_p,value_p,value_size,rv); // never fail
			soft_lock(offset); // not this! use hash lock
			tp = (unsigned char*)point_entry->kv_p;
			point_entry->kv_p = rp;
			hard_unlock(offset);

			if (tp != NULL) // old key
				*((uint16_t*)(tp+key_size)) |= (1<<15); // invalidate
//			point_entry->kv_p = kv_p;

			dec_ref(offset);
			break;
		}
		else // rv == -1 and it means we will split
		{
			//failed and need split
			
			if (range_entry == NULL)
			{
				range_entry = find_range_entry(key_p,&continue_len);
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
			
			if (continue_len < 64) // split
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

				if (split(offset,key_p,continue_len)<0)
				{
					printf("split lock failed\n");
	//				e_unlock(offset); //NEVER RELEASE E LOCK unless fail...
					dec_ref(offset);
					test++;
					if (test > 1000)
						printf("???\n");
				}
				else
				{
					range_entry->offset = SPLIT_OFFSET; //splited
					// may need fence
 					dec_ref(offset); // have to be after offset 1
					continue_len++;		
				}
//				continue; // try again after split
				if (print)
					printf("split end\n");				
			}
			else// compaction
			{
			/*	
				if ((rv=compact(offset)) < 0)
					printf("compaction lock failed\n");
//				else
//					range_entry->offset = rv;
					*/
				if ((rv=compact(offset)) >= 0)
				{
					if (range_entry == NULL)
					{
						continue_len = 64;
						range_entry = find_range_entry(key_p,&continue_len);
						if (range_entry == NULL)
						{
							printf("error!!!! %d\n",continue_len);
							scanf("%d\n",&test);
						}
					}
					range_entry->offset = rv;
				}
//				else
//					printf("compaction lock failed\n");
//				e_unlock(offset);
				dec_ref(offset);				
			}
//int t;
//					scanf("%d",&t);// test
		}
		if (print)
			printf("insert retry\n");				
	}
	return 0;

}

void delete_query_scan_entry(Query* query)
{
	Node* node;
	while(1)
	{
	pthread_mutex_lock(&query->scan_mutex);
	if (query->scan_offset != TAIL_OFFSET)
	{
		node = offset_to_node(query->scan_offset);
		pthread_mutex_lock(&node->mutex);
		if (node->state > 0)
		{
			pthread_mutex_unlock(&node->mutex);
			pthread_mutex_unlock(&query->scan_mutex);
			continue;
		}
		delete_scan_entry(query->scan_offset,query);
		query->scan_offset = TAIL_OFFSET;
		pthread_mutex_unlock(&node->mutex);
		pthread_mutex_unlock(&query->scan_mutex);
		break;
	}
	else
	{
		pthread_mutex_unlock(&query->scan_mutex);
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
	point_hash_entry* entry;
	range_hash_entry* range_entry;
	unsigned char* kv_p;
	unsigned int offset;
	int continue_len;
	Node* node;

	if (query->node == NULL)
		query->node = (Node*)malloc(sizeof(Node));

	delete_query_scan_entry(query);

	node = (Node*)query->node;

	offset = 0;
	entry = find_or_insert_point_entry(query->key_p,0); // don't create
	if (entry != NULL)
	{
		while(1)
		{
			kv_p = (unsigned char*)entry->kv_p;
			if (kv_p == NULL)
				break;
			offset = point_to_offset(kv_p);

			if (try_hard_lock(offset))
			{
				if (kv_p != (unsigned char*)entry->kv_p)
				{
					hard_unlock(offset);
					continue;
				}

//			copy_node(node,offset_to_node(offset));
			*node = *offset_to_node(offset); //copy node			
			insert_scan_list(offset_to_node(offset),(void*)query);
			hard_unlock(offset);
			sort_node(node,(int*)(query->sorted_index),&(query->index_max));
		
//			query->kv_p = node->buffer;
			query->index_num = 0;			
			pthread_mutex_lock(&query->scan_mutex);
			query->scan_offset = offset;
			pthread_mutex_unlock(&query->scan_mutex);
/*
			while (*((uint64_t*)query->kv_p) < *((uint64_t*)query->key_p))
			{
				if (advance(&(query->kv_p),&(query->offset),node) < 0)
					return 0;
			}
*/
			while (*((uint64_t*)(node->buffer+query->sorted_index[query->index_num])) < *((uint64_t*)query->key_p))
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
					sort_node(node,(int*)(query->sorted_index),&(query->index_max));
					query->index_num=0;
				}
			}
//			dec_ref(offset);
			return 0;
			}

		}
	}
	continue_len = 0;
	while(1)
	{
		range_entry = find_range_entry(query->key_p,&continue_len);
		if (range_entry == NULL)
			continue;
		offset = range_entry->offset;
		if (try_hard_lock(offset))
		{
			if (offset != range_entry->offset)
			{
				hard_unlock(offset);
				offset = 0;
				continue;
			}

//			copy_node(node,offset_to_node(offset));
			*node = *offset_to_node(offset); //copy node			
			insert_scan_list(offset_to_node(offset),(void*)query);
			hard_unlock(offset);
			sort_node(node,(int*)(query->sorted_index),&(query->index_max));
		
//			query->kv_p = node->buffer;
			query->index_num = 0;			
			pthread_mutex_lock(&query->scan_mutex);
			query->scan_offset = offset;
			pthread_mutex_unlock(&query->scan_mutex);
/*
			while (*((uint64_t*)query->kv_p) < *((uint64_t*)query->key_p))
			{
				if (advance(&(query->kv_p),&(query->offset),node) < 0)
					return 0;
			}
*/
			while (*((uint64_t*)(node->buffer+query->sorted_index[query->index_num])) < *((uint64_t*)query->key_p))
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
					sort_node(node,(int*)(query->sorted_index),&(query->index_max));
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
	if (query->scan_offset == TAIL_OFFSET)
	{
//		*result = empty;
		memcpy(result_p,empty,empty_len);		
		*result_len_p = empty_len;
		return 0;
	}
	Node* node = (Node*)query->node;

	*result_len_p = *((uint16_t*)(node->buffer+query->sorted_index[query->index_num]+key_size));
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
	memcpy(result_p,node->buffer+query->sorted_index[query->index_num],*result_len_p);
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
		sort_node(node,(int*)(query->sorted_index),&(query->index_max));
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
	free(query->node);
}
