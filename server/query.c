#include "query.h"
#include "hash.h"
#include "data.h"

#ifndef NULL
#define NULL 0
#endif

#include <stdio.h> //test

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

int parse_query(Query* query)
{
//get 0
//lookup 1
//delete 2
//put 3
//insert 4
//update 5
//scan 6
if (print)
printf("parse\n");
	if (query->buffer[0] == 'g' && query->buffer[1] == 'e' && query->buffer[2] == 't')
		query->op = 0;
	else if (query->buffer[0] == 'l' && query->buffer[1] == 'o' && query->buffer[2] == 'o' && query->buffer[3] == 'k' && query->buffer[4] == 'u' && query->buffer[5] == 'p')
		query->op = 1;
	else if (query->buffer[0] == 'd' && query->buffer[1] == 'e' && query->buffer[2] == 'l' && query->buffer[3] == 'e' && query->buffer[4] == 't' && query->buffer[5] == 'e')
		query->op = 2;
	else if (query->buffer[0] == 'p' && query->buffer[1] == 'u' && query->buffer[2] == 't')
		query->op = 3;
	else if (query->buffer[0] == 'i' && query->buffer[1] == 'n' && query->buffer[2] == 's' && query->buffer[3] == 'e' && query->buffer[4] == 'r' && query->buffer[5] == 't')
		query->op = 4;
	else if (query->buffer[0] == 'u' && query->buffer[1] == 'p' && query->buffer[2] == 'd' && query->buffer[3] == 'a' && query->buffer[4] == 't' && query->buffer[5] == 'e')
		query->op = 5;
	else if (query->buffer[0] == 's' && query->buffer[1] == 'c' && query->buffer[2] == 'a' && query->buffer[3] == 'n')
		query->op = 6;
	else
		return -1;

//	printf("query->op %d\n",query->op);


	int i=0,ki;
	for(;i<query->length;i++)
	{
//		printf("[%c]",query->buffer[i]);
		if (query->buffer[i] == ' ')
			break;
	}
//	printf("\n");
	if (query->buffer[i] != ' ')
		return -1;
//	query->key_p = query->buffer+i+1;
	i++;
	ki = i;
//	printf("len %d\n",query->length);

	if (query->op <= 2)
	{
		query->key_len = query->length - i-2; // ??

//		printf("key len %d\n",query->key_len);
		for (i=0;i<8-query->key_len;i++)
					query->key_p[i] = 0;
		for (;i<8;i++)
		{
					query->key_p[i] = query->buffer[ki+i-(8-query->key_len)];
//					printf("%c",query->key_p[i]);
		}
//		printf("\n");
		if (print)
		print_query(query);		
		return 0;
	}
	/*
	for(;i<query->length;i++)
	{
//		printf("[%c]",query->buffer[i]);
		if (query->buffer[i] == ' ')
			break;
	}
//	printf("\n");
//	*/
	i+=key_size;

	if (query->buffer[i] != ' ')
		return -1;
	query->value_p = query->buffer+i+1;

//	query->key_len = query->value_p-query->key_p-1;
	query->key_len = i-ki;	
	query->value_len = query->length - i - 3; // ??

	for (i=0;i<8-query->key_len;i++)
		query->key_p[i] = 0;
	for (;i<8;i++)
		query->key_p[i] = query->buffer[ki+i-(8-query->key_len)];
//printf("query len %d\n",query->length);
//printf("key len %d\n",query->key_len);
//printf("value len %d %c\n",query->value_len,query->value_p[0]);
	// len = 8???
//printf("pe\n");
if (print)
	print_query(query);
	return 0;
}

int lookup_query(Query* query,unsigned char** result,int* result_len)
{
		point_hash_entry* entry;
		unsigned char* kv_p;
		unsigned int offset;
		entry = find_or_insert_point_entry(query->key_p,0); // don't create
		if (entry == NULL)
		{
			*result = empty;
			*result_len = empty_len;
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
				query->offset = offset; // lock ref

				print_kv(kv_p);	

				*result_len = *((uint16_t*)(kv_p+key_size));
				if ((*result_len & (1 << 15)) != 0) // deleted
					break;
				*result = kv_p+key_size+len_size;
//				s_unlock(offset); // it will be released after result
//				break;
				return 0;				
			}
			


//			if (kv_p == (unsigned char*)entry->kv_p)
//				break;
		}
		//deleted
		*result = empty;
		*result_len = empty_len;
		return 0;
}


int delete_query(Query* query,unsigned char** result,int* result_len)
{
		point_hash_entry* entry;
		unsigned char* kv_p;
		unsigned int offset;
		entry = find_or_insert_point_entry(query->key_p,0); // don't create
		if (entry == NULL || entry->kv_p == NULL)
		{
			*result = empty;
			*result_len = empty_len;
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
//				query->offset = offset; // restore ref cnt

				entry->kv_p = NULL; // what should be first?
				delete_kv(kv_p);

//				e_unlock(offset);
				dec_ref(offset);				
				
				*result = empty;
				*result_len = empty_len;

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

int insert_query(Query* query,unsigned char** result,int* result_len)
{
	if (print)
		printf("insert\n");
	*result = empty;
	*result_len = empty_len;


	point_hash_entry* point_entry;
	range_hash_entry* range_entry;
	unsigned char* kv_p;
	unsigned int offset;
	int continue_len;	
	continue_len = 0;
	int rv;
	point_entry = find_or_insert_point_entry(query->key_p,1); // find or create
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
//				if (inc_ref(offset,0) != 0) // init ok
				if ((rv = check_size(offset,query->value_len)) >= -1) // node is not spliting
				{
					break;
				}
//				if (print)
				printf("node is spliting??\n");
			}
		}
		if (offset == 0)
		{
			if (print)
			printf("find node\n");
			while(1)
			{
				range_entry = find_range_entry(query->key_p,&continue_len);
				if (range_entry == NULL) // spliting...
				{
//					sleep(1); //test
//					return -1; //test
//					int t;
//					scanf("%d",&t); // test		
					printf("split collision\n");					
					continue;
				}
				offset = range_entry->offset;
				if ((rv = check_size(offset,query->value_len)) >= -1) // node is not spliting
				{
					break;
				}


			}
			if (print)
			printf("node found offset %d\n",offset);
		}
		//e locked
//		if ((kv_p = insert_kv(offset,query->key_p,query->value_p,query->value_len)) == NULL)
		if (rv >= 0) // node is not spliting we will insert
		{
			point_entry->kv_p = insert_kv(offset,query->key_p,query->value_p,query->value_len,rv); // never fail
			if (kv_p != NULL) // old key
				*((uint16_t*)(kv_p+key_size)) |= (1<<15); // invalidate
//			point_entry->kv_p = kv_p;
			dec_ref(offset);
			break;
		}
		else // rv == -1 and it means we will split
		{
			//failed and need split
			if (range_entry == NULL)
				range_entry = find_range_entry(query->key_p,&continue_len);
			if (continue_len < 64) // split
			{
				if (print)
				printf("split\n");
				range_entry->offset = 1; //splited
				if (split(offset,query->key_p,continue_len)<0)
				{
					printf("split lock failed\n");
	//				e_unlock(offset); //NEVER RELEASE E LOCK unless fail...
				}
//				continue; // try again after split
				if (print)
					printf("split end\n");				
				continue_len++;		
			}
			else if(compact(offset,range_entry) < 0)
// compaction
			{
				printf("compaction lock failed\n");
//				e_unlock(offset);
			}
//int t;
//					scanf("%d",&t);// test
		}
		if (print)
			printf("insert retry\n");				
	}
	return 0;

}


int process_query(Query* query,unsigned char** result,int* result_len)
{
	if (query->op == 0 || query->op == 1) // get // lookup
	{
		return lookup_query(query,result,result_len);
	}
	/*
	else if (query->op == 1) // lookup
	{
	}
	*/

//delete
//1 find point
//2 e lock
//3 pointer NULL
//4 length 0
//5 e unlock

	else if (query->op == 2) // delete
	{
		delete_query(query,result,result_len);
	}
	else if (query->op == 3 || query->op == 4 || query->op == 5) // put // insert //update
	{
		insert_query(query,result,result_len);
	}
	/*
	else if (query->op == 4) // insert
	{
	}
	else if (query->op == 5) // update
	{
	}
	*/
	else if (query->op == 6) // scan
	{
		range_hash_entry* range_entry;
	}
	else
		return -1;
	return 0;
}

void complete_query(Query* query)
{
	if (query->op == 0 || query->op == 1) // get // lookup
	{
//				s_unlock(query->offset);
		dec_ref(query->offset);				
	}
	/*
	else if (query->op == 1) // lookup
	{
	}
	*/
	else if (query->op == 2) // delete
	{
//		dec_ref(query->offset);
	}
	else if (query->op == 3 || query->op == 4 || query->op == 5) // put // insert //update
	{
	}
	/*
	else if (query->op == 4) // insert
	{
	}
	else if (query->op == 5) // update
	{
	}
	*/
	else if (query->op == 6) // scan
	{
	}
	else
		printf("complete query error\n");
	query->op = -1;

}
