#include "query.h"
#include "hash.h"
#include "data.h"

#ifndef NULL
#define NULL 0
#endif

#include <stdio.h> //test

int parse_query(Query* query)
{
//get 0
//lookup 1
//delete 2
//put 3
//insert 4
//update 5
//scan 6

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
		if (query->buffer[i] == ' ')
			break;
	}
	if (query->buffer[i] != ' ')
		return -1;
//	query->key_p = query->buffer+i+1;
	ki = i;

	if (query->op <= 2)
	{
		query->key_len = query->length - i; // ??
		for (i=0;i<8-query->key_len;i++)
					query->key_p[i] = 0;
		for (;i<8-query->key_len;i++)
					query->key_p[i] = query->buffer[ki+i];
		return 0;
	}

	for(;i<query->length;i++)
	{
		if (query->buffer[i] == ' ')
			break;
	}
	if (query->buffer[i] != ' ')
		return -1;
	query->value_p = query->buffer+i+1;

	query->key_len = query->value_p-query->key_p-1;
	query->value_len = query->length - i; // ??

	for (i=9;i<8-query->key_len;i++)
		query->key_p[i] = 0;
	for (;i<8-query->key_len;i++)
		query->key_p[i] = query->buffer[ki+i];

	return 0;
}


int process_query(Query* query,unsigned char** result,int* result_len)
{
	if (query->op == 0 || query->op == 1) // get // lookup
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
			offset = point_to_offset(kv_p);
			if (try_s_lock(offset))
			{
				query->offset = offset;

				*result_len = *((uint16_t*)(kv_p+key_size));
				if (*result_len == 0) // deleted
					break;
				*result = kv_p+key_size+len_size;
//				s_unlock(offset);
				
//				break;
				return 0;				
			}
		}
		//deleted
		*result = empty;
		*result_len = empty_len;
		return 0;
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
			if (try_e_lock(offset))
			{

				entry->kv_p = NULL; // what should be first?
				delete_kv(kv_p);

				e_unlock(offset);
				
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
	else if (query->op == 3 || query->op == 4 || query->op == 5) // put // insert //update
	{
		*result = empty;
		*result_len = empty_len;


		point_hash_entry* point_entry;
		range_hash_entry* range_entry;
		unsigned char* kv_p;
		unsigned int offset;
		int continue_len;	
			continue_len = 0;

		point_entry = find_or_insert_point_entry(query->key_p,1); // find or create

		while(1) // offset can be changed when retry
		{
		offset = 0;
		range_entry = NULL;
		if (point_entry)
		{
			while(1)
			{
				kv_p = (unsigned char*)point_entry->kv_p;
				if (kv_p == NULL) // deleted during !!!
				{
					offset = 0;
					break;
				}
				offset = point_to_offset(kv_p);
				if (try_e_lock(offset))
				{
					break;
				}
			}
		}

		if (offset == 0)
		{
			while(1)
			{
				range_entry = find_range_entry(query->key_p,&continue_len);
				if (range_entry == NULL) // spliting...
					continue;
				offset = range_entry->offset;
				if (try_e_lock(offset))
				{
					break;
				}


			}
		}

		//e locked
		if ((kv_p = insert_kv(offset,query->key_p,query->value_p,query->value_len)) == NULL)
		{
			//failed and need split
			if (range_entry == NULL)
				range_entry = find_range_entry(query->key_p,&continue_len);
			range_entry->offset = 1; //splited
			if (split(offset,query->key_p,continue_len)<0)
				e_unlock(offset); //NEVER RELEASE E LOCK unless fail...
//			continue; // try again after split
		}
		else
		{
			point_entry->kv_p = kv_p;
			e_unlock(offset);
			break;
		}

		}
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
				s_unlock(query->offset);
	}
	/*
	else if (query->op == 1) // lookup
	{
	}
	*/
	else if (query->op == 2) // delete
	{
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

}
