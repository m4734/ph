#include <stdlib.h>

#include <stdio.h> //test

#include "global.h"
#include "hash.h"

namespace PH
{

extern int point_hash_table_size;
extern int range_hash_table_size;

point_hash_entry* point_hash_table;
range_hash_entry** range_hash_table_array;

pthread_mutex_t* point_hash_mutex;
pthread_mutex_t* range_hash_mutex;

// OP it will be no hash until 16

#define print 0
//#define print 0

static unsigned int hash_function(const unsigned char *buf/*,int len*/) // test hash from hiredis
{
	unsigned int hash = 5381;
	int len=8;
	while (len--)
		hash = ((hash << 5) + hash) + (*buf++);
	return hash;
}

struct point_hash_entry* find_or_insert_point_entry(unsigned char* key_p/*,int key_len*/,int insert)
{
	unsigned int hash;
	struct point_hash_entry* pep;
	struct point_hash_entry* ppep;
	if (print)
	printf("start point hash\n");
	hash = hash_function(key_p/*,key_len*/) % point_hash_table_size;
	if (print)
	printf("hash %u\n",hash);
	pep = &(point_hash_table[hash]);
	while(pep != NULL)
	{
		//align 8
		if (print)
		{
		printf("key compare ");
		printf("%lu ",*((uint64_t*)key_p));
		printf("%lu\n",*((uint64_t*)pep->key));
		}
		if (*((uint64_t*)key_p) == *((uint64_t*)pep->key))
			return pep;
		ppep = pep;
		pep = pep->next;
	}
	if (print)
	printf("not found\n");
	if (insert) // need to check double - CAS or advance
	{
//		if (ppep->kv_p == NULL)
		if (ppep->kv_p == (unsigned char*)ppep) // init
		{
//			ppep->kv_p = update; //CAS
			pthread_mutex_lock(&point_hash_mutex[hash]);
			if (ppep->kv_p == (unsigned char*)ppep)
			{		
				ppep->kv_p = NULL;
				*((uint64_t*)ppep->key) = *((uint64_t*)key_p);
				pthread_mutex_unlock(&point_hash_mutex[hash]);
				return ppep;
			}
			pthread_mutex_unlock(&point_hash_mutex[hash]);
		}

		// need CAS
		while(ppep->next)
		{
			ppep = ppep->next;	
			if (*((uint64_t*)key_p) == *((uint64_t*)ppep->key))
				return ppep;
		}
		pthread_mutex_lock(&point_hash_mutex[hash]);
		while(ppep->next)
		{
			ppep = ppep->next;
			if (*((uint64_t*)key_p) == *((uint64_t*)ppep->key))
			{
				pthread_mutex_unlock(&point_hash_mutex[hash]);
				return ppep;
			}
		}

		struct point_hash_entry* new_entry;
		new_entry = (point_hash_entry*)malloc(sizeof(point_hash_entry));
		*((uint64_t*)new_entry->key) = *((uint64_t*)key_p);
		new_entry->kv_p = NULL;
//		new_entry->kv_p = update;
		new_entry->next = NULL;

		ppep->next = new_entry;

		pthread_mutex_unlock(&point_hash_mutex[hash]);

		return new_entry;
	}
	else
		return NULL;
}

struct range_hash_entry* find_range_entry(unsigned char* key_p,int* continue_len)// OP binary
{
	int i;
	unsigned int hash;
	unsigned char prefix[8]={0,};
	struct range_hash_entry* entry;

	uint64_t b;

//	for (i=0;i<*continue_len;i++)
//		b=(b<<1)+1;
//
	if (*continue_len < 64)
	{
	b = ((uint64_t)1 << *continue_len) -1;
	b = b << (64-*continue_len);
	*((uint64_t*)prefix) = (b & *((uint64_t*)key_p));
	}
	else
		*((uint64_t*)prefix) = (*((uint64_t*)key_p));

	b = (uint64_t)1 << (63 -*continue_len);
	if (print)
	printf("key %lx clen %d\n",*((uint64_t*)key_p),*continue_len);
	for (i=*continue_len;i<=64;i++)
	{
//		(*((uint64_t*)prefix)) |= (*((uint64_t*)key_p) & b);
//		b = b >> 1;
		if (print)		
		printf("prefix %lx\n",*((uint64_t*)prefix));
		hash = hash_function(prefix) % range_hash_table_size;
		entry = &(range_hash_table_array[i][hash]);
		if (print)
		{
		printf("hash %u\n",hash);
		printf("entry %p key %lu\n",entry,*((uint64_t*)entry->key));
		}
		while(entry)
		{
			if (print)
			printf("entry %lx prefix %lx\n",*((uint64_t*)entry->key),*((uint64_t*)prefix));
//			int t;
//			scanf("%d",&t);
			if (*((uint64_t*)entry->key) == *((uint64_t*)prefix))
			{
				if (entry->offset == SPLIT_OFFSET) // splited
				{
					break;
				}
				*continue_len = i;
/*
				inc_ref(entry->offset);
				if (entry->offset == 1)
				{
					dec_ref(entry->offset);
					break;
				}
				*/
				return entry;
			}
			entry = entry->next;
		}
		if (!entry) // doesn't exist - spliting... it will be generated next time
		{
			*continue_len=i; // retry same len

			return NULL;
		}
		(*((uint64_t*)prefix)) |= (*((uint64_t*)key_p) & b);
		b = b >> 1;

	}

	//never here
	printf("range hash error\n");
	return NULL;
}


// there will be no double because it use e lock on the split node but still need cas
void insert_range_entry(unsigned char* key_p,int len,unsigned int offset) // need to check double
{
//	if (len >= 64)
//		return;

	unsigned char prefix[8] = {0,};
	int i;
	unsigned int hash;
	struct range_hash_entry* entry;
	struct range_hash_entry* new_entry;

	uint64_t b=0;	
	/*
	for (i=0;i<len;i++)
		b=(b<<1)+1;
		*/

	if (len < 64)
	{
	b = ((uint64_t)1 << len)-1;
	b = b << (64-len);
	
	*((uint64_t*)prefix) = (b & *((uint64_t*)key_p));
	}
	else
		*((uint64_t*)prefix) = (*((uint64_t*)key_p));


if (print)
	printf("key_p %lx range %lx len %d offset %d insert\n",*((uint64_t*)key_p),*((uint64_t*)prefix),len,offset);	

	hash = hash_function(prefix) % range_hash_table_size;

	entry = &(range_hash_table_array[len][hash]);
#if 0
	// DEBUG!!!
	while (entry != NULL)
	{
		if ((entry->offset != INIT_OFFSET) && (*((uint64_t*)entry->key) == *((uint64_t*)prefix)))
		{
			printf("i--------------------------------elfnelnfelknrlk\n");
			scanf("%d",&i);
		}
		entry = entry->next;
	}
#endif
	entry = &(range_hash_table_array[len][hash]);

	if (entry->offset == INIT_OFFSET) // first
	{
		pthread_mutex_lock(&range_hash_mutex[hash]);
		if (entry->offset == INIT_OFFSET)
		{
			if (print)
				printf("0 hash %u\n",hash);
			entry->offset = offset;
			*((uint64_t*)entry->key) = *((uint64_t*)prefix);
			pthread_mutex_unlock(&range_hash_mutex[hash]);
			return;
		}
		pthread_mutex_unlock(&range_hash_mutex[hash]);
	}

	new_entry = (struct range_hash_entry*)malloc(sizeof(struct range_hash_entry));

	new_entry->next = NULL;
	*((uint64_t*)new_entry->key) = *((uint64_t*)prefix);
	new_entry->offset = offset;

	//DEBUG
//	new_entry->c1 = NULL;


//	pthread_mutex_lock(&range_hash_mutex[hash]);


	// USE TAIL!

	while(entry->next)
		entry = entry->next;

	pthread_mutex_lock(&range_hash_mutex[hash]);
	while(entry->next)
		entry = entry->next;
	entry->next = new_entry; //CAS
	pthread_mutex_unlock(&range_hash_mutex[hash]);

}

void init_hash()
{

	int i,j;

	// init point hash
	point_hash_table = (point_hash_entry*)malloc(sizeof(point_hash_entry)*point_hash_table_size);
	point_hash_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)*point_hash_table_size);
	for (i=0;i<point_hash_table_size;i++)
	{
//		point_hash_table[i].kv_p = NULL;
		point_hash_table[i].kv_p = (unsigned char*)&(point_hash_table[i]);		
		point_hash_table[i].next = NULL;
		pthread_mutex_init(&point_hash_mutex[i],NULL);
	}

	range_hash_table_array = (range_hash_entry**)malloc(sizeof(range_hash_entry*)*(64+1));
	for (i=0;i<64+1;i++)
	{
		range_hash_table_array[i] = (range_hash_entry*)malloc(sizeof(range_hash_entry)*range_hash_table_size); // OP it will be no hash until 16
		for (j=0;j<range_hash_table_size;j++)
		{
			range_hash_table_array[i][j].offset = INIT_OFFSET;
		        range_hash_table_array[i][j].next = NULL;
		}
	}

	range_hash_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)*range_hash_table_size);
	for (i=0;i<range_hash_table_size;i++)
		pthread_mutex_init(&range_hash_mutex[i],NULL);
/*
	// insert 0
	uint64_t zero=0;
	unsigned int hash = hash_function(&zero);
	range_hash_table_array[0][hash]->eky
*/
}

void clean_hash()
{
	// clean point hash
	int i,j;
	point_hash_entry* pe;
	point_hash_entry* ppe;
	for (i=0;i<point_hash_table_size;i++)
	{
		pe = point_hash_table[i].next;
		while(pe != NULL)
		{
//			free(e->key);
			ppe = pe;
			pe = pe->next;
			free(ppe);
		}
	}
	free(point_hash_table);

	range_hash_entry* re;
	range_hash_entry* pre;

	for (i=0;i<64;i++)
	{
		for (j=0;j<range_hash_table_size;j++)
		{
//			re = (range_hash_entry[i]+j*sizeof(range_hash_entry)).next;
			re = range_hash_table_array[i][j].next;			
			while(re)
			{
				pre = re;
				re = re->next;
				free(pre);
			}
		}
		free(range_hash_table_array[i]);
	}
	free(range_hash_table_array);
}

}
