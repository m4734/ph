#include <stdlib.h>

#include <stdio.h> //test

#include "global.h"
#include "hash.h"

extern int point_hash_table_size;
extern int range_hash_table_size;

point_hash_entry* point_hash_table;
range_hash_entry** range_hash_table_array;
// OP it will be no hash until 16

static unsigned int hash_function(const unsigned char *buf/*,int len*/) // test hash from hiredis
{
	unsigned int hash = 5381;
	int len=8;
	while (len--)
		hash = ((hash << 5) + hash) + (*buf++);
	return hash;
}

#if 0
char* find_entry_pointer(char* key_p,int key_len)
{
	/*
	unsigned int hash;
	hash = hash_function(key_p,len);
	return point_hash_table[hash%point_hash_table_size]
	*/
}
char* find_node_pointer(char* key_p,int key_len)
{
}

void insert_entry_pointer(char* key_p,int key_len,char* p,int update)
{
}

void insert_node_pointer(char* key_p,int key_len,char* p,int update)
{
}
#endif

struct point_hash_entry* find_or_insert_point_entry(unsigned char* key_p/*,int key_len*/,int insert)
{
	unsigned int hash;
	struct point_hash_entry* pep;
	struct point_hash_entry* ppep;
	printf("start point hash\n");
	hash = hash_function(key_p/*,key_len*/) % point_hash_table_size;
	printf("hash %u\n",hash);
	pep = &(point_hash_table[hash]);
	while(pep != NULL)
	{
		//align 8
		printf("key compare ");
		printf("%lu ",*((uint64_t*)key_p));
		printf("%lu\n",*((uint64_t*)pep->key));
		if (*((uint64_t*)key_p) == *((uint64_t*)pep->key))
		{
//			if (update)
//				pep->kv_p = update;
			return pep;
		}
		ppep = pep;
		pep = pep->next;
	}
	printf("not found\n");
	if (insert) // need to check double - CAS or advance
	{
		if (ppep->kv_p == NULL)
		{
//			ppep->kv_p = update; //CAS

			*((uint64_t*)ppep->key) = *((uint64_t*)key_p);
			return ppep;
		}
		struct point_hash_entry* new_entry;
		new_entry = (point_hash_entry*)malloc(sizeof(point_hash_entry));
		*((uint64_t*)new_entry->key) = *((uint64_t*)key_p);
//		new_entry->kv_p = update;
		new_entry->next = NULL;

		// need CAS
		ppep->next = new_entry;


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
	b = ((uint64_t)1 << *continue_len) -1;
	b = b << (64-*continue_len);
	*((uint64_t*)prefix) = (b & *((uint64_t*)key_p));

	b = (uint64_t)1 << (63 -*continue_len);
	printf("key %lx clen %d\n",*((uint64_t*)key_p),*continue_len);
	for (i=*continue_len;i<64;i++)
	{
//		(*((uint64_t*)prefix)) |= (*((uint64_t*)key_p) & b);
//		b = b >> 1;
		printf("prefix %lx\n",*((uint64_t*)prefix));
		hash = hash_function(prefix) % range_hash_table_size;
		entry = &(range_hash_table_array[i][hash]);
		printf("hash %u\n",hash);
		printf("entry %p key %lu\n",entry,*((uint64_t*)entry->key));
		while(entry)
		{
			printf("entry %lx prefix %lx\n",*((uint64_t*)entry->key),*((uint64_t*)prefix));
//			int t;
//			scanf("%d",&t);
			if (*((uint64_t*)entry->key) == *((uint64_t*)prefix))
			{
				if (entry->offset == 1) // splited
					break;
				*continue_len = i;
				return entry;
			}
			entry = entry->next;
		}
		if (!entry) // doesn't exist - spliting... it will be generated next time
			return NULL;
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

	b = ((uint64_t)1 << len)-1;
	b = b << (64-len);
	
	*((uint64_t*)prefix) = (b & *((uint64_t*)key_p));


	printf("key_p %lx range %lx len %d offset %d insert\n",*((uint64_t*)key_p),*((uint64_t*)prefix),len,offset);	

	hash = hash_function(prefix) % range_hash_table_size;

	entry = &(range_hash_table_array[len][hash]);
	if (entry->offset == 0) // first
	{
		printf("0 hash %u\n",hash);
		entry->offset = offset; //CAS
		*((uint64_t*)entry->key) = *((uint64_t*)prefix);

		return;
	}

	new_entry->next = NULL;
	*((uint64_t*)new_entry->key) = *((uint64_t*)prefix);
	new_entry->offset = offset;

	while(entry->next)
		entry = entry->next;

	entry->next = new_entry; //CAS

}

void init_hash()
{

	int i,j;

	// init point hash
	point_hash_table = (point_hash_entry*)malloc(sizeof(point_hash_entry)*point_hash_table_size);
	for (i=0;i<point_hash_table_size;i++)
	{
		point_hash_table[i].kv_p = NULL;
		point_hash_table[i].next = NULL;
	}

	range_hash_table_array = (range_hash_entry**)malloc(sizeof(range_hash_entry*)*64);
	for (i=0;i<64;i++)
	{
		range_hash_table_array[i] = (range_hash_entry*)malloc(sizeof(range_hash_entry)*range_hash_table_size); // OP it will be no hash until 16
		for (j=0;j<range_hash_table_size;j++)
		{
			range_hash_table_array[i][j].offset = 0;
		        range_hash_table_array[i][j].next = NULL;
		}
	}
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
