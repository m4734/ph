#include<libpmem.h>
#include<queue>

#include <stdio.h> //test
#include <string.h>

#include "data.h"
#include "hash.h"

//extern unsigned long long int pmem_size;
//extern char pmem_file[100];
//extern int key_size;
//extern int value_size;

// we need to traverse linked list when we recover and if it is never visited, it is garbage


unsigned char* pmem_addr;
int is_pmem;
size_t pmem_len;
size_t pmem_used;

std::queue <Node*> node_free_list;

//----------------------------------------------------------------

// OP all

unsigned int calc_offset(Node* node) // it will be optimized with define
{
	return ((unsigned char*)node-pmem_addr)/sizeof(Node);
}
Node* offset_to_node(unsigned int offset) // it will be ..
{
	return (Node*)(pmem_addr + offset*sizeof(Node));
}
void init_entry(unsigned char* entry) // need to be ...
{
	// 8 align
	*((uint64_t*)entry) = 0; 

	// else
	/*
	int i;
	for (i=0;i<key_size;i++)
		entry[i] = 0;
		*/
}

unsigned int point_to_offset(unsigned char* kv_p)
{
	return (kv_p - pmem_addr)/node_size;
}


//---------------------------------------------------------------
Node* alloc_node()
{
	Node* node;

	if (!node_free_list.empty()) // need lock
	{
		node = node_free_list.front();
		node_free_list.pop();
		return node;
	}

	node = (Node*)(pmem_addr + pmem_used);
	pmem_used += sizeof(Node);
	return node;
}
void free_node(Node* node)
{
	node_free_list.push(node);
}


int data_init()
{
	pmem_addr = (unsigned char*)pmem_map_file(pmem_file,pmem_size,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);

	if (pmem_addr == NULL)
	{
		printf("pmem error\n");
		return -1;
	}

	pmem_used = 0;

	//insert 0 node
	
	uint64_t zero=0;

	//push for reserve
	alloc_node(); // 0
	alloc_node(); // 1
	alloc_node(); // 2

	Node* node = alloc_node();
	node->size = 0;
	node->prev_offset = 0;
	node->next_offset = 0;
	printf("node 0 %p\n",node);

	insert_range_entry((unsigned char*)(&zero),0,calc_offset(node)); // the length is important!!!
	int zero2=0;
	if (find_range_entry((unsigned char*)(&zero),&zero2) == NULL)
		printf("range error\n");
	return 0;
}

void data_clean()
{
	/*
	free_node(offset_to_node(0));
	free_node(offset_to_node(1));
	free_node(offset_to_node(2));
*/
	//release free list

	pmem_unmap(pmem_addr,pmem_len);
}

//don't use
/*
int compare(unsigned char* key1,unsigned char* key2)
{
	int i;
	for (i=0;i<key_size;i++)
	{
		if (key1[i] < key2[i])
			return -1;
		else if (key1[i] > key2[i])
			return 1;
	}
	return 0;
}
*/
int try_s_lock(Node* node)
{
	return 1;
}

int try_s_lock(unsigned int offset)
{
	return try_s_lock(offset_to_node(offset));
}

void s_lock(Node* node)
{
}

void s_unlock(Node* node)
{
}

void s_unlock(unsigned int offset)
{
	s_unlock(offset_to_node(offset));
}

int try_e_lock(Node* node)
{
	return 1;
}
int try_e_lock(unsigned int offset)
{
	return try_e_lock(offset_to_node(offset));
}

void e_lock(Node* node)
{
}

void e_unlock(Node* node)
{
}
void e_unlock(unsigned int offset)
{
	e_unlock(offset_to_node(offset));
}

//delete
//1 find point
//2 e lock
//3 pointer NULL
//4 length 0
//5 e unlock


void delete_kv(unsigned char* kv_p) // OP may need
{
	*((uint16_t*)(kv_p+key_size)) = 0;
}

//insert

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length)
{
	printf("insert kv offset %u\n",offset);
	Node* node;
	unsigned char* kv_p;

	node = offset_to_node(offset); 
	printf("node %p \n",node);
	if (node->size + key_size + len_size + value_length > NODE_BUFFER)
	{
		printf("size %d\n",node->size);
		return NULL;
	}
	*(uint64_t*)(node->buffer + node->size) = *((uint64_t*)key);
	*(uint16_t*)(node->buffer + node->size + key_size) = value_length;
	int i;
	for (i=0;i<value_length;i++)
		node->buffer[node->size+key_size+len_size] = value[i];
	kv_p = node->buffer + node->size;
	node->size += key_size + len_size + value_length;
	return kv_p;
}

// 1 prev s lock / e lock / next s lock
// 2 copy to new1 new2
// 3 link
// 4 unlock
//
//1 e lock
//2 copy to new & link
//3 gc old & flush?
//4 unlock
//
//

int split(unsigned int offset,unsigned char* prefix, int continue_len) // locked
{
	int i;
	uint8_t size,size1,size2;
	unsigned char* buffer;
	unsigned char buffer1[NODE_BUFFER],buffer2[NODE_BUFFER];
	Node* new_node1;
	Node* new_node2;
	Node* prev_node;
	Node* next_node;
	Node* node;

	node = offset_to_node(offset);

	new_node1 = alloc_node();
	new_node2 = alloc_node();

	prev_node = offset_to_node(node->prev_offset);

		if (try_s_lock(prev_node) == 0)
			return -1; // failed
//			continue;
//		e_lock(node); //already locked

		next_node = offset_to_node(node->next_offset);
		if (try_s_lock(next_node) == 0)
		{
			s_unlock(prev_node);
			return -1;//failed
		}
		

	size = node->size;
	buffer = node->buffer;
//	size = meta & 63; // 00111111

	size1 = size2 = 0;

	//if 8 align
	uint64_t prefix_64 = *((uint64_t*)prefix);
	/*
	uint64_t prefix_64=0;
	for (i=0;i<key_size;i++)
		prefix_64=prefix_64*64 + prefix[i];
		*/

//	point_hash_entry* point_entry;

	int value_len,cur;
	cur = 0;
	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid
		if (*((uint16_t*)(buffer+cur+key_size)) == 0) // invalid length		
			continue;
		value_len = *((uint16_t*)(buffer+cur+key_size));
		if (*((uint64_t*)(buffer+cur)) < prefix_64) 

//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
			size1+= key_size + len_size + value_len;
		}
		else
		{
			// rehash later
			memcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
			size2+= key_size + len_size + value_len;
		}
		cur+=key_size+len_size+value_len;
	}

	//why?? fill zero?
	/*
	if (size1 + entry_size < NODE_BUFFER)
		init_entry(buffer1 + size1*entry_size);
	if (size2 + entry_size < NODE_BUFFER)
		init_entry(buffer2 + size2*entry_size);
*/
	//init node meta
	new_node1->lock = 0; // ??
	new_node2->lock = 0; // ??

	new_node1->size = size1;
	memcpy(new_node1->buffer,buffer1,size1);

	new_node2->size = size2;
	memcpy(new_node2->buffer,buffer2,size2);

	//need lock
	new_node2->next_offset = node->next_offset;
	new_node2->prev_offset = calc_offset(new_node1);

	new_node1->next_offset = calc_offset(new_node2);
	new_node2->prev_offset = node->prev_offset;


	next_node->prev_offset = calc_offset(new_node2);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node1);

	// flush

	s_unlock(prev_node);
	s_unlock(next_node);
//	e_unlock(node); never release e lock!!!


	// rehash
	cur = 0;
	size1 = size2 = 0;
	struct point_hash_entry* point_entry;

	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid
		if (*((uint16_t*)(buffer+cur+key_size)) == 0) // invalid length		
			continue;
		value_len = *((uint16_t*)(buffer+cur+key_size));
		if (*((uint64_t*)(buffer+cur)) < prefix_64) 

//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = buffer1+size1;
			size1+= key_size + len_size + value_len;
		}
		else
		{
			// rehash later
//			strcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = buffer2+size2;	
			size2+= key_size + len_size + value_len;
		}
		cur+=key_size+len_size+value_len;
	}

//	unsigned char sp[8];
	uint64_t sp;	//split prefix??
	uint64_t v;
	sp = *((uint64_t*)prefix);
	v = 1 <<(63-continue_len-1);
	if (sp & v)
		sp-=v;
	insert_range_entry((unsigned char*)&sp,continue_len+1,calc_offset(new_node1));
	sp|=v;
	insert_range_entry((unsigned char*)&sp,continue_len+1,calc_offset(new_node2));

	free_node(node);//???
	return 0;
}
