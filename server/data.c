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

#define print 1
//#define print 0

unsigned char* pmem_addr;
int is_pmem;
size_t pmem_len;
size_t pmem_used;

std::queue <Node*> node_free_list;

pthread_mutex_t alloc_mutex;

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
	return (kv_p - pmem_addr)/sizeof(Node);
}


//---------------------------------------------------------------
Node* alloc_node()
{
	Node* node;
	pthread_mutex_lock(&alloc_mutex);
	if (!node_free_list.empty()) // need lock
	{
		node = node_free_list.front();
		node_free_list.pop();
		pthread_mutex_unlock(&alloc_mutex);
		while(node->ref > 0); //wait
		if (print)
	printf("alloc node %p\n",node); //test
		return node;
	}
	if (pmem_size < pmem_used + sizeof(Node))
		printf("error!!! need more memory]n");

	node = (Node*)(pmem_addr + pmem_used);
	pmem_used += sizeof(Node);
	pthread_mutex_unlock(&alloc_mutex);
	if (print)
	printf("alloc node %p\n",node); //test
	return node;
}
void free_node(Node* node)
{
	node->state = 2; // free
	pthread_mutex_lock(&alloc_mutex);
	node_free_list.push(node);
	pthread_mutex_unlock(&alloc_mutex);
	if (print)
	printf("free node %p\n",node);
}


int data_init()
{
	pthread_mutex_init(&alloc_mutex,NULL);

	pmem_addr = (unsigned char*)pmem_map_file(pmem_file,pmem_size,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);

	if (pmem_addr == NULL)
	{
		printf("pmem error\n");
		return -1;
	}

	pmem_used = 0;

	//insert 0 node
	
	uint64_t zero=0;

	Node* head_node;
	Node* tail_node;
	//push for reserve
	alloc_node(); // 0
	head_node = alloc_node(); // 1 head
	tail_node = alloc_node(); // 2 tail

	Node* node = alloc_node();
	node->state = 0;
	node->size = 0;
	node->ref = 0;
	node->prev_offset = 1;
	node->next_offset = 2;
	pthread_mutex_init(&node->mutex,NULL);
	if (print)
	printf("node 0 %p\n",node);

	head_node->next_offset = 3;
	tail_node->prev_offset = 3;
	head_node->ref = 0;
	head_node->state = 0;
	tail_node->ref = 0;
	tail_node->state = 0;
	pthread_mutex_init(&head_node->mutex,NULL);
	pthread_mutex_init(&tail_node->mutex,NULL);

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
	pthread_mutex_destroy(&alloc_mutex);
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

int inc_ref(Node* node,uint16_t limit)
{
	if (print)
	printf("inc_ref\n");
	pthread_mutex_lock(&node->mutex);
			//node->m.lock();
	if (node->state > limit)
	{
//		node->m.unlock(); // fail
	pthread_mutex_unlock(&node->mutex);	

		return 0;
	}
	node->ref++;
//	node->m.unlock();
	pthread_mutex_unlock(&node->mutex);	
	return 1;
}
void dec_ref(Node* node)
{
	if (print)
	printf("dec_ref\n");
//	node->m.lock();
	pthread_mutex_lock(&node->mutex);	
	node->ref--;
//	node->m.unlock();
	pthread_mutex_unlock(&node->mutex);	
}
int inc_ref(unsigned int offset,uint16_t limit)
{
	return inc_ref(offset_to_node(offset),limit);
}
void dec_ref(unsigned int offset)
{
	return dec_ref(offset_to_node(offset));
}


void print_kv(unsigned char* kv_p)
{
	int i,value_len;
	unsigned char* v_p;
	printf("key ");
	for (i=0;i<8;i++)
		printf("[%d]",(int)(kv_p[i]));
	value_len = *((uint16_t*)(kv_p+key_size));
	if (value_len & (1 << 15))
	{
		printf(" invalidated\n");
		return;
	}
	printf(" value len %d ",value_len);
	v_p = kv_p + key_size + len_size;
	for (i=0;i<value_len;i++)
		printf("[%d]",(int)(v_p[i]));
	printf("\n");
}

void print_node(Node* node)
{
	int cur=0,value_len;
	printf("node size %d\n",node->size);
	while (cur < node->size)
	{
		print_kv(&node->buffer[cur]);
		value_len = *((uint16_t*)(node->buffer+cur+key_size));
		if ((value_len & (1 <<15)) != 0)
			value_len-= (1<<15);
		cur+=value_len+key_size+len_size;
	}
}


//delete
//1 find point
//2 e lock
//3 pointer NULL
//4 length 0
//5 e unlock


void delete_kv(unsigned char* kv_p) // OP may need
{
	*((uint16_t*)(kv_p+key_size))|= (1<<15); // invalidate
}

//insert

int check_size(unsigned int offset,int value_length)
{
	if (print)
		printf("check_size\n");
	int ns;
	Node* node = offset_to_node(offset);
	pthread_mutex_lock(&node->mutex);
	if (node->state > 0)
	{
	pthread_mutex_unlock(&node->mutex);	
		return -2; // node is spliting
	}
	if (node->size + value_length > NODE_BUFFER)
	{
		pthread_mutex_unlock(&node->mutex);
		return -1; // node need split
	}
	node->ref++;
	ns = node->size;
	node->size+=key_size+len_size+value_length;
	*((uint16_t*)(node->buffer+ns+key_size)) = value_length | (1 << 15); // invalidate first
	// fence and flush?
	pthread_mutex_unlock(&node->mutex);	
	if (print)
	printf("inc ref - insert\n");
	return ns;

}

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length,int old_size)
{
	if (print)
	printf("insert kv offset %u\n",offset);
	Node* node;
	unsigned char* kv_p;

	node = offset_to_node(offset); 
	if (print)
	printf("node %p size %d \n",node,(int)old_size);
	/*
	if ((int)node->size + key_size + len_size + value_length > NODE_BUFFER)
	{
		printf("size %d\n",(int)node->size);
		return NULL;
	}
	*/
	*(uint64_t*)(node->buffer + old_size) = *((uint64_t*)key);
	int i;
	for (i=0;i<value_length;i++)
		node->buffer[old_size+key_size+len_size+i] = value[i];
//fence and flush
	*(uint16_t*)(node->buffer + old_size + key_size) = value_length;
	kv_p = node->buffer + old_size;
//	node->size += key_size + len_size + value_length;
	if (print)
	printf("kv_p %p\n",kv_p);
//	print_kv(kv_p);
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
if (print)
	printf("start split offset %d len %d\n",offset,continue_len);

	int i;
	uint16_t size,size1,size2;
	unsigned char* buffer;
	unsigned char buffer1[NODE_BUFFER],buffer2[NODE_BUFFER];
	Node* new_node1;
	Node* new_node2;
	Node* prev_node;
	Node* next_node;
	Node* node;

	node = offset_to_node(offset);

	prev_node = offset_to_node(node->prev_offset);
		next_node = offset_to_node(node->next_offset);

//------------------------------------------------------------ check

//	node->m.lock(); // order?
	pthread_mutex_lock(&node->mutex);	
	node->state = 1; // split
	pthread_mutex_unlock(&node->mutex);
//	node->m.unlock();
	while(node->ref > 0); // spin... wait

//	prev_node->m.lock();
	pthread_mutex_lock(&prev_node->mutex);	


		if (prev_node->state > 0)
		{
//			prev_node->m.unlock();
			pthread_mutex_unlock(&prev_node->mutex);

//			node->m.lock();
			pthread_mutex_lock(&node->mutex);	
			node->state = 0;
//			node->m.unlock();
			pthread_mutex_unlock(&node->mutex);

			return -1; // failed
		}
//			prev_node->m.unlock();
				pthread_mutex_unlock(&prev_node->mutex);


//			continue;
//		e_lock(node); //already locked

//		next_node->m.lock();
			pthread_mutex_lock(&next_node->mutex);	

		if (next_node->state > 0)
		{
//			next_node->m.unlock();
			pthread_mutex_unlock(&next_node->mutex);
//			node->m.lock();
			pthread_mutex_lock(&node->mutex);	
			node->state = 0;
//			node->m.unlock();
			pthread_mutex_unlock(&node->mutex);

			return -1;//failed
		}
//			next_node->m.unlock();
			pthread_mutex_unlock(&next_node->mutex);

if (print)
		printf("locked\n");

		//----------------------------------------------------------

	size = node->size;
	buffer = node->buffer;
//	size = meta & 63; // 00111111

	new_node1 = alloc_node();
	new_node2 = alloc_node();

	size1 = size2 = 0;

	//if 8 align
	uint64_t prefix_64 = *((uint64_t*)prefix),m;
	/*
	uint64_t prefix_64=0;
	for (i=0;i<key_size;i++)
		prefix_64=prefix_64*64 + prefix[i];
		*/

//	point_hash_entry* point_entry;

	m = ~(((uint64_t)1 << (63-continue_len))-1);
	prefix_64 = (prefix_64 & m) | ((uint64_t)1 << (63-continue_len));
if (print)
	printf("pivot %lx m %lx size %d\n",prefix_64,m,size);

	int value_len,cur;
	cur = 0;
	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
		value_len = *((uint16_t*)(buffer+cur+key_size));

		if ((value_len & (1 << 15)) == 0) // valid length		
		{
			if (print)
		printf("pivot %lx key %lx\n",prefix_64,*((uint64_t*)(buffer+cur)));

			if (value_len != 100)
				print_kv(buffer+cur);

			// we use double copy why?
		if (*((uint64_t*)(buffer+cur)) < prefix_64) 
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			print_kv(buffer1+size1); // test
			size1+= key_size + len_size + value_len;
		}
		else
		{
			// rehash later
			memcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			print_kv(buffer2+size2); // test
			size2+= key_size + len_size + value_len;
		}
//		print_kv(buffer+cur);//test
		}
		else
			value_len-= (1 << 15);
		cur+=key_size+len_size+value_len;
	}
if (print)
{
	printf("size %d size1 %d size2 %d\n",size,size1,size2);
	if (size != 220)
	{
		int t;
		scanf("%d",&t);
	}
}

	//why?? fill zero?
	/*
	if (size1 + entry_size < NODE_BUFFER)
		init_entry(buffer1 + size1*entry_size);
	if (size2 + entry_size < NODE_BUFFER)
		init_entry(buffer2 + size2*entry_size);
*/
	//init node meta
	if (print)
	printf("nn1 %p nn2 %p\n",new_node1,new_node2);
	new_node1->state = 0; // 
	new_node1->ref = 0;
	new_node2->state = 0; // 
	new_node2->ref = 0;
	pthread_mutex_init(&new_node1->mutex,NULL);
	pthread_mutex_init(&new_node2->mutex,NULL);

	new_node1->size = size1;
	memcpy(new_node1->buffer,buffer1,size1);

	new_node2->size = size2;
	memcpy(new_node2->buffer,buffer2,size2);

	//need lock
	new_node2->next_offset = node->next_offset;
	new_node2->prev_offset = calc_offset(new_node1);

	new_node1->next_offset = calc_offset(new_node2);
	new_node1->prev_offset = node->prev_offset;


	next_node->prev_offset = calc_offset(new_node2);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node1);

	// flush

//	s_unlock(prev_node);
//	s_unlock(next_node);
//	e_unlock(node); never release e lock!!!
if (print)
printf("rehash\n");
	// rehash
	cur = 0;
	size1 = size2 = 0;
	struct point_hash_entry* point_entry;

	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid
		value_len = *((uint16_t*)(buffer+cur+key_size));
		if ((value_len & (1 << 15)) == 0) // valid length	
		{
		if (*((uint64_t*)(buffer+cur)) < prefix_64) 

//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node1->buffer+size1;
			size1+= key_size + len_size + value_len;
		}
		else
		{
			// rehash 
//			strcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node2->buffer+size2;	
			size2+= key_size + len_size + value_len;
		}
		}
		else
			value_len-= (1 << 15);

		cur+=key_size+len_size+value_len;
	}
if (print)
	printf("insert range\n");
//	b = b << 1 +1;
//	unsigned char sp[8];
	uint64_t sp;	//split prefix??
	uint64_t v;
	sp = *((uint64_t*)prefix);
	v = (uint64_t)1 <<(63-continue_len);
	if (sp & v)
		sp-=v;
	insert_range_entry((unsigned char*)&sp,continue_len+1,calc_offset(new_node1));
	sp|=v;
	insert_range_entry((unsigned char*)&sp,continue_len+1,calc_offset(new_node2));

//	print_node(new_node1);
//	print_node(new_node2);


	free_node(node);//???
	printf("splitend0\n");
	return 0;
}

int compact(unsigned int offset, struct range_hash_entry* range_entry)//,unsigned char* prefix, int continue_len)
{
//	printf("start compaction offset %d len %d\n",offset,continue_len);
if(print)	
	printf("compaction offset %d\n",offset);	

	int i;
	uint8_t size,size1;
	unsigned char* buffer;
	unsigned char buffer1[NODE_BUFFER];
	Node* new_node1;
	Node* prev_node;
	Node* next_node;
	Node* node;

	node = offset_to_node(offset);

	new_node1 = alloc_node();

	prev_node = offset_to_node(node->prev_offset);
		next_node = offset_to_node(node->next_offset);
	
		//------------------------------------------------

		pthread_mutex_lock(&node->mutex);	
	node->state = 1; // split
	pthread_mutex_unlock(&node->mutex);
//	node->m.unlock();
	while(node->ref > 0); // spin... wait

//	prev_node->m.lock();
	pthread_mutex_lock(&prev_node->mutex);	


		if (prev_node->state > 0)
		{
//			prev_node->m.unlock();
			pthread_mutex_unlock(&prev_node->mutex);

//			node->m.lock();
			pthread_mutex_lock(&node->mutex);	
			node->state = 0;
//			node->m.unlock();
			pthread_mutex_unlock(&node->mutex);

			return -1; // failed
		}
//			prev_node->m.unlock();
				pthread_mutex_unlock(&prev_node->mutex);


//			continue;
//		e_lock(node); //already locked

//		next_node->m.lock();
			pthread_mutex_lock(&next_node->mutex);	

		if (next_node->state > 0)
		{
//			next_node->m.unlock();
			pthread_mutex_unlock(&next_node->mutex);
//			node->m.lock();
			pthread_mutex_lock(&node->mutex);	
			node->state = 0;
//			node->m.unlock();
			pthread_mutex_unlock(&node->mutex);

			return -1;//failed
		}
//			next_node->m.unlock();
			pthread_mutex_unlock(&next_node->mutex);
if (print)
		printf("locked\n");

	//-------------------------------------------------------------------	

	size = node->size;
	buffer = node->buffer;
//	size = meta & 63; // 00111111

	size1 = 0;

	int value_len,cur;
	cur = 0;
	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
			value_len = *((uint16_t*)(buffer+cur+key_size));
		if ((value_len & (1 << 15)) == 0) // valid length		
		{
			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
			size1+= key_size + len_size + value_len;
		}
		else
			value_len-= (1 << 15);
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
	new_node1->state = 0; // ??
	new_node1->ref = 0;
	pthread_mutex_init(&new_node1->mutex,NULL);

	new_node1->size = size1;
	memcpy(new_node1->buffer,buffer1,size1);

	new_node1->next_offset = node->next_offset;
	new_node1->prev_offset = node->prev_offset;


	next_node->prev_offset = calc_offset(new_node1);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node1);

	// flush

//	s_unlock(prev_node);
//	s_unlock(next_node);
//	e_unlock(node); never release e lock!!!
if (print)
printf("rehash\n");
	// rehash
	cur = 0;
	size1 = 0;
	struct point_hash_entry* point_entry;

	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid
		value_len = *((uint16_t*)(buffer+cur+key_size));

		if ((value_len & (1 << 15)) == 0) // valid length	
		{	
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node1->buffer+size1;
			size1+= key_size + len_size + value_len;
		}
		else
			value_len -= (1 << 15);
		cur+=key_size+len_size+value_len;
	}

//	printf("insert range\n");
//	insert_range_entry((unsigned char*)prefix,continue_len,calc_offset(new_node1));

	range_entry->offset = calc_offset(new_node1);
	free_node(node);//???
	return 0;

}
