#include<libpmem.h>
#include<queue>
#include<atomic>

#include <stdio.h> //test
#include <string.h>
#include <stdlib.h> // malloc
#include <x86intrin.h> // mm fence

#include "data.h"
#include "hash.h"

#include "query.h"
#include "thread.h"

#include <mutex> //test

//extern unsigned long long int pmem_size;
//extern char pmem_file[100];
//extern int key_size;
//extern int value_size;

// we need to traverse linked list when we recover and if it is never visited, it is garbage

#define print 0
//#define print 0

//using namespace PH;

namespace PH
{

unsigned char* pmem_addr;
int is_pmem;
size_t pmem_len;
//size_t pmem_used;
Node* node_data_array;

unsigned char* meta_addr;
uint64_t meta_size;
/*volatile */uint64_t meta_used;
Node_meta* meta_array;

//std::queue <Node_meta*> node_free_list;

//pthread_mutex_t alloc_mutex;

std::atomic <uint8_t> alloc_lock;

volatile unsigned int free_cnt; // free_max // atomic or lock?
volatile unsigned int free_min;
volatile unsigned int free_index;

#define FREE_QUEUE_LEN 1000
unsigned int free_queue[FREE_QUEUE_LEN]; // queue len

uint64_t tt1,tt2,tt3,tt4,tt5; //test
uint64_t qtt1,qtt2,qtt3,qtt4,qtt5,qtt6,qtt7,qtt8;
//----------------------------------------------------------------

// OP all

void at_lock(std::atomic<uint8_t> &lock)
{
	uint8_t z;
	while(true)
	{
		z = 0;
		if (lock.compare_exchange_strong(z,1))
			return;
	}	
}
void at_unlock(std::atomic<uint8_t> &lock)
{
//	return;
	lock = 0;
//	lock.store(0,std::memory_order_release);	
}
int try_at_lock(std::atomic<uint8_t> &lock)
{
//	return 1;
//	const uint8_t z = 0;
	uint8_t z = 0;
//	return lock.compare_exchange_weak(z,1);
/*	
	if (lock == 2) // it can be happen during range entry change
	{
		int t;
		printf("status 2\n");
		scanf("%d",&t);
	}	
	*/
	return lock.compare_exchange_strong(z,1);
//return 1;
}

unsigned int calc_offset(Node_meta* node) // it will be optimized with define
{
	return ((unsigned char*)node-meta_addr)/sizeof(Node_meta);
}
Node_meta* offset_to_node(unsigned int offset) // it will be ..
{
	return &meta_array[offset];
//	return (Node_meta*)(meta_addr + offset*sizeof(Node_meta));
}
Node* offset_to_node_data(unsigned int offset)
{
	return &node_data_array[offset];
//	return &(((Node*)(pmem_addr))[offset]);
//	return (Node*)(pmem_addr + offset*sizeof(Node));
}

unsigned int point_to_offset(unsigned char* kv_p)
{
	return (kv_p - meta_addr)/sizeof(Node_meta);
}

unsigned int data_point_to_offset(unsigned char* kv_p)
{
	return (kv_p - pmem_addr)/sizeof(Node);
}

unsigned int calc_offset_data(void* node) // it will be optimized with define
{
	return ((unsigned char*)node-pmem_addr)/sizeof(Node);
}

//---------------------------------------------------------------
#if 1
void flush_meta(unsigned int offset)
{
//	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),&(((Node_meta*)(meta_addr + offset*sizeof(Node_meta)))->size),sizeof(uint16_t)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

//	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(uint16_t)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
//	_mm_sfence();

	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

//	node_data_array[offset].next_offset = meta_array[offset].next_offset;
//	pmem_persist(&node_data_array[offset],sizeof(unsigned int));

//	memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(uint16_t)+sizeof(unsigned int));

}
#endif
Node_meta* alloc_node()
{
#ifdef dtt
	timespec ts1,ts2; // test
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
	Node_meta* node;

//	pthread_mutex_lock(&alloc_mutex);
	at_lock(alloc_lock);

	if (free_index == free_min)
	{
		int temp;
		temp = min_free_cnt();
		if (temp > free_index) // because it is not volatile
			free_min = temp;
//		free_min = min_free_cnt();
	}

	if (free_index < free_min)
	{
//	if (!node_free_list.empty()) // need lock
	{
//		node = node_free_list.front();
		node = offset_to_node(free_queue[free_index%FREE_QUEUE_LEN]);		
//		node_free_list.pop();
//
//	printf("alloc node %d index %d\n",calc_offset(node),free_index); //test
		++free_index;
//		pthread_mutex_unlock(&alloc_mutex);
		at_unlock(alloc_lock);		
//		while(node->ref > 0); //wait		
		if (print)
	printf("alloc node re %p\n",node); //test
#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	_mm_mfence();
	tt5 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
		return node;
	}
//	else
//		printf("alloc error\n");
	}
	if (meta_size < meta_used + sizeof(Node_meta))
	{
		printf("error!!! need more memory\n");		
		printf("index %d min %d cnt %d\n",free_index,free_min,free_cnt);
		int t;
		scanf("%d",&t);
	}

	node = (Node_meta*)(meta_addr + meta_used);
//	pmem_used += sizeof(Node);
	meta_used += sizeof(Node_meta);	
//	pthread_mutex_unlock(&alloc_mutex);
	at_unlock(alloc_lock);	
	if (print)
	printf("alloc node %p\n",node); //test
//	printf("alloc node %d\n",calc_offset(node)); //test

//	pthread_mutex_init(&node->mutex,NULL);
	node->state = 0;//need here? yes because it was 2 // but need here?	
#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	_mm_mfence();
	tt5 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
	return node;
}
void free_node(Node_meta* node)
{
//	pthread_mutex_lock(&node->mutex);
		
	node->state = 2; // free // without mutex?? // not need really
//	pthread_mutex_unlock(&node->mutex);
//	if (node->ref == 0)
//	{
//		pthread_mutex_lock(&alloc_mutex);
		while(1) // test
		{
			if (free_index + FREE_QUEUE_LEN < free_cnt)
			{
				printf("queue full %d %d\n",free_index,free_cnt);
				print_thread_info();
				int t;
				scanf("%d",&t);
				continue;
			}
		at_lock(alloc_lock);		
//		node_free_list.push(node);
//
		free_queue[free_cnt%FREE_QUEUE_LEN] = calc_offset(node);	
//	printf("free node %d cnt %d\n",calc_offset(node),free_cnt);
		++free_cnt;
//		printf("aa %d a\n",free_cnt);
//		pthread_mutex_unlock(&alloc_mutex);
		at_unlock(alloc_lock);	
		break;
		}
//	}
	if (print)
	printf("free node %p\n",node);
}


int init_data() // init hash first!!!
{
//	pthread_mutex_init(&alloc_mutex,NULL); // moved to thread.c

	if (USE_DRAM)
		pmem_addr=(unsigned char*)mmap(NULL,pmem_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS/*|MAP_POPULATE*/,-1,0);

	else
		pmem_addr = (unsigned char*)pmem_map_file(pmem_file,pmem_size,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);
	node_data_array = (Node*)pmem_addr;

	meta_size = pmem_size/sizeof(Node)*sizeof(Node_meta);
	meta_addr = (unsigned char*)mmap(NULL,meta_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);
	meta_array = (Node_meta*)meta_addr;

	if (pmem_addr == NULL)
	{
		printf("pmem error\n");
		return -1;
	}

//	pmem_used = 0;
	meta_used = 0;

	//insert 0 node
	//

	free_cnt = free_min = free_index = 0;

	uint64_t zero=0;

	Node_meta* head_node;
	Node_meta* tail_node;

	update_free_cnt(); // temp main thread

	//push for reserve
	alloc_node(); // 0
	head_node = alloc_node(); // 1 head
	tail_node = alloc_node(); // 2 tail

	Node_meta* node = alloc_node();
	node->state = 0;
	node->size = 0;
	node->ic = 0;
//	node->ref = 0;
	node->scan_list = NULL;
	node->prev_offset = HEAD_OFFSET;
	node->next_offset = TAIL_OFFSET;
//	pthread_mutex_init(&node->mutex,NULL);
	if (print)
	printf("node 0 %p\n",node);

	head_node->next_offset = 3;
	tail_node->prev_offset = 3;
//	head_node->ref = 0;
	head_node->state = 0;
	head_node->scan_list = NULL;
//	tail_node->ref = 0;
	tail_node->state = 0;
	tail_node->scan_list = NULL;
//	pthread_mutex_init(&head_node->mutex,NULL);
//	pthread_mutex_init(&tail_node->mutex,NULL);

	flush_meta(HEAD_OFFSET);
	flush_meta(TAIL_OFFSET);
	_mm_sfence();

//	inc_ref(HEAD_OFFSET,0);
//	inc_ref(TAIL_OFFSET,0); //don't free these node	

	insert_range_entry((unsigned char*)(&zero),0,calc_offset(node)); // the length is important!!!


	exit_thread(); // remove main thread

	//test code-------------------


//	int zero2=0;
//	if (find_range_entry((unsigned char*)(&zero),&zero2) == NULL)
//		printf("range error\n");

	if (USE_DRAM)
		printf("USE_DRAM\n");
	else
		printf("USE_PM\n");
	printf("size of node %ld\n",sizeof(Node));
	printf("size of node_meta %ld\n",sizeof(Node_meta));

	tt1 = 0;
	tt2 = 0;
	tt3 = 0;
	tt4 = tt5 = 0;

	qtt1 = qtt2 = qtt3 = qtt4 = qtt5 = qtt6 = qtt7 = qtt8 = 0;

	return 0;
}

void clean_data()
{
	/*
	free_node(offset_to_node(0));
	free_node(offset_to_node(1));
	free_node(offset_to_node(2));
*/
	//release free list

	if (USE_DRAM)
		munmap(pmem_addr,pmem_size);
	else
		pmem_unmap(pmem_addr,pmem_size);
	munmap(meta_addr,meta_size);
//	pthread_mutex_destroy(&alloc_mutex); // moved to ...

	//destroy all node mutex
	//
#ifdef dtt
	printf("data\n");
	printf("insert %ld %ld\n",tt1/1000000000,tt1%1000000000);
	printf("split %ld %ld\n",tt2/1000000000,tt2%1000000000);
	printf("compact %ld %ld\n",tt3/1000000000,tt3%1000000000);
	printf("check_size %ld %ld\n",tt4/1000000000,tt4%1000000000);
	printf("alloc node %ld %ld\n",tt5/1000000000,tt5%1000000000);
#endif

	printf("used %ld size %ld\n",(uint64_t)meta_used/sizeof(Node_meta),(uint64_t)meta_used/sizeof(Node_meta)*sizeof(Node));
	printf("index %d min %d cnt %d\n",free_index,free_min,free_cnt);

	//query test
#ifdef qtt

	printf("query\n");
	printf("insert query %ld %ld\n",qtt1/1000000000,qtt1%1000000000);
	printf("insert index %ld %ld\n",qtt2/1000000000,qtt2%1000000000);
	printf("insert data %ld %ld\n",qtt3/1000000000,qtt3%1000000000);
	printf("insert kv %ld %ld\n",qtt4/1000000000,qtt4%1000000000);
	printf("split kv %ld %ld\n",qtt5/1000000000,qtt5%1000000000);
	printf("\n");
	printf("lookup query %ld %ld\n",qtt8/1000000000,qtt8%1000000000);
	printf("lookup index %ld %ld\n",qtt6/1000000000,qtt6%1000000000);
	printf("lookup data %ld %ld\n",qtt7/1000000000,qtt7%1000000000);

#endif

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
/*
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
*/
int inc_ref(Node_meta* node)
{
//	printf("inc_ref %d\n",calc_offset(node));
	 // it can be by multiple insert on same node
	 /*
	if (node->state > 1)
	{
		int t;
		printf("inc ref error\n");
		scanf("%d",&t);
	}
	*/
//	if (node->state == 2)
//		printf("state 2 %d\n",calc_offset(node));
	return try_at_lock(node->state);
	/*
	if (print)
	printf("inc_ref\n");
//	pthread_mutex_lock(&node->mutex);
			//node->m.lock();

	if (node->state > limit)
	{
//		node->m.unlock(); // fail
//	pthread_mutex_unlock(&node->mutex);	

		return 0;
	}
	
	node->ref++;
	if (node->state > limit)
	{
		node->ref--;
		return 0;
	}
//	node->m.unlock();
//	pthread_mutex_unlock(&node->mutex);	
	return 1;
	*/
}
void dec_ref(Node_meta* node)
{
//	printf("dec_ref %d\n",calc_offset(node));
/*	
	if (node->state != 1)
	{
		int t;
		printf("dec ref error\n");
		scanf("%d",&t);
	}
	*/
	at_unlock(node->state);
#if 0
	if (print)
	printf("dec_ref\n");
//	node->m.lock();
//	pthread_mutex_lock(&node->mutex);	
/*	
	if (node->ref == 0)
	{
		printf("ref error\n");
		int t;
		scanf("%d",&t);
	}
	*/
	node->ref--;
//	node->m.unlock();
//	pthread_mutex_unlock(&node->mutex);	
	/*
	if (node->state == 2 && node->ref == 0)
	{
		pthread_mutex_lock(&alloc_mutex);
		node_free_list.push(node);
		pthread_mutex_unlock(&alloc_mutex);
	}
	*/
#endif
}
int inc_ref(unsigned int offset) // nobody use this
{
	return inc_ref(offset_to_node(offset));
}
void dec_ref(unsigned int offset)
{
//	((Node_meta*)(meta_addr + offset*sizeof(Node_meta)))->state = 0;

	return dec_ref(offset_to_node(offset));
}

int try_hard_lock(Node_meta* node) // need change
{
	/*
//	pthread_mutex_lock(&node->mutex);
	if (node->state > 0)
		return 0;
	while(node->ref>0);
	return 1;
	*/
	return try_at_lock(node->state);
}
void hard_unlock(Node_meta* node) // need change
{
//	pthread_mutex_unlock(&node->mutex);
	at_unlock(node->state);	
}

int try_hard_lock(unsigned int offset)
{
	return try_hard_lock(offset_to_node(offset));
}

void hard_unlock(unsigned int offset)
{
	hard_unlock(offset_to_node(offset));
}

void soft_lock(Node_meta* node) // need change
{
//	pthread_mutex_lock(&node->mutex);
}

void soft_lock(unsigned int offset)
{
	soft_lock(offset_to_node(offset));
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
#if 0
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
#endif

//delete
//1 find point
//2 e lock
//3 pointer NULL
//4 length 0
//5 e unlock


void delete_kv(unsigned char* kv_p) // OP may need
{
	if (USE_DRAM)
	*((uint16_t*)(kv_p/*+key_size*/))|= (1<<15); // invalidate
	else
	{
		uint16_t vl16;
		vl16 = *((uint16_t*)(kv_p/*+key_size*/)) | (1<<15);
		pmem_memcpy(kv_p/*+key_size*/,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}
}

//insert

int check_size(unsigned int offset,int value_length)
{
#ifdef dtt
	timespec ts1,ts2; // test
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif

	if (print)
		printf("check_size\n");
//	uint16_t new_size,vl16;
	Node_meta* node = offset_to_node(offset);
//	pthread_mutex_lock(&node->mutex);
/*	
	if (node->state > 0)
	{
//	pthread_mutex_unlock(&node->mutex);
#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt4 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

		return -2; // node is spliting
	}
	*/
//	do
//	{
	const int es = key_size + len_size + value_length;
	const int ns = node->size;
	if (ns + es > NODE_BUFFER)
	{
//		node->ref++; // i will try split
//		pthread_mutex_unlock(&node->mutex);
#ifdef dtt
		_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt4 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

		return -1; // node need split
	}
//	node->ref++;
//	}while (node->size.compare_exchange_weak(ns,ns + key_size+len_size+value_length) == 0);
	node->size+= es;	
	// fence and flush?
	// don't do this now
	/*
	if (!USE_DRAM)
	{
//		*((uint16_t*)(node->buffer+ns+key_size)) = value_length | (1 << 15); // invalidate first
		if (print)
			printf("PM size first\n");
		Node* node_data = offset_to_node_data(offset);		
		vl16 = value_length | (1 << 15);		
		pmem_memcpy(node_data->buffer+ns+key_size,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
		pmem_memcpy((void*)&node_data->size,&new_size,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}
//	else
//	*/
//		node->size = new_size;


//	pthread_mutex_unlock(&node->mutex);	
	if (print)
	printf("inc ref - insert\n");
#ifdef dtt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt4 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

	return ns;

}

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length,int old_size)
{
#ifdef dtt
	timespec ts1,ts2; // test
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif

	if (print)
	printf("insert kv offset %u\n",offset);
//	Node* node_data;
//	unsigned char* kv_p;
//	uint16_t vl16 = value_length;
	uint16_t vl16 = value_size;
//	int new_size = old_size;	

//	node_data = offset_to_node_data(offset); 

	unsigned char* const buffer = offset_to_node_data(offset)->buffer;

//	if (print)
//	printf("node %p size %d \n",node_data,(int)old_size);
	/*
	if ((int)node->size + key_size + len_size + value_length > NODE_BUFFER)
	{
		printf("size %d\n",(int)node->size);
		return NULL;
	}
	*/
#if 1
	if (!USE_DRAM)
	{
#if 1

//		unsigned char buffer[NODE_BUFFER];
//		memcpy(buffer,key,key_size);
//		memcpy(buffer+key_size,&vl16,len_size);
//		memcpy(buffer+key_size+len_size,value,value_length);
/*		
		pmem_memcpy(node_data->buffer+old_size,key,key_size,PMEM_F_MEM_NONTEMPORAL);
		pmem_memcpy(node_data->buffer+old_size+key_size,&vl16,len_size,PMEM_F_MEM_NONTEMPORAL);
		pmem_memcpy(node_data->buffer+old_size+key_size+len_size,value,value_length,PMEM_F_MEM_NONTEMPORAL);
		*/
		memcpy(buffer+old_size+len_size,key,key_size);
//		memcpy(node_data->buffer+old_size+key_size,&vl16,len_size);
		memcpy(buffer+old_size+key_size+len_size,value,value_length);

		pmem_persist(buffer+old_size+len_size,key_size+value_length);


//		pmem_memcpy(node_data->buffer+old_size,buffer,key_size+len_size+value_length,PMEM_F_MEM_NONTEMPORAL);
//		memcpy(node_data->buffer+old_size,buffer,key_size+len_size+value_length);

		_mm_sfence();

		pmem_memcpy(buffer+old_size,&vl16,len_size,PMEM_F_MEM_NONTEMPORAL);


//i		vl16 = old_size + key_size+len_size+value_length;
//		pmem_memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL); // we will remove this
		_mm_sfence();
//		memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t)); // we will remove this
#endif
#if 0
		memcpy(node_data->buffer+new_size,&vl16,len_size);
		new_size+=len_size;
		memcpy(node_data->buffer+new_size,key,key_size);
		new_size+=key_size;
		memcpy(node_data->buffer+new_size,value,value_length);
		new_size+=value_length;

		pmem_persist(node_data->buffer+old_size,key_size+len_size+value_length);

		_mm_sfence();

		pmem_memcpy((void*)&node_data->size,&new_size,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL); // we will remove this
//		memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t)); // we will remove this

//		pmem_persist((void*)&node_data->size,sizeof(uint16_t)); // we will remove this
		_mm_sfence();
//		memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t)); // we will remove this
#endif

/*
		pmem_memcpy(node_data->buffer + old_size,key,sizeof(uint64_t),PMEM_F_MEM_NONTEMPORAL); //write key
		pmem_memcpy(node_data->buffer + old_size + key_size + len_size, value, value_length, PMEM_F_MEM_NONTEMPORAL); //write value
		_mm_sfence();
		pmem_memcpy(node_data->buffer + old_size + key_size,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL); // validate
		_mm_sfence();
		*/
	}
	else
	{
		memcpy(buffer+old_size+len_size,key,key_size);
		memcpy(buffer+old_size+key_size+len_size,value,value_length);

//		_mm_sfence();

		memcpy(buffer+old_size,&vl16,len_size);


//i		vl16 = old_size + key_size+len_size+value_length;
//		pmem_memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL); // we will remove this
//		_mm_sfence();
#if 0
		*(uint64_t*)(node_data->buffer + old_size) = *((uint64_t*)key);
		/*
		int i;
		for (i=0;i<value_length;i++)
			node->buffer[old_size+key_size+len_size+i] = value[i];
			*/
		memcpy(node_data->buffer + old_size + key_size + len_size, value, value_length);
		*((uint16_t*)(node_data->buffer + old_size + key_size)) = vl16;
#endif
	}
#endif
//fence and flush
//fence
//	*(uint16_t*)(node->buffer + old_size + key_size) = value_length;
//fence flush
//	kv_p = buffer + old_size;
//	node->size += key_size + len_size + value_length;
//	if (print)
//	printf("kv_p %p\n",kv_p);
//	print_kv(kv_p);
//
#ifdef dtt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt1+= (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
//
//	return kv_p;
	return buffer+old_size;	
}

void move_scan_list(Node_meta* node_old,Node_meta* node_new)
{
	if (node_old->scan_list == NULL)
	{
		node_new->scan_list = NULL;
		return;
	}
	Scan_list* sl;
	Scan_list** slp;
	int old_offset,new_offset;
	Query* query;
	old_offset = point_to_offset((unsigned char*)node_old);
	new_offset = point_to_offset((unsigned char*)node_new);
	sl = node_old->scan_list;
	slp = &(node_old->scan_list);
	while(sl)
	{
		query = (Query*)(sl->query);
//		pthread_mutex_lock(&query->scan_mutex);
		at_lock(query->scan_lock);		
		if (query->scan_offset == old_offset)
		{
			query->scan_offset = new_offset;
//			pthread_mutex_unlock(&query->scan_mutex);
			at_unlock(query->scan_lock);			
			slp = &(sl->next);
			sl = sl->next;
		}
		else
		{
//			pthread_mutex_unlock(&query->scan_mutex);
			at_unlock(query->scan_lock);			
			printf("abandoned scan entry??\n");
			*slp = sl->next;
			free(sl);
			sl = *slp;
		}
	}
	node_new->scan_list = node_old->scan_list;
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
/*
struct len_and_key
{
	uint16_t len;
	uint64_t key; // may be chagned
};
*/
int split(unsigned int offset,unsigned char* prefix, int continue_len) // locked
{
#ifdef dtt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif

static int ec=0;

if (print)
	printf("start split offset %d len %d\n",offset,continue_len);

	int i;
	uint16_t size;//,size1,size2;
	unsigned char* buffer;
//	unsigned char buffer0[NODE_BUFFER];	
	unsigned char* buffer1;
	unsigned char* buffer2;

	int tc;
	uint64_t temp_key[100];
//	unsigned char* temp_kvp[100];
//	int temp_len[100];
	ValueEntry vea[100];	

//	unsigned char buffer1[NODE_BUFFER],buffer2[NODE_BUFFER];
	Node_meta* new_node1;
	Node_meta* new_node2;
	Node_meta* prev_node;
	Node_meta* next_node;
	Node_meta* node;

	Node* node_data;
	Node* new_node1_data;
	Node* new_node2_data;
//	Node* prev_node_data;


	node = offset_to_node(offset);
	node_data = offset_to_node_data(offset);

	prev_node = offset_to_node(node->prev_offset);
	next_node = offset_to_node(node->next_offset);

//	prev_node_data = offset_to_node_data(node->prev_offset);

//------------------------------------------------------------ check

//	node->m.lock(); // order?
	/*
	pthread_mutex_lock(&node->mutex);	
	if (node->state > 0)
	{
		pthread_mutex_unlock(&node->mutex);
//		printf("ref1 %d %d %d\n",offset,node->state,node->ref);
		return -1;
	}
	node->state = 1; // split
	pthread_mutex_unlock(&node->mutex);
	*/
// we already have lock from inc_ref
//	if (try_at_lock(node->state) == 0)
//		return -1;

//	node->m.unlock();
//	while(node->ref > 1); // spin... wait except me
/*
	if (node->ref > 1) // read may
	{
		while(1)
			printf("xxxxxx");
	}
	*/
// may not need lock
//	prev_node->m.lock();
//	pthread_mutex_lock(&prev_node->mutex);	

// check prev next node
		if (prev_node->state > 0)
		{
//			prev_node->m.unlock();
//			pthread_mutex_unlock(&prev_node->mutex);

//			node->m.lock();
//			pthread_mutex_lock(&node->mutex);	
//			node->state = 0;
//			node->m.unlock();
//			pthread_mutex_unlock(&node->mutex);
printf("split offset %d prev %d state %d\n",offset,node->prev_offset,(int)prev_node->state);
ec++;
if (ec >= 1000)
{
	printf("ec 1000\n");
	scanf("%d",&ec);
}
			return -1; // failed
		}
//			prev_node->m.unlock();
//	pthread_mutex_unlock(&prev_node->mutex);


//			continue;
//		e_lock(node); //already locked

//		next_node->m.lock();
//	pthread_mutex_lock(&next_node->mutex);	

//		_mm_lfence();

		if (next_node->state > 0)
		{
//			next_node->m.unlock();
//			pthread_mutex_unlock(&next_node->mutex);
//			node->m.lock();
//			pthread_mutex_lock(&node->mutex);	
//			node->state = 0;
//			node->m.unlock();
//			pthread_mutex_unlock(&node->mutex);
printf("split offset %d next %d state %d\n",offset,node->next_offset,(int)next_node->state);
ec++;
if (ec >= 1000)
{
	printf("ec 1000\n");
	scanf("%d",&ec);
}
/*
			while (next_node->state > 0) // spin // ...ok?
				next_node = offset_to_node(node->next_offset);
				*/
/*
			struct timespec tsl0,tsl1;
			clock_gettime(CLOCK_MONOTONIC,&tsl0);
			while(next_node->state > 0)
			{
				next_node = offset_to_node(node->next_offset);
				clock_gettime(CLOCK_MONOTONIC,&tsl1);
				if ((tsl1.tv_sec-tsl0.tv_sec)*1000000000-tsl1.tv_nsec-tsl0.tv_nsec > 1000000)
				{
					printf("too long %d %d %d\n",offset,node->next_offset,(int)next_node->state);
					int t;
					scanf("%d",&t);
				}
			}
			*/
			while(next_node->state > 0)
{
	next_node = offset_to_node(node->next_offset);
	if (check_slow())
	{
		printf("in loop!!\n");
		int t;
		scanf("%d",&t);
	}
}

//			return -1;//failed
		}
//			next_node->m.unlock();
//	pthread_mutex_unlock(&next_node->mutex);

if (print)
		printf("locked\n");
//ec = 0;
		//----------------------------------------------------------

	size = node->size;
//	buffer = node_data->buffer;

	new_node1 = alloc_node();
	new_node2 = alloc_node();

	new_node1->state = 0; // 
//	new_node1->ref = 0;
	new_node1->scan_list = NULL;
	new_node2->state = 0; // 
//	new_node2->ref = 0;
	new_node2->scan_list = NULL;
//	pthread_mutex_init(&new_node1->mutex,NULL);
//	pthread_mutex_init(&new_node2->mutex,NULL);

	new_node1->ic = new_node2->ic = 0;

//	new_node1->size = size1;

//	new_node2->size = size2;

	//need lock
	new_node2->next_offset = node->next_offset;
	new_node2->prev_offset = calc_offset(new_node1);

	new_node1->next_offset = calc_offset(new_node2);
	new_node1->prev_offset = node->prev_offset;


	move_scan_list(node,new_node2);

//printf("----------------\n%d %d %d\n %d %d %d %d\n-------------------\n",node->prev_offset,offset,node->next_offset,new_node1->prev_offset,calc_offset(new_node1),calc_offset(new_node2),new_node2->next_offset);

	_mm_sfence();

	next_node->prev_offset = calc_offset(new_node2);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node1);

	// flush



//	size1 = size2 = 0;

	new_node1_data = offset_to_node_data(calc_offset(new_node1));
	new_node2_data = offset_to_node_data(calc_offset(new_node2));

	memcpy(new_node1_data,new_node1,sizeof(unsigned int));
	memcpy(new_node2_data,new_node2,sizeof(unsigned int));



	int j,temp;
	for (i=0;i<node->ic;i++)
	{
		temp = 0;
		for (j=0;j<node->ic-i-1;j++)
		{
			if (node->inv_kv[j] > node->inv_kv[j+1])
			{
				temp = node->inv_kv[j+1];
				node->inv_kv[j+1] = node->inv_kv[j];
				node->inv_kv[j] = temp;
			}
		}
		if (temp == 0)
			break;
	}

	//if 8 align
	uint64_t prefix_64 = *((uint64_t*)prefix),m;
	/*
	uint64_t prefix_64=0;
	for (i=0;i<key_size;i++)
		prefix_64=prefix_64*64 + prefix[i];
		*/

//	point_hash_entry* point_entry;
/*
	if (continue_len == 0)
		m = 0;
	else
		m = ~(((uint64_t)1 << (64-continue_len))-1);
		*/
	m = ~(((uint64_t)1 << (63-continue_len))-1); // it seems wrong but ok because of next line it is always overwrited

	prefix_64 = (prefix_64 & m) | ((uint64_t)1 << (63-continue_len));
if (print)
	printf("pivot %lx m %lx size %d\n",prefix_64,m,size);
#if 1
	uint16_t value_len;
	const int kls = key_size + len_size;
	uint16_t kvs;
//	int cur;
//	cur = 0;
	buffer = node_data->buffer;
//	memcpy(buffer0,node_data->buffer,NODE_BUFFER);
//	buffer = buffer0;

	buffer1 = new_node1_data->buffer;
	buffer2 = new_node2_data->buffer;
	unsigned char* const be = buffer+size;
	tc = 0;

	const unsigned int new_offset1 = calc_offset(new_node1);
	const unsigned int new_offset2 = calc_offset(new_node2);

//	struct len_and_key lak;

	j = 0;
	node->inv_kv[node->ic] = 0;
	while(buffer < be)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		memcpy(&lak,buffer,kls);
		value_len = *((uint16_t*)(buffer/*+cur+key_size*/));
//		kvs = kls + lak.len;
		kvs = kls + value_len;
//		if ((lak.len & (1 << 15)) == 0)
		if ((unsigned char*)node_data + node->inv_kv[j] != buffer)		
//		if ((value_len & (1 << 15)) == 0) // valid length		
		{
			if (print)
				printf("pivot %lx key %lx\n",prefix_64,*((uint64_t*)(buffer)));

//			if (value_len != 100)
//				print_kv(buffer+cur);

//			temp_key[tc] = lak.key;
			temp_key[tc] = *((uint64_t*)(buffer+len_size));		
//		temp_len[tc] = value_len;	
			// we use double copy why?
//			if (*((uint64_t*)(buffer+len_size)) < prefix_64)
			if (temp_key[tc] < prefix_64)			
//			if (lak.key < prefix_64)	
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
			{
			// rehash later
//			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node1_data->buffer+size1,buffer,kvs);
				memcpy(buffer1,buffer,kvs);			
//			print_kv(buffer1+size1); // test
//			size1+= kvs;
//			insert_point_entry(buffer+len_size,buffer1); // can we do this here?
//			temp_kvp[tc] = buffer1;
			vea[tc].node_offset = new_offset1;
			vea[tc].kv_offset = buffer1-(unsigned char*)new_node1_data;


				buffer1+=kvs;			
			}
			else
			{
			// rehash later
//			memcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node2_data->buffer+size2,buffer,kvs);
				memcpy(buffer2,buffer,kvs);			
//			print_kv(buffer2+size2); // test
//			size2+= kvs;
//			insert_point_entry(buffer+len_size,buffer2); // can we do this here?
//			temp_kvp[tc] = buffer2;
			vea[tc].node_offset = new_offset2;
			vea[tc].kv_offset = buffer2-(unsigned char*)new_node2_data;

				buffer2+=kvs;			
			}
//			vea[tc].len = lak.len;
			vea[tc].len = value_len;		
			tc++;
//		print_kv(buffer+cur);//test
		}
		else
		{
			j++;
//			value_len-= (1 << 15);
//			kvs-= (1 << 15);			
			kvs&= ~((uint16_t)1 << 15);			
		}
//		cur+=key_size+len_size+value_len;
		buffer+=kvs;		
	}

	new_node1->size = buffer1-new_node1_data->buffer;
	new_node2->size = buffer2-new_node2_data->buffer;
#endif
	//why?? fill zero?
	/*
	if (size1 + entry_size < NODE_BUFFER)
		init_entry(buffer1 + size1*entry_size);
	if (size2 + entry_size < NODE_BUFFER)
		init_entry(buffer2 + size2*entry_size);
*/
	//init node meta
	if (print)
	printf("nn1 %p nn2 %p next %p prev %p\n",new_node1,new_node2,next_node,prev_node);
#if 0
//	if (size1 > 0 && size2 > 0)
	
	new_node1->state = 0; // 
//	new_node1->ref = 0;
	new_node1->scan_list = NULL;
	new_node2->state = 0; // 
//	new_node2->ref = 0;
	new_node2->scan_list = NULL;
//	pthread_mutex_init(&new_node1->mutex,NULL);
//	pthread_mutex_init(&new_node2->mutex,NULL);


	new_node1->size = size1;

	new_node2->size = size2;

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


	move_scan_list(node,new_node2);

//	new_node1_data = offset_to_node_data(calc_offset(new_node1));
//	new_node2_data = offset_to_node_data(calc_offset(new_node2));
#endif
#if 1
	if (!USE_DRAM) //data copy and link node
	{
//		pmem_memcpy((unsigned char*)new_node1+sizeof(pthread_mutex_t),(unsigned char*)&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
//
/*
		flush_meta(calc_offset(new_node1));
		pmem_memcpy(new_node1_data->buffer,buffer1,size1,PMEM_F_MEM_NONTEMPORAL);
		*/
		pmem_persist(new_node1_data,sizeof(unsigned int)+new_node1->size);//sizeof(new_node1_data));
//		memcpy(new_node1_data->buffer,buffer1,size1);

//pmem_memcpy((unsigned char*)new_node2+sizeof(pthread_mutex_t),(unsigned char*)&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
/*
		flush_meta(calc_offset(new_node2));
		pmem_memcpy(new_node2_data->buffer,buffer2,size2,PMEM_F_MEM_NONTEMPORAL);
		*/
		pmem_persist(new_node2_data,sizeof(unsigned int)+new_node2->size);//sizeof(new_node2_data));
//		memcpy(new_node2_data->buffer,buffer2,size2);

/*
		unsigned int to;
		to = calc_offset(new_node2);
		pmem_memcpy(&next_node->prev_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		*/
//	next_node->prev_offset = calc_offset(new_node2);

// flush

		_mm_sfence();
		/*
		unsigned int to;
		to = calc_offset(new_node1);
		pmem_memcpy(&prev_node_data->next_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		*/
		flush_meta(new_node1->prev_offset);
		_mm_sfence();
//		memcpy(&prev_node_data->next_offset,&to,sizeof(unsigned int));

	}
	else
	{
//		memcpy(new_node1_data->buffer,buffer1,size1);
//		memcpy(new_node2_data->buffer,buffer2,size2);
	}
#endif
if (print)
	printf("insert range\n");
//	b = b << 1 +1;
//	unsigned char sp[8];
//	uint64_t sp;	//split prefix??
	uint64_t v;
//	sp = *((uint64_t*)prefix);
//	sp = prefix_64;	
	v = (uint64_t)1 <<(63-continue_len);
//	if (sp & v)
//		sp-=v;
//
//static std::mutex mu;
//mu.lock();
	insert_range_entry((unsigned char*)&prefix_64,continue_len,SPLIT_OFFSET);
	// disappear here
	/*
	int z = 0;
	if (find_range_entry2((unsigned char*)&prefix_64,&z) != 0)
{
	printf("erer\n");
	int t;
	scanf("%d",&t);
}
*/
	prefix_64-=v;		
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,calc_offset(new_node1));
	prefix_64+=v;
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,calc_offset(new_node2));
//mu.unlock();
	
#if 0
	else
	{
		Node_meta* new_node;
		Node* new_node_data;
		unsigned char* new_buffer;
		if (size2 == 0) // node 1 first
		{
			new_node = new_node1;
			new_node->size = size1;
			new_buffer = buffer1;
			new_node_data = offset_to_node_data(calc_offset(new_node1));
		}
		else
		{
			new_node = new_node2;
			new_node->size = size2;
			new_buffer = buffer2;
			new_node_data = offset_to_node_data(calc_offset(new_node2));
		}
	new_node->state = 0; // ??
	new_node->ref = 0;
//	pthread_mutex_init(&new_node1->mutex,NULL);

	new_node->next_offset = node->next_offset;
	new_node->prev_offset = node->prev_offset;


	next_node->prev_offset = calc_offset(new_node);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node);

	// flush
	move_scan_list(node,new_node);

	if (!USE_DRAM)
	{
//		pmem_memcpy(new_node1+sizeof(pthread_mutex_t),&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

		flush_meta(calc_offset(new_node));
		pmem_memcpy(new_node_data->buffer, new_buffer, new_node->size, PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();

		unsigned int to;
		to = calc_offset(new_node);
		pmem_memcpy(&prev_node_data->next_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}
	else
	{
		memcpy(new_node_data->buffer,new_buffer,new_node->size);
	}

	//MAY NEED OP HERE

	insert_range_entry((unsigned char*)&prefix_64,continue_len,calc_offset(new_node));

	}
#endif
//	s_unlock(prev_node);
//	s_unlock(next_node);
//	e_unlock(node); never release e lock!!!

//	print_node(new_node1);
//	print_node(new_node2);

	for (i=0;i<tc;i++)
		insert_point_entry((unsigned char*)&temp_key[i],vea[i]);//temp_kvp[i],temp_len[i]);


#if 0 // can we rehash during data copy?

if (print)
printf("rehash\n");
	// rehash after data copy
//	cur = 0;
//	size1 = size2 = 0;
//	struct point_hash_entry* point_entry;
	buffer = node_data->buffer;
	buffer1 = new_node1_data->buffer;
	buffer2 = new_node2_data->buffer;
//	while(cur < size)
	while (buffer < be)	
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid
		value_len = *((uint16_t*)(buffer));
		kvs = kls + value_len;
		if ((value_len & (1 << 15)) == 0) // valid length	
		{
			/*
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			if (point_entry == NULL)
			{
				printf("eeererereee333333333------------\n");
			point_entry = find_or_insert_point_entry(buffer+cur,0);

						scanf("%d",&i);

			}
			*/

		if (*((uint64_t*)(buffer+len_size)) < prefix_64) 
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			point_entry = find_or_insert_point_entry(buffer+cur,0);
//			point_entry->kv_p = new_node1_data->buffer+size1;
			insert_point_entry(buffer+len_size,buffer1);
			buffer1+= kvs;//key_size + len_size + value_len;
		}
		else
		{
			// rehash 
//			strcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			point_entry = find_or_insert_point_entry(buffer+cur,0);
//			point_entry->kv_p = new_node2_data->buffer+size2;	
			insert_point_entry(buffer+len_size,buffer2);		
			buffer2+=kvs;
//			size2+= key_size + len_size + value_len;
		}
		}
		else
			kvs-= (1 << 15);
		buffer+=kvs;
//		cur+=key_size+len_size+value_len;
	}

#endif
// need this fence!!	
	_mm_sfence(); // for free after rehash
//	_mm_mfence();	

//	dec_ref(offset);
	free_node(node);//???

#ifdef dtt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

/*
	if (size2 == 0) // node1 first
	{
		free_node(new_node2);
		return 0;
	}
	else if (size1 == 0)
	{
		free_node(new_node1);
		return 0;
	}
*/
	return 1;
}

int compact(unsigned int offset)//, struct range_hash_entry* range_entry)//,unsigned char* prefix, int continue_len)
{
#ifdef dtt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
//	tt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;

//	printf("start compaction offset %d len %d\n",offset,continue_len);
if(print)	
	printf("compaction offset %d\n",offset);
/*
printf("compaction code is not ready!!!\n");
int t;
scanf("%d",&t);
*/
	int i;
	uint16_t size;
	unsigned char* buffer;
//	unsigned char buffer1[NODE_BUFFER];
	unsigned char* buffer1;	

	int tc;
	uint64_t temp_key[100];
//	unsigned char* temp_kvp[100];
//	int temp_len[100];
	ValueEntry vea[100];	

	Node_meta* new_node1;
	Node_meta* prev_node;
	Node_meta* next_node;
	Node_meta* node;

	Node* node_data;
	Node* new_node1_data;
//	Node* prev_node_data;

	node = offset_to_node(offset);
	node_data = offset_to_node_data(offset);


	prev_node = offset_to_node(node->prev_offset);
	next_node = offset_to_node(node->next_offset);

//	prev_node_data = offset_to_node_data(node->prev_offset);
	
		//------------------------------------------------
/*
	pthread_mutex_lock(&node->mutex);	
	if (node->state > 0)
	{
		pthread_mutex_unlock(&node->mutex);
//		printf("ref4 %d\n",offset);

		return -1;
	}
	node->state = 1; // split
	pthread_mutex_unlock(&node->mutex);
//	node->m.unlock();
	while(node->ref > 1); // spin... wait
*/
//	prev_node->m.lock();
//	pthread_mutex_lock(&prev_node->mutex);	


		if (prev_node->state > 0)
		{

//			prev_node->m.unlock();
//			pthread_mutex_unlock(&prev_node->mutex);

//			node->m.lock();
//			pthread_mutex_lock(&node->mutex);	
//			node->state = 0;
//			node->m.unlock();
//			pthread_mutex_unlock(&node->mutex);
//		printf("ref5 %d\n",node->prev_offset);

			return -1; // failed
		}
//			prev_node->m.unlock();
//				pthread_mutex_unlock(&prev_node->mutex);


//			continue;
//		e_lock(node); //already locked

//		next_node->m.lock();
//			pthread_mutex_lock(&next_node->mutex);	

		if (next_node->state > 0)
		{
//			next_node->m.unlock();
//			pthread_mutex_unlock(&next_node->mutex);
//			node->m.lock();
//			pthread_mutex_lock(&node->mutex);	
//			node->state = 0;
//			node->m.unlock();
//			pthread_mutex_unlock(&node->mutex);
//		printf("ref6 %d\n",node->next_offset);
		while(next_node->state > 0)
		next_node = offset_to_node(node->next_offset);
/*
			struct timespec tsl0,tsl1;
			clock_gettime(CLOCK_MONOTONIC,&tsl0);
			while(next_node->state > 0)
			{
				next_node = offset_to_node(node->next_offset);
				clock_gettime(CLOCK_MONOTONIC,&tsl1);
				if ((tsl1.tv_sec-tsl0.tv_sec)*1000000000-tsl1.tv_nsec-tsl0.tv_nsec > 1000000000)
				{
					printf("too long %d %d %d\n",offset,node->next_offset,(int)next_node->state);
					int t;
					scanf("%d",&t);
				}
			}
			*/
//			return -1;//failed
		}
//			next_node->m.unlock();
//			pthread_mutex_unlock(&next_node->mutex);
if (print)
		printf("locked\n");

	//-------------------------------------------------------------------	

	size = node->size;
	buffer = node_data->buffer;
//	size = meta & 63; // 00111111

	new_node1 = alloc_node();

	new_node1->state = 0; // ??
//	new_node1->ref = 0;
//	pthread_mutex_init(&new_node1->mutex,NULL);

//	new_node1->size = size1;
//	new_node1->size = buffer1-new_node1_data->buffer;	

	new_node1->ic = 0;

	new_node1->next_offset = node->next_offset;
	new_node1->prev_offset = node->prev_offset;


	next_node->prev_offset = calc_offset(new_node1);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node1);

	// flush
	move_scan_list(node,new_node1);

	new_node1_data = offset_to_node_data(calc_offset(new_node1));

	memcpy(new_node1_data,new_node1,sizeof(unsigned int));


	int j,temp;
	for (i=0;i<node->ic;i++)
	{
		temp = 0;
		for (j=0;j<node->ic-i-1;j++)
		{
			if (node->inv_kv[j] > node->inv_kv[j+1])
			{
				temp = node->inv_kv[j+1];
				node->inv_kv[j+1] = node->inv_kv[j];
				node->inv_kv[j] = temp;
			}
		}
		if (temp == 0)
			break;
	}


	uint16_t value_len;

//	uint64_t k = *((uint64_t*)(buffer+cur));

	buffer1 = new_node1_data->buffer;

	unsigned char* const be = buffer+size;

	const int kls = key_size + len_size;
	uint16_t kvs;
	const unsigned int new_offset = calc_offset(new_node1);
tc = 0;

//	struct len_and_key lak;
	j = 0;
	node->inv_kv[node->ic] = 0;
	while(buffer < be)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		if (*((uint64_t*)(buffer+cur)) != k)
//			printf("compaction error\n");	
//		memcpy(&lak,buffer,kls);				
		value_len = *((uint16_t*)(buffer));
//		kvs = kls + lak.len;
		kvs = kls + value_len;
//		if ((/*value_len*/ lak.len & (1 << 15)) == 0) // valid length		
		if ((unsigned char*)node_data + node->inv_kv[j] != buffer)		
		{
			memcpy(buffer1,buffer,kvs);
//			temp_key[tc] = lak.key;
			temp_key[tc] = *((uint64_t*)(buffer+len_size));
//			temp_kvp[tc] = buffer1;
//			temp_len[tc] = value_len;
			vea[tc].node_offset = new_offset;
			vea[tc].kv_offset = buffer1-(unsigned char*)new_node1_data;
//			vea[tc].len = lak.len;
			vea[tc].len = value_len;		
			buffer1+=kvs;
			tc++;
		}
		else
		{
			j++;
			kvs&= ~((uint16_t)1 << 15);
		}
//		cur+=key_size+len_size+value_len;
		buffer+=kvs;		
	}

	new_node1->size = buffer1-new_node1_data->buffer;

	//why?? fill zero?
	/*
	if (size1 + entry_size < NODE_BUFFER)
		init_entry(buffer1 + size1*entry_size);
	if (size2 + entry_size < NODE_BUFFER)
		init_entry(buffer2 + size2*entry_size);
*/

	if (!USE_DRAM)
	{
//		pmem_memcpy(new_node1+sizeof(pthread_mutex_t),&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
/*
		flush_meta(calc_offset(new_node1));
		pmem_memcpy(new_node1_data->buffer, buffer1, size1, PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();

		unsigned int to;
		to = calc_offset(new_node1);
		pmem_memcpy(&prev_node_data->next_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
		*/
		pmem_persist(new_node1_data,sizeof(unsigned int)+new_node1->size);
		_mm_sfence();
		flush_meta(new_node1->prev_offset);
		_mm_sfence();
	}
	else
	{
//		memcpy(new_node1_data->buffer,buffer1,size1);
	}

//	s_unlock(prev_node);
//	s_unlock(next_node);
//	e_unlock(node); never release e lock!!!
	for (i=0;i<tc;i++)	
		insert_point_entry((unsigned char*)&temp_key[i],vea[i]);//temp_kvp[i],temp_len[i]);

#if 0	
if (print)
printf("rehash\n");
	// rehash
//	struct point_hash_entry* point_entry;

	buffer = node1_data->buffer1;

	while(buffer < be)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid
		value_len = *((uint16_t*)(buffer));

		if ((value_len & (1 << 15)) == 0) // valid length	
		{	
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
			/*
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			if (point_entry == NULL)
			{
				printf("point entry NULL error\n");
				int t;
				scanf("&t");
			}
			point_entry->kv_p = new_node1_data->buffer+size1;
			*/
			insert_point_entry(buffer+cur,new_node1_data->buffer+size1);
			size1+= key_size + len_size + value_len;
		}
		else
			value_len -= (1 << 15);
		cur+=key_size+len_size+value_len;
	}
#endif
//	printf("insert range\n");
//	insert_range_entry((unsigned char*)prefix,continue_len,calc_offset(new_node1));
	insert_range_entry((unsigned char*)new_node1_data->buffer+len_size,key_bit,calc_offset(new_node1)); // who use 64 range
//	range_entry->offset = calc_offset(new_node1);

	_mm_sfence(); // for free after rehash
//
	free_node(node);//???

#ifdef dtt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt3+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

	return calc_offset(new_node1);
//	return 0;

}
#if 0
int advance(unsigned char** kv_pp,int* offset,Node* node_p)
{
	uint16_t size;
	int old_offset,new_offset,inv;
	while(1)
	{
		while (1)
		{
			inv = 0;
			size = *((uint16_t*)(*kv_pp+key_size));
			if (size & (1 << 15))
			{
				inv = 1;
				size-=(1<<15);
			}
			*kv_pp+=size+key_size+len_size;
			if (*kv_pp >= node_p->buffer+node_p->size)
				break;
			if (inv == 0)
				return 0;
		}

		old_offset = *offset;
		while(1)
		{
			new_offset = offset_to_node(old_offset)->next_offset;
			if (new_offset == TAIL_OFFSET)
			{
				*offset = TAIL_OFFSET;
				dec_ref(old_offset);
				*kv_pp = NULL;
				return -1;
			}
			if (inc_ref(new_offset,0))
			{
				copy_node(node_p,offset_to_node(new_offset));
				break;
			}
		}
		dec_ref(old_offset);
		*offset = new_offset;
		*kv_pp = node_p->buffer;
	}
	return 0;
}
#endif

int advance_offset(Query* query)
{
	int old_offset,new_offset,inv;
	Node_meta* node;
	Node_meta* next_node;

//	pthread_mutex_lock(&query->scan_mutex);
	at_lock(query->scan_lock);

	old_offset = query->scan_offset;
	while(1)
	{
		node = offset_to_node(old_offset);
			/*
			if (inc_ref(new_offset,0))
			{
				copy_node(node_p,offset_to_node(new_offset));
				break;
			}
			*/
/*
		pthread_mutex_lock(&node->mutex);
		if (node->state > 0)
		{
			pthread_mutex_unlock(&node->mutex);
			continue;
		}
*/
		at_lock(node->state); // never fail?

		new_offset = node->next_offset;
		if (new_offset == TAIL_OFFSET)
		{
			delete_scan_entry(old_offset,query);
//			pthread_mutex_unlock(&node->mutex);
			at_unlock(node->state);			
			query->scan_offset = TAIL_OFFSET;
//				dec_ref(old_offset);
//				*kv_pp = NULL;
//			pthread_mutex_unlock(&query->scan_mutex);		
			at_unlock(query->scan_lock);			
			return -1;
		}

		next_node = offset_to_node(new_offset);

//		pthread_mutex_lock(&next_node->mutex);		

		if (next_node->state > 0) // it can't be splited because we have lock of 
		{
//			pthread_mutex_unlock(&next_node->mutex);
//			pthread_mutex_unlock(&node->mutex);
			at_unlock(node->state);			
			continue;
		}
		break;
	}

	/*
	sl = (Scan_list)malloc(sizeof(Scan_list));
	sl->query = query;
	sl->next = next_node->scan_list;
	next_node->scan_list = sl;
	*/

	Scan_list* sl;
	Scan_list** slp;
	//move scan list
	sl = node->scan_list;
	slp = &(node->scan_list);
	while(sl)
	{
		if (sl->query == query)
		{
			*slp = sl->next;
			break;
		}
		slp = &(sl->next);
		sl = sl->next;
	}
//	pthread_mutex_unlock(&node->mutex);
//	at_unlock(node->state);	// not here

	sl->next = next_node->scan_list;
	next_node->scan_list = sl;

	at_lock(next_node->state);
	at_unlock(node->state);

//	next_node->state = 1; // not split it is copy ....
//	insert_scan_list(next_node,query);
//	pthread_mutex_unlock(&next_node->mutex);

//	while(next_node->ref > 0); // remove insert
//	*((Node*)query->node) = *next_node; // copy_node
	Node* next_node_data;
	next_node_data = offset_to_node_data(calc_offset(next_node));
	// read will not need this ... 
	/*
	if (!USE_DRAM)
	{
		pmem_memcpy(
	}
	else
	*/
		*((Node*)query->node_data) = *next_node_data;	


//	pthread_mutex_lock(&next_node->mutex);
//	next_node->state = 0; // not split it is copy ....
//	pthread_mutex_unlock(&next_node->mutex);
	at_unlock(next_node->state);	


//		dec_ref(old_offset);
		
//		dec_ref(new_offset);		
	query->scan_offset = new_offset;
//		*kv_pp = node_p->buffer;
//	pthread_mutex_unlock(&query->scan_mutex);		
	at_unlock(query->scan_lock);	
	return 0;
}


int advance_offset(void* query)
{
	return advance_offset((Query*)query);
}

/*
void copy_node(Node* node1,Node* node2)
{
	pthread_mutex_lock(&node2->mutex);
	while(node2->ref > 1); // except me
	*node1 = *node2;
	pthread_mutex_unlock(&node2->mutex);
}
*/
void sort_node(Node* node_data,int* sorted_index,int* max)
{
	unsigned char* kv;
	uint16_t v_size;
	const int node_size = offset_to_node(data_point_to_offset((unsigned char*)node_data))->size; 
	kv = node_data->buffer;
	*max = 0;
	while (kv < node_data->buffer + node_size)
	{
		v_size = *((uint16_t*)(kv+key_size));
		if (v_size & (1 << 15))
			v_size-=(1<<15);
		else
			(sorted_index)[(*max)++] = kv-node_data->buffer;
		kv+=key_size+len_size+v_size;
	}

	// sort
	int i,j,t;
	for (i=0;i<*max;i++)
	{
		for (j=i+1;j<*max;j++)
		{
			if (*((uint64_t*)(node_data->buffer+(sorted_index)[i])) > *((uint64_t*)(node_data->buffer+(sorted_index)[j])))
			{
				t = (sorted_index)[i];
				(sorted_index)[i] = (sorted_index)[j];
				(sorted_index)[j] = t;
			}
		}
	}	
}

void insert_scan_list(Node_meta* node,void* query)
{
	Scan_list* sl;
	sl = (Scan_list*)malloc(sizeof(Scan_list)); // OP use allocator
	sl->query = query;
	sl->next = node->scan_list;
	node->scan_list = sl;
}

void delete_scan_entry(unsigned int scan_offset,void* query)
{
	Node_meta* node;
	Scan_list* sl;
	Scan_list** slp;
//	Query* query_p;
//	query_p = (Query*)query;
//	pthread_mutex_lock(&query_p->scan_mutex);
	node = offset_to_node(scan_offset);
//	pthread_mutex_lock(&node->mutex);
	sl = node->scan_list;
	slp = &(node->scan_list);
	while(sl != NULL)
	{
		if (sl->query == query)
		{

			*slp = sl->next;
			free(sl);
			break;
		}
		slp = &(sl->next);
		sl = sl->next;
	}

//	pthread_mutex_unlock(&node->mutex);
//	pthread_mutex_unlock(&query_p->scan_mutex);

}

void invalidate_kv(unsigned int node_offset, unsigned int kv_offset)
{
	Node_meta* meta;
	meta = offset_to_node(node_offset);
	meta->inv_kv[meta->ic++] = kv_offset;
}

}
