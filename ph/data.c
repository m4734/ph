#include<libpmem.h>
#include<queue>

#include <stdio.h> //test
#include <string.h>
#include <stdlib.h> // malloc
#include <x86intrin.h> // mm fence

#include "data.h"
#include "hash.h"

#include "query.h"

//extern unsigned long long int pmem_size;
//extern char pmem_file[100];
//extern int key_size;
//extern int value_size;

// we need to traverse linked list when we recover and if it is never visited, it is garbage

#define print 0
//#define print 0

unsigned char* pmem_addr;
int is_pmem;
size_t pmem_len;
//size_t pmem_used;

unsigned char* meta_addr;
uint64_t meta_size;
uint64_t meta_used;

std::queue <Node_meta*> node_free_list;

pthread_mutex_t alloc_mutex;

//----------------------------------------------------------------

// OP all

unsigned int calc_offset(Node_meta* node) // it will be optimized with define
{
	return ((unsigned char*)node-meta_addr)/sizeof(Node_meta);
}
Node_meta* offset_to_node(unsigned int offset) // it will be ..
{
	return (Node_meta*)(meta_addr + offset*sizeof(Node_meta));
}
Node* offset_to_node_data(unsigned int offset)
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
	return (kv_p - meta_addr)/sizeof(Node_meta);
}

unsigned int data_point_to_offset(unsigned char* kv_p)
{
	return (kv_p - pmem_addr)/sizeof(Node);
}


//---------------------------------------------------------------

void flush_meta(unsigned int offset)
{
//	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),&(((Node_meta*)(meta_addr + offset*sizeof(Node_meta)))->size),sizeof(uint16_t)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(uint16_t)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

	_mm_sfence();
}

Node_meta* alloc_node()
{
	Node_meta* node;
	pthread_mutex_lock(&alloc_mutex);
	if (!node_free_list.empty()) // need lock
	{
		node = node_free_list.front();
		node_free_list.pop();
		pthread_mutex_unlock(&alloc_mutex);
//		while(node->ref > 0); //wait		
		if (print)
	printf("alloc node %p\n",node); //test
		return node;
	}
	if (meta_size < meta_used + sizeof(Node_meta))
	{
		printf("error!!! need more memory]n");
		int t;
		scanf("%d",&t);
	}

	node = (Node_meta*)(meta_addr + meta_used);
//	pmem_used += sizeof(Node);
	meta_used += sizeof(Node_meta);	
	pthread_mutex_unlock(&alloc_mutex);
	if (print)
	printf("alloc node %p\n",node); //test

	pthread_mutex_init(&node->mutex,NULL);

	return node;
}
void free_node(Node_meta* node)
{
	pthread_mutex_lock(&node->mutex);
	node->state = 2; // free // without mutex??
	pthread_mutex_unlock(&node->mutex);
	if (node->ref == 0)
	{
		pthread_mutex_lock(&alloc_mutex);
		node_free_list.push(node);
		pthread_mutex_unlock(&alloc_mutex);
	}
	if (print)
	printf("free node %p\n",node);
}


int init_data() // init hash first!!!
{
	pthread_mutex_init(&alloc_mutex,NULL);

	if (USE_DRAM)
		pmem_addr=(unsigned char*)mmap(NULL,pmem_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);

	else
		pmem_addr = (unsigned char*)pmem_map_file(pmem_file,pmem_size,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);
	meta_size = pmem_size/sizeof(Node)*sizeof(Node_meta);
	meta_addr = (unsigned char*)mmap(NULL,meta_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);

	if (pmem_addr == NULL)
	{
		printf("pmem error\n");
		return -1;
	}

//	pmem_used = 0;
	meta_used = 0;

	//insert 0 node
	
	uint64_t zero=0;

	Node_meta* head_node;
	Node_meta* tail_node;
	//push for reserve
	alloc_node(); // 0
	head_node = alloc_node(); // 1 head
	tail_node = alloc_node(); // 2 tail

	Node_meta* node = alloc_node();
	node->state = 0;
	node->size = 0;
	node->ref = 0;
	node->scan_list = NULL;
	node->prev_offset = HEAD_OFFSET;
	node->next_offset = TAIL_OFFSET;
//	pthread_mutex_init(&node->mutex,NULL);
	if (print)
	printf("node 0 %p\n",node);

	head_node->next_offset = 3;
	tail_node->prev_offset = 3;
	head_node->ref = 0;
	head_node->state = 0;
	head_node->scan_list = NULL;
	tail_node->ref = 0;
	tail_node->state = 0;
	tail_node->scan_list = NULL;
//	pthread_mutex_init(&head_node->mutex,NULL);
//	pthread_mutex_init(&tail_node->mutex,NULL);

	flush_meta(HEAD_OFFSET);
	flush_meta(TAIL_OFFSET);

	inc_ref(HEAD_OFFSET,0);
	inc_ref(TAIL_OFFSET,0); //don't free these node	

	insert_range_entry((unsigned char*)(&zero),0,calc_offset(node)); // the length is important!!!
	int zero2=0;
	if (find_range_entry((unsigned char*)(&zero),&zero2) == NULL)
		printf("range error\n");

	if (USE_DRAM)
		printf("USE_DRAM\n");
	else
		printf("USE_PM\n");
	printf("size of node %ld\n",sizeof(Node));
	printf("size of node_meta %ld\n",sizeof(Node_meta));



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
	pthread_mutex_destroy(&alloc_mutex);

	//destroy all node mutex
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
int inc_ref(Node_meta* node,uint16_t limit)
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
	if (node->state >= 2)
	{
		while(1)
		printf("fkenslfksenf");
	}
	node->ref++;
//	node->m.unlock();
	pthread_mutex_unlock(&node->mutex);	
	return 1;
}
void dec_ref(Node_meta* node)
{
	if (print)
	printf("dec_ref\n");
//	node->m.lock();
	pthread_mutex_lock(&node->mutex);	
	if (node->ref == 0)
	{
		printf("ref error\n");
		int t;
		scanf("%d",&t);
	}
	node->ref--;
//	node->m.unlock();
	pthread_mutex_unlock(&node->mutex);	
	if (node->state == 2 && node->ref == 0)
	{
		pthread_mutex_lock(&alloc_mutex);
		node_free_list.push(node);
		pthread_mutex_unlock(&alloc_mutex);
	}
}
int inc_ref(unsigned int offset,uint16_t limit)
{
	return inc_ref(offset_to_node(offset),limit);
}
void dec_ref(unsigned int offset)
{
	return dec_ref(offset_to_node(offset));
}

int try_hard_lock(Node_meta* node)
{
	pthread_mutex_lock(&node->mutex);
	if (node->state > 0)
		return 0;
	while(node->ref>0);
	return 1;
}
void hard_unlock(Node_meta* node)
{
	pthread_mutex_unlock(&node->mutex);
}

int try_hard_lock(unsigned int offset)
{
	return try_hard_lock(offset_to_node(offset));
}

void hard_unlock(unsigned int offset)
{
	hard_unlock(offset_to_node(offset));
}

void soft_lock(Node_meta* node)
{
	pthread_mutex_lock(&node->mutex);
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
	uint16_t new_size,vl16;
	Node_meta* node = offset_to_node(offset);
	pthread_mutex_lock(&node->mutex);
	if (node->state > 0)
	{
	pthread_mutex_unlock(&node->mutex);	
		return -2; // node is spliting
	}
	if (node->size + key_size + len_size + value_length > NODE_BUFFER)
	{
//		node->ref++; // i will try split
		pthread_mutex_unlock(&node->mutex);
		return -1; // node need split
	}
//	node->ref++;
	ns = node->size;
	new_size = node->size + key_size+len_size+value_length;
	// fence and flush?
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
		node->size = new_size;

	pthread_mutex_unlock(&node->mutex);	
	if (print)
	printf("inc ref - insert\n");
	return ns;

}

unsigned char* insert_kv(unsigned int offset,unsigned char* key,unsigned char* value,int value_length,int old_size)
{
	if (print)
	printf("insert kv offset %u\n",offset);
	Node* node_data;
	unsigned char* kv_p;
	uint16_t vl16 = value_length;

	node_data = offset_to_node_data(offset); 
	if (print)
	printf("node %p size %d \n",node_data,(int)old_size);
	/*
	if ((int)node->size + key_size + len_size + value_length > NODE_BUFFER)
	{
		printf("size %d\n",(int)node->size);
		return NULL;
	}
	*/
	if (!USE_DRAM)
	{
		pmem_memcpy(node_data->buffer + old_size,key,sizeof(uint64_t),PMEM_F_MEM_NONTEMPORAL); //write key
		pmem_memcpy(node_data->buffer + old_size + key_size + len_size, value, value_length, PMEM_F_MEM_NONTEMPORAL); //write value
		_mm_sfence();
		pmem_memcpy(node_data->buffer + old_size + key_size,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL); // validate
		_mm_sfence();
	}
	else
	{
		*(uint64_t*)(node_data->buffer + old_size) = *((uint64_t*)key);
		/*
		int i;
		for (i=0;i<value_length;i++)
			node->buffer[old_size+key_size+len_size+i] = value[i];
			*/
		memcpy(node_data->buffer + old_size + key_size + len_size, value, value_length);
		*((uint16_t*)(node_data->buffer + old_size + key_size)) = vl16;
	}
//fence and flush
//fence
//	*(uint16_t*)(node->buffer + old_size + key_size) = value_length;
//fence flush
	kv_p = node_data->buffer + old_size;
//	node->size += key_size + len_size + value_length;
	if (print)
	printf("kv_p %p\n",kv_p);
//	print_kv(kv_p);
	return kv_p;
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
		pthread_mutex_lock(&query->scan_mutex);
		if (query->scan_offset == old_offset)
		{
			query->scan_offset = new_offset;
			pthread_mutex_unlock(&query->scan_mutex);
			slp = &(sl->next);
			sl = sl->next;
		}
		else
		{
			pthread_mutex_unlock(&query->scan_mutex);
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

int split(unsigned int offset,unsigned char* prefix, int continue_len) // locked
{
if (print)
	printf("start split offset %d len %d\n",offset,continue_len);

	int i;
	uint16_t size,size1,size2;
	unsigned char* buffer;
	unsigned char buffer1[NODE_BUFFER],buffer2[NODE_BUFFER];
	Node_meta* new_node1;
	Node_meta* new_node2;
	Node_meta* prev_node;
	Node_meta* next_node;
	Node_meta* node;

	Node* node_data;
	Node* new_node1_data;
	Node* new_node2_data;
	Node* prev_node_data;


	node = offset_to_node(offset);
	node_data = offset_to_node_data(offset);

	prev_node = offset_to_node(node->prev_offset);
	next_node = offset_to_node(node->next_offset);

	prev_node_data = offset_to_node_data(node->prev_offset);

//------------------------------------------------------------ check

//	node->m.lock(); // order?
	pthread_mutex_lock(&node->mutex);	
	if (node->state > 0)
	{
		pthread_mutex_unlock(&node->mutex);
//		printf("ref1 %d %d %d\n",offset,node->state,node->ref);
		return -1;
	}
	node->state = 1; // split
	pthread_mutex_unlock(&node->mutex);
//	node->m.unlock();
	while(node->ref > 1); // spin... wait except me
/*
	if (node->ref > 1) // read may
	{
		while(1)
			printf("xxxxxx");
	}
	*/
// may not need lock
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
//printf("ref2 %d\n",node->prev_offset);
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
//printf("ref3 %d\n",node->next_offset);
			return -1;//failed
		}
//			next_node->m.unlock();
	pthread_mutex_unlock(&next_node->mutex);

if (print)
		printf("locked\n");

		//----------------------------------------------------------

	size = node->size;
	buffer = node_data->buffer;

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

//			if (value_len != 100)
//				print_kv(buffer+cur);

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
	printf("size %d size1 %d size2 %d\n",size,size1,size2);

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

	new_node1->state = 0; // 
	new_node1->ref = 0;
	new_node1->scan_list = NULL;
	new_node2->state = 0; // 
	new_node2->ref = 0;
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

	new_node1_data = offset_to_node_data(calc_offset(new_node1));
	new_node2_data = offset_to_node_data(calc_offset(new_node2));


	if (!USE_DRAM) //data copy and link node
	{
//		pmem_memcpy((unsigned char*)new_node1+sizeof(pthread_mutex_t),(unsigned char*)&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		flush_meta(calc_offset(new_node1));
		pmem_memcpy(new_node1_data->buffer,buffer1,size1,PMEM_F_MEM_NONTEMPORAL);

//pmem_memcpy((unsigned char*)new_node2+sizeof(pthread_mutex_t),(unsigned char*)&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		flush_meta(calc_offset(new_node2));
		pmem_memcpy(new_node2_data->buffer,buffer2,size2,PMEM_F_MEM_NONTEMPORAL);
/*
		unsigned int to;
		to = calc_offset(new_node2);
		pmem_memcpy(&next_node->prev_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		*/
//	next_node->prev_offset = calc_offset(new_node2);

// flush

		_mm_sfence();
		unsigned int to;
		to = calc_offset(new_node1);
		pmem_memcpy(&prev_node_data->next_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();

	}
	else
	{
		memcpy(new_node1_data->buffer,buffer1,size1);
		memcpy(new_node2_data->buffer,buffer2,size2);
	}

//	s_unlock(prev_node);
//	s_unlock(next_node);
//	e_unlock(node); never release e lock!!!

#if 0
	
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
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			if (point_entry == NULL)
			{
				printf("eeererereee333333333------------\n");
						scanf("%d",&i);

			}

		if (*((uint64_t*)(buffer+cur)) < prefix_64) 
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node1->buffer+size1;
			size1+= key_size + len_size + value_len;
		}
		else
		{
			// rehash 
//			strcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node2->buffer+size2;	
			size2+= key_size + len_size + value_len;
		}
		}
		else
			value_len-= (1 << 15);

		cur+=key_size+len_size+value_len;
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
	prefix_64-=v;		
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,calc_offset(new_node1));
	prefix_64+=v;
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,calc_offset(new_node2));

#if 0
	// debug!!!!
	struct range_hash_entry* p;
	struct range_hash_entry* c1;
	struct range_hash_entry* c2;

	int cl = continue_len;

	p = find_range_entry((unsigned char*)&prefix_64,&cl);
	cl = continue_len+1;
	c2 = find_range_entry((unsigned char*)&prefix_64,&cl);
	prefix_64-=v;
	cl = continue_len+1;
	c1 = find_range_entry((unsigned char*)&prefix_64,&cl);
	prefix_64+=v;

	p->c1 = c1;
	p->c2 = c2;

	if (p == NULL || c1 == NULL || c2 == NULL)
	{
		printf("-------------------------debug error1 %p %p %p\n",p,c1,c2);
		int t;
		scanf("%d",&t);

	cl = continue_len;
	p = find_range_entry((unsigned char*)&prefix_64,&cl);
	cl = continue_len+1;
	c2 = find_range_entry((unsigned char*)&prefix_64,&cl);
	prefix_64-=v;
	cl - continue_len+1;
	c1 = find_range_entry((unsigned char*)&prefix_64,&cl);

	}
#endif

//	print_node(new_node1);
//	print_node(new_node2);



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
			point_entry = find_or_insert_point_entry(buffer+cur,0);
			if (point_entry == NULL)
			{
				printf("eeererereee333333333------------\n");
			point_entry = find_or_insert_point_entry(buffer+cur,0);

						scanf("%d",&i);

			}

		if (*((uint64_t*)(buffer+cur)) < prefix_64) 
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
		{
			// rehash later
//			strcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node1_data->buffer+size1;
			size1+= key_size + len_size + value_len;
		}
		else
		{
			// rehash 
//			strcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			point_entry = find_or_insert_point_entry(buffer+cur,0);
			point_entry->kv_p = new_node2_data->buffer+size2;	
			size2+= key_size + len_size + value_len;
		}
		}
		else
			value_len-= (1 << 15);

		cur+=key_size+len_size+value_len;
	}



//	dec_ref(offset);
	free_node(node);//???
	return 0;
}

int compact(unsigned int offset)//, struct range_hash_entry* range_entry)//,unsigned char* prefix, int continue_len)
{
//	printf("start compaction offset %d len %d\n",offset,continue_len);
if(print)	
	printf("compaction offset %d\n",offset);
/*
printf("compaction code is not ready!!!\n");
int t;
scanf("%d",&t);
*/
	int i;
	uint16_t size,size1;
	unsigned char* buffer;
	unsigned char buffer1[NODE_BUFFER];
	Node_meta* new_node1;
	Node_meta* prev_node;
	Node_meta* next_node;
	Node_meta* node;

	Node* node_data;
	Node* new_node1_data;
	Node* prev_node_data;

	node = offset_to_node(offset);
	node_data = offset_to_node_data(offset);


	prev_node = offset_to_node(node->prev_offset);
	next_node = offset_to_node(node->next_offset);

	prev_node_data = offset_to_node_data(node->prev_offset);
	
		//------------------------------------------------

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
//		printf("ref5 %d\n",node->prev_offset);

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
//		printf("ref6 %d\n",node->next_offset);

			return -1;//failed
		}
//			next_node->m.unlock();
			pthread_mutex_unlock(&next_node->mutex);
if (print)
		printf("locked\n");

	//-------------------------------------------------------------------	

	size = node->size;
	buffer = node_data->buffer;
//	size = meta & 63; // 00111111

	new_node1 = alloc_node();
	new_node1_data = offset_to_node_data(calc_offset(new_node1));

	size1 = 0;

	int value_len,cur;
	cur = 0;

//	uint64_t k = *((uint64_t*)(buffer+cur));


	while(cur < size)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		if (*((uint64_t*)(buffer+cur)) != k)
//			printf("compaction error\n");		
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

	new_node1->state = 0; // ??
	new_node1->ref = 0;
//	pthread_mutex_init(&new_node1->mutex,NULL);

	new_node1->size = size1;

	new_node1->next_offset = node->next_offset;
	new_node1->prev_offset = node->prev_offset;


	next_node->prev_offset = calc_offset(new_node1);

	// flush
	//
	prev_node->next_offset = calc_offset(new_node1);

	// flush
	move_scan_list(node,new_node1);

	if (!USE_DRAM)
	{
//		pmem_memcpy(new_node1+sizeof(pthread_mutex_t),&nm+sizeof(pthread_mutex_t),sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint16_t)+sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

		flush_meta(calc_offset(new_node1));
		pmem_memcpy(new_node1_data->buffer, buffer1, size1, PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();

		unsigned int to;
		to = calc_offset(new_node1);
		pmem_memcpy(&prev_node_data->next_offset,&to,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}
	else
	{
		memcpy(new_node1_data->buffer,buffer1,size1);
	}

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
			if (point_entry == NULL)
			{
				printf("point entry NULL error\n");
				int t;
				scanf("&t");
			}
			point_entry->kv_p = new_node1_data->buffer+size1;
			size1+= key_size + len_size + value_len;
		}
		else
			value_len -= (1 << 15);
		cur+=key_size+len_size+value_len;
	}

//	printf("insert range\n");
//	insert_range_entry((unsigned char*)prefix,continue_len,calc_offset(new_node1));

//	range_entry->offset = calc_offset(new_node1);
	free_node(node);//???
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

	pthread_mutex_lock(&query->scan_mutex);

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

		pthread_mutex_lock(&node->mutex);
		if (node->state > 0)
		{
			pthread_mutex_unlock(&node->mutex);
			continue;
		}

		new_offset = node->next_offset;
		if (new_offset == TAIL_OFFSET)
		{
			delete_scan_entry(old_offset,query);
			pthread_mutex_unlock(&node->mutex);
			query->scan_offset = TAIL_OFFSET;
//				dec_ref(old_offset);
//				*kv_pp = NULL;
			pthread_mutex_unlock(&query->scan_mutex);		
			return -1;
		}

		next_node = offset_to_node(new_offset);

		pthread_mutex_lock(&next_node->mutex);

		if (next_node->state > 0)
		{
			pthread_mutex_unlock(&next_node->mutex);
			pthread_mutex_unlock(&node->mutex);
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
	pthread_mutex_unlock(&node->mutex);

	sl->next = next_node->scan_list;
	next_node->scan_list = sl;


	next_node->state = 1; // not split it is copy ....
//	insert_scan_list(next_node,query);
	pthread_mutex_unlock(&next_node->mutex);

	while(next_node->ref > 0); // remove insert
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


	pthread_mutex_lock(&next_node->mutex);
	next_node->state = 0; // not split it is copy ....
	pthread_mutex_unlock(&next_node->mutex);


//		dec_ref(old_offset);
		
//		dec_ref(new_offset);		
	query->scan_offset = new_offset;
//		*kv_pp = node_p->buffer;
	pthread_mutex_unlock(&query->scan_mutex);		
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
	kv = node_data->buffer;
	*max = 0;
	while (kv < node_data->buffer + node_data->size)
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
