#include<libpmem.h>
//#include<queue>
#include<atomic>

#include <stdio.h> //test
#include <string.h>
#include <stdlib.h> // malloc
#include <x86intrin.h> // mm fence

#include <unistd.h> //usleep

#include "data.h"
#include "hash.h"

#include "query.h"
#include "thread.h"

#include "copy.h"

// we need to traverse linked list when we recover and if it is never visited, it is garbage

#define print 0
//#define print 0

//using namespace PH;

#define DRAM_BUF

namespace PH
{

//#define MAX_FILE_NUM (1 << 16)
//#define MAX_OFFSET (1 << 16)

//unsigned char* pmem_addr;
unsigned char** pmem_addr;
int is_pmem;
size_t pmem_len;
//uint64_t file_size;
//size_t pmem_used;
Node** node_data_array;

//unsigned char* meta_addr;
unsigned char** meta_addr;
//uint64_t meta_size;
///*volatile */uint64_t meta_used;
Node_meta** meta_array;
volatile int file_num;
//volatile int offset_cnt;
//int file_num;
//int offset_cnt;

volatile int part_file_num[PM_N];
volatile int part_offset_cnt[PM_N];

std::atomic <uint8_t> alloc_lock;

// FREE_QUEUE_LEN * PM_N * NODE_SIZE
// = 100000 * 4 * 1024 = 400MB

//#define FREE_QUEUE_LEN 100000 // moved to thread.h
volatile unsigned int free_cnt[PM_N]; // free_max // atomic or lock?
volatile unsigned int free_min[PM_N];
volatile unsigned int free_index[PM_N];
Node_offset free_queue[PM_N][FREE_QUEUE_LEN]; // queue len

// free queue [------------------------------------------------------]
// free_cnt                                                   x
// free_min                                     x
// free_index                  x                
// free_local                                   <------------->


#define LOCAL_QUEUE_LEN 20
thread_local Node_offset local_batch_alloc[PM_N][LOCAL_QUEUE_LEN];
thread_local int lbac[PM_N];//={LOCAL_QUEUE_LEN,};
thread_local Node_offset local_batch_free[PM_N][LOCAL_QUEUE_LEN];
thread_local int lbfc[PM_N];//={0,};

thread_local int part_rotation=0;

/*
// static test
thread_local uint64_t size_sum;
thread_local int cnt_sum;
*/

// Node alloc
thread_local Node *d0,*d1,*d2;

void clean_thread_local()
{
	printf("ctr\n");
	int i,j;
	at_lock(alloc_lock);
	for (i=0;i<PM_N;i++)
	{
		for (j=lbac[i];j<LOCAL_QUEUE_LEN;j++)
		{
			if (free_cnt[i]-free_index[i] >= FREE_QUEUE_LEN)
			{
				printf("queue full tl\n");
				return;
			}
			free_queue[i][free_cnt[i]%FREE_QUEUE_LEN] = local_batch_alloc[i][j];
			++free_cnt[i];
		}
		for (j=0;j<lbfc[i];j++)
		{
			if (free_cnt[i]-free_index[i] >= FREE_QUEUE_LEN)
			{
				printf("queue full tl2\n");
				return;
			}
			free_queue[i][free_cnt[i]%FREE_QUEUE_LEN] = local_batch_free[i][j];
			++free_cnt[i];
		}

	}
	at_unlock(alloc_lock);

	free(d0);
	free(d1);
	free(d2);

//	if (cnt_sum > 0)
//		printf("size %ld cnt %d s/c %ld\n",size_sum,cnt_sum,size_sum/cnt_sum);
}

//#define alloc_test

//test
bool test_alloc_check[1000][MAX_OFFSET];
void test_alloc(Node_offset offset)
{
//	printf("alloc %d %d\n",offset.file,offset.offset);
	if (test_alloc_check[offset.file][offset.offset])
	{
		int t;
		printf("error alloc\n");
		scanf("%d\n",&t);
	}
}
void test_free(Node_offset offset)
{
//	printf("free %d %d\n",offset.file,offset.offset);
	if (test_alloc_check[offset.file][offset.offset] == 0)
	{
		int t;
		printf("error free\n");
		scanf("%d\n",&t);
	}
}


uint64_t tt1,tt2,tt3,tt4,tt5; //test
uint64_t qtt1,qtt2,qtt3,qtt4,qtt5,qtt6,qtt7,qtt8;
//----------------------------------------------------------------


//#define SPLIT_LIMIT 8
//std::atomic <uint8_t> split_cnt=0;

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
/*
void at_unlock(std::atomic<uint8_t> &lock)
{
//	return;
	lock = 0;
//	lock.store(0,std::memory_order_release);	
}
*/
/*
int try_at_lock(std::atomic<uint8_t> &lock)
{
//	return 1;
//	const uint8_t z = 0;
	uint8_t z = 0;
//	return lock.compare_exchange_weak(z,1);
	return lock.compare_exchange_strong(z,1);
}
*/
/*
inline unsigned int calc_offset(Node_meta* node) // it will be optimized with define
{
	return ((unsigned char*)node-meta_addr)/sizeof(Node_meta);
}
*/

//---------------------------------------------------------------
#if 1
inline void flush_meta(Node_offset offset)
{
//	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),&(((Node_meta*)(meta_addr + offset*sizeof(Node_meta)))->size),sizeof(uint16_t)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

//	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(uint16_t)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
//	_mm_sfence();

//	pmem_memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

//	node_data_array[offset].next_offset = meta_array[offset].next_offset;
//	pmem_persist(&node_data_array[offset],sizeof(unsigned int));

//	memcpy((Node*)(pmem_addr + offset*sizeof(Node)),(Node_meta*)(meta_addr + offset*sizeof(Node_meta)),sizeof(uint16_t)+sizeof(unsigned int));

	pmem_memcpy(offset_to_node_data(offset),(Node_meta*)offset_to_node(offset),sizeof(unsigned int)+sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

}
#endif

inline void flush_next_offset(Node_offset offset,Node_offset next_offset)
{
	pmem_memcpy(offset_to_node_data(offset),&next_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

}

inline void flush_next_offset_ig(Node_offset offset,Node_offset next_offset_ig)
{
	pmem_memcpy((unsigned char*)offset_to_node_data(offset)+sizeof(unsigned int),&next_offset_ig,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);

}


void new_file()
{
	char file_name[100];
	char buffer[10];
	int len,num,i;
	file_num++;
	strcpy(file_name,pmem_file);
	strcat(file_name,"data");
	len = strlen(file_name);
	num = file_num+1;
	i = 0;
	while(num > 0)
	{
		buffer[i] = num%10+'0';
		i++;
		num/=10;
	}
	for (i=i-1;i>=0;i--)
		file_name[len++] = buffer[i];
	file_name[len] = 0;

	if (USE_DRAM)
		pmem_addr[file_num]=(unsigned char*)mmap(NULL,FILE_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS/*|MAP_POPULATE*/,-1,0);

	else
		pmem_addr[file_num] = (unsigned char*)pmem_map_file(file_name,FILE_SIZE,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);
	meta_addr[file_num] = (unsigned char*)mmap(NULL,META_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);

	node_data_array[file_num] = (Node*)pmem_addr[file_num];
	meta_array[file_num] = (Node_meta*)meta_addr[file_num];

//	meta_used = 0;
//	offset_cnt = 0;
}

Node_offset alloc_node0(int part)
{
#ifdef dtt
	timespec ts1,ts2; // test
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
	Node_meta* node;
	Node_offset offset;

	at_lock(alloc_lock);

	if (free_index[part] == free_min[part])
	{
		int temp;
		temp = min_free_cnt(part);
		if (temp > free_index[part]) // because it is not volatile
			free_min[part] = temp;
//		free_min = min_free_cnt();
	}

	if (free_index[part] < free_min[part])
	{
		offset = free_queue[part][free_index[part]%FREE_QUEUE_LEN];		
//
//	printf("alloc node %d index %d\n",calc_offset(node),free_index); //test
		++free_index[part];
		at_unlock(alloc_lock);		
		if (print)
	printf("alloc node re %p\n",node); //test
#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	_mm_mfence();
	tt5 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

	//test
#ifdef alloc_test
	test_alloc(offset);
	test_alloc_check[offset.file][offset.offset] = 1;
#endif
		return offset;
	}
//	if (meta_size < meta_used + sizeof(Node_meta)) // we need check offset_cnt
	if (part_offset_cnt[part] == MAX_OFFSET/PM_N)	
	{
		if (part_file_num[part] == file_num)
			new_file();
		part_file_num[part]++;
		part_offset_cnt[part] = 0;
		/*
		printf("error!!! need more memory\n");		
		printf("index %d min %d cnt %d\n",free_index,free_min,free_cnt);
		int t;
		scanf("%d",&t);
		*/
	}

//	node = (Node_meta*)(meta_addr + meta_used);
//	pmem_used += sizeof(Node);
//	meta_used += sizeof(Node_meta);	
//	pthread_mutex_unlock(&alloc_mutex);
//	if (print)
//	printf("alloc node %p\n",node); //test
//	printf("alloc node %d\n",calc_offset(node)); //test

//	pthread_mutex_init(&node->mutex,NULL);
	offset.file = part_file_num[part];
//	offset.offset = offset_cnt;

	int offset_cnt = part_offset_cnt[part];
#ifdef SMALL_NODE
	offset.offset = (offset_cnt/(PAGE_SIZE/PM_N/sizeof(Node))*PAGE_SIZE + part*(PAGE_SIZE/PM_N) + (offset_cnt%(PAGE_SIZE/PM_N/sizeof(Node)))*sizeof(Node)) / sizeof(Node);
#else
	offset.offset = offset_cnt * PM_N + part;
#endif

	node = offset_to_node(offset);
	++part_offset_cnt[part];
	at_unlock(alloc_lock);	
//	node->state = 0;//need here? yes because it was 2 // but need here?
	node->inv_kv = (uint16_t*)malloc(sizeof(uint16_t)*4);
	node->inv_max = 4;
	node->inv_cnt = 0;
#ifdef DOUBLE_LOG
	node->flush_kv = (unsigned char**)malloc(sizeof(unsigned char*)*4);
	node->flush_max = 4;
	node->flush_cnt = 0;
#endif

	//test
#ifdef alloc_test
	test_alloc(offset);
	test_alloc_check[offset.file][offset.offset] = 1;
#endif

#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	_mm_mfence();
	tt5 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
	return offset;
}

Node_offset alloc_node(int part)
{
	if (lbac[part] < LOCAL_QUEUE_LEN)
	{

#ifdef alloc_test
	//test
		Node_offset offset = local_batch_alloc[part][lbac[part]];
	test_alloc(offset);
	test_alloc_check[offset.file][offset.offset] = 1;
#endif

		return local_batch_alloc[part][lbac[part]++];
	}
	Node_meta* node;
	Node_offset offset;

//	printf("alloc\n");
	at_lock(alloc_lock);
	int i;
	for (i=0;i<LOCAL_QUEUE_LEN;i++)
	{

	if (free_index[part] == free_min[part])
	{
		int temp;
		temp = min_free_cnt(part);
		if (temp > free_index[part]) // because it is not volatile
			free_min[part] = temp;
	}

	if (free_index[part] < free_min[part])
	{
		local_batch_alloc[part][i]  = free_queue[part][free_index[part]%FREE_QUEUE_LEN];		
		++free_index[part];
		continue;
	}
	if (part_offset_cnt[part] == MAX_OFFSET/PM_N)	
	{
		if (part_file_num[part] == file_num)
			new_file();
		part_file_num[part]++;
		part_offset_cnt[part] = 0;
	}

	offset.file = part_file_num[part];

	int offset_cnt = part_offset_cnt[part];
#ifdef SMALL_NODE
	offset.offset = (offset_cnt/(PAGE_SIZE/PM_N/sizeof(Node))*PAGE_SIZE + part*(PAGE_SIZE/PM_N) + (offset_cnt%(PAGE_SIZE/PM_N/sizeof(Node)))*sizeof(Node))	/ sizeof(Node);
#else
	offset.offset = offset_cnt * PM_N + part;
#endif


	node = offset_to_node(offset);
	++part_offset_cnt[part];

	node->inv_kv = (uint16_t*)malloc(sizeof(uint16_t)*4);
	node->inv_max = 4;
	node->inv_cnt = 0;

#ifdef DOUBLE_LOG
	node->flush_kv = (unsigned char**)malloc(sizeof(unsigned char*)*4);
	node->flush_max = 4;
	node->flush_cnt = 0;
#endif


	local_batch_alloc[part][i]  = offset;
	}
	at_unlock(alloc_lock);
	lbac[part] = 0;

#ifdef alloc_test
	//test
	offset = local_batch_alloc[part][lbac[part]];
	test_alloc(offset);
	test_alloc_check[offset.file][offset.offset] = 1;
#endif

	return local_batch_alloc[part][lbac[part]++];
}
void free_node(Node_offset offset)
{
#ifdef SMALL_NODE
	int part = offset.offset%(PAGE_SIZE/sizeof(Node))/(PAGE_SIZE/PM_N/sizeof(Node));
#else
	int part = offset.offset%PM_N;
#endif

	if (lbfc[part] < LOCAL_QUEUE_LEN)
	{
#ifdef alloc_test
	test_free(offset);
	test_alloc_check[offset.file][offset.offset] = 0;
#endif
		local_batch_free[part][lbfc[part]++] = offset;
		return;
	}

//		printf("free\n");
		at_lock(alloc_lock);	

		int i;
		for (i=0;i<LOCAL_QUEUE_LEN;i++)
		{
#if 0
		while(free_index[part] + FREE_QUEUE_LEN/2 < free_cnt[part]) // test
		{
//			if (free_index + FREE_QUEUE_LEN/2 < free_cnt)
			{
				update_idle();
//				printf("queue warn\n");
				int t;
//				scanf("%d",&t);
//				printf ("%d %d %d %d \n",part,free_index[part],free_min[part],free_cnt[part]);

			if (free_index[part] + FREE_QUEUE_LEN < free_cnt[part])
			{
				printf("queue full %d %d %d %d\n",part,free_index[part],free_min[part],free_cnt[part]);
			printf("queue full\n");
				print_thread_info();
				scanf("%d",&t);
//				break;
				continue;
			}
			}
			break;
		}
#endif
		free_queue[part][free_cnt[part]%FREE_QUEUE_LEN] = local_batch_free[part][i];
		++free_cnt[part];
		}
		at_unlock(alloc_lock);
#ifdef alloc_test
	test_free(offset);
	test_alloc_check[offset.file][offset.offset] = 0;
#endif
		lbfc[part]=0;
		local_batch_free[part][lbfc[part]++] = offset;
}

void free_node0(Node_offset offset) // without local batch ...
{
#ifdef SMALL_NODE
	int part = offset.offset%(PAGE_SIZE/sizeof(Node)/(PAGE_SIZE/PM_N/sizeof(Node)));
#else
	int part = offset.offset % PM_N;
#endif

		while(1) // test
		{
			if (free_index[part] + FREE_QUEUE_LEN/2 < free_cnt[part])
			{
				update_idle();
//				printf ("%d %d %d %d \n",part,free_index[part],free_min[part],free_cnt[part]);

			if (free_index[part] + FREE_QUEUE_LEN < free_cnt[part])
			{
//				printf("queue full %d %d %d\n",free_min,free_index,free_cnt);
			printf("queue full\n");
//				print_thread_info();
				int t;
				scanf("%d",&t);
				continue;
			}
			}
		at_lock(alloc_lock);		
//
#ifdef alloc_test
	//test
	test_free(offset);
	test_alloc_check[offset.file][offset.offset] = 0;
#endif

		free_queue[part][free_cnt[part]%FREE_QUEUE_LEN] = offset;	
//	printf("free node %d cnt %d\n",calc_offset(node),free_cnt);
		++free_cnt[part];
		at_unlock(alloc_lock);	
		break;
		}
//	if (print)
//	printf("free node %p\n",node);
}


void init_file()
{
#if 0
	if (USE_DRAM)
		pmem_addr=(unsigned char*)mmap(NULL,pmem_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS/*|MAP_POPULATE*/,-1,0);

	else
		pmem_addr = (unsigned char*)pmem_map_file(pmem_file,pmem_size,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);
#endif
	file_num = -1; // 0 is INV
	pmem_addr = (unsigned char**)malloc(sizeof(unsigned char*)*MAX_FILE_NUM);
	meta_addr = (unsigned char**)malloc(sizeof(unsigned char*)*MAX_FILE_NUM);
	node_data_array = (Node**)malloc(sizeof(unsigned char*)*MAX_FILE_NUM);
	meta_array = (Node_meta**)malloc(sizeof(unsigned char*)*MAX_FILE_NUM);
//	node_data_array = (Node*)pmem_addr;
//	file_size = 1024*1024*1024;
//	file_size = sizeof(Node)*(1<<16);	

//	meta_size = file_size/sizeof(Node)*sizeof(Node_meta);
//	meta_addr = (unsigned char*)mmap(NULL,meta_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);
//	meta_array = (Node_meta*)meta_addr;
/*
	if (pmem_addr == NULL)
	{
		printf("pmem error\n");
		return -1;
	}
*/
//	pmem_used = 0;
//	meta_used = 0;

}

int check_recover()
{

#ifndef try_recover
	printf("don't try recover\n");
	return -1;
#endif

	printf("try recover\n");



	char file_name[100];
	char buffer[10];
	int len,num,i;
	while(1)
	{
	file_num++;
	strcpy(file_name,pmem_file);
	strcat(file_name,"data");
	len = strlen(file_name);
	num = file_num+1;
	i = 0;
	while(num > 0)
	{
		buffer[i] = num%10+'0';
		i++;
		num/=10;
	}
	for (i=i-1;i>=0;i--)
		file_name[len++] = buffer[i];
	file_name[len] = 0;

	if (access(file_name,F_OK) == -1)
	{
		file_num--;
		return file_num;
	}


	if (USE_DRAM)
	{
		printf("???");
	}
//		pmem_addr[file_num]=(unsigned char*)mmap(NULL,FILE_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS/*|MAP_POPULATE*/,-1,0);

	else
		pmem_addr[file_num] = (unsigned char*)pmem_map_file(file_name,FILE_SIZE,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);
	meta_addr[file_num] = (unsigned char*)mmap(NULL,META_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);

	node_data_array[file_num] = (Node*)pmem_addr[file_num];
	meta_array[file_num] = (Node_meta*)meta_addr[file_num];
	}

//	meta_used = 0;
//	offset_cnt = 0;


}

uint64_t pre_LSB[65];

#define DUP_HASH_MAX 1024
Node_offset dup_hash[DUP_HASH_MAX];

void recover_node(Node_offset node_offset) // recover record and meta
{
	static uint64_t prefix64=0;
	static int prefix_len=0;
	static Node node_data_temp[PART_MAX];

	Node* node_data;
	Node_meta* node_meta;
	Node_meta* meta0;

	int new_len;

	node_meta = offset_to_node(node_offset);
	node_data = offset_to_node_data(node_offset);

	meta0 = node_meta;

	new_len = node_data->continue_len;

	if (prefix_len > 0)
		prefix64+=pre_LSB[prefix_len-1];

	int i;
	for (i=prefix_len;i<new_len;i++)
	{
		//inesrt to range hash table
		insert_range_entry((unsigned char*)&prefix64,i,SPLIT_OFFSET);
		prefix64 &= ~pre_LSB[i];
	}

	insert_range_entry((unsigned char*)&prefix64,new_len,node_offset);
	prefix_len = new_len;

	int part=0;
	int tc = 0;
	uint64_t temp_key[PART_MAX*100];
	ValueEntry vea[PART_MAX*100];
	Node_offset temp_offset[PART_MAX];

	unsigned char* buffer;

	node_meta->part = 0;
	node_meta->state = 0;
	node_meta->group_size = 0;
	node_meta->invalidated_size = 0;
	node_meta->start_offset = node_offset;
	node_meta->continue_len = prefix_len;
//	node_meta->prev_offset = 


	while(1)
	{
		temp_offset[part] = node_offset;

		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		cp256((unsigned char*)&node_data_temp[part],(unsigned char*)node_data,sizeof(Node));
//		memcpy((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);

		//alloc node
	node_meta->inv_kv = (uint16_t*)malloc(sizeof(uint16_t)*4);
	node_meta->inv_max = 4;
	node_meta->inv_cnt = 0;


		node_meta->size = 0;
		node_meta->part = part;
//		node_meta->inv_cnt = 0;
		node_meta->start_offset = temp_offset[0];
		node_meta->next_offset_ig = INIT_OFFSET;

		buffer = node_data_temp[part].buffer;
		while(1)
		{
			uint16_t vl16;
			vl16 = *((uint16_t*)buffer);
			if (vl16 == 0)
				break;
			if (vl16 & INV_BIT)
				vl16-=INV_BIT;
			else
			{
				temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE));
				if (dup_hash[temp_key[tc]%DUP_HASH_MAX] == temp_offset[0]) // dup???
				{
					for (i=tc-1;i>=0;i--)
					{
						if (temp_key[i] == temp_key[tc])
							break;
					}
					if (i >= 0)
					{
						invalidate_kv(vea[i]);
						vea[i].kv_offset = 0;
					}
				}
				dup_hash[temp_key[tc]%DUP_HASH_MAX] = temp_offset[0];
				vea[tc].node_offset = temp_offset[part];
				vea[tc].kv_offset = buffer-(unsigned char*)&node_data_temp[part];
				vea[tc].len = vl16;
				tc++;
			}
			buffer+=LK_SIZE+vl16;
		}

		node_meta->size = buffer-node_data_temp[part].buffer;
		meta0->group_size+=node_meta->size;



//size_sum+=node_meta->size+meta_size;

		node_meta->next_offset_ig = node_data_temp[part].next_offset_ig;
		node_offset = node_data_temp[part].next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;

		part++;

	}

	for (i=0;i<tc;i++)
	{
		if (vea[i].kv_offset > 0)
			insert_point_entry((unsigned char*)&temp_key[i],vea[i]);

	}

	for (i=0;i<part;i++)
		offset_to_node(temp_offset[i])->end_offset = temp_offset[part];

	Node_offset_u nu;
	nu.no = node_data_temp[0].next_offset;
	meta0->next_offset = nu.no_32;
//	meta0->next_offset = d0[0].next_offset;

}

void recover()
{

	//init recover
	int i,j;
	pre_LSB[63] = 1;
	for (i=62;i>=0;i--)
		pre_LSB[i] = pre_LSB[i+1] << 1;

	for (i=0;i<=file_num;i++) // file num xxx
	{
		for (j=0;j<MAX_OFFSET;j++)
			meta_array[i][j].part = PART_MAX+1; // free check
	}

	Node_offset_u node_offset;
	Node_offset_u prev_offset;
	Node* node_data;
	Node_meta* node_meta;

	node_offset.no = HEAD_OFFSET;
	node_meta = offset_to_node(node_offset.no);
	node_data = offset_to_node_data(node_offset.no);

	//head recover
	node_meta->state = 0;
	node_meta->part = 0;
	node_meta->scan_list = NULL;
	node_meta->prev_offset = 0;
//	node_meta->next_offset = node_data->next_offset;
	node_meta->next_offset_ig = INIT_OFFSET;

	prev_offset = node_offset;
	node_offset.no = node_data->next_offset;
	node_meta->next_offset = node_offset.no_32;

//	uint64_t prefix64=0;
//	int prefix_len=0;

	while(1)
	{

		node_meta = offset_to_node(node_offset.no);
		node_meta->prev_offset = prev_offset.no_32;

		if (node_offset.no == TAIL_OFFSET)
			break;

//		node_data = offset_to_node_data(node_offset.no);

		recover_node(node_offset.no); // recover record and meta

		prev_offset = node_offset;
		node_offset.no_32 = node_meta->next_offset;


	}

	//tail recover
		node_meta = offset_to_node(node_offset.no);
		node_meta->state = 0;
		node_meta->part = 0;
		node_meta->scan_list = NULL;
		node_meta->next_offset = 0;//INIT_OFFSET;
		node_meta->next_offset_ig = INIT_OFFSET;

	// collect free nodes without alloc lock
		Node_offset no;
		int part;
	for (i=0;i<=file_num;i++) // file num xxx
	{
		for (j=0;j<MAX_OFFSET;j++)
		{
			if (meta_array[i][j].part == PART_MAX+1) // free check
			{
				no.file = i;
				no.offset = j;
//				free_node0(no);
				part = no.offset % PM_N; // ???

				if (free_index[part] + FREE_QUEUE_LEN < free_cnt[part])
				{
					printf("full!!\n");
					scanf("%d",&part);
					return;
				}

				free_queue[part][free_cnt[part]%FREE_QUEUE_LEN] = no;
				++free_cnt[part];
				
			}
		}

	}

}

void init_data() // init hash first!!!
{
	if (check_recover() >= 0)
	{

		timespec st,et;
		uint64_t tt;

		printf("start recover\n");
		clock_gettime(CLOCK_MONOTONIC,&st);
		recover();
		clock_gettime(CLOCK_MONOTONIC,&et);

		tt = (et.tv_sec-st.tv_sec)*1000000000+et.tv_nsec-st.tv_nsec;

		printf("end recover %lds %ldns\n",tt/1000000000,tt%1000000000);


//		return;
	}
	else
	{
		printf("no recovery. new DB\n");
		file_num = -1;
	
	new_file();

	int i;
	for (i=0;i<PM_N;i++)
		free_cnt[i] = free_min[i] = free_index[i] = 0;

	uint64_t zero=0;

	Node_offset_u head_offset;
	Node_offset_u tail_offset;

	Node_meta* head_node;// = offset_to_node(head_offset.no);
	Node_meta* tail_node;// = offset_to_node(tail_offset.no);


//	update_free_cnt(); // temp main thread

	//push for reserve
#if PM_N > 1
	alloc_node0(0); // 0 // INIT_OFFSET
	alloc_node0(1); // 1 SPLIT_OFFSET
	head_offset.no = alloc_node0(2); // 2 head
	tail_offset.no = alloc_node0(3); // 3 tail
#else
	alloc_node0(0); // 0 // INIT_OFFSET
	alloc_node0(0); // 1 SPLIT_OFFSET
	head_offset.no = alloc_node0(0); // 2 head
	tail_offset.no = alloc_node0(0); // 3 tail

#endif

	head_node = offset_to_node(head_offset.no);
	tail_node = offset_to_node(tail_offset.no);

//	Node_offset node_offset = alloc_node();
	Node_offset_u node_offset;
	node_offset.no = alloc_node0(0);	//4 ROOT OFFSET // no thread local
	Node_meta* node = offset_to_node(node_offset.no);
	node->state = 0;
	node->size = 0;
	node->invalidated_size = 0;
	node->inv_cnt = 0;
#ifdef DOUBLE_LOG
	node->flush_size = 0;
	node->flush_cnt = 0;
#endif
	node->continue_len = 0;
	node->scan_list = NULL;
	node->prev_offset = head_offset.no_32;
	node->next_offset = tail_offset.no_32;
	node->end_offset = node_offset.no;
	node->start_offset = node_offset.no;
	node->part = 0;
	node->group_size = 0;
	node->next_offset_ig = INIT_OFFSET;//0

	if (print)
	printf("node 0 %p\n",node);


	head_node->next_offset = node_offset.no_32;
	head_node->prev_offset = 0;
	tail_node->prev_offset = node_offset.no_32;
	tail_node->next_offset = 0;
	head_node->state = 0;
	head_node->scan_list = NULL;
	head_node->part = 0;
	tail_node->state = 0;
	tail_node->scan_list = NULL;
	tail_node->part = 0;
	head_node->next_offset_ig = INIT_OFFSET;//0;
	tail_node->next_offset_ig = INIT_OFFSET;//0;


	flush_meta(head_offset.no);
	flush_meta(tail_offset.no);
//	flush_next_offset(head_offset.no,node_offset.no);	
	_mm_sfence();

//	inc_ref(HEAD_OFFSET,0);
//	inc_ref(TAIL_OFFSET,0); //don't free these node	

	insert_range_entry((unsigned char*)(&zero),0,node_offset.no); // the length is important!!!


//	exit_thread(); // remove main thread // moved to global
	}

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

}
void init_data_local()
{
	int i;
	for (i=0;i<PM_N;i++)
	{
		lbac[i] = LOCAL_QUEUE_LEN;
		lbfc[i] = 0;
	}

	posix_memalign((void**)&d0,sizeof(Node),sizeof(Node)*PART_MAX);
	posix_memalign((void**)&d1,sizeof(Node),sizeof(Node)*PART_MAX);
	posix_memalign((void**)&d2,sizeof(Node),sizeof(Node)*PART_MAX);

}
void clean_node(Node_offset offset)
{
	Node_meta* node = offset_to_node(offset);
	free(node->inv_kv);
#ifdef DOUBLE_LOG
	free(node->flush_kv);
#endif
}
void clean_inv()
{
//not now
	// what was it???
	/*
	Node_offset offset;
	for (i=0;i<file_num-1;i++)
	{
		for (j=0;j<MAX_OFFSET;j++)
		{
			offset.file = i;
			offset.offset = j;
			clean_node(offset);
		}
	}
	offset.file = i;
	for (j=0;j<offset_cnt;j++)
	{
		offset.offset = j;
		clean_node(offset);
	}
	*/

	// not like this
	// inv all nodes

	// should set INV_BIT before shutdown
	// maybe hash will ...
}
void clean_data()
{
	printf("clean data\n");
	/*
	free_node(offset_to_node(0));
	free_node(offset_to_node(1));
	free_node(offset_to_node(2));
*/
	//release free list
	clean_inv();

//	size_test();
	int i;
	if (USE_DRAM)
	{
		for (i=0;i<file_num;i++)
			munmap(pmem_addr[i],FILE_SIZE);
	}
	else
	{
		for (i=0;i<file_num;i++)
			pmem_unmap(pmem_addr[i],FILE_SIZE);
	}
	for (i=9;i<file_num;i++)
		munmap(meta_addr[i],META_SIZE);

	free(pmem_addr);
	free(meta_addr);
	free(node_data_array);
	free(meta_array);
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

//	printf("used %ld size %ld\n",(uint64_t)meta_used/sizeof(Node_meta),(uint64_t)meta_used/sizeof(Node_meta)*sizeof(Node));
	printf("meta %lfGB\n",double(file_num*MAX_OFFSET*sizeof(Node_meta))/1024/1024/1024);
	printf("total %lfGB file cnt %d file size %ld\n",double(file_num*FILE_SIZE)/1024/1024/1024,file_num,FILE_SIZE);
//	printf("index %d min %d cnt %d\n",free_index,free_min,free_cnt);

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

#if 0
inline int inc_ref(Node_offset offset) // nobody use this
{
	return try_at_lock(offset_to_node(offset)->state);	
}
inline void dec_ref(Node_offset offset)
{
//	((Node_meta*)(meta_addr + offset*sizeof(Node_meta)))->state = 0;
	at_unlock(offset_to_node(offset)->state);
//	return dec_ref(offset_to_node(offset));
}
#endif
#if 0
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
#endif
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
	*((uint16_t*)(kv_p/*+key_size*/))|= INV_BIT; // invalidate
	else
	{
		uint16_t vl16;
		vl16 = *((uint16_t*)(kv_p/*+key_size*/)) | INV_BIT;
		pmem_memcpy(kv_p/*+key_size*/,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}
}

//insert
#if 0
Node_offset append_node(Node_offset& start_offset)
{
//	Node_offset start_offset = offset_to_node(offset)->start_offset;
	Node_meta* start_meta = offset_to_node(start_offset);
	Node_meta* meta = offset_to_node(start_meta->end_offset);
	Node_offset end_offset;
	Node_meta* end_meta;
	Node* end_data;

//		if (end_meta->part == PART_MAX-1)
//			return NULL; // need split
		//append node

		end_offset = alloc_node();
		end_meta = offset_to_node(end_offset);
		end_data = offset_to_node_data(end_offset);

//		end_meta->next_offset = mid_meta->next_offset;

		end_meta->next_offset_ig = INIT_OFFSET;

		end_meta->part = meta->part+1;
		end_meta->size = 0;
		end_meta->inv_cnt = 0;
#ifdef DOUBLE_LOG
		end_meta->flush_size = 0;
		end_meta->flush_cnt = 0;
#endif
		end_meta->start_offset = start_offset;
//		end_meta->continue_len = meta->continue_len;
//
		end_meta->end_offset = end_offset;
/*
		uint16_t z=0;
		memcpy((unsigned char*)end_data,end_meta,sizeof(uint32_t)*2);
		memcpy((unsigned char*)offset_to_node_data(end_offset)+sizeof(uint32_t)*2,&(start_meta->continue_len),sizeof(uint16_t));
		memcpy((unsigned char*)offset_to_node_data(end_offset)+sizeof(uint32_t)*3,&z,len_size);
		*/

		end_data->next_offset = end_data->next_offset_ig = INIT_OFFSET;
//		end_data->continue_len = start_meta->continue_len;
		end_data->buffer[0] = end_data->buffer[1] = 0;
		pmem_persist((unsigned char*)end_data,sizeof(uint16_t)+sizeof(uint16_t)+sizeof(uint32_t)+len_size);
		_mm_sfence();

		meta->next_offset_ig = end_offset;
		pmem_memcpy((unsigned char*)offset_to_node_data(start_meta->end_offset) + sizeof(unsigned int),(uint32_t*)&end_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		start_meta->end_offset = end_offset;
		_mm_sfence();

	return end_offset;	
}
#endif

unsigned char* insert_kv(Node_offset& offset,unsigned char* key,unsigned char* value,int value_len/*,int old_size*/)
{
#ifdef dtt
	timespec ts1,ts2; // test
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif

//	if (print)
//	printf("insert kv offset %u\n",offset);
//	Node* node_data;
//	unsigned char* kv_p;
//	uint16_t vl16 = value_length;
	Node_meta* meta = offset_to_node(offset);
	Node_offset end_offset = meta->end_offset;
	Node_meta* end_meta = offset_to_node(end_offset);

	Node_offset mid_offset;
	Node_meta* mid_meta = NULL;

	int entry_size = LK_SIZE/*key_size + len_size*/ + value_len;

	if (entry_size % 2)
		++entry_size;

	if (end_meta->size + entry_size +PH_LEN_SIZE/*len_size*/ > NODE_BUFFER) // append node
	{
		if (end_meta->part == PART_MAX-1)
			return NULL; // need split

		if (split_or_compact(offset) == 0) //need compact
			return NULL;

		//append node
		mid_offset = end_offset;
		mid_meta = end_meta;
#ifdef SMALL_NODE
		int part = end_offset.offset%(PAGE_SIZE/sizeof(Node))/(PAGE_SIZE/PM_N/sizeof(Node));
#else
		int part = end_offset.offset % PM_N;
#endif

		end_offset = meta->end_offset = alloc_node((part+1)%PM_N);
		end_meta = offset_to_node(end_offset);

//		end_meta->next_offset = mid_meta->next_offset;
		end_meta->part = mid_meta->part+1;
		end_meta->size = 0;
		end_meta->inv_cnt = 0;
#ifdef DOUBLE_LOG
		end_meta->flush_size = 0;
		end_meta->flush_cnt = 0;
#endif
		end_meta->start_offset = offset;
		end_meta->next_offset_ig = INIT_OFFSET;
//		end_meta->state = 0; // appended node doesn't use lock at all
//		new_meta->invalidated_size
//
		meta->end_offset = end_offset;
//		mid_meta->next_offset_ig = end_offset; //not now
//		offset = end_offset;
	}



	const uint16_t vl16 = value_len;
//	int new_size = old_size;	

//	node_data = offset_to_node_data(offset); 

	unsigned char* buffer = offset_to_node_data(end_offset)->buffer;
	int old_size = end_meta->size;

	end_meta->size+=entry_size;
	meta->group_size+=entry_size;

	const uint16_t z = 0;
	if (!USE_DRAM)
	{
		if (mid_meta) // new end node flush
		{
			Node* node_data = offset_to_node_data(end_offset);
//			node_data->next_offset = *((Node_offset*)&meta->next_offset);//mid_meta->next_offset;
//			node_data->next_offset_ig = INIT_OFFSET; // this is the end of group
//			node_data->part = end_meta->part;

			node_data->next_offset = node_data->next_offset_ig = INIT_OFFSET;
//			node_data->continue_len = meta->continue_len; // continue len is used by start node

			memcpy(buffer+old_size,&vl16,PH_LEN_SIZE/*len_size*/);
			memcpy(buffer+old_size+PH_LEN_SIZE/*len_size*/,key,PH_KEY_SIZE/*key_size*/);
			memcpy(buffer+old_size+LK_SIZE/*key_size+len_size*/,value,value_len);
			memcpy(buffer+old_size+entry_size,&z,PH_LEN_SIZE);//len_size);
			pmem_persist(node_data,sizeof(uint32_t)*3+entry_size+PH_LEN_SIZE);//len_size);
			_mm_sfence();
		}
		else
		{
//		unsigned char buffer[NODE_BUFFER];
//		memcpy(buffer,key,key_size);
//		memcpy(buffer+key_size,&vl16,len_size);
//		memcpy(buffer+key_size+len_size,value,value_length);
/*		
		pmem_memcpy(node_data->buffer+old_size,key,key_size,PMEM_F_MEM_NONTEMPORAL);
		pmem_memcpy(node_data->buffer+old_size+key_size,&vl16,len_size,PMEM_F_MEM_NONTEMPORAL);
		pmem_memcpy(node_data->buffer+old_size+key_size+len_size,value,value_length,PMEM_F_MEM_NONTEMPORAL);
		*/
		memcpy(buffer+old_size+PH_LEN_SIZE/*len_size*/,key,PH_KEY_SIZE);//key_size);
//		memcpy(node_data->buffer+old_size+key_size,&vl16,len_size);
		memcpy(buffer+old_size+LK_SIZE/*key_size+len_size*/,value,value_len);
		memcpy(buffer+old_size+entry_size,&z,PH_LEN_SIZE);//len_size);

		pmem_persist(buffer+old_size+PH_LEN_SIZE/*len_size*/,entry_size); // key + value + (1) + len


//		pmem_memcpy(node_data->buffer+old_size,buffer,key_size+len_size+value_length,PMEM_F_MEM_NONTEMPORAL);
//		memcpy(node_data->buffer+old_size,buffer,key_size+len_size+value_length);

		_mm_sfence();

		pmem_memcpy(buffer+old_size,&vl16,PH_LEN_SIZE/*len_size*/,PMEM_F_MEM_NONTEMPORAL); //validate

//i		vl16 = old_size + key_size+len_size+value_length;
//		pmem_memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL); // we will remove this
		_mm_sfence();
//		memcpy((void*)&node_data->size,&vl16,sizeof(uint16_t)); // we will remove this
		}
	}
	else
	{
		memcpy(buffer+old_size+PH_LEN_SIZE/*len_size*/,key,PH_KEY_SIZE);//key_size);
		memcpy(buffer+old_size+LK_SIZE/*key_size+len_size*/,value,value_len);
		memcpy(buffer+old_size,&vl16,PH_LEN_SIZE);//len_size);
	}
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
	if (mid_meta)
	{
//		mid_meta->next_offset = *((uint32_t*)&end_offset); // no scan durint expand
//		flush_meta(mid_offset);

//		_mm_sfence();

		mid_meta->next_offset_ig = end_offset;
		pmem_memcpy((unsigned char*)offset_to_node_data(mid_offset) + sizeof(unsigned int),(uint32_t*)&end_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}
	return buffer+old_size;	
}

//void move_scan_list(Node_meta* node_old,Node_meta* node_new)
#if 1 
void move_scan_list(Node_offset &old_offset,Node_offset &new_offset)
{
	Node_meta* node_old = offset_to_node(old_offset);
	Node_meta* node_new = offset_to_node(new_offset);
	if (node_old->scan_list == NULL)
	{
		node_new->scan_list = NULL;
		return;
	}
	Scan_list* sl;
	Scan_list** slp;
//	Node_offset old_offset,new_offset;
	Query* query;
//	old_offset = point_to_offset((unsigned char*)node_old);
//	new_offset = point_to_offset((unsigned char*)node_new);
	sl = node_old->scan_list;
	slp = &(node_old->scan_list);
	Node_offset_u oou,nou;
	oou.no = old_offset;
	nou.no = new_offset;
	while(sl)
	{
		query = (Query*)(sl->query);
//		pthread_mutex_lock(&query->scan_mutex);
//		at_lock(query->scan_lock);		
		if (query->scan_offset == oou.no_32)
		{
			query->scan_offset = nou.no_32;
//			pthread_mutex_unlock(&query->scan_mutex);
//			at_unlock(query->scan_lock);			
			slp = &(sl->next);
			sl = sl->next;
		}
		else
		{
//			pthread_mutex_unlock(&query->scan_mutex);
//			at_unlock(query->scan_lock);			
			printf("abandoned scan entry??\n");
			*slp = sl->next;
			free(sl);
			sl = *slp;
		}
	}
	node_new->scan_list = node_old->scan_list;
}
#endif
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

void sort_inv(int cnt,uint16_t* array)
{
	int i,j;
	uint16_t temp;
	for (i=0;i<cnt;i++)
	{
		temp = 0;
		for (j=0;j<cnt-i-1;j++)
		{
			if (array[j] > array[j+1])
			{
				temp = array[j+1];
				array[j+1] = array[j];
				array[j] = temp;
			}
		}
		if (temp == 0)
			break;
	}

}


int lock_check(Node_offset offset)
{
	Node_meta* prev_node;
	Node_meta* next_node;
	Node_meta* node;

	node = offset_to_node(offset);

	Node_offset_u prev_offset,next_offset;
       prev_offset.no_32	= node->prev_offset;
	prev_node = offset_to_node(prev_offset.no);

//------------------------------------------------------------ check

		if (prev_node->state == 1)
		{
			return -1; // failed
		}

//	_mm_mfence();

next_offset.no_32 = node->next_offset;
	next_node = offset_to_node(next_offset.no);
		if (next_node->state == 1)
		{
			return -1; //split thread has to giveup
			/*
			while(next_node->state == 1)
{
next_offset.no_32 = node->next_offset;
	next_node = offset_to_node(next_offset.no);

}
*/
		}


		return 1;
}


int split2(Node_offset offset)//,unsigned char* prefix)//,unsigned char* prefix, int continue_len) // locked
{

//0 -> 1 + 2

// read p d
// split d d (and alloc)
// write d p
// meta//

//	Node d0[PART_MAX];
//	Node d1[PART_MAX];
//	Node d2[PART_MAX];

	Node_offset node_offset;
	Node_meta* node_meta;
	Node* node_data;


	const int meta_size = d0[0].buffer-(unsigned char*)&d0[0];
	int part0,part1,part2;

	part0 = 0;


	node_offset = offset;
	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		cp256((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);
		part0++;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;
	}
/*
	if (part0 > 4) // test
	{
		printf("part %d\n",part0);
		scanf("%d",&part0);
	}
*/
	//---------------------------------------------------------------------------------

	int i;
	uint16_t size;//,size1,size2;

	int tc;
	uint64_t temp_key[100*PART_MAX];
	ValueEntry vea[100*PART_MAX];
	Node_offset temp_offset[PART_MAX];	
	int oc;

	int continue_len = offset_to_node(offset)->continue_len;
	uint64_t prefix_64;
	uint64_t m;
	prefix_64 = *((uint64_t*)(d0[0].buffer+PH_LEN_SIZE));//len_size)); // find key from node
		// always can find because it needs split
	m = ~(((uint64_t)1 << (63-continue_len))-1); // it seems wrong but ok because of next line it is always overwrited
	prefix_64 = (prefix_64 & m) | ((uint64_t)1 << (63-continue_len));

	part1 = 0;
	part2 = 0;

//	unsigned char buffer1[NODE_BUFFER],buffer2[NODE_BUFFER];
	Node_meta* new_node1;
	Node_meta* new_node2;
	Node_meta* prev_node;
	Node_meta* next_node;
//	Node* node_data;
//	Node* new_node1_data;
//	Node* new_node2_data;
//	Node* prev_node_data;

	Node_offset_u prev_offset,next_offset;

	node_meta = offset_to_node(offset);
	node_data = offset_to_node_data(offset);

	prev_offset.no_32 = node_meta->prev_offset;
	next_offset.no_32 = node_meta->next_offset;
	next_node = offset_to_node(next_offset.no);
	prev_node = offset_to_node(prev_offset.no);

	Node_offset_u new_node1_offset;
	Node_offset_u new_node2_offset;

	int rotation1,rotation2;
	new_node1_offset.no = alloc_node(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	rotation1 = part_rotation;

	new_node2_offset.no = alloc_node(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	rotation2 = part_rotation;

	new_node1 = offset_to_node(new_node1_offset.no);
	new_node2 = offset_to_node(new_node2_offset.no);

	new_node1->part = 0;
	new_node2->part = 0;

	new_node1->state = 1; // 
	new_node2->state = 1; //

	//need fence

	new_node1->inv_cnt = new_node2->inv_cnt = 0;
	new_node1->group_size = new_node2->group_size = 0;
	new_node1->invalidated_size = new_node2->invalidated_size = 0;

	new_node1->start_offset = new_node1_offset.no;
	new_node2->start_offset = new_node2_offset.no;

	new_node1->continue_len = new_node2->continue_len = continue_len+1;

	//need lock
	new_node2->next_offset = next_offset.no_32;//node->next_offset;
	new_node2->prev_offset = new_node1_offset.no_32;//calc_offset(new_node1);

	new_node1->next_offset = new_node2_offset.no_32;//calc_offset(new_node2);
	new_node1->prev_offset = prev_offset.no_32;//node->prev_offset;

	//no scan

//	new_node1_data = offset_to_node_data(new_node1_offset.no);//calc_offset(new_node1));
//	new_node2_data = offset_to_node_data(new_node2_offset.no);//calc_offset(new_node2));

	d1[0].next_offset = new_node2_offset.no;
	d2[0].next_offset = next_offset.no;
	d1[0].continue_len = d2[0].continue_len = continue_len+1;
//	memcpy(new_node1_data,new_node1,sizeof(unsigned int)*3);
//	memcpy(new_node2_data,new_node2,sizeof(unsigned int)*3);
	// flush later

	//if 8 align
//	uint64_t prefix_64 = *((uint64_t*)prefix),m;



	uint16_t vl16;
//	const int kls = LK_SIZE;//key_size + len_size;
	uint16_t kvs;

	unsigned char* buffer;
	unsigned char* buffer1;
	unsigned char* buffer2;

//	buffer = d0[0].buffer;
	buffer1 = d1[0].buffer;
	buffer2 = d2[0].buffer;

	tc = 0;

	int j;
//	node->inv_kv[node->inv_cnt] = 0;

//	Node_offset current_offset0 = offset;
//	Node_offset_u next_offset0;
//	Node_offset next_offset0;
//	Node* current_node0_data = node_data;
//	Node* current_node1_data = new_node1_data;
//	Node* current_node2_data = new_node2_data;
	Node_meta* current_node0_meta;
	Node_meta* current_node1_meta = new_node1;
	Node_meta* current_node2_meta = new_node2;
	Node_offset current_node0_offset = offset;
	Node_offset current_node1_offset = new_node1_offset.no;
	Node_offset current_node2_offset = new_node2_offset.no;
	unsigned char* buffer_end;
	unsigned char* buffer1_end = buffer1+NODE_BUFFER;
	unsigned char* buffer2_end = buffer2+NODE_BUFFER;

	unsigned char* kvp;
oc = 0;
i=0;
	while(1)
	{
	current_node0_meta = offset_to_node(current_node0_offset);
//	current_node0_data = offset_to_node_data(current_node0_offset); 
//	next_offset0 = current_node0_data->next_offset_ig;
//	next_offset0.no_32 = current_node0_meta->next_offset_ig;
//	next_offset0 = current_node0_meta->next_offset_ig;

//#ifdef DRAM_BUF
//	memcpy(d,current_node0_data,sizeof(Node));
//	buffer = buffer_o+data_offset;

//	d_node0 = *current_node0_data;
//	buffer = d_node0.buffer;
	buffer = d0[i].buffer;
//#else
//	buffer = current_node0_data->buffer;
//#endif

	buffer_end = buffer + current_node0_meta->size; 
	sort_inv(current_node0_meta->inv_cnt,current_node0_meta->inv_kv);
	j = 0;
	current_node0_meta->inv_kv[current_node0_meta->inv_cnt] = 0;
//node0 start here?

	temp_offset[oc++] = current_node0_offset;


	while(buffer < buffer_end)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		memcpy(&lak,buffer,kls);
		vl16 = *((uint16_t*)(buffer/*+cur+key_size*/));
//		kvs = kls + lak.len;
		kvs = LK_SIZE + vl16;

		if (kvs % 2)
			kvs++;
//		if ((lak.len & (1 << 15)) == 0)
//#ifdef DRAM_BUF
		if (((unsigned char*)&d0[i] + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/)
//#else
//		if (((unsigned char*)current_node0_data + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/)
//#endif
		{
			j++;
			kvs &= ~(INV_BIT);
		}
		else
		{
			temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE));//len_size));
//		temp_len[tc] = value_len;	
			// we use double copy why?
//			if (*((uint64_t*)(buffer+len_size)) < prefix_64)
			if (temp_key[tc] < prefix_64)	
//			if (lak.key < prefix_64)	
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
			{
				if (buffer1+kvs+PH_LEN_SIZE/*len_size*/ > buffer1_end) // append node
				{
// add end len
					buffer1[0] = buffer1[1] = 0;
//#ifdef DRAM_BUF
					current_node1_meta->size = buffer1-d1[part1].buffer;
//#else
//					current_node1_meta->size = buffer1-current_node1_data->buffer;
//#endif
//					current_node1_meta->invalidated_size = 0;
					new_node1->group_size+=current_node1_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
//				       temp_offset	= alloc_node(current_node1_meta->part+1);
					temp_offset = alloc_node(rotation1);
					rotation1 = (rotation1+1)%PM_N;
					Node_meta* temp_meta = offset_to_node(temp_offset);
//#ifdef DRAM_BUF
					d1[part1].next_offset_ig = temp_offset;
//					pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//					nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));
//#else
//					current_node1_data->next_offset_ig = temp_offset;
//					pmem_persist(current_node1_data,sizeof(Node));
//#endif
					current_node1_meta->next_offset_ig = temp_offset;
					// node 1 finish here!
					// can flush node1 here

//					temp_meta->state=0; // appended node doesn't...
					temp_meta->part = current_node1_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node1_offset.no;
					temp_meta->inv_cnt=0;

					current_node1_offset = temp_offset;
					current_node1_meta = temp_meta;
//					current_node1_data = offset_to_node_data(temp_offset);
//#ifdef DRAM_BUF
					part1++;
					buffer1 = d1[part1].buffer;
//#else
//					buffer1 = current_node1_data->buffer;
//#endif
					buffer1_end = buffer1+NODE_BUFFER;
				}

			// rehash later
//			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node1_data->buffer+size1,buffer,kvs);

//#ifdef DRAM_BUF
				memcpy(buffer1,buffer,kvs);		
//#else
//				pmem_memcpy(buffer1,buffer,kvs,PMEM_F_MEM_NODRAIN);
//#endif

//			size1+= kvs;
//			insert_point_entry(buffer+len_size,buffer1); // can we do this here?
//			temp_kvp[tc] = buffer1;
//			vea[tc].node_offset = new_node1_offset;			
//			vea[tc].kv_offset = buffer1-(unsigned char*)new_node1_data;
			vea[tc].node_offset = current_node1_offset;		
//#ifdef DRAM_BUF
			vea[tc].kv_offset = buffer1-(unsigned char*)&d1[part1];
//#else
//			vea[tc].kv_offset = buffer1-(unsigned char*)current_node1_data;
//#endif

				buffer1+=kvs;			
			}
			else
			{
				if (buffer2+kvs+PH_LEN_SIZE/*len_size*/ > buffer2_end) // append node
				{
					buffer2[0] = buffer2[1] = 0;
//#ifdef DRAM_BUF
					current_node2_meta->size = buffer2-d2[part2].buffer;
//#else
//					current_node2_meta->size = buffer2-current_node2_data->buffer;
//#endif
//					current_node2_meta->invalidated_size = 0;
					new_node2->group_size+=current_node2_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
//				       temp_offset	= alloc_node(current_node2_meta->part+1);
				       temp_offset	= alloc_node(rotation2);
				       rotation2 = (rotation2+1)%PM_N;

					Node_meta* temp_meta = offset_to_node(temp_offset);
//#ifdef DRAM_BUF
					d2[part2].next_offset_ig = temp_offset;
//					pmem_memcpy(current_node2_data,&d_node2,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//					nt256((unsigned char*)current_node2_data,(unsigned char*)&d_node2,sizeof(Node));
//#else
//					current_node2_data->next_offset_ig = temp_offset;
//					pmem_persist(current_node2_data,sizeof(Node));
//#endif
					current_node2_meta->next_offset_ig = temp_offset;
					// node2 finish here!
					// can flush node2 here

//					temp_meta->state = 0; // will not use
					temp_meta->part = current_node2_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node2_offset.no;
					temp_meta->inv_cnt = 0;

					current_node2_offset = temp_offset;
					current_node2_meta = temp_meta;
//					current_node2_data = offset_to_node_data(temp_offset);
//#ifdef DRAM_BUF
					part2++;
					buffer2 = d2[part2].buffer;
//#else
//					buffer2 = current_node2_data->buffer;
//#endif
					buffer2_end = buffer2+NODE_BUFFER;
				}

			// rehash later
//			memcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node2_data->buffer+size2,buffer,kvs);
//#ifdef DRAM_BUF
				memcpy(buffer2,buffer,kvs);			
//#else
//				pmem_memcpy(buffer2,buffer,kvs,PMEM_F_MEM_NODRAIN);
//#endif

//			print_kv(buffer2+size2); // test
//			size2+= kvs;
//			insert_point_entry(buffer+len_size,buffer2); // can we do this here?
//			temp_kvp[tc] = buffer2;
			vea[tc].node_offset = current_node2_offset;//new_node2_offset.no;
//#ifdef DRAM_BUF
			vea[tc].kv_offset = buffer2-(unsigned char*)&d2[part2];
//#else
//			vea[tc].kv_offset = buffer2-(unsigned char*)current_node2_data;//new_node2_data;
//#endif

				buffer2+=kvs;			
			}
			vea[tc].len = vl16;		
			tc++;
		}
		buffer+=kvs;		
	}

	current_node0_offset = current_node0_meta->next_offset_ig;//.no;
	if (current_node0_offset == INIT_OFFSET)
		break;
	i++;
	}

buffer1[0] = buffer1[1] = 0;
buffer2[0] = buffer2[1] = 0;

/*
	new_node1->size = buffer1-new_node1_data->buffer;
	new_node2->size = buffer2-new_node2_data->buffer;
	new_node1->invalidated_size = 0;
	new_node2->invalidated_size = 0;
*/

new_node1->end_offset = current_node1_offset;

//	current_node1_meta->invalidated_size = 0;
//	current_node1_data->next_offset = new_node2_offset.no;//_32;
//#ifdef DRAM_BUF
	d1[part1].next_offset_ig = INIT_OFFSET;
	current_node1_meta->next_offset_ig = INIT_OFFSET;
//	pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));
	current_node1_meta->size = buffer1-d1[part1].buffer;
//#else
///*	current_node1_data->next_offset = */current_node1_data->next_offset_ig = INIT_OFFSET;
//	pmem_persist(current_node1_data,sizeof(Node));
//	current_node1_meta->size = buffer1-current_node1_data->buffer;
//#endif
	new_node1->group_size+=current_node1_meta->size;

//	current_node1_data->continue_len = continue_len+1;




	new_node2->end_offset = current_node2_offset;
//	current_node2_meta->invalidated_size = 0;
//	current_node2_data->next_offset = next_offset.no;//node->next_offset;
//#ifdef DRAM_BUF
	d2[part2].next_offset_ig = INIT_OFFSET;
	current_node2_meta->next_offset_ig = INIT_OFFSET;
//	pmem_memcpy(current_node2_data,&d_node2,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	nt256((unsigned char*)current_node2_data,(unsigned char*)&d_node2,sizeof(Node));
	current_node2_meta->size = buffer2-d2[part2].buffer;
//#else
///*	current_node2_data->next_offset = */current_node2_data->next_offset_ig = INIT_OFFSET;
//	current_node2_data->continue_len = continue_len+1;
//	pmem_persist(current_node2_data,sizeof(Node));
//	current_node2_meta->size = buffer2-current_node2_data->buffer;
//#endif
	new_node2->group_size+=current_node2_meta->size;

	//--------------------------------------------------------------------------------------------------------

	//flush write

	node_offset = new_node1_offset.no;
	i = 0;
	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		nt256((unsigned char*)node_data,(unsigned char*)&d1[i],node_meta->size + meta_size + PH_LEN_SIZE/*len_size*/);
		i++;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;
	}

	node_offset = new_node2_offset.no;
	i = 0;
	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		nt256((unsigned char*)node_data,(unsigned char*)&d2[i],node_meta->size + meta_size + PH_LEN_SIZE/*len_size*/);
		i++;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;
	}

//---------------------------------------------------------------------------------------------------------------------
	_mm_sfence(); // make sure before connect

	next_node->prev_offset = new_node2_offset.no_32;//calc_offset(new_node2);
	prev_node->next_offset = new_node1_offset.no_32;
	// end offset of prev node !!!!!!!!!!
//	pmem_memcpy((Node*)offset_to_node_data(prev_node->end_offset/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy((Node*)offset_to_node_data(prev_offset.no/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL); // design change
	// layout change

	_mm_sfence();

//----------------------------------------------------------------------------------------------------------------------------------------------------

	uint64_t v;
	v = (uint64_t)1 <<(63-continue_len);

	insert_range_entry((unsigned char*)&prefix_64,continue_len,SPLIT_OFFSET);
	// disappear here
	prefix_64-=v;		
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,new_node1_offset.no);
//	new_node1->continue_len = continue_len+1;
	prefix_64+=v;
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,new_node2_offset.no);
//	new_node2->continue_len = continue_len+1;
	
	for (i=0;i<tc;i++)
{
//	if (vea[i].kv_offset > NODE_BUFFER)
//		printf("kv offset %d\n",vea[i].kv_offset);
		insert_point_entry((unsigned char*)&temp_key[i],vea[i]);//temp_kvp[i],temp_len[i]);
}


// need this fence!!	
	_mm_sfence(); // for free after rehash
//	_mm_mfence();	

	new_node1->state = 0;
	new_node2->state = 0;

//	dec_ref(offset);
//	free_node(offset);//???
	for (i=0;i<oc;i++)
		free_node(temp_offset[i]);

	return 1;
}

int split2p(Node_offset offset)
{
	/*
	uint8_t split_cnt_l,t;
	while(1)
	{

	split_cnt_l = split_cnt;
	t = split_cnt_l+1;

	if (split_cnt_l >= SPLIT_LIMIT)
		return -1;
	if (split_cnt.compare_exchange_strong(split_cnt_l,t))
	{
	if (lock_check(offset) == -1)
	{
		split_cnt--;
		return -1;
	}
	split2(offset);
	split_cnt--;
	
	return 1;
	}
	}
	*/
	if (lock_check(offset) == -1)
		return -1;
	return split2(offset);

}

int split(Node_offset offset)//,unsigned char* prefix)//,unsigned char* prefix, int continue_len) // locked
{
#ifdef dtt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif

//static int ec=0;

//if (print)
//	printf("start split offset %d/%d len %d\n",offset.file,offset.offset,continue_len);
	int i;
	uint16_t size;//,size1,size2;
	unsigned char* buffer;
//	unsigned char buffer0[NODE_BUFFER];	
	unsigned char* buffer1;
	unsigned char* buffer2;

	int tc;
	uint64_t temp_key[100*PART_MAX];
//	unsigned char* temp_kvp[100];
//	int temp_len[100];
	ValueEntry vea[100*PART_MAX];
	Node_offset temp_offset[100*PART_MAX];	
	int oc;

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

	int continue_len = node->continue_len;

/*
	if (continue_len > 50)
	{
	printf("continue len %d gs %d is %d\n",node->continue_len,node->group_size,node->invalidated_size);
	tf2();
	}
*/
	Node_offset_u prev_offset,next_offset;
       prev_offset.no_32	= node->prev_offset;
	prev_node = offset_to_node(prev_offset.no);

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
		if (prev_node->state == 1)
		{
//			prev_node->m.unlock();
//			pthread_mutex_unlock(&prev_node->mutex);

//			node->m.lock();
//			pthread_mutex_lock(&node->mutex);	
//			node->state = 0;
//			node->m.unlock();
//			pthread_mutex_unlock(&node->mutex);
//printf("split offset %d/%d prev %d/%d state %d\n",offset.file,offset.offset,prev_offset.no.file,prev_offset.no.offset,(int)prev_node->state);
		/*	
ec++;
if (ec >= 1000)
{
	printf("ec 1000\n");
	ec = 0;
#ifdef LOCK_FAIL_STOP
	scanf("%d",&ec);
#endif
}
*/
			return -1; // failed
		}
//			prev_node->m.unlock();
//	pthread_mutex_unlock(&prev_node->mutex);


//			continue;
//		e_lock(node); //already locked

//		next_node->m.lock();
//	pthread_mutex_lock(&next_node->mutex);	

//		_mm_lfence();

//	_mm_mfence();

next_offset.no_32 = node->next_offset;
	next_node = offset_to_node(next_offset.no);
		if (next_node->state == 1)
		{
//			return -1; //split thread has to giveup


			while(next_node->state == 1)
{
next_offset.no_32 = node->next_offset;
	next_node = offset_to_node(next_offset.no);

}

//			return -1;//failed
		}

if (print)
		printf("locked\n");
//ec = 0;
		//----------------------------------------------------------

// locked here we can split this node from now

//	size = node->size;
//	buffer = node_data->buffer;

	Node_offset_u new_node1_offset;
	Node_offset_u new_node2_offset;

	// not modified
	new_node1_offset.no = alloc_node(0);
	new_node2_offset.no = alloc_node(0);

	new_node1 = offset_to_node(new_node1_offset.no);
	new_node2 = offset_to_node(new_node2_offset.no);

	new_node1->part = 0;
	new_node2->part = 0;

	new_node1->state = 1; // 
	new_node1->scan_list = NULL;
	new_node2->state = 1; // 
	new_node2->scan_list = NULL;

	new_node1->inv_cnt = new_node2->inv_cnt = 0;
	new_node1->group_size = new_node2->group_size = 0;
	new_node1->invalidated_size = new_node2->invalidated_size = 0;
#ifdef DOUBLE_LOG
	new_node1->flush_cnt = new_node2->flush_cnt = 0;
	new_node1->flush_size = new_node2->flush_size = 0;
#endif

	new_node1->start_offset = new_node1_offset.no;
	new_node2->start_offset = new_node2_offset.no;

	new_node1->continue_len = new_node2->continue_len = continue_len+1;

//	new_node1->size = size1;

//	new_node2->size = size2;

	//need lock
	new_node2->next_offset = next_offset.no_32;//node->next_offset;
	new_node2->prev_offset = new_node1_offset.no_32;//calc_offset(new_node1);
//	if (new_node2->prev_offset == new_node2_offset.no_32)
//	printf("erekwrjlw\n");

	new_node1->next_offset = new_node2_offset.no_32;//calc_offset(new_node2);
	new_node1->prev_offset = prev_offset.no_32;//node->prev_offset;
//	if (new_node1->prev_offset == new_node1_offset.no_32)
//	printf("erekwrjlw22222222\n");

	if (node->scan_list != NULL)
		move_scan_list(offset,new_node2_offset.no); // just use lock!!!!i ????

//printf("----------------\n%d %d %d\n %d %d %d %d\n-------------------\n",prev_offset.no.offset,offset.offset,next_offset.no.offset,prev_offset.no.offset,new_node1_offset.no.offset,new_node2_offset.no.offset,next_offset.no.offset);

//	_mm_sfence(); // fence for what? i forgot


		// not here
//	next_node->prev_offset = new_node2_offset.no_32;//calc_offset(new_node2);
	// flush
	//
//	prev_node->next_offset = new_node1_offset.no_32;//calc_offset(new_node1);
	// flush



//	size1 = size2 = 0;

	new_node1_data = offset_to_node_data(new_node1_offset.no);//calc_offset(new_node1));
	new_node2_data = offset_to_node_data(new_node2_offset.no);//calc_offset(new_node2));

	memcpy(new_node1_data,new_node1,sizeof(unsigned int)*3);
	memcpy(new_node2_data,new_node2,sizeof(unsigned int)*3);
	// flush later

//	sort_inv(node->inv_cnt,node->inv_kv);

	//if 8 align
//	uint64_t prefix_64 = *((uint64_t*)prefix),m;


	uint64_t prefix_64;
	uint64_t m;
//	if (node->flush_cnt > 0)
//		prefix_64 = *((uint64_t*)(node->flush_kv[node->flush_cnt-1]+len_size)); // find key from log
#ifdef DOUBLE_LOG
	if (node->flush_cnt != 0)
		prefix_64 = *((uint64_t*)(node->flush_kv[0]+PH_LEN_SIZE));//len_size)); // find key from log

	else
#endif
//	if (*((uint16_t*)(node_data->buffer)) == 0)
//		printf("error lll\n");
		prefix_64 = *((uint64_t*)(node_data->buffer+PH_LEN_SIZE));//len_size)); // find key from node
		// always can find because it needs split

/*
	if (continue_len == 0)
		m = 0;
	else
		m = ~(((uint64_t)1 << (64-continue_len))-1);
		*/
	m = ~(((uint64_t)1 << (63-continue_len))-1); // it seems wrong but ok because of next line it is always overwrited

//	if ((*(uint64_t*)prefix) & m != prefix_64 & m)
//		printf("ppp error\n");


	prefix_64 = (prefix_64 & m) | ((uint64_t)1 << (63-continue_len));
if (print)
	printf("pivot %lx m %lx size %d\n",prefix_64,m,size);
#if 1
	uint16_t vl16;
//	const int kls = LK_SIZE;//key_size + len_size;
	uint16_t kvs;

#ifdef DRAM_BUF
	Node d_node0;
	Node d_node1;
	Node d_node2;

	buffer = d_node0.buffer;
	buffer1 = d_node1.buffer;
	buffer2 = d_node2.buffer;
#else

	buffer = node_data->buffer;
	buffer1 = new_node1_data->buffer;
	buffer2 = new_node2_data->buffer;

#endif

	tc = 0;

	int j;
//	node->inv_kv[node->inv_cnt] = 0;

//	Node_offset current_offset0 = offset;
//	Node_offset_u next_offset0;
	Node_offset next_offset0;
	Node* current_node0_data = node_data;
	Node* current_node1_data = new_node1_data;
	Node* current_node2_data = new_node2_data;
	Node_meta* current_node0_meta;
	Node_meta* current_node1_meta = new_node1;
	Node_meta* current_node2_meta = new_node2;
	Node_offset current_node0_offset = offset;
	Node_offset current_node1_offset = new_node1_offset.no;
	Node_offset current_node2_offset = new_node2_offset.no;
	unsigned char* buffer_end;
	unsigned char* buffer1_end = buffer1+NODE_BUFFER;
	unsigned char* buffer2_end = buffer2+NODE_BUFFER;

	unsigned char* kvp;
oc = 0;
i=0;
	while(1)
	{
	current_node0_meta = offset_to_node(current_node0_offset);
	current_node0_data = offset_to_node_data(current_node0_offset); 
//	next_offset0 = current_node0_data->next_offset_ig;
//	next_offset0.no_32 = current_node0_meta->next_offset_ig;
	next_offset0 = current_node0_meta->next_offset_ig;

#ifdef DRAM_BUF
//	memcpy(d,current_node0_data,sizeof(Node));
//	buffer = buffer_o+data_offset;

//	d_node0 = *current_node0_data;
//	buffer = d_node0.buffer;
	buffer = d_node0.buffer;
#else
	buffer = current_node0_data->buffer;
#endif

	buffer_end = buffer + current_node0_meta->size; 
	sort_inv(current_node0_meta->inv_cnt,current_node0_meta->inv_kv);
	j = 0;
//	if (current_node0_meta->inv_cnt == 0)
//		current_node0_meta->inv_kv[0] = 0;
	current_node0_meta->inv_kv[current_node0_meta->inv_cnt] = 0;
//node0 start here?

	temp_offset[oc++] = current_node0_offset;


	while(buffer < buffer_end)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		memcpy(&lak,buffer,kls);
		vl16 = *((uint16_t*)(buffer/*+cur+key_size*/));
//		kvs = kls + lak.len;
		kvs = LK_SIZE + vl16;

		if (kvs % 2)
			kvs++;
//		if ((lak.len & (1 << 15)) == 0)
#ifdef DRAM_BUF
		if (((unsigned char*)&d_node0 + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/)
#else
		if (((unsigned char*)current_node0_data + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/)
#endif
		{
			j++;
			/*
			if (j == current_node0_meta->inv_cnt)
			{
				j = 0;
				current_node0_meta->inv_kv[0] = 0;
			}
			*/
			kvs &= ~(INV_BIT);
		}
		else
		{
			/*
		if ((value_len & (1 << 15)) != 0) // valid length	
		{
			value_len-= (1<<15);
			kvs-=(1<<15);
			error0();
			uint16_t cnt;
			memcpy(&cnt,&current_node0_meta->inv_cnt,sizeof(uint16_t));
			printf("%d\n",(int)cnt);
		}
		*/

			if (print)
				printf("pivot %lx key %lx\n",prefix_64,*((uint64_t*)(buffer)));

//			if (value_len != 100)
//				print_kv(buffer+cur);

//			temp_key[tc] = lak.key;
			temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE));//len_size));
//		temp_len[tc] = value_len;	
			// we use double copy why?
//			if (*((uint64_t*)(buffer+len_size)) < prefix_64)
			if (temp_key[tc] < prefix_64)	
//			if (lak.key < prefix_64)	
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
			{
				if (buffer1+kvs+PH_LEN_SIZE/*len_size*/ > buffer1_end) // append node
				{
// add end len
					buffer1[0] = buffer1[1] = 0;
#ifdef DRAM_BUF
					current_node1_meta->size = buffer1-d_node1.buffer;
#else
					current_node1_meta->size = buffer1-current_node1_data->buffer;
#endif
#ifdef DOUBLE_LOG
					current_node1_meta->flush_size=0;
#endif
//					current_node1_meta->invalidated_size = 0;
					new_node1->group_size+=current_node1_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
				       temp_offset	= alloc_node(current_node1_meta->part+1);
					Node_meta* temp_meta = offset_to_node(temp_offset);
#ifdef DRAM_BUF
					d_node1.next_offset_ig = temp_offset;
//					pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
					nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));
#else
					current_node1_data->next_offset_ig = temp_offset;
//					pmem_persist(current_node1_data,sizeof(Node));
#endif
					current_node1_meta->next_offset_ig = temp_offset;
					// node 1 finish here!
					// can flush node1 here

//					temp_meta->state=0; // appended node doesn't...
					temp_meta->part = current_node1_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node1_offset.no;
					temp_meta->inv_cnt=0;
#ifdef DOUBLE_LOG
					temp_meta->flush_cnt=0;
#endif

					current_node1_offset = temp_offset;
					current_node1_meta = temp_meta;
					current_node1_data = offset_to_node_data(temp_offset);
#ifdef DRAM_BUF
					buffer1 = d_node1.buffer;
#else
					buffer1 = current_node1_data->buffer;
#endif
					buffer1_end = buffer1+NODE_BUFFER;
				}

			// rehash later
//			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node1_data->buffer+size1,buffer,kvs);

#ifdef DRAM_BUF
				memcpy(buffer1,buffer,kvs);		
#else
				pmem_memcpy(buffer1,buffer,kvs,PMEM_F_MEM_NODRAIN);
#endif

				//			print_kv(buffer1+size1); // test
//			size1+= kvs;
//			insert_point_entry(buffer+len_size,buffer1); // can we do this here?
//			temp_kvp[tc] = buffer1;
//			vea[tc].node_offset = new_node1_offset;			
//			vea[tc].kv_offset = buffer1-(unsigned char*)new_node1_data;
			vea[tc].node_offset = current_node1_offset;		
#ifdef DRAM_BUF
			vea[tc].kv_offset = buffer1-(unsigned char*)&d_node1;
#else
			vea[tc].kv_offset = buffer1-(unsigned char*)current_node1_data;
#endif

				buffer1+=kvs;			
			}
			else
			{
				if (buffer2+kvs+PH_LEN_SIZE/*len_size*/ > buffer2_end) // append node
				{
					buffer2[0] = buffer2[1] = 0;
#ifdef DRAM_BUF
					current_node2_meta->size = buffer2-d_node2.buffer;
#else
					current_node2_meta->size = buffer2-current_node2_data->buffer;
#endif
#ifdef DOUBLE_LOG
					current_node2_meta->flush_size = 0;
#endif
//					current_node2_meta->invalidated_size = 0;
					new_node2->group_size+=current_node2_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
				       temp_offset	= alloc_node(current_node2_meta->part+1);
					Node_meta* temp_meta = offset_to_node(temp_offset);
#ifdef DRAM_BUF
					d_node2.next_offset_ig = temp_offset;
//					pmem_memcpy(current_node2_data,&d_node2,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
					nt256((unsigned char*)current_node2_data,(unsigned char*)&d_node2,sizeof(Node));
#else
					current_node2_data->next_offset_ig = temp_offset;
//					pmem_persist(current_node2_data,sizeof(Node));
#endif
					current_node2_meta->next_offset_ig = temp_offset;
					// node2 finish here!
					// can flush node2 here

//					temp_meta->state = 0; // will not use
					temp_meta->part = current_node2_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node2_offset.no;
					temp_meta->inv_cnt = 0;
#ifdef DOUBLE_LOG
					temp_meta->flush_cnt = 0;
#endif

					current_node2_offset = temp_offset;
					current_node2_meta = temp_meta;
					current_node2_data = offset_to_node_data(temp_offset);
#ifdef DRAM_BUF
					buffer2 = d_node2.buffer;
#else
					buffer2 = current_node2_data->buffer;
#endif
					buffer2_end = buffer2+NODE_BUFFER;
				}

			// rehash later
//			memcpy(buffer2+size2,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node2_data->buffer+size2,buffer,kvs);
#ifdef DRAM_BUF
				memcpy(buffer2,buffer,kvs);			
#else
				pmem_memcpy(buffer2,buffer,kvs,PMEM_F_MEM_NODRAIN);
#endif

//			print_kv(buffer2+size2); // test
//			size2+= kvs;
//			insert_point_entry(buffer+len_size,buffer2); // can we do this here?
//			temp_kvp[tc] = buffer2;
			vea[tc].node_offset = current_node2_offset;//new_node2_offset.no;
#ifdef DRAM_BUF
			vea[tc].kv_offset = buffer2-(unsigned char*)&d_node2;
#else
			vea[tc].kv_offset = buffer2-(unsigned char*)current_node2_data;//new_node2_data;
#endif

				buffer2+=kvs;			
			}
//			vea[tc].len = lak.len;
			vea[tc].len = vl16;		
			tc++;
//		print_kv(buffer+cur);//test
		}
		/*
		else
		{
			j++;
			if (j == current_node0_meta->inv_cnt)
			{
				j = 0;
				current_node0_meta->inv_kv[0] = 0;
			}
//			value_len-= (1 << 15);
//			kvs-= (1 << 15);			
			kvs&= ~((uint16_t)1 << 15);			
		}
		*/
//		cur+=key_size+len_size+value_len;
		buffer+=kvs;		
	}
#ifdef DOUBLE_LOG
	//flush log now
	for (i=0;i<current_node0_meta->flush_cnt;i++)
	{
		kvp = current_node0_meta->flush_kv[i];
//		vl16 = *((uint16_t*)current_node0_meta->flush_kv[i]);
		vl16 = *((uint16_t*)kvp);
		if ((vl16 & INV_BIT))// || (buffer == (unsigned char*)current_node0_data + current_node0_meta->inv_kv[j]))
			continue;
		kvs = LK_SIZE+vl16;
		if (kvs%2)
			++kvs;

		{
			temp_key[tc] = *((uint64_t*)(kvp+PH_LEN_SIZE));//len_size));
			if (temp_key[tc] < prefix_64)
			{
				if (buffer1+kvs+PH_LEN_SIZE/*len_size*/ > buffer1_end)
				{
					buffer1[0] = buffer1[1] = 0;


					current_node1_meta->size = buffer1-current_node1_data->buffer;
					current_node1_meta->flush_size=0;
//					current_node1_meta->invalidated_size = 0;
					new_node1->group_size+=current_node1_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
				       temp_offset	= alloc_node(current_node1_meta->part+1);
					Node_meta* temp_meta = offset_to_node(temp_offset);

					current_node1_data->next_offset_ig = temp_offset;
					current_node1_meta->next_offset_ig = temp_offset;
					pmem_persist(current_node1_data,sizeof(Node));
					// can flush node1 here

//					temp_meta->state = 0; // not use
					temp_meta->part = current_node1_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node1_offset.no;
					temp_meta->inv_cnt=0;
					temp_meta->flush_cnt=0;

					current_node1_offset = temp_offset;
					current_node1_meta = temp_meta;
					current_node1_data = offset_to_node_data(temp_offset);

					buffer1 = current_node1_data->buffer;
					buffer1_end = buffer1+NODE_BUFFER;

				}

			memcpy(buffer1,kvp,kvs);
			*((uint16_t*)kvp) |= INV_BIT;
//			memcpy(buffer1,current_node0_meta->flush_kv[i],kvs);
//			temp_kvp[tc] = buffer1;
//			temp_len[tc] = value_len;
			vea[tc].node_offset = current_node1_offset;//new_offset;
			vea[tc].kv_offset = buffer1-(unsigned char*)current_node1_data;//new_node1_data;
//			vea[tc].len = lak.len;
			buffer1+=kvs;
			}
			else
			{
				if (buffer2+kvs+PH_LEN_SIZE/*len_size*/ > buffer2_end)
				{
					buffer2[0] = buffer2[1] = 0;

					current_node2_meta->size = buffer2-current_node2_data->buffer;
					current_node2_meta->flush_size=0;
//					current_node1_meta->invalidated_size = 0;
					new_node2->group_size+=current_node2_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
				       temp_offset	= alloc_node(current_node2_meta->part+1);
					Node_meta* temp_meta = offset_to_node(temp_offset);

					current_node2_data->next_offset_ig = temp_offset;
					current_node2_meta->next_offset_ig = temp_offset;
					pmem_persist(current_node2_data,sizeof(Node));
					// can flush node1 here

//					temp_meta->state = 0; // not use
					temp_meta->part = current_node2_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node2_offset.no;
					temp_meta->inv_cnt=0;
					temp_meta->flush_cnt=0;

					current_node2_offset = temp_offset;
					current_node2_meta = temp_meta;
					current_node2_data = offset_to_node_data(temp_offset);

					buffer2 = current_node2_data->buffer;
					buffer2_end = buffer2+NODE_BUFFER;

				}

			memcpy(buffer2,kvp,kvs);
//			memcpy(buffer2,current_node0_meta->flush_kv[i],kvs);
			*((uint16_t*)kvp) |= INV_BIT;
//			temp_kvp[tc] = buffer1;
//			temp_len[tc] = value_len;
			vea[tc].node_offset = current_node2_offset;//new_offset;
			vea[tc].kv_offset = buffer2-(unsigned char*)current_node2_data;//new_node1_data;
//			vea[tc].len = lak.len;
			buffer2+=kvs;

			}

			vea[tc].len = vl16;	
			tc++;
		}
	}
#endif
	if (current_node0_offset == node->end_offset)
		break;

	current_node0_offset = next_offset0;//.no;
 
	}

buffer1[0] = buffer1[1] = 0;
buffer2[0] = buffer2[1] = 0;
/*
if (j > 0)
{
	printf("invalid error\n");
	scanf("%d",&j);
	printf("%d\n",j);
}
*/
/*
	new_node1->size = buffer1-new_node1_data->buffer;
	new_node2->size = buffer2-new_node2_data->buffer;
	new_node1->invalidated_size = 0;
	new_node2->invalidated_size = 0;
*/

new_node1->end_offset = current_node1_offset;

#ifdef DOUBLE_LOG
	current_node1_meta->flush_size = 0;
#endif
//	current_node1_meta->invalidated_size = 0;
//	current_node1_data->next_offset = new_node2_offset.no;//_32;
#ifdef DRAM_BUF
	d_node1.next_offset_ig = INIT_OFFSET;
//	pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
	nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));
	current_node1_meta->size = buffer1-d_node1.buffer;
#else
/*	current_node1_data->next_offset = */current_node1_data->next_offset_ig = INIT_OFFSET;
//	pmem_persist(current_node1_data,sizeof(Node));
	current_node1_meta->size = buffer1-current_node1_data->buffer;
#endif
	new_node1->group_size+=current_node1_meta->size;

//	current_node1_data->continue_len = continue_len+1;




	new_node2->end_offset = current_node2_offset;
#ifdef DOUBLE_LOG
	current_node2_meta->flush_size = 0;
#endif
//	current_node2_meta->invalidated_size = 0;
//	current_node2_data->next_offset = next_offset.no;//node->next_offset;
#ifdef DRAM_BUF
	d_node2.next_offset_ig = INIT_OFFSET;
//	pmem_memcpy(current_node2_data,&d_node2,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
	nt256((unsigned char*)current_node2_data,(unsigned char*)&d_node2,sizeof(Node));
	current_node2_meta->size = buffer2-d_node2.buffer;
#else
/*	current_node2_data->next_offset = */current_node2_data->next_offset_ig = INIT_OFFSET;
//	current_node2_data->continue_len = continue_len+1;
//	pmem_persist(current_node2_data,sizeof(Node));
	current_node2_meta->size = buffer2-current_node2_data->buffer;
#endif
	new_node2->group_size+=current_node2_meta->size;

#ifndef DRAM_BUF
	pmem_drain();
#endif
	_mm_sfence(); // make sure before connect

	next_node->prev_offset = new_node2_offset.no_32;//calc_offset(new_node2);
	prev_node->next_offset = new_node1_offset.no_32;
	// end offset of prev node !!!!!!!!!!
//	pmem_memcpy((Node*)offset_to_node_data(prev_node->end_offset/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy((Node*)offset_to_node_data(prev_offset.no/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL); // design change
	// layout change

	_mm_sfence();

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
	unsigned char prefix_t[8];
	*((uint64_t*)prefix_t) = prefix_64;
	if (find_range_entry2(prefix,&z) != INIT_OFFSET)
{
	printf("erer\n");
//	int t;
//	scanf("%d",&t);

//	insert_range_entry((unsigned char*)&prefix_64,continue_len,SPLIT_OFFSET);
	if (find_range_entry2(prefix,&z) != INIT_OFFSET)
		printf("erer\n");

}
*/
	prefix_64-=v;		
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,new_node1_offset.no);
//	if (find_in_log
	new_node1->continue_len = continue_len+1;
	prefix_64+=v;
	insert_range_entry((unsigned char*)&prefix_64,continue_len+1,new_node2_offset.no);
	new_node2->continue_len = continue_len+1;
/*	
if (find_range_entry2(prefix,&z) == INIT_OFFSET)
{
	printf("erer22222\n");
	int t;
	scanf("%d",&t);
}
*/
	for (i=0;i<tc;i++)
{
//	if (vea[i].kv_offset > NODE_BUFFER)
//		printf("kv offset %d\n",vea[i].kv_offset);
		insert_point_entry((unsigned char*)&temp_key[i],vea[i]);//temp_kvp[i],temp_len[i]);
}


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

	new_node1->state = 0;
	new_node2->state = 0;


//	dec_ref(offset);
//	free_node(offset);//???
	for (i=0;i<oc;i++)
		free_node(temp_offset[i]);

#ifdef dtt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

	return 1;
}

int compact2(Node_offset offset)//,unsigned char* prefix)//,unsigned char* prefix, int continue_len) // locked
{

//0 -> 1 + 2

// read p d
// split d d (and alloc)
// write d p
// meta//

//	Node d0[PART_MAX];
//	Node d1[PART_MAX];

	Node_offset node_offset;
	Node_meta* node_meta;
	Node* node_data;

	const int meta_size = d0[0].buffer-(unsigned char*)&d0[0];
	int part0,part1;

	part0 = 0;


	node_offset = offset;
	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		cp256((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);
		part0++;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;
	}

	//---------------------------------------------------------------------------------

	int i;
	uint16_t size;//,size1,size2;

	int tc;
	uint64_t temp_key[100*PART_MAX];
	ValueEntry vea[100*PART_MAX];
	Node_offset temp_offset[PART_MAX];	
	int oc;

	int continue_len = offset_to_node(offset)->continue_len;

	part1 = 0;

	Node_meta* new_node1;
	Node_meta* prev_node;
	Node_meta* next_node;

	Node_offset_u prev_offset,next_offset;

	node_meta = offset_to_node(offset);
	node_data = offset_to_node_data(offset);

	prev_offset.no_32 = node_meta->prev_offset;
	next_offset.no_32 = node_meta->next_offset;
	next_node = offset_to_node(next_offset.no);
	prev_node = offset_to_node(prev_offset.no);

	Node_offset_u new_node1_offset;

	int rotation1;
	new_node1_offset.no = alloc_node(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	rotation1 = part_rotation;

	new_node1 = offset_to_node(new_node1_offset.no);

	new_node1->part = 0;

	new_node1->state = 1; // 

	//need fence

	new_node1->inv_cnt = 0;
	new_node1->group_size = 0;
	new_node1->invalidated_size = 0;

	new_node1->start_offset = new_node1_offset.no;

	new_node1->continue_len = continue_len;

	//need lock
	new_node1->next_offset = next_offset.no_32;//calc_offset(new_node2);
	new_node1->prev_offset = prev_offset.no_32;//node->prev_offset;

	//no scan

//	new_node1_data = offset_to_node_data(new_node1_offset.no);//calc_offset(new_node1));
//	new_node2_data = offset_to_node_data(new_node2_offset.no);//calc_offset(new_node2));

	d1[0].next_offset = next_offset.no;
	d1[0].continue_len = continue_len;
//	memcpy(new_node1_data,new_node1,sizeof(unsigned int)*3);
//	memcpy(new_node2_data,new_node2,sizeof(unsigned int)*3);
	// flush later

	//if 8 align
//	uint64_t prefix_64 = *((uint64_t*)prefix),m;



	uint16_t vl16;
//	const int kls = LK_SIZE;//PH_KEY_SIZE+PH_LEN_SIZE;//key_size + len_size;
	uint16_t kvs;

	unsigned char* buffer;
	unsigned char* buffer1;
	unsigned char* buffer2;

//	buffer = d0[0].buffer;
	buffer1 = d1[0].buffer;

	tc = 0;

	int j;
//	node->inv_kv[node->inv_cnt] = 0;

//	Node_offset current_offset0 = offset;
//	Node_offset_u next_offset0;
//	Node_offset next_offset0;
//	Node* current_node0_data = node_data;
//	Node* current_node1_data = new_node1_data;
//	Node* current_node2_data = new_node2_data;
	Node_meta* current_node0_meta;
	Node_meta* current_node1_meta = new_node1;
	Node_offset current_node0_offset = offset;
	Node_offset current_node1_offset = new_node1_offset.no;
	unsigned char* buffer_end;
	unsigned char* buffer1_end = buffer1+NODE_BUFFER;

	unsigned char* kvp;
oc = 0;
i=0;
	while(1)
	{
	current_node0_meta = offset_to_node(current_node0_offset);
//	current_node0_data = offset_to_node_data(current_node0_offset); 
//	next_offset0 = current_node0_data->next_offset_ig;
//	next_offset0.no_32 = current_node0_meta->next_offset_ig;
//	next_offset0 = current_node0_meta->next_offset_ig;

//#ifdef DRAM_BUF
//	memcpy(d,current_node0_data,sizeof(Node));
//	buffer = buffer_o+data_offset;

//	d_node0 = *current_node0_data;
//	buffer = d_node0.buffer;
	buffer = d0[i].buffer;
//#else
//	buffer = current_node0_data->buffer;
//#endif

	buffer_end = buffer + current_node0_meta->size; 
	sort_inv(current_node0_meta->inv_cnt,current_node0_meta->inv_kv);
	j = 0;
	current_node0_meta->inv_kv[current_node0_meta->inv_cnt] = 0;
//node0 start here?

	temp_offset[oc++] = current_node0_offset;

	while(buffer < buffer_end)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		memcpy(&lak,buffer,kls);
		vl16 = *((uint16_t*)(buffer/*+cur+key_size*/));
//		kvs = kls + lak.len;
		kvs = LK_SIZE + vl16;

		if (kvs % 2)
			kvs++;
//		if ((lak.len & (1 << 15)) == 0)
//#ifdef DRAM_BUF
		if (((unsigned char*)&d0[i] + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/)
//#else
//		if (((unsigned char*)current_node0_data + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/)
//#endif
		{
			j++;
			kvs &= ~(INV_BIT);
		}
		else
		{
			temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE));//len_size));
//		temp_len[tc] = value_len;	
			// we use double copy why?
//			if (*((uint64_t*)(buffer+len_size)) < prefix_64)
//		if (compare(buffer+i*entry_size,prefix) < 0) //insert1		
			{
				if (buffer1+kvs+PH_LEN_SIZE/*len_size*/ > buffer1_end) // append node
				{
// add end len
					buffer1[0] = buffer1[1] = 0;
//#ifdef DRAM_BUF
					current_node1_meta->size = buffer1-d1[part1].buffer;
//#else
//					current_node1_meta->size = buffer1-current_node1_data->buffer;
//#endif
//					current_node1_meta->invalidated_size = 0;
					new_node1->group_size+=current_node1_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
//				       temp_offset	= alloc_node(current_node1_meta->part+1);
					temp_offset = alloc_node(rotation1);
					rotation1 = (rotation1+1)%PM_N;
					Node_meta* temp_meta = offset_to_node(temp_offset);
//#ifdef DRAM_BUF
					d1[part1].next_offset_ig = temp_offset;
//					pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//					nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));
//#else
//					current_node1_data->next_offset_ig = temp_offset;
//					pmem_persist(current_node1_data,sizeof(Node));
//#endif
					current_node1_meta->next_offset_ig = temp_offset;
					// node 1 finish here!
					// can flush node1 here

//					temp_meta->state=0; // appended node doesn't...
					temp_meta->part = current_node1_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node1_offset.no;
					temp_meta->inv_cnt=0;

					current_node1_offset = temp_offset;
					current_node1_meta = temp_meta;
//					current_node1_data = offset_to_node_data(temp_offset);
//#ifdef DRAM_BUF
					part1++;
					buffer1 = d1[part1].buffer;
//#else
//					buffer1 = current_node1_data->buffer;
//#endif
					buffer1_end = buffer1+NODE_BUFFER;
				}

			// rehash later
//			memcpy(buffer1+size1,buffer+cur,key_size + len_size + value_len);
//			memcpy(new_node1_data->buffer+size1,buffer,kvs);

//#ifdef DRAM_BUF
				memcpy(buffer1,buffer,kvs);		
//#else
//				pmem_memcpy(buffer1,buffer,kvs,PMEM_F_MEM_NODRAIN);
//#endif

//			size1+= kvs;
//			insert_point_entry(buffer+len_size,buffer1); // can we do this here?
//			temp_kvp[tc] = buffer1;
//			vea[tc].node_offset = new_node1_offset;			
//			vea[tc].kv_offset = buffer1-(unsigned char*)new_node1_data;
			vea[tc].node_offset = current_node1_offset;		
//#ifdef DRAM_BUF
			vea[tc].kv_offset = buffer1-(unsigned char*)&d1[part1];
//#else
//			vea[tc].kv_offset = buffer1-(unsigned char*)current_node1_data;
//#endif

				buffer1+=kvs;			
			}
			vea[tc].len = vl16;		
			tc++;
		}
		buffer+=kvs;		
	}

	current_node0_offset = current_node0_meta->next_offset_ig;//.no;
	if (current_node0_offset == INIT_OFFSET)
		break;
	i++;
	}

buffer1[0] = buffer1[1] = 0;

/*
	new_node1->size = buffer1-new_node1_data->buffer;
	new_node2->size = buffer2-new_node2_data->buffer;
	new_node1->invalidated_size = 0;
	new_node2->invalidated_size = 0;
*/

new_node1->end_offset = current_node1_offset;

//	current_node1_meta->invalidated_size = 0;
//	current_node1_data->next_offset = new_node2_offset.no;//_32;
//#ifdef DRAM_BUF
	d1[part1].next_offset_ig = INIT_OFFSET;
	current_node1_meta->next_offset_ig = INIT_OFFSET;
//	pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));
	current_node1_meta->size = buffer1-d1[part1].buffer;
//#else
///*	current_node1_data->next_offset = */current_node1_data->next_offset_ig = INIT_OFFSET;
//	pmem_persist(current_node1_data,sizeof(Node));
//	current_node1_meta->size = buffer1-current_node1_data->buffer;
//#endif
	new_node1->group_size+=current_node1_meta->size;

//	current_node1_data->continue_len = continue_len+1;

	//--------------------------------------------------------------------------------------------------------

	//flush write

	node_offset = new_node1_offset.no;
	i = 0;
	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		nt256((unsigned char*)node_data,(unsigned char*)&d1[i],node_meta->size + meta_size + PH_LEN_SIZE);//len_size);
		i++;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;
	}

//---------------------------------------------------------------------------------------------------------------------
	_mm_sfence(); // make sure before connect

	next_node->prev_offset = new_node1_offset.no_32;//calc_offset(new_node1);
prev_node->next_offset = new_node1_offset.no_32;
// end offset of prev node !!!!!!!!!!
//pmem_memcpy(offset_to_node_data(prev_node->end_offset/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
pmem_memcpy(offset_to_node_data(prev_offset.no/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);// the design is changed
// layout change

	_mm_sfence();

//----------------------------------------------------------------------------------------------------------------------------------------------------

	insert_range_entry((unsigned char*)d0[0].buffer+PH_LEN_SIZE/*len_size*/,continue_len,new_node1_offset.no); // prefix array will be re gen // do not use new node use old node

	for (i=0;i<tc;i++)	
		insert_point_entry((unsigned char*)&temp_key[i],vea[i]);//temp_kvp[i],temp_len[i]);

// need this fence!!	
	_mm_sfence(); // for free after rehash
//	_mm_mfence();	

	new_node1->state = 0;

//	dec_ref(offset);
//	free_node(offset);//???
	for (i=0;i<oc;i++)
		free_node(temp_offset[i]);

	return 1;
}


int compact2p(Node_offset offset)
{
	/*
	uint8_t split_cnt_l,t;
	while(1)
	{

	split_cnt_l = split_cnt;
	t = split_cnt_l+1;

	if (split_cnt_l >= SPLIT_LIMIT)
		return -1;
	if (split_cnt.compare_exchange_strong(split_cnt_l,t))
	{
	if (lock_check(offset) == -1)
	{
		split_cnt--;
		return -1;
	}
	compact2(offset);
	split_cnt--;
	
	return 1;
	}
	}
	*/

	if (lock_check(offset) == -1)
		return -1;
	return compact2(offset);
	
}

int compact(Node_offset offset)//,int continue_len)//, struct range_hash_entry* range_entry)//,unsigned char* prefix, int continue_len)
{
//	printf("compact\n");//test
#ifdef dtt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
//	tt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;

//	printf("start compaction offset %d len %d\n",offset,continue_len);
if(print)	
	printf("compaction offset %d/%d\n",offset.file,offset.offset);
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
	uint64_t temp_key[100 * PART_MAX];
//	unsigned char* temp_kvp[100];
//	int temp_len[100];
	ValueEntry vea[100 * PART_MAX];	
	Node_offset temp_offset[100 * PART_MAX];
	int oc;

	Node_meta* new_node1;
	Node_meta* prev_node;
	Node_meta* next_node;
	Node_meta* node;

	Node* node_data;
	Node* new_node1_data;
//	Node* prev_node_data;

	node = offset_to_node(offset);
	node_data = offset_to_node_data(offset);

	int continue_len = node->continue_len;


	Node_offset_u prev_offset,next_offset;
	prev_offset.no_32 = node->prev_offset;
	prev_node = offset_to_node(prev_offset.no);
//	prev_node = offset_to_node(node->prev_offset);

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


		if (prev_node->state == 1)
		{

			return -1; // failed
		}
//			prev_node->m.unlock();
//				pthread_mutex_unlock(&prev_node->mutex);


//			continue;
//		e_lock(node); //already locked

//		next_node->m.lock();
//			pthread_mutex_lock(&next_node->mutex);	

		_mm_mfence();

		next_offset.no_32 = node->next_offset;
		next_node = offset_to_node(next_offset.no);
//	next_node = offset_to_node(node->next_offset);
		if (next_node->state == 1)
		{
		while(next_node->state == 1)
		{
			next_offset.no_32 = node->next_offset;
			next_node = offset_to_node(next_offset.no);
		}
		}
if (print)
		printf("locked\n");

	//-------------------------------------------------------------------	

//	size = node->size;
//	buffer = node_data->buffer;
//	size = meta & 63; // 00111111

Node_offset_u new_node1_offset;
	new_node1_offset.no = alloc_node(0); // not modified

	new_node1 = offset_to_node(new_node1_offset.no);

//	new_node1->end_offset = new_node1_offset.no;

	new_node1->part = 0;

	new_node1->state = 1; // ??
//	new_node1->ref = 0;
//	pthread_mutex_init(&new_node1->mutex,NULL);

//	new_node1->size = size1;
//	new_node1->size = buffer1-new_node1_data->buffer;	

	new_node1->inv_cnt = 0;
	new_node1->group_size = 0;
	new_node1->start_offset = new_node1_offset.no;
	new_node1->invalidated_size = 0;

	new_node1->scan_list = NULL; // do we need this?
#ifdef DOUBLE_LOG
	new_node1->flush_cnt = 0;
	new_node1->flush_size = 0;
#endif

	new_node1->next_offset = node->next_offset;
	new_node1->prev_offset = node->prev_offset;

	new_node1->continue_len = continue_len;


//	next_node->prev_offset = new_node1_offset.no_32;//calc_offset(new_node1);
	// flush
//	prev_node->next_offset = new_node1_offset.no_32;//calc_offset(new_node1);
	// flush

	if (node->scan_list != NULL)
		move_scan_list(offset,new_node1_offset.no);

	new_node1_data = offset_to_node_data(new_node1_offset.no);//calc_offset(new_node1));

	memcpy(new_node1_data,new_node1,sizeof(unsigned int)*3);

/*
	int j,temp;
	for (i=0;i<node->inv_cnt;i++)
	{
		temp = 0;
		for (j=0;j<node->inv_cnt-i-1;j++)
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
*/

	uint16_t vl16;

//	uint64_t k = *((uint64_t*)(buffer+cur));

#ifdef DRAM_BUF
	Node d_node0;
	Node d_node1;

	buffer = d_node0.buffer;
	buffer1 = d_node1.buffer;
#else

	buffer = node_data->buffer;
	buffer1 = new_node1_data->buffer;

#endif

//	unsigned char* const buffer_end = buffer+size;

//	const int kls = LK_SIZE;//PH_KEY_SIZE+PH_LEN_SIZE;//key_size + len_size;
	uint16_t kvs;
//	const unsigned int new_offset = calc_offset(new_node1);
tc = 0;

//	struct len_and_key lak;
//	j = 0;

//	Node_offset current_offset0 = offset;
//	Node_offset_u next_offset0;
	Node_offset next_offset0;
       Node* current_node0_data = node_data;
Node* current_node1_data = new_node1_data;
Node_meta* current_node0_meta;
Node_meta* current_node1_meta = new_node1;
Node_offset current_node0_offset = offset;
Node_offset current_node1_offset = new_node1_offset.no;

unsigned char* buffer_end;
unsigned char* buffer1_end = buffer1+NODE_BUFFER;
int j;
unsigned char* kvp;

//	node->inv_kv[node->ic] = 0;
oc = 0;
	while(1)
	{
	current_node0_meta = offset_to_node(current_node0_offset);
	current_node0_data = offset_to_node_data(current_node0_offset); 
//	next_offset0 = current_node0_data->next_offset_ig;
//	next_offset0.no_32 = current_node0_meta->next_offset_ig;
	next_offset0 = current_node0_meta->next_offset_ig;

#ifdef DRAM_BUF
	d_node0 = *current_node0_data;
	buffer = d_node0.buffer;
#else

	buffer = current_node0_data->buffer;
#endif
	buffer_end = buffer + current_node0_meta->size; 
	sort_inv(current_node0_meta->inv_cnt,current_node0_meta->inv_kv);
		j = 0;
//		if (current_node0_meta->inv_cnt == 0)
//			current_node0_meta->inv_kv[0] = 0;
		current_node0_meta->inv_kv[current_node0_meta->inv_cnt] = 0;
		temp_offset[oc++] = current_node0_offset;
	while(buffer < buffer_end)
	{
		// 8 align
//		if (*((uint64_t*)(buffer+i*entry_size)) == 0) // invalid key
//		if (*((uint64_t*)(buffer+cur)) != k)
//			printf("compaction error\n");	
//		memcpy(&lak,buffer,kls);				
		vl16 = *((uint16_t*)(buffer));
//		kvs = kls + lak.len;
		kvs = LK_SIZE + vl16;
		if (kvs%2)
			++kvs;
//		if ((/*value_len*/ lak.len & (1 << 15)) == 0) // valid length		
#ifdef DRAM_BUF
		if (((unsigned char*)&d_node0 + current_node0_meta->inv_kv[j] == buffer))
#else
		if (((unsigned char*)current_node0_data + current_node0_meta->inv_kv[j] == buffer)/* || (vl16 & INV_BIT)*/) // INV BIT for recovery
#endif
		{
			j++;
			/*
			if (j == current_node0_meta->inv_cnt)//node->inv_cnt)
			{
				j = 0;
//				node->inv_kv[0] = 0;
				current_node0_meta->inv_kv[0] = 0;
			}
			*/
			kvs&= ~(INV_BIT);//((uint16_t)1 << 15);
		}
		else
		{
			/*
		if ((value_len & (1 << 15)) != 0) // valid length	
		{
			value_len-= (1<<15);
			kvs-=(1<<15);
			error0();
			uint16_t cnt;
			memcpy(&cnt,&current_node0_meta->inv_cnt,sizeof(uint16_t));
			printf("%d\n",(int)cnt);
		}
		*/

//			temp_key[tc] = lak.key;
			temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE/*len_size*/));
			if (buffer1+kvs+PH_LEN_SIZE/*len_size*/ > buffer1_end)
			{
				buffer1[0] = buffer1[1] = 0;
#ifdef DRAM_BUF
				current_node1_meta->size = buffer1-d_node1.buffer;
#else
					current_node1_meta->size = buffer1-current_node1_data->buffer;
#endif
#ifdef DOUBLE_LOG
					current_node1_meta->flush_size = 0;
#endif
//					current_node1_meta->invalidated_size = 0;
					new_node1->group_size+=current_node1_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
				       temp_offset	= alloc_node(current_node1_meta->part+1);
					Node_meta* temp_meta = offset_to_node(temp_offset);
#ifdef DRAM_BUF
					d_node1.next_offset_ig = temp_offset;
//					pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
					nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));	
#else
					current_node1_data->next_offset_ig = temp_offset;
//					pmem_persist(current_node1_data,sizeof(Node));
#endif
					current_node1_meta->next_offset_ig = temp_offset;
					// can flush node1 here

//					temp_meta->state = 0; // not use
					temp_meta->part = current_node1_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node1_offset.no;
					temp_meta->inv_cnt=0;
#ifdef DOUBLE_LOG
					temp_meta->flush_cnt=0;
#endif

					current_node1_offset = temp_offset;
					current_node1_meta = temp_meta;
					current_node1_data = offset_to_node_data(temp_offset);
#ifdef DRAM_BUF
					buffer1 = d_node1.buffer;
#else
					buffer1 = current_node1_data->buffer;
#endif
					buffer1_end = buffer1+NODE_BUFFER;

			}
#ifdef DRAM_BUF
			memcpy(buffer1,buffer,kvs);
#else
			pmem_memcpy(buffer1,buffer,kvs,PMEM_F_MEM_NODRAIN);
#endif

//			temp_kvp[tc] = buffer1;
//			temp_len[tc] = value_len;
			vea[tc].node_offset = current_node1_offset;//new_offset;
#ifdef DRAM_BUF
			vea[tc].kv_offset = buffer1-(unsigned char*)&d_node1;
#else
			vea[tc].kv_offset = buffer1-(unsigned char*)current_node1_data;//new_node1_data;
#endif
//			vea[tc].len = lak.len;
			vea[tc].len = vl16;		
			buffer1+=kvs;
			tc++;
		}
		/*
		else
		{
			j++;
			if (j == current_node0_meta->inv_cnt)//node->inv_cnt)
			{
				j = 0;
//				node->inv_kv[0] = 0;
				current_node0_meta->inv_kv[0] = 0;
			}
			kvs&= ~(INV_BIT);//((uint16_t)1 << 15);
		}
		*/
//		cur+=key_size+len_size+value_len;
		buffer+=kvs;		
	}
#ifdef DOUBLE_LOG
	//flush log now
	for (i=0;i<current_node0_meta->flush_cnt;i++)
	{
		kvp = current_node0_meta->flush_kv[i];
//		vl16 = *((uint16_t*)current_node0_meta->flush_kv[i]);
		vl16 = *((uint16_t*)kvp);
		if ((vl16 & INV_BIT))// || (buffer == (unsigned char*)current_node0_data + current_node0_meta->inv_kv[j]))
			continue;
		kvs = LK_SIZE+vl16;
		if (kvs%2)
			++kvs;

		{
			temp_key[tc] = *((uint64_t*)(kvp+PH_LEN_SIZE));//len_size));
			if (buffer1+kvs+PH_LEN_SIZE/*len_size*/ > buffer1_end)
			{
				buffer1[0] = buffer1[1] = 0;
					current_node1_meta->size = buffer1-current_node1_data->buffer;
					current_node1_meta->flush_size=0;
//					current_node1_meta->invalidated_size = 0;
					new_node1->group_size+=current_node1_meta->size;

//					Node_offset_u temp_offset;
					Node_offset temp_offset;
				       temp_offset	= alloc_node(current_node1_meta->part+1);
					Node_meta* temp_meta = offset_to_node(temp_offset);

					current_node1_data->next_offset_ig = temp_offset;
					current_node1_meta->next_offset_ig = temp_offset;
					pmem_persist(current_node1_data,sizeof(Node));
					// can flush node1 here

//					temp_meta->state = 0; // not use
					temp_meta->part = current_node1_meta->part+1;
//					temp_meta->size = 0;
//					temp_meta->invalidated_size = 0;
					temp_meta->start_offset = new_node1_offset.no;
					temp_meta->inv_cnt=0;
					temp_meta->flush_cnt=0;

					current_node1_offset = temp_offset;
					current_node1_meta = temp_meta;
					current_node1_data = offset_to_node_data(temp_offset);

					buffer1 = current_node1_data->buffer;
					buffer1_end = buffer1+NODE_BUFFER;

			}

			memcpy(buffer1,kvp,kvs);
//			memcpy(buffer1,current_node0_meta->flush_kv[i],kvs);
			*((uint16_t*)kvp) |= INV_BIT;
//			temp_kvp[tc] = buffer1;
//			temp_len[tc] = value_len;
			vea[tc].node_offset = current_node1_offset;//new_offset;
			vea[tc].kv_offset = buffer1-(unsigned char*)current_node1_data;//new_node1_data;
//			vea[tc].len = lak.len;
			vea[tc].len = vl16;		
			buffer1+=kvs;
			tc++;
		}
	}
#endif

	if (current_node0_offset == node->end_offset)
		break;
	current_node0_offset = next_offset0;//.no;
	}

	buffer1[0] = buffer1[1] = 0;

	//swap inv array
	uint16_t* temp_kv;
	uint16_t temp_max;
	temp_kv = node->inv_kv;
	node->inv_kv = new_node1->inv_kv;
	new_node1->inv_kv = temp_kv;
	temp_max = node->inv_max;
	node->inv_max = new_node1->inv_max;
	new_node1->inv_max = temp_max;
#ifdef DOUBLE_LOG
	unsigned char** temp_flush;
	temp_flush = node->flush_kv;
	node->flush_kv = new_node1->flush_kv;
	new_node1->flush_kv = temp_flush;
	temp_max = node->flush_max;
	node->flush_max = new_node1->flush_max;
	new_node1->flush_max = temp_max;
#endif
//	new_node1->size = buffer1-new_node1_data->buffer;
//	new_node1->invalidated_size = 0;

	new_node1->end_offset = current_node1_offset;
#ifdef DOUBLE_LOG
	current_node1_meta->flush_size = 0;
#endif
//	current_node1_meta->invalidated_size = 0;
#ifdef DRAM_BUF
	d_node1.next_offset_ig = INIT_OFFSET;
//	pmem_memcpy(current_node1_data,&d_node1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
	nt256((unsigned char*)current_node1_data,(unsigned char*)&d_node1,sizeof(Node));	
	current_node1_meta->size = buffer1-d_node1.buffer;
#else
//	current_node1_data->next_offset = new_node1_offset.no;//_32;
	current_node1_data->next_offset_ig = INIT_OFFSET;
	current_node1_meta->size = buffer1-current_node1_data->buffer;
//	pmem_persist(current_node1_data,sizeof(Node));
#endif
	new_node1->group_size+=current_node1_meta->size;

#ifndef DRAM_BUF
	pmem_drain();
#endif

	_mm_sfence(); // make sure before connect

	next_node->prev_offset = new_node1_offset.no_32;//calc_offset(new_node1);
prev_node->next_offset = new_node1_offset.no_32;
// end offset of prev node !!!!!!!!!!
//pmem_memcpy(offset_to_node_data(prev_node->end_offset/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);
pmem_memcpy(offset_to_node_data(prev_offset.no/*node->prev_offset*/),&new_node1_offset,sizeof(unsigned int),PMEM_F_MEM_NONTEMPORAL);// the design is changed
// layout change
_mm_sfence();

	//why?? fill zero?
	/*
	if (size1 + entry_size < NODE_BUFFER)
		init_entry(buffer1 + size1*entry_size);
	if (size2 + entry_size < NODE_BUFFER)
		init_entry(buffer2 + size2*entry_size);
*/
#if 0
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
#endif
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
//	insert_range_entry((unsigned char*)new_node1_data->buffer+len_size,key_bit,calc_offset(new_node1)); // who use 64 range
	
//	insert_range_entry((unsigned char*)new_node1_data->buffer+len_size,continue_len,calc_offset(new_node1)); // prefix array will be re gen // do not use new node use old node
	insert_range_entry((unsigned char*)node_data->buffer+PH_LEN_SIZE/*len_size*/,continue_len,new_node1_offset.no); // prefix array will be re gen // do not use new node use old node

	new_node1->continue_len = continue_len;

//	range_entry->offset = calc_offset(new_node1);

	_mm_sfence(); // for free after rehash

	new_node1->state = 0;
//
//	free_node(offset);//???
	for (i=0;i<oc;i++)
	free_node(temp_offset[i]);	

#ifdef dtt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	tt3+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

	return 1;//calc_offset(new_node1);
//	return 0;

}




//-------------------------------------------------------------

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
void delete_scan_entry(Node_offset &scan_offset,void* query)
{
	Node_meta* node;
	Scan_list* sl;
	Scan_list** slp;
	node = offset_to_node(scan_offset);
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
}

int advance_offset(Query* query)
{
	Node_offset_u old_offset,new_offset;
	int inv;
	Node_meta* node;
	Node_meta* next_node;

//	pthread_mutex_lock(&query->scan_mutex);
//	at_lock(query->scan_lock);

	while(1)
	{
		old_offset.no_32 = query->scan_offset;
		node = offset_to_node(old_offset.no);
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
//		at_lock(node->state); // never fail?
		if (try_at_lock(node->state) == 0)
			continue;

		new_offset.no_32 = node->next_offset;

		if (new_offset.no == TAIL_OFFSET)
		{
			delete_scan_entry(old_offset.no,query);
//			pthread_mutex_unlock(&node->mutex);
			at_unlock(node->state);			
			query->scan_offset = TAIL_OFFSET_u.no_32;
//				dec_ref(old_offset);
//				*kv_pp = NULL;
//			pthread_mutex_unlock(&query->scan_mutex);		
//			at_unlock(query->scan_lock);			
			return -1;
		}

		while(1)
		{

		next_node = offset_to_node(new_offset.no);

		if (try_at_lock(next_node->state) ) // it can't be splited because we have lock of 
//			if (next_node->state == 0) // don't lock but check
				break;
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

//	at_lock(next_node->state);
	sl->next = next_node->scan_list;
	next_node->scan_list = sl;

	query->scan_offset = new_offset.no_32;

//	next_node->state = 1; // not split it is copy ....
//	insert_scan_list(next_node,query);
//	pthread_mutex_unlock(&next_node->mutex);

//	while(next_node->ref > 0); // remove insert
//	*((Node*)query->node) = *next_node; // copy_node

//	Node* next_node_data;
//	next_node_data = offset_to_node_data(new_offset.no);

// read will not need this ... 
	/*
	if (!USE_DRAM)
	{
		pmem_memcpy(
	}
	else
	*/
//		*((Node*)query->node_data) = *next_node_data;	
//	copy_node((unsigned char*)query->node_data,query->scan_offset);
	copy_and_sort_node(query);
	at_unlock(node->state);

//	sort_node(query);
//	query->sorted_kv_i = 0;


//	pthread_mutex_lock(&next_node->mutex);
//	next_node->state = 0; // not split it is copy ....
//	pthread_mutex_unlock(&next_node->mutex);
	at_unlock(next_node->state);	


//		dec_ref(old_offset);
		
//		dec_ref(new_offset);		
//		*kv_pp = node_p->buffer;
//	pthread_mutex_unlock(&query->scan_mutex);		
//	at_unlock(query->scan_lock);	
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

void copy_and_sort_node(Query *query)//Node* &node_data,Node_offset node_offset)
{
	Node* node_data = (Node*)query->node_data;
	Node_offset node_offset;// = query->scan_offset;
	Node_meta* node_meta;// = offset_to_node(node_offset);
	Node_offset end_offset;// = node_meta->end_offset;
	int i = 0;
	int part;
	int node_size[100*PART_MAX];
	Node_offset_u nou;
	nou.no_32 = query->scan_offset;
	node_offset = nou.no;
//	node_offset = query->scan_offset;
	node_meta = offset_to_node(node_offset);
	end_offset = node_meta->end_offset;
	int meta_size = (unsigned char*)node_data->buffer-(unsigned char*)node_data;
	while(1)
	{
//		node_data[i] = *offset_to_node_data(node_offset); // copy
		node_size[i] = offset_to_node(node_offset)->size;
		memcpy(&node_data[i],offset_to_node_data(node_offset),node_size[i]+meta_size);

		if (node_offset == end_offset)
			break;
//		node_offset = node_data[i].next_offset_ig;
		node_offset = offset_to_node(node_offset)->next_offset_ig;
		i++;
	}
	part = i+1;
//}

//void sort_node(Node* node_data,int* sorted_index,int* max,const int node_size)
//void sort_node(Query *query)
//{
	unsigned char* kv;
	uint16_t v_size;
//	const int node_size = offset_to_node(data_point_to_offset((unsigned char*)node_data))->size; 
//	Node* node_data;
	query->sorted_kv_max = 0;
	query->sorted_kv_i = 0;
	int j;
//	const int kls = LK_SIZE;//PH_KEY_SIZE+PH_LEN_SIZE;//key_size+len_size;
	unsigned char* node_end;
//	const unsigned char* offset0 = query->node_data;
	j = 0;
	if (node_meta->inv_max == 0)
		node_meta->inv_kv = 0;
	for (i=0;i<part;i++)
	{
		node_data = &(((Node*)query->node_data)[i]);
		kv = node_data->buffer;
		node_end = (unsigned char*)node_data+node_size[i];
		//for (j=0;j<node_size[i];j++)
		while (kv < node_end)
		{
			v_size = *((uint16_t*)(kv));
			if (v_size & INV_BIT)
				v_size-=INV_BIT;
			else if (kv-(unsigned char*)node_data == node_meta->inv_kv[j])
			{
				j++;
				if (j == node_meta->inv_max)
					j = 0;
			}
			else
				query->sorted_kv[(query->sorted_kv_max)++] = kv;
			kv+=LK_SIZE+v_size;
		}
	}
	// sort
//	int i,j;
/*
	for (i=0;i<query->sorted_kv_max;i++)
	{
		for (j=i+1;j<query->sorted_kv_max;j++)
		{
			if (*((uint64_t*)(query->sorted_kv[i] + len_size)) > *((uint64_t*)(query->sorted_kv[j] + len_size))) // 8 byte ver
			{
				kv = query->sorted_kv[i];
				query->sorted_kv[i] = query->sorted_kv[j];
				query->sorted_kv[j] = kv;
			}
		}
	}	
	*/
}

void insert_scan_list(Node_offset &node_offset,void* query)
{
	Node_meta* node = offset_to_node(node_offset);
	Scan_list* sl;
	sl = (Scan_list*)malloc(sizeof(Scan_list)); // OP use allocator
	sl->query = query;
	sl->next = node->scan_list;
	node->scan_list = sl;
}

//void invalidate_kv(Node_offset node_offset, unsigned int kv_offset,unsigned int kv_len)

// pmem log can not be invalidated
// it has to be overwrite with last log

void invalidate_kv(ValueEntry& ve)
{
//	const int kl_size = LK_SIZE;//PH_KEY_SIZE+PH_LEN_SIZE;//key_size+len_size;
	Node_meta* meta;

#ifdef DOUBLE_LOG
	if (ve.node_offset.file & LOG_BIT)
	{
		unsigned char* kvp;
//		uint16_t vl16;
		kvp = (unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset;
		*((uint16_t*)kvp) |= INV_BIT;
//		vl16 = *((uint16_t*)kvp);
//		vl16&= ~(INV_BIT);
		Node_offset node_offset;
		node_offset = *(Node_offset*)(kvp+PH_LEN_SIZE/*len_size*/+key_size+ve.len);
		meta = offset_to_node(node_offset);
	offset_to_node(meta->start_offset)->invalidated_size+=ve.len+LK_SIZE;//kl_size; // kv_len is value len

		return;
		/*
		ve.len = vl16;
		if (vl16%2)
			vl16++;
		ve.node_offset = *((Node_offset*)(kvp+len_size+key_size+vl16));
		ve.kv_offset = *((uint16_t*)(kvp+len_size+key_size+vl16+sizeof(Node_offset)));
		*/
	}
#endif
	meta = offset_to_node(ve.node_offset);
	if (meta->inv_cnt+1 == meta->inv_max)
	{
		meta->inv_max*=2;
		meta->inv_kv = (uint16_t*)realloc(meta->inv_kv,sizeof(uint16_t)*meta->inv_max);
	}
	
	meta->inv_kv[meta->inv_cnt++] = ve.kv_offset;
	offset_to_node(meta->start_offset)->invalidated_size+=ve.len+LK_SIZE;//kl_size; // kv_len is value len
	if (ve.len%2)
		offset_to_node(meta->start_offset)->invalidated_size++;

//	delete_kv((unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset); // test
	
}

int split_or_compact(Node_offset node_offset)
{
	Node_meta* meta;
	meta = offset_to_node(node_offset);
//	if (meta->continue_len < 60) // test
//		return 1;
//	return meta->invalidated_size == 0;
	return meta->group_size > meta->invalidated_size*2;//2;//4;
}














//-----------------------------------------------------------------------------

#ifdef DOUBLE_LOG
int flush(Node_offset node_offset)
{
	Node_meta* meta;
	Node* node_data;
	meta = offset_to_node(node_offset);
	node_data = offset_to_node_data(node_offset);
	if (meta->part == PART_MAX-1)
	{
		Node_offset start_offset = get_start_offset(node_offset);
		if (split_or_compact(start_offset))
		{
//			return split(start_offset);
			if ( split(start_offset) == -1)
				return -1;
			else
				return 0;

		}
		else
		{
//			return compact(start_offset);
			if ( compact(start_offset) == -1)
				return -1;
			else
				return 0;
		}
	}

	int i,size;
	unsigned char* kvp;
	unsigned char* buffer;
	uint16_t vl16;
//	offset = meta->size;
	buffer = node_data->buffer+meta->size;
//	inv_index = 0;

	//scan need invalid data
	//i abandon invalid data here now
	//i flush data as possible as i can in this node without 256B

//	const int lks = len_size + key_size;
//	unsigned char* first = NULL;
	uint16_t first_vl16=0;

// flush doesn't use inv kv
// we use inv kv in pmem
// dram use inv bit in header

	ValueEntry ve;
	ve.node_offset = node_offset;
	for (i=0;i<meta->flush_cnt;i++) // batch or not?
	{
//		while(inv_index < meta->inv_cnt && offset < meta->inv_kv[inv_index])
//			++inv_index;

		kvp = meta->flush_kv[i]; // in log

		vl16 = *((uint16_t*)kvp);
		if (/*(meta->inv_kv[inv_index] == offset) || */(vl16 & INV_BIT))
		{
//			vl16&= ~INV_BIT;
//			offset+=vl16+lks;
			continue;
		}
		size = LK_SIZE+vl16;
		if (first_vl16)
		{
			memcpy(buffer,kvp,size);
		}
		else
		{
			memcpy(buffer+PH_LEN_SIZE/*len_size*/,kvp+PH_LEN_SIZE/*len_size*/,size-PH_LEN_SIZE/*len_size*/);
			first_vl16 = *((uint16_t*)kvp);
		}
		ve.kv_offset = buffer-(unsigned char*)node_data;
		ve.len = vl16;
		insert_point_entry((unsigned char*)kvp+PH_LEN_SIZE/*len_size*/,ve);
		_mm_sfence();
		*((uint16_t*)kvp) |= INV_BIT;
		if (size%2)
			size++;
		buffer+=size;

	}

//	vl16 = 0;
//	memcpy(node_data->buffer+meta->size,&vl16,len_size);
//	memcpy(node_data->buffer+meta->size+meta->flush_size,&vl16,len_size);
//	node_data->buffer[meta->size+meta->flush_size] = 0;
//	node_data->buffer[meta->size+meta->flush_size+1] = 0;
	buffer[0] = 0;
	buffer[1] = 0; 
	pmem_persist(node_data->buffer+meta->size,buffer-(node_data->buffer+meta->size) + PH_LEN_SIZE);//len_size);
//	pmem_persist(node_data->buffer+meta->size,meta->flush_size+len_size); // do not use flush size because there are invalid

	_mm_sfence();

//	vl16 = *((uint16_t*)first);
	pmem_memcpy(node_data->buffer+meta->size,&first_vl16,PH_LEN_SIZE/*len_size*/,PMEM_F_MEM_NONTEMPORAL);
//	*((uint16_t*)first) |= INV_BIT;

	_mm_sfence();

	meta->size = buffer-node_data->buffer;
//	meta->size+=meta->flush_size; // do not use flush_size
	meta->flush_cnt = 0;
	meta->flush_size = 0;

	return 1;
}
#else
int flush(Node_offset node_offset)
{
	return 0;
}
#endif

















//--------------------------------------------------------------------

std::atomic<int> split_queue_start[SPLIT_NUM];
std::atomic<int> split_queue_end[SPLIT_NUM];
std::atomic<Node_offset> split_queue[SPLIT_NUM][SPLIT_QUEUE_LEN];
std::atomic<uint8_t> split_queue_lock[SPLIT_NUM];

#ifdef idle_thread

extern thread_local PH_Thread* my_thread;
#define THREAD_RUN my_thread->running=1;
#define THREAD_IDLE my_thread->running=0;

#else

#define THREAD_RUN
#define THREAD_IDLE

#endif

#ifdef split_thread
void* split_work(void* iid)
{
	int i,id = *((int*)iid);
	Node_offset node_offset;
//	Node_meta* meta;
	update_free_cnt();
	while(1)
	{
		update_free_cnt();
		while(split_queue_start[id] < split_queue_end[id])
		{
	THREAD_RUN
			update_free_cnt();
			i = split_queue_start[id].fetch_add(1);
			node_offset = split_queue[id][i%SPLIT_QUEUE_LEN];

//			meta = offset_to_node(node_offset);
			if (try_split(node_offset) == 0)
				continue;
	
			if (split_or_compact(node_offset))
			{
				if (split2p(node_offset) < 0)
					dec_ref(node_offset);
			}
			else
			{
				if (compact2p(node_offset) < 0)
					dec_ref(node_offset);
			}
			// if fail do not try again

	THREAD_IDLE
		}
		usleep(1); // or nop
//		printf("idle %d %d %d\n",id,(int)split_queue_start[id],(int)split_queue_end[id]);
		if (split_queue_end[id] == -1)
			break;
	}
		clean_thread_local();
		return NULL;
}

thread_local int split_index=0;
int add_split(Node_offset node_offset)
{
//	int i,j;
	int j;
	for (j=0;j<num_of_split;j++)
	{
		split_index++;
		if (split_index>=num_of_split)
			split_index = 0;
//	for (;i<num_of_split;i++)
//	{
		if (split_queue_end[split_index] - split_queue_start[split_index] < SPLIT_MAX && try_at_lock(split_queue_lock[split_index]))
		{
//			j = split_queue_end[i].fetch_add(1);
//			j = split_queue_end[i];
			Node_meta* meta = offset_to_node(node_offset);
			meta->state = 2;
			split_queue[split_index][split_queue_end[split_index]%SPLIT_QUEUE_LEN] = node_offset;
			split_queue_end[split_index]++;
			split_queue_lock[split_index] = 0;
			split_index++;
			return 1;
		}
//	}
//	i = 0;
	}
	return -1;
}

void init_split()
{
	int i;
	for(i=0;i<num_of_split;i++)
		split_queue_lock[i] = split_queue_start[i] = split_queue_end[i] = 0;

}
void clean_split()
{
	int i;
	for (i=0;i<num_of_split;i++)
	{
		printf("split thread %d qe %d\n",i,(int)split_queue_end[i]);
		split_queue_end[i] = -1;
	}
}
#endif

int scan_node(Node_offset offset,unsigned char* key,int result_req,std::string* scan_result)
{
//fail:
//	unsigned char temp_buffer[NODE_BUFFER];
	uint64_t temp_key[100*PART_MAX],tk; // we need max cnt
//	uint16_t temp_len[100],tl;
	unsigned char* temp_offset[100*PART_MAX], *to;
	int tc=0;

	int cnt=0;
	uint64_t start_key = *(uint64_t*)key;


	unsigned char* buffer;
	unsigned char* buffer_end;

	Node* node_data;
	Node_meta* node_meta;

//	node_meta = offset_to_node(offset);
//	node_data = offset_to_node_data(offset);

	// here, we have ref and ve.node_offset and we should copy it and def it

//	sort_inv(node_meta->inv_cnt,node_meta->inv_kv);


//	Node d0[PART_MAX];
	int part0=0;
	const int meta_size = d0[0].buffer-(unsigned char*)&d0[0];

	Node_offset node_offset = offset;

	// prefetch failed
/*
	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		cp256h1((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);
//		memcpy((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);
	
size_sum+=node_meta->size+meta_size;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;

	}
*/
	//Node_offset node_offset = offset;
	node_offset = offset;

	while(1)
	{
		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		cp256((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);
//		memcpy((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);
	
		part0++;

//size_sum+=node_meta->size+meta_size;

		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;

	}
/*
	if (part0 > 4) //tset
	{
		printf("part 22 %d\n",part0);
		scanf("%d",&part0);
	}
*/

	int i,j,k;
	uint16_t size;
//	const int kls = key_size+len_size;

	node_offset = offset;
//	for (i=0;i<part0;i++)
	i = 0;
	while(1)
	{
		// read node
		node_meta = offset_to_node(node_offset);
		node_data = &d0[i];
		buffer  = node_data->buffer;
		buffer_end = buffer + node_meta->size;
		sort_inv(node_meta->inv_cnt,node_meta->inv_kv);
		j = 0;
		node_meta->inv_kv[node_meta->inv_cnt] = 0;

		while(buffer < buffer_end)
		{
			size = *((uint16_t*)buffer);
			if (((unsigned char*)node_data + node_meta->inv_kv[j] == buffer) /*|| (size & INV_BIT)*/)
			{
				j++;
				size&= ~(INV_BIT);
			}
			else
			{
				temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE));//len_size));
//				temp_len[tc] = *((uint16_t*)buffer);
				temp_offset[tc] = buffer;//+len_size;
				tc++;
			}
			buffer+=size+LK_SIZE;///kls;

		}
/*
		if (tc > 16) // test
		{
			printf("fail\n");
			goto fail;
		}
*/
		node_offset = node_meta->next_offset_ig;
		if (node_offset == INIT_OFFSET)
			break;
		i++;

	}

		//sort node
		for (j=0;j<tc;j++)
		{
			for (k=j+1;k<tc;k++)
			{
				if (temp_key[j] > temp_key[k])
				{
					tk = temp_key[j];
					temp_key[j] = temp_key[k];
					temp_key[k] = tk;
					to = temp_offset[j];
					temp_offset[j] = temp_offset[k];
					temp_offset[k] = to;
				}
			}
		}

		for (i=0;i<tc;i++)
		{
			if (start_key > temp_key[i])
				continue;
			scan_result[cnt].assign((char*)(temp_offset[i]),*(uint16_t*)temp_offset[i] + LK_SIZE);
			cnt++;
			if (result_req <= cnt)
				break;
		}

//cnt_sum+=cnt;
	return cnt; //temp

}

}
