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

#if 0
//#define FREE_QUEUE_LEN 100000 // moved to thread.h
volatile unsigned int free_cnt[PM_N]; // free_max // atomic or lock?
volatile unsigned int free_min[PM_N];
volatile unsigned int free_index[PM_N];
//Node_offset free_queue[PM_N][FREE_QUEUE_LEN]; // queue len
Node_offset free_queue[PM_N][10]; // queue len
#endif

std::atomic<uint64_t> at_part_offset_cnt[PM_N];
std::atomic<uint32_t> free_cnt[PM_N];
std::atomic<uint32_t> free_min_cnt[PM_N];
std::atomic<uint32_t> free_index[PM_N];
std::atomic<uint32_t> free_min_index[PM_N];
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
thread_local Node *d0,*d1,*d2,*append_templete;

thread_local Node_offset init_node[PM_N]; //initiailized

#define PART_MAX2 1000
thread_local Node* temp_data;

#define MAX_FILE_CNT ((uint64_t)(16)*1024*1024*1024/FILE_SIZE)
#define INV_RATIO 50

inline int need_comp(int group, int invalidated)
{
	if (file_num < MAX_FILE_CNT-1 || 
			(free_cnt[0]-free_index[0] > 1000 &&
			 free_cnt[1]-free_index[1] > 1000 &&
			 free_cnt[2]-free_index[2] > 1000 &&
			 free_cnt[3]-free_index[3] > 1000))//FREE_QUEUE_LEN/2)
		return group<invalidated*11/10;
	return group < invalidated*(100+INV_RATIO)/INV_RATIO;
}



void clean_thread_local()
{
//	printf("ctr\n");
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


	free(temp_data);
//	for (i=0;i<PART_MAX2;i++)
//		free(temp_data[i]);

	free(d0);
	free(d1);
	free(d2);

	free(append_templete);

//	if (cnt_sum > 0)
//		printf("size %ld cnt %d s/c %ld\n",size_sum,cnt_sum,size_sum/cnt_sum);
}

//#define alloc_test

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

#define NU (4096*PM_N/sizeof(Node))
Node_offset alloc_node2_s(int part)
{

	if (free_index[part] < free_cnt[part])
	{
		free_index[part]++;
		return free_queue[part][free_index[part]-1];
	}

	uint64_t offset_num = at_part_offset_cnt[part].fetch_add(1),fff,ooo;

	fff = offset_num/(MAX_OFFSET/PM_N);
	ooo = offset_num%(MAX_OFFSET/PM_N);

	if (fff >= file_num)
	{
		at_lock(alloc_lock);
		while (fff >= file_num)
			new_file();
		at_unlock(alloc_lock);
	}

	Node_offset offset;
	offset.file = fff;
//	offset.offset = ooo * PM_N + part;

	offset.offset = ooo/(4096/sizeof(Node))*NU + part*(4096/sizeof(Node)) + ooo%(4096/sizeof(Node));


	Node_meta* node;
	node = offset_to_node(offset);
//	node->state = 0;//need here? yes because it was 2 // but need here?
//	node->length_array = (uint16_t*)malloc(sizeof(uint16_t)*ARRAY_INIT_SIZE);
//	node->ll_cnt = 0;
	node->size_r_cnt = 0;
	node->ll = NULL;
	new_ll(&node->ll);

#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	_mm_mfence();
	tt5 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
	return offset;
}


Node_offset alloc_node2(int part)
{
//	return alloc_node2_s(part);
#ifdef dtt
	timespec ts1,ts2; // test
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
	Node_meta* node;
	Node_offset offset;

	if (free_index[part]+10 >= free_min_cnt[part])
	{
		uint32_t old_min,new_min;
		old_min = free_min_cnt[part];
		new_min = get_min_free_cnt(part);
		if (old_min < new_min)
			free_min_cnt[part].compare_exchange_strong(old_min,new_min);
	}

	uint32_t index,min,i2;
	while(1)
	{
		index = free_index[part];
		min = free_min_cnt[part];
		if (index+10 >= min)
			break;
		i2 = index;
		if (free_index[part].compare_exchange_strong(index,index+1))
		{
#ifdef alloc_print
			Node_offset no;
			no = free_queue[part][i2%FREE_QUEUE_LEN];
			printf("alloc %d %d /  %d %d\n",part,i2,no.file,no.offset);
#endif
			return free_queue[part][i2%FREE_QUEUE_LEN];
		}
	}
//	if (meta_size < meta_used + sizeof(Node_meta)) // we need check offset_cnt

	uint64_t offset_num = at_part_offset_cnt[part].fetch_add(1),fff,ooo;

	fff = offset_num/(MAX_OFFSET/PM_N);
	ooo = offset_num%(MAX_OFFSET/PM_N);

	if (fff >= file_num)
	{
		at_lock(alloc_lock);
		while (fff >= file_num)
			new_file();
		at_unlock(alloc_lock);
	}

	offset.file = fff;
//	offset.offset = ooo * PM_N + part;
	offset.offset = ooo/(4096/sizeof(Node))*NU + part*(4096/sizeof(Node)) + ooo%(4096/sizeof(Node));

	node = offset_to_node(offset);
//	node->state = 0;//need here? yes because it was 2 // but need here?
//	node->length_array = (uint16_t*)malloc(sizeof(uint16_t)*ARRAY_INIT_SIZE);
//	node->ll_cnt = 0;
	node->size_r_cnt = 0;
	node->ll = NULL;
	new_ll(&node->ll);

#ifdef dtt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	_mm_mfence();
	tt5 += (ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
#ifdef alloc_print
	printf("alloc2 %d %d\n",offset.file,offset.offset);
#endif
	return offset;
}

void free_node2(Node_offset offset)
{
//	free_node_s(offset);

	int part = offset.offset % PM_N;
	uint32_t old,temp;

	while(1)
	{
		temp = old = free_cnt[part];
		if (free_min_index[part] + FREE_QUEUE_LEN - 10 > old)
		{
			if (free_cnt[part].compare_exchange_strong(old,old+1))
			{
#ifdef alloc_print
				printf("free %d %d / %d %d\n",part,temp,offset.file,offset.offset);
#endif
				free_queue[part][temp%FREE_QUEUE_LEN] = offset;
				return;
			}
		}
//		my_thread->local_free_cnt[part] = free_cnt[part];
		if (my_thread)
			update_free_cnt0();
		temp = get_min_free_index(part);
		old = free_min_index[part];
		if (old < temp)
			free_min_index[part].compare_exchange_strong(old,temp);
	}
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
		printf("no dram ???");
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
	uint32_t ll_cnt;

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
	Node_offset_u temp_offset[PART_MAX];

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
		temp_offset[part].no = node_offset;

		node_meta = offset_to_node(node_offset);
		node_data = offset_to_node_data(node_offset);

		cp256((unsigned char*)&node_data_temp[part],(unsigned char*)node_data,sizeof(Node));
//		memcpy((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);

		//alloc node

	ll_cnt = 0; // ???
	node_meta->ll = NULL;
	new_ll(&node_meta->ll);




		node_meta->size_r_cnt = node_meta->size_l = 0; // repair again you should erase right
		node_meta->local_inv = 0;
		node_meta->part = part;
//		node_meta->inv_cnt = 0;
		node_meta->start_offset = temp_offset[0].no;
//		node_meta->next_offset_ig = 0;//INIT_OFFSET;

		buffer = node_data_temp[part].buffer;
		while(1)
		{
			uint16_t vl16;
			vl16 = *((uint16_t*)buffer);
			if (vl16 == 0)
				break;
//			if (vl16 & INV_BIT)
//				vl16-=INV_BIT;
//			else

			set_ll(node_meta->ll,ll_cnt,vl16);

			i = 0;
			if (vl16 & INV_BIT) // val bit
			{
				vl16-=INV_BIT;
				if (vl16 % KV_ALIGN)
					vl16+=(KV_ALIGN-vl16%KV_ALIGN);
				temp_key[tc] = *((uint64_t*)(buffer+PH_LEN_SIZE+PH_TS_SIZE));
				if (dup_hash[temp_key[tc]%DUP_HASH_MAX] == temp_offset[0].no) // dup???
				{
					for (i=tc-1;i>=0;i--)
					{
						if (temp_key[i] == temp_key[tc])
						{
							break;
						}
					}
				
					if (i >= 0)
					{
						invalidate_kv2(vea[i]);
						vea[i].kv_offset = 0;
//						node_meta->local_inv+=vl16;
					}
				}
					

				dup_hash[temp_key[tc]%DUP_HASH_MAX] = temp_offset[0].no;
				vea[tc].node_offset = temp_offset[part].no;
				vea[tc].kv_offset = buffer-(unsigned char*)&node_data_temp[part];
				vea[tc].ts = 0;
				vea[tc].index = ll_cnt;

				//!!!need new recovery
//				vea[tc].len = vl16;
				tc++;
			}
			else
			{
				if (vl16 % KV_ALIGN)
					vl16+=(KV_ALIGN-vl16%KV_ALIGN);
			}

			ll_cnt++;

			buffer+=PH_LTK_SIZE+vl16;


		}

		node_meta->size_l = buffer-node_data_temp[part].buffer;
		uint32_t size_l = node_meta->size_l;
		node_meta->size_r_cnt = (ll_cnt << 16) + size_l;
		meta0->group_size+=node_meta->size_l;



//size_sum+=node_meta->size+meta_size;

		Node_offset_u temp;
//		node_meta->next_offset_ig = node_data_temp[part].next_offset_ig;

		temp.no = node_data_temp[part].next_offset_ig;
		node_meta->next_offset_ig = temp.no_32;


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

	for (i=0;i<part;i++)//???
		offset_to_node(temp_offset[i].no)->end_offset = temp_offset[part].no_32;
	offset_to_node(temp_offset[0].no)->end_offset = temp_offset[part].no_32;


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
		{
			meta_array[i][j].continue_len = 128; // free check

//			meta_array[i][j].part = PART_MAX+1; // free check
		}
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
	node_meta->next_offset_ig = 0;//INIT_OFFSET;

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
		node_meta->next_offset_ig = 0;//INIT_OFFSET;

	// collect free nodes without alloc lock
		Node_offset no;
		int part;
	for (i=0;i<=file_num;i++) // file num xxx
	{
		for (j=0;j<MAX_OFFSET;j++)
		{
//			if (meta_array[i][j].part == PART_MAX+1) // free check
			if (meta_array[i][j].continue_len == 128)
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
	//it is for single thread
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
		free_cnt[i] = free_min_cnt[i] = free_min_index[i] = free_index[i] = 0;

	uint64_t zero=0;

	Node_offset_u head_offset;
	Node_offset_u tail_offset;

	Node_meta* head_node;// = offset_to_node(head_offset.no);
	Node_meta* tail_node;// = offset_to_node(tail_offset.no);


//	update_free_cnt(); // temp main thread

	//push for reserve
	/*
#if PM_N > 1
	alloc_node2(0); // 0 // INIT_OFFSET
	alloc_node2(1); // 1 SPLIT_OFFSET
	head_offset.no = alloc_node2(2); // 2 head
	tail_offset.no = alloc_node2(3); // 3 tail
#else
	alloc_node2(0); // 0 // INIT_OFFSET
	alloc_node2(0); // 1 SPLIT_OFFSET
	head_offset.no = alloc_node2(0); // 2 head
	tail_offset.no = alloc_node2(0); // 3 tail

#endif
*/
	if (NSK == 4)
	{
	alloc_node2(0); // 0 // INIT_OFFSET
	alloc_node2(1); // 1 SPLIT_OFFSET
	head_offset.no = alloc_node2(2); // 2 head
	tail_offset.no = alloc_node2(3); // 3 tail
	}
	else // NSK == 1
	{
	alloc_node2(0); // 0 // INIT_OFFSET
	alloc_node2(0); // 1 SPLIT_OFFSET
	head_offset.no = alloc_node2(0); // 2 head
	tail_offset.no = alloc_node2(0); // 3 tail
	}

	head_node = offset_to_node(head_offset.no);
	tail_node = offset_to_node(tail_offset.no);

//	Node_offset node_offset = alloc_node();
	Node_offset_u node_offset;
	node_offset.no = alloc_node2(0);	//4 ROOT OFFSET // no thread local
	Node_meta* node = offset_to_node(node_offset.no);
	node->state = 0;
	node->local_inv = node->size_l = node->size_r_cnt = 0;
	node->invalidated_size = 0;
//	node->ll_cnt = 0;
	node->ll = NULL;
	new_ll(&node->ll);

	node->tf = 0;
	node->continue_len = 0;
	node->scan_list = NULL;
	node->prev_offset = head_offset.no_32;
	node->next_offset = tail_offset.no_32;
	node->end_offset = node_offset.no_32;
	node->start_offset = node_offset.no;
	node->part = 0;
	node->group_size = 0;
	node->next_offset_ig = 0;//INIT_OFFSET;//0

	node->end_offset1 = node->end_offset2 = 0;

	Node* root_node_data;

	root_node_data = offset_to_node_data(node_offset.no);
	root_node_data->next_offset = tail_offset.no;
	root_node_data->next_offset_ig = INIT_OFFSET;
	root_node_data->continue_len = 0;
	memset(root_node_data->buffer,0,NODE_BUFFER);
	pmem_persist(root_node_data,sizeof(Node));

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
	head_node->next_offset_ig = 0;//INIT_OFFSET;//0;
	tail_node->next_offset_ig = 0;//INIT_OFFSET;//0;

	head_node->size_l = head_node->size_r_cnt = 0;
	head_node->group_size = head_node->invalidated_size=0;
	tail_node->size_l = tail_node->size_r_cnt = 0;
	tail_node->group_size = tail_node->invalidated_size=0;



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

	posix_memalign((void**)&append_templete,sizeof(Node),sizeof(Node));
	memset(append_templete,0,sizeof(Node));

	for (i=0;i<PM_N;i++)
	{
		lbac[i] = LOCAL_QUEUE_LEN;
		lbfc[i] = 0;


		init_node[i] = alloc_node2(i);
		pmem_memcpy(offset_to_node_data(init_node[i]),append_templete,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
	}

//	for (i=0;i<PART_MAX2;i++)
//		posix_memalign((void**)&temp_data[i],sizeof(Node),sizeof(Node));
	posix_memalign((void**)&temp_data,sizeof(Node),sizeof(Node)*PART_MAX2);


	posix_memalign((void**)&d0,sizeof(Node),sizeof(Node)*PART_MAX);
	posix_memalign((void**)&d1,sizeof(Node),sizeof(Node)*PART_MAX);
	posix_memalign((void**)&d2,sizeof(Node),sizeof(Node)*PART_MAX);


}
void clean_node(Node_offset offset)
{
	Node_meta* node = offset_to_node(offset);
	LENGTH_LIST* llp;
	LENGTH_LIST* llp_old;

	llp = (LENGTH_LIST*)node->ll.load();

	while(llp != NULL)
	{
		llp_old = llp;
		llp = (LENGTH_LIST*)llp->next.load();
		free(llp_old);
	}

//	free(node->length_array);
	
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

void size_test()
{
	Node_offset_u offset,offset2;
	Node_meta* meta,*meta2;
	offset.no = HEAD_OFFSET;
	uint64_t gs=0,is=0,nc=0;
	while(offset.no!=TAIL_OFFSET)
	{
		meta = offset_to_node(offset.no);

		offset2 = offset;
		while(offset2.no_32)
		{
			meta2 = offset_to_node(offset2.no);
			offset2.no_32 = meta2->next_offset_ig;
			nc++;
		}

		gs+=meta->group_size;
		is+=meta->invalidated_size;
//		if (reed_split(offset.no))
//		if (meta->group_size < meta->invalidated_size*3)
//		if (meta->group_size < meta->invalidated_size*140/40)
		if (need_comp(meta->group_size,meta->invalidated_size))
			printf("st %d %d\n",meta->group_size.load(),meta->invalidated_size.load());
		offset.no_32 = meta->next_offset;
	}
	printf("gs %ld is %ld nc %ld\n",gs,is,nc);
#if 0
	offset.no = HEAD_OFFSET;
		meta = offset_to_node(offset.no);
		offset.no_32 = meta->next_offset;

	while(offset.no!=TAIL_OFFSET)
	{
		update_idle();
		update_free_cnt0();
		int i;
		for (i=0;i<PM_N;i++)
			free_index[i] = free_cnt[i].load();
		compact3(offset.no);
		meta = offset_to_node(offset.no);
		gs+=meta->group_size;
		is+=meta->invalidated_size;
		offset.no_32 = meta->next_offset;
	}

	offset.no = HEAD_OFFSET;
	gs=0;
	is=0;
	nc = 0;

	while(offset.no!=TAIL_OFFSET)
	{
		meta = offset_to_node(offset.no);

		offset2 = offset;
		while(offset2.no_32)
		{
			meta2 = offset_to_node(offset2.no);
			offset2.no_32 = meta2->next_offset_ig;
			nc++;
		}

		gs+=meta->group_size;
		is+=meta->invalidated_size;
		offset.no_32 = meta->next_offset;
	}
	printf("gs %ld is %ld nc %ld\n",gs,is,nc);
#endif

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

	size_test();
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
	printf("meta %lfGB\n",double((file_num+1)*MAX_OFFSET*sizeof(Node_meta))/1024/1024/1024);
	printf("total %lfGB file cnt %d file size %ld\n",double((file_num+1)*FILE_SIZE)/1024/1024/1024,(file_num+1),FILE_SIZE);
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

	int sum=0;
	for (i=0;i<PM_N;i++)
	{
		printf("part %d %ld\n",i,at_part_offset_cnt[i].load());
		sum+=at_part_offset_cnt[i];
		printf("free queue %d %d %d\n",free_index[i].load(),free_cnt[i].load(),free_cnt[i].load()-free_index[i].load());
	}
	printf("sum %d\n",sum);


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

#define INV0 0xffffffffffffffff

extern int time_check;

void test3(unsigned char* &key_p, unsigned char* &value_p,int &value_len)
{
		Node* tcd;
		Node_offset offset;
//		offset = INIT_OFFSET;
		uint64_t rn;
		int rp,o;
		rp = rand()%PM_N;
//		rp = 0;
		rn = rand()%at_part_offset_cnt[rp];
		offset.file = rn/(MAX_OFFSET/PM_N);
		o = (rn%(MAX_OFFSET/PM_N));
		offset.offset = o/NU*NU + rp*4096 + o%(4096/sizeof(Node));

//		offset.offset = (rn%(MAX_OFFSET/PM_N))*PM_N+rp;

		tcd = offset_to_node_data(offset);

		unsigned char buffer2[sizeof(Node)];

		memcpy(buffer2,&value_len,PH_LEN_SIZE); // validation
		memcpy(buffer2+PH_LEN_SIZE+PH_TS_SIZE,key_p,PH_KEY_SIZE);
		memcpy(buffer2+PH_LEN_SIZE+PH_TS_SIZE+PH_KEY_SIZE,value_p,value_len);
				/*
				if ((uint64_t)buffer % 256 == 0)
					pmem_memcpy(buffer,buffer2,256,PMEM_F_MEM_NONTEMPORAL); // validation
				else
					pmem_memcpy(buffer,buffer2,PH_LTK_SIZE+value_len,PMEM_F_MEM_NONTEMPORAL); // validation
					*/
		memcpy((unsigned char*)tcd+512+100,buffer2,PH_LTK_SIZE+value_len); // validation
		pmem_persist((unsigned char*)tcd+512+100,PH_LTK_SIZE+value_len);

//				pmem_memcpy((unsigned char*)tcd+512,buffer2,256,PMEM_F_MEM_NONTEMPORAL); // validation

		_mm_sfence();

}

int insert_kv2(Node_offset &start_off,unsigned char* &key,unsigned char* &value,int &value_len,std::atomic<uint64_t>* &v64_p)
{
/*
	if (time_check)
	{
	test3(key,value,value_len);
	return 1;
	}
*/
	unsigned char buffer2[sizeof(Node)];

	Node_meta* start_meta;
	Node_meta* end_meta;
	Node_offset_u end_offset;
	Node_offset_u start_offset;
	start_offset.no = start_off;

//	inc_ref(start_offset.no);

//	std::atomic<uint16_t>* len_cas;
	uint16_t aligned_size,size_r;

	aligned_size = PH_LTK_SIZE+value_len;

	if (aligned_size % KV_ALIGN)
		aligned_size+=(KV_ALIGN-aligned_size%KV_ALIGN);


	uint8_t state;

	start_meta = offset_to_node(start_offset.no);

	uint64_t check_bit = (uint64_t)1 << (63-start_meta->continue_len);

	while(1)
	{

		state = start_meta->state;
		if (state & NODE_SPLIT_BIT)
		{
			if (((*((uint64_t*)(key))) & check_bit) == 0)
				start_offset.no_32 = start_meta->end_offset1;
			else
				start_offset.no_32 = start_meta->end_offset2;
			if (start_offset.no_32 == 0)
			{
//				printf("insert fail\n");
				return 0;
			}
			start_meta = offset_to_node(start_offset.no);
			continue;
		}
		inc_ref(start_offset.no);
		_mm_sfence();
		state = start_meta->state;
		if (state & NODE_SPLIT_BIT)
		{
			dec_ref(start_offset.no);
			if (((*((uint64_t*)(key))) & check_bit) == 0)
				start_offset.no_32 = start_meta->end_offset1;
			else
				start_offset.no_32 = start_meta->end_offset2;
			if (start_offset.no_32 == 0)
			{
//				printf("insert fail2\n");
				return 0;
			}
			start_meta = offset_to_node(start_offset.no);
			continue;
		}
		break;
	}

	uint16_t ll_cnt;
	uint32_t size_r_cnt;


	uint16_t empty;
	int off_l,off_r;
	const int meta_size = 16;

	if (false && time_check)
	{
	test3(key,value,value_len);
	dec_ref(start_offset.no);
	return 1;
	}


	// fix start!!!
	while(1) // insert and append
	{
		end_offset.no_32 = start_meta->end_offset;//.load(std::memory_order_seq_cst);
		end_meta = offset_to_node(end_offset.no);

		size_r_cnt = end_meta->size_r_cnt;
		ll_cnt = size_r_cnt >> 16;
		size_r = size_r_cnt & SIZE_R_MASK;

#ifdef empty256
		off_l = size_r+meta_size;
		off_r = off_l+aligned_size+PH_LEN_SIZE;
		empty = 256-off_l%256;

		if (empty < PH_LTK_SIZE || off_r/256 != (off_r+empty)/256)
			empty = 0;
#else
		empty = 0;
#endif
		if (size_r + empty + aligned_size + PH_LEN_SIZE > NODE_BUFFER) // need append
		{
			if (end_meta->next_offset_ig == 0)//INIT_OFFSET) // try new
			{
#if 0
				if (end_meta->part > PART_MAX+1)
				{
					continue;
				}
#endif
				Node_meta* new_meta;
				Node_offset_u new_offset;
//				int part = (end_meta->part+1)%PM_N;

//				new_offset = alloc_node((end_offset->part+1)%PM_N); // rotation?
				new_offset.no = init_node[part_rotation];
				new_meta = offset_to_node(new_offset.no);

				new_meta->part = end_meta->part+1;
				if (new_meta->part > PART_MAX)
					new_meta->part = PART_MAX;
				new_meta->size_l = 1;//2; // creation and end of node
				new_meta->size_r_cnt = 0;
				new_meta->local_inv=0;
//				new_meta->ll_cnt = 0;
				new_meta->start_offset = start_offset.no;
				new_meta->next_offset_ig = 0;//INIT_OFFSET;
				new_meta->next_offset = 0;
//				new_meta->end_offset = new_offset.no_32;

//				Node_data* node_data = offset_to_node_data(new_offset);

//				memset(offset_to_node_data(new_offset)->buffer,0,NODE_BUFFER);

				_mm_sfence();

				uint32_t z = 0,off = new_offset.no_32;
				if (end_meta->next_offset_ig.compare_exchange_strong(z,off))
				{
					uint32_t ttt = end_offset.no_32;
#if 1
					if (start_meta->end_offset.compare_exchange_strong(ttt,off) == false)
						// what if .... fail? size_l? // zero is split and it should not be restored
						printf("false\n");
#else

						static std::atomic<int> gc=0;
						int gg;
					if (start_meta->end_offset.compare_exchange_strong(ttt,off)) // what if .... fail? size_l? // zero is split and it should not be restored
					{
						gg = gc.fetch_add(1);
						printf("ok gc %d start offset %d %d end_offset %d %d new_offset %d %d\n",gg,start_offset.no.file,start_offset.no.offset,end_offset.no.file,end_offset.no.offset,new_offset.no.file,new_offset.no.offset);
					}
					else
					{
						gg = gc.fetch_add(1);
						printf("fail gc %d start offset %d %d end_offset %d %d new_offset %d %d\n",gg,start_offset.no.file,start_offset.no.offset,end_offset.no.file,end_offset.no.offset,new_offset.no.file,new_offset.no.offset);
					}
#endif
//					pmem_memcpy(&offset_to_node_data(end_offset.no)->next_offset_ig,&off,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//						if (time_check == 0)
						{
					memcpy(&offset_to_node_data(end_offset.no)->next_offset_ig,&off,sizeof(uint32_t));
					pmem_persist(&offset_to_node_data(end_offset.no)->next_offset_ig,sizeof(uint32_t));
						}


//					end_offset.no = new_offset.no;
//					end_meta = new_meta;

//					Node_offset_u temp_offset;
//					temp_offset.no_32 = start_offset->end_offset;
//					if (temp_offset.no_32 != 0)
//						start_offset->end_offset.compare_exchange_strong(temp_offset.no_32,off);

					//alloc new init node

					_mm_sfence(); // creation link
					new_meta->size_l--;
#if 0
					if (end_meta->size_r == end_meta->size_l)
					{
						size_r = end_meta->size_r;
						if (end_meta->size_r.compare_exchange_strong(size_r,NODE_BUFFER+1))
							new_meta->size_l--;
					}
#endif

					init_node[part_rotation] = alloc_node2(part_rotation);
//						if (time_check == 0)
					pmem_memcpy(offset_to_node_data(init_node[part_rotation]),append_templete,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
					part_rotation = (part_rotation+1)%PM_N;

//					_mm_sfence();

	//				continue;
				}
			}
/*
			{
				uint32_t off = end_meta->next_offset_ig.load();
				Node_offset_u new_offset;
				new_offset.no_32 = off;
				end_offset.no = new_offset.no;
				end_meta = offset_to_node(end_offset.no);
			}
*/
			//retry from finding end node
		}
		else // try insert
		{
			/*
	if (time_check)
	{
	test3(key,value,value_len);
	dec_ref(start_offset.no);
	return 1;
	}
	*/

			uint32_t ll_add=0x00010000;
			if (empty > 0)
				ll_add=0x00020000;


//			end_meta->size_r+= empty +aligned_size;
			if (end_meta->size_r_cnt.compare_exchange_strong(size_r_cnt,size_r_cnt+ll_add + empty +aligned_size))
			{

			uint8_t index = ll_cnt;
			uint16_t vl;
			unsigned char* buffer;
			Node* node_data;
			node_data = offset_to_node_data(end_offset.no);

			buffer = node_data->buffer + size_r + empty;
//			len_cas = (std::atomic<uint16_t>*)(buffer+e_size); // really?
//			uint16_t z = 0;
			vl = value_len;
//			uint8_t ts;
//			uint16_t ts;

				// lock from size
				
//				index = end_meta->ll_cnt.fetch_add(1);
				// not now
				/*
				index = end_meta->ll_cnt;
				set_ll(end_meta->ll.load(),index,vl);
				*/
				//may fence
				vl|=(1<<15); ////here??
//				end_meta->ll_cnt++;

				//important!!! other threand can do
//				end_meta->size+=aligned_size;
//#ifdef emp
			if (empty > 0)
			{
				set_ll(end_meta->ll.load(),index,empty-PH_LTK_SIZE);
//				end_meta->ll_cnt++;
				end_meta->local_inv+=empty;
				start_meta->invalidated_size+=empty;
//				start_meta->group_size+=empty;

//				if ((uint64_t)buffer % 256 != 0)
//					printf("%p\n",buffer);
				index++;
			}
//#endif

//				index = ll_cnt+1;
				set_ll(end_meta->ll.load(),index,vl); // validate ll
//				end_meta->ll_cnt++;
//				_mm_sfence();

				ValueEntry rve,ove;
				uint64_t ove64,rve64;
				rve.node_offset = end_offset.no;
				rve.kv_offset = buffer-(unsigned char*)node_data;
				rve.index = index;
//				rv.ts = ts;
				rve.ts = 0;

				rve64 = *(uint64_t*)(&rve);

#if 0
				ts = start_meta->ts.fetch_add(1); // ok??
#else
//				ts = 0;
#endif

//				memcpy(buffer+PH_LEN_SIZE,&ts,PH_TS_SIZE);
//				if (time_check == 0)
				{
#if 1
				memcpy(buffer+PH_LEN_SIZE+PH_TS_SIZE,key,PH_KEY_SIZE);
				memcpy(buffer+PH_LEN_SIZE+PH_TS_SIZE+PH_KEY_SIZE,value,value_len);
				pmem_persist(buffer,aligned_size);
#else
#ifdef pmemcopy_test
				if (false && time_check == 0)
#endif
//					if (time_check == 0)
				{
					if (time_check)
					{
		test3(key,value,value_len);
	dec_ref(start_offset.no);
	return 1;
					}
					else
					{

				memcpy(buffer2,&vl,PH_LEN_SIZE); // validation
				memcpy(buffer2+PH_LEN_SIZE+PH_TS_SIZE,key,PH_KEY_SIZE);
				memcpy(buffer2+PH_LEN_SIZE+PH_TS_SIZE+PH_KEY_SIZE,value,value_len);

				memcpy(buffer,buffer2,PH_LTK_SIZE+value_len); // validation
				pmem_persist(buffer,PH_LTK_SIZE+value_len);
					}

//				if ((uint64_t)buffer / 256 != ((uint64_t)buffer+PH_LTK_SIZE+value_len)/256)
//					printf("%p %d\n",buffer,PH_LTK_SIZE+value_len);


				}
			
#endif
				_mm_sfence();


//				pmem_memcpy(buffer,&vl,PH_LEN_SIZE,PMEM_F_MEM_NONTEMPORAL); // validation
				memcpy(buffer,&vl,PH_LEN_SIZE); // validation
				pmem_persist(buffer,PH_LEN_SIZE);
//				_mm_sfence();
				}
				if (end_meta->size_l != size_r)
				{
					start_meta->tf = 1;

					while (end_meta->size_l != size_r);
				}



_mm_sfence();

//if (time_check == 0)
{
	while(1)
	{
		ove64 = v64_p->load();
		ove = *(ValueEntry*)(&ove64);
//		if ((ove.ts <= rve.ts) || ((ove.ts & (1<<7)) != (rve.ts & (1<<7))))
		{
			if (v64_p->compare_exchange_strong(ove64,rve64))
			{
				if (ove64 != INV0)
					invalidate_kv2(ove);
				break;
			}
			//retry
		}
//		else // timeout?i
//			break;
	}
}

				_mm_sfence();
				end_meta->size_l+=aligned_size + empty; // end of query
//				while(end_meta->size_l.compare_exchange_strong(size_r,size_r+aligned_size)); // wait until come
#if 0
				uint16_t last_size = size_r+aligned_size;
				if (end_meta->next_offset_ig != 0 && end_meta->size_r.compare_exchange_strong(last_size,NODE_BUFFER+1))
					offset_to_node(*((Node_offset*)&end_meta->next_offset_ig))->size_l--; //activate next node if it is end of this node
#endif

				start_meta->group_size+=aligned_size+empty;

				dec_ref(start_offset.no);

				return 1;
			}
		}

//		dec_ref(start_offset.no);

	}

//never come here
//	dec_ref(start_offset);

}


void new_ll(std::atomic<void*>* next)
{
	LENGTH_LIST* llp;
	posix_memalign((void**)&llp,sizeof(LENGTH_LIST),sizeof(LENGTH_LIST));
	memset(llp,0,sizeof(LENGTH_LIST));

		void* n = NULL;
	if(next->compare_exchange_strong(n,llp) == false)
		free(llp);
	
}

uint16_t get_ll(void* ll,int index)
{
	LENGTH_LIST* llp = (LENGTH_LIST*)ll;
	if (index >= 12)
		return get_ll(llp->next,index-12);
	return llp->value[index];
}
void set_ll(void* ll,int index, uint16_t value)
{
	LENGTH_LIST* llp = (LENGTH_LIST*)ll;
	if (index >= 12)
	{
		if (llp->next == NULL)
			new_ll(&llp->next);
		set_ll(llp->next.load(),index-12,value);
	}
	else
		llp->value[index] = value;
}

//#define INVALID_MASK (1<<16)-1
#define INVALID_MASK 0x7fff

uint16_t inv_ll(void* ll,int index)
{
	LENGTH_LIST* llp = (LENGTH_LIST*)ll;
	if (index >= 12)
		return inv_ll(llp->next.load(),index-12);
	else
	{
		llp->value[index]&=INVALID_MASK;
		return llp->value[index];
	}
}


void split_node_init(Node_offset offset)
{
	Node_meta* meta = offset_to_node(offset);
	meta->part = 0;
//	meta->state = NODE_SR_BIT;
	meta->size_l = meta->size_r_cnt = 0;
//	meta->ll_cnt = 0;
	meta->group_size = 0;
	meta->invalidated_size = 0;
	meta->next_offset = meta->next_offset_ig = 0;
	meta->tf = 0;
	meta->local_inv=0;

	meta->end_offset1 = meta->end_offset2 = 0;
//	meta->scan li
	Node* data = offset_to_node_data(offset);
	pmem_memcpy(data,append_templete,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
}

inline int get_next_pm(Node_offset offset)
{
	return (offset.offset+1)%PM_N;
}

#define NODE_ENTRY_MAX 1000
thread_local ValueEntry old_vea1[NODE_ENTRY_MAX],old_vea2[NODE_ENTRY_MAX],new_vea1[NODE_ENTRY_MAX],new_vea2[NODE_ENTRY_MAX];
thread_local uint64_t temp_key1[NODE_ENTRY_MAX],temp_key2[NODE_ENTRY_MAX];

//int mm=0;

#define MAX_VAL 8
#define HOT_RATIO 4
#define VI_RATIO 2

#define MAX_INV 0//1//8//16 // 4



//int cnt=0;
int hot_to_node(int hot)
{
/*	
	cnt++;
	
	if (cnt % 1000000 == 0)
	{
		printf("%d\n",hot);
	}
*/	
	int i;
	hot>>=1;
	for (i=0;;i++)
	{
		hot>>=1;
		if (!hot)
		{
			return i*HOT_RATIO+MAX_INV;
		}
	}

	// 4 250
	// 6 4 16.5 11.83
	// 10 4 16.5 11.92
	// 20 4 16.6 12.02
	// 20 10 17.25 12.41
	// 0 16 17.37 12.36
	// 100 4 16.31 11.55
	// 200 4 16.31 11.69
	// 20 24 17.65 12.49

	//8 247
	//20 48 17.18 12.46
	//20 24 16.75 12.36
	//20 16 16.43 12.25
	//20 8 15.93 11.87

	// 100 4 15.87 10.87
	// 100 4 15.87 10.87


}

// 10.687500
// 16.437500 7.34906
// 12.937500 4.05123
// 15.562500 10.6918
// 24.312500 14.2151 // no comp

// non block
// 14.187500 6.73659

// gc 20 4
// 15.937500 10.9187
// 22.187500 12.4453 // no comp
// 24.312500 14.2626 // no comp 24 thread
// 15.437500 10.4759

// 15.312500 10.517 // non block
// 15.437500 10.7953

// gc 16 8
// 14.562500 8.25418
// 20.062500 10.6861 // no comp

int compact_threshold = 6;

int need_split(Node_offset &offset)
{
	Node_meta* meta;
//	Node_offset_u end_offset;
	meta = offset_to_node(offset);
//	end_offset.no_32 = meta->end_offset;

//	if (end_offset.no_32 == 0)
//		return 0;

//	if (meta->group_size <= NODE_BUFFER*2)
//		return 0;

	if (meta->state & NODE_SR_BIT)
		return 0;


/*
	if (meta->invalidated_size > 0)
		return 2;
	return 1;
*/
	if (/*meta->tf && */(meta->group_size-meta->invalidated_size) >= NODE_BUFFER*8)
		return 1;
//	if (meta->invalidated_size < hot_to_node(hot)*NODE_BUFFER)
//		return 0;
//	return 0;
//	if (meta->group_size < meta->invalidated_size*5)
//	if (meta->invalidated_size >= MAX_INV*NODE_BUFFER)
//	if (meta->invalidated_size > NODE_BUFFER*4)// * compact_threshold)
//	if (meta->group_size < meta->invalidated_size * 2)
	if (need_comp(meta->group_size,meta->invalidated_size))
	{
//		offset_to_node(offset)->state |= NODE_SR_BIT;
		meta->state |= NODE_SR_BIT;
		return 2;
		if (meta->tf)
			return 2; // non
		else
			return 3; // lock
	}
	return 0;
//	return 0;
/*
	if (meta->group_size < meta->invalidated_size*2)
	{
			offset_to_node(offset)->state |= NODE_SR_BIT;
			return 2;
	}
	else
	{
		offset_to_node(offset)->state |= NODE_SR_BIT;
		return 1;

	}
*/
//	return 0;
	if (meta->tf == 0)
	{
		return 2;
		if (meta->invalidated_size>NODE_BUFFER*4)
		{
			return 2;
		}
		else
			return 0;
	}
	else
	{
		if ((meta->group_size-meta->invalidated_size) >= NODE_BUFFER*2)
		{
			return 1;
		}
		else
		{
			return 2;
		}
	}


	if (offset_to_node(offset)->group_size >= NODE_BUFFER*2)
	{
		offset_to_node(offset)->state |= NODE_SR_BIT;
		return 1;
	}
	else
		return 0;
//	if (offset_to_node(end_offset.no)->part >= PART_MAX)
/*	
	if (offset_to_node(offset)->group_size >= NODE_BUFFER*3)
	{
		offset_to_node(offset)->state |= NODE_SR_BIT;
		return 1;
	}
*/
//	if (offset_to_node(offset)->group_size-offset_to_node(offset)->invalidated_size >= NODE_BUFFER*4*2)
//		return 2;
	if (offset_to_node(offset)->group_size-offset_to_node(offset)->invalidated_size >= NODE_BUFFER*MAX_VAL)
	{
		offset_to_node(offset)->state |= NODE_SR_BIT;
		return 1;
	}

//	if (meta->group_size-meta->invalidated_size >= NODE_BUFFER*8)
//		return 1;
/*
	if (offset_to_node(offset) != meta)
		printf("??????\n");

	if (&(offset_to_node(offset)->group_size) != &meta->group_size)
		printf("?ee\n");

	uint32_t a,b;
	a = offset_to_node(offset)->group_size;
	b = meta->group_size;

	if (a != b)
		printf("ab %d %d\n",a,b);

	if (offset_to_node(offset)->group_size != meta->group_size)
		printf("??? %d %d\n",offset_to_node(offset)->group_size.load(),meta->group_size.load());
*/
//	if (meta->invalidated_size >= NODE_BUFFER*8*2)
//		return 1;
	/*
	if (meta->invalidated_size >= NODE_BUFFER*hot_to_node(hot))//MAX_INV)
	{
		offset_to_node(offset)->state |= NODE_SR_BIT;
		return 1;
	}
	*/
	if (meta->invalidated_size > NODE_BUFFER && meta->invalidated_size * 4 > meta->group_size)//meta->group_size)
	{
		offset_to_node(offset)->state |= NODE_SR_BIT;
		return 1;
	}

//	if (meta->group_size > meta->invalidated_size * VI_RATIO)
//		return 1;

//	if (meta->group_size >= NODE_BUFFER*10)
//		return 1;
#if 0
	if (offset_to_node(offset)->group_size >= PART_MAX*NODE_BUFFER*2)
		return 1;
	else if (offset_to_node(offset)->group_size >= PART_MAX*NODE_BUFFER)// || (meta->group_size > meta->invalidated_size*2) == 0)
		return 2;
#endif

	return 0;
}

void split3_point_update(Node_meta* meta, ValueEntry* old_vea,ValueEntry* new_vea,uint64_t* temp_key,int veai)
{
	int i;
	uint64_t ov64,nv64;
	std::atomic<uint64_t>* v64_p;
	void* unlock;
	unsigned char* uc;

	ValueEntry ve;

	for (i=0;i<veai;i++)
	{
		uc = (unsigned char*)&(temp_key[i]);
		if (old_vea == NULL)
		{
		insert_point_entry(uc,new_vea[i]);
		}
		else
		{

		v64_p = find_or_insert_point_entry(uc,unlock);
		ov64 = *(uint64_t*)(&old_vea[i]);
		nv64 = *(uint64_t*)(&new_vea[i]);
		if (v64_p->compare_exchange_strong(ov64,nv64) == false)
			inv_ll(meta->ll,i);
		unlock_entry(unlock);
		}
	}

}

int check_lock(Node_offset offset)
{
	Node_meta* start_meta;

	Node_meta* temp_meta;
	Node_offset_u temp_offset;

	start_meta = offset_to_node(offset);

	// get lock and check prev and next

	while(1) // wait for next node split
	{
		uint8_t state = start_meta->state;
		if (state & NODE_SPLIT_BIT)
		{
			return 0; // lock fail???
		}
		if (start_meta->state.compare_exchange_strong(state , state|NODE_SPLIT_BIT))
				break;
	}

//	while(1)
	{
		temp_offset.no_32 = start_meta->prev_offset;
		temp_meta = offset_to_node(temp_offset.no);
		uint8_t state = temp_meta->state & NODE_SPLIT_BIT;
		if (state)
		{
			start_meta->state-=NODE_SPLIT_BIT;
			return -1; // retry
		}
/*
		uint8_t state = temp_meta->state;
		if (state & NODE_SPLIT_BIT)
			return 1; // lock fail
		if (temp_meta->state.compare_exchange_strong(state , state|NODE_SPLIT_BIT))
				break;
				*/
	}
	// we will split because we got prev lock


//	while(1)
	{
		temp_offset.no_32 = start_meta->next_offset;
		temp_meta = offset_to_node(temp_offset.no);
		/*
		uint8_t state = temp_meta->state;
		if ((state & NODE_SPLIT_BIT) == 0)
			break;
			*/
		uint8_t state = temp_meta->state & NODE_SPLIT_BIT;
		if (state)
		{
			start_meta->state-=NODE_SPLIT_BIT;
			return -1;
		}
	}

	return 1; //success
}

int split3(Node_offset start_offset)
{
	Node_meta* start_meta;
//	Node_meta* end_meta;
	Node_offset_u end_offset;

	start_meta = offset_to_node(start_offset);
/*
#ifdef split_thread
	if ((start_meta->state & NODE_SPLIT_BIT)

#endif
*/
	if (start_meta->state & NODE_SPLIT_BIT)
		return 0; // already

//	if ((start_meta->state & NODE_SR_BIT) == 0)
//		return 0;

	end_offset.no_32 = start_meta->end_offset;
	if (end_offset.no_32 == 0)
		return 0;
//	end_meta = offset_to_node(end_offset.no);

	// start offset should not be end offset!!!

	Node_meta* temp_meta;
	Node_offset_u temp_offset;

	int rv;
	rv = check_lock(start_offset);
	if (rv == 0)
		return 0; // fail
	else if (rv == -1)
		return 1; // retry

	_mm_sfence();

#ifdef split_thread
	while((start_meta->state & ~(NODE_SPLIT_BIT | NODE_SR_BIT)) != 0); // wiat until end
#else
	while((start_meta->state  != (NODE_SPLIT_BIT)) ); // wiat until end
#endif

	_mm_sfence();

	//set split

	Node_offset_u s1o,s2o;//,e1o,e2o;

	Node_meta* s1m;
	Node_meta* s2m;
//	Node_meta* e1m;
//	Node_meta* e2m;

	s1o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	s1m = offset_to_node(s1o.no);
	s2o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	s2m = offset_to_node(s2o.no);
	/*
	e1o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	e1m = offset_to_node(e1o.no);
	e2o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	e2m = offset_to_node(e2o.no);
*/
	split_node_init(s1o.no);
	s1m->state = NODE_SPLIT_BIT;
//	s1m->state = NODE_SR_BIT;
	s1m->start_offset = s1o.no;
	s1m->prev_offset = start_meta->prev_offset;
	s1m->next_offset = s2o.no_32;
	s1m->continue_len = start_meta->continue_len+1;
	s1m->tf = 0;
//	s1m->end_offset = s1o.no_32;

	split_node_init(s2o.no);
	s2m->state = NODE_SPLIT_BIT;
//	s2m->state = NODE_SR_BIT;
	s2m->start_offset = s2o.no;
	s2m->prev_offset = s1o.no_32;
	s2m->next_offset = start_meta->next_offset;
	s2m->continue_len = start_meta->continue_len+1;
	s2m->tf = 0;
//	s2m->end_offset = s2o.no_32;
/*
	split_node_init(e1o.no);
	e1m->start_offset = s1o.no;
	e1m->next_offset = e2o.no_32; // for crash

	split_node_init(e2o.no);
	e2m->start_offset = s2o.no;
*/
//	_mm_sfence();

// end to e1m
// e1m to w2m

//	pmem_memcpy(&offset_to_node_data(end_offset.no)->next_offset,&e1o.no_32,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(s1o)->next_offset,&s1m->next_offset,16/*sizeof(uint32_t)*/,PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(s2o)->next_offset,&s2m->next_offset,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(e1o.no)->next_offset,&e2o.no_32,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);

//	_mm_sfence();
/*
	start_meta->end_offset1 = 0;
	start_meta->end_offset2 = 0;

	_mm_sfence();
*/
//	start_meta->end_offset = 0;

	// do split

	_mm_sfence();


//	printf("split3 file %d offset %d\n",start_offset.file,start_offset.offset);


	Node_offset_u offset0,offset1,offset2;
	Node_meta *meta0,*meta1,*meta2;
//	Node data0,data1,data2;

	offset0.no = start_offset;
	offset1.no = s1o.no;
	offset2.no = s2o.no;
	meta1 = s1m;
	meta2 = s2m;

	unsigned char* buffer0,*buffer_end0,*buffer1,*buffer2;
	uint16_t len,aligned_size;
	uint64_t key,bit,fk;

	buffer1 = d1->buffer;
	buffer2 = d2->buffer;

	bit = (uint64_t)1 << (63-start_meta->continue_len);

	int vea1i,vea2i,vea0i;
	vea1i = 0;
	vea2i = 0;
	vea0i = 0;

//	uint16_t new_ts;
//	new_ts = 0; // flip


// valid move inv?

	buffer0 = NULL;

	int node_cnt=0;

	Node_offset_u new_offset;

	while(offset0.no_32)
	{
		node_cnt++;
		meta0 = offset_to_node(offset0.no);
		memcpy(d0,offset_to_node_data(offset0.no),sizeof(Node));
	
		if (meta0->size_l > 0)
			fk = *((uint64_t*)(d0->buffer+PH_LEN_SIZE+PH_TS_SIZE));

		buffer0 = d0->buffer;
		buffer_end0 = buffer0+meta0->size_l;
		vea0i = 0;

		while(buffer0 < buffer_end0)
		{
			len = get_ll(meta0->ll,vea0i);
			aligned_size = PH_LTK_SIZE+(len & ~(INV_BIT));
			if (aligned_size % KV_ALIGN)
				aligned_size+= (KV_ALIGN-aligned_size%KV_ALIGN);
			if (len & INV_BIT)
			{
				key = *((uint64_t*)(buffer0+PH_LEN_SIZE+PH_TS_SIZE));
				if ((key & bit) == 0)
				{
					if (buffer1 + aligned_size+PH_LEN_SIZE > d1->buffer+NODE_BUFFER)
					{
						*((uint16_t*)buffer1) = 0; // end len

						new_offset.no = alloc_node2(get_next_pm(offset1.no));
						d1->next_offset_ig = new_offset.no;
						meta1->next_offset_ig = new_offset.no_32;
						meta1->size_l = buffer1-d1->buffer;
						s1m->group_size+=meta1->size_l;

						pmem_memcpy(offset_to_node_data(offset1.no),d1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);

						_mm_sfence();
//						split3_point_update(meta1,old_vea1,new_vea1,temp_key1,vea1i);
						split3_point_update(meta1,NULL,new_vea1,temp_key1,vea1i);

//						meta1->ll_cnt = vea1i;
						meta1->size_r_cnt = (vea1i << 16) + meta1->size_l;



						meta1 = offset_to_node(new_offset.no);
						meta1->start_offset = s1o.no;
						meta1->next_offset = 0;
						meta1->local_inv=0;
//						data1 = offset_to_node_data(offset1);

//						meta1->part = temp_meta->part+1;
//						meta1->size_l = 0;
						vea1i = 0;
						buffer1 = d1->buffer;
						offset1 = new_offset;

					//alloc new
					}
/*
					old_vea1[vea1i].node_offset = offset0.no;
					old_vea1[vea1i].kv_offset = buffer0-(unsigned char*)&data0;
					old_vea1[vea1i].index = vea0i;
					old_vea1[vea1i].ts = *((uint16_t*)(buffer0+PH_LEN_SIZE)); //timestamp
*/
					new_vea1[vea1i].node_offset = offset1.no;
					new_vea1[vea1i].kv_offset = buffer1-(unsigned char*)d1;
					new_vea1[vea1i].index = vea1i;
					new_vea1[vea1i].ts = 0;//new_ts; //timestamp

					temp_key1[vea1i] = key;

//					*((uint16_t*)(buffer0+PH_LEN_SIZE)) = 0; //timestamp

					memcpy(buffer1,buffer0,aligned_size);
					buffer1+=aligned_size;

					set_ll(meta1->ll,vea1i,len);
					vea1i++;
					
				}
				else
				{
					if (buffer2 + aligned_size+PH_LEN_SIZE > d2->buffer+NODE_BUFFER)
					{
						*((uint16_t*)buffer2) = 0; // end len

						new_offset.no = alloc_node2(get_next_pm(offset2.no));
						d2->next_offset_ig = new_offset.no;
						meta2->next_offset_ig = new_offset.no_32;
						meta2->size_l = buffer2-d2->buffer;
						s2m->group_size+=meta2->size_l;

						pmem_memcpy(offset_to_node_data(offset2.no),d2,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);

//						split3_point_update(meta2,old_vea2,new_vea2,temp_key2,vea2i);
						split3_point_update(meta2,NULL,new_vea2,temp_key2,vea2i);

//						meta2->ll_cnt = vea2i;
						meta2->size_r_cnt = (vea1i << 16) + meta2->size_l;


						meta2 = offset_to_node(new_offset.no);
						meta2->start_offset = s2o.no;
						meta2->next_offset = 0;
						meta2->local_inv=0;
//						data1 = offset_to_node_data(offset1);

//						meta1->part = temp_meta->part+1;
//						meta2->size_l = 0;
						vea2i = 0;
						buffer2 = d2->buffer;
						offset2 = new_offset;

					//alloc new
					}
/*
					old_vea2[vea2i].node_offset = offset0.no;
					old_vea2[vea2i].kv_offset = buffer0-(unsigned char*)&data0;
					old_vea2[vea2i].index = vea0i;
					old_vea2[vea2i].ts = *((uint16_t*)(buffer0+PH_LEN_SIZE)); //timestamp
*/
					new_vea2[vea2i].node_offset = offset2.no;
					new_vea2[vea2i].kv_offset = buffer2-(unsigned char*)d2;
					new_vea2[vea2i].index = vea2i;
					new_vea2[vea2i].ts = 0;//new_ts; //timestamp

					temp_key2[vea2i] = key;

//					*((uint16_t*)(buffer0+PH_LEN_SIZE)) = 0; //timestamp

					memcpy(buffer2,buffer0,aligned_size);
					buffer2+=aligned_size;

					set_ll(meta2->ll,vea2i,len);
					vea2i++;


				}
			}
			vea0i++;
			buffer0+=aligned_size;
		}
		offset0.no_32 = meta0->next_offset_ig;
	}

//	printf("node_cnt %d gs0 %d gs1 %d gs2 %d\n",node_cnt,start_meta->group_size.load(),s1m->group_size.load(),s2m->group_size.load());

//	*((uint16_t*)buffer1) = 0; // end len
	buffer1[0] = buffer1[1] = 0;
//	offset1 = alloc_node(get_next_pm(offset1));
	d1->next_offset_ig = INIT_OFFSET;//e1o.no;
	meta1->next_offset_ig = 0;//e1o.no_32;//offset1;
	meta1->size_l = buffer1-d1->buffer;					

	s1m->group_size+=meta1->size_l;
	pmem_memcpy(offset_to_node_data(offset1.no),d1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	split3_point_update(meta1,old_vea1,new_vea1,temp_key1,vea1i);
	split3_point_update(meta1,NULL,new_vea1,temp_key1,vea1i);
//	meta1->ll_cnt = vea1i;
	meta1->size_r_cnt = (vea1i << 16) + meta1->size_l;

//	*((uint16_t*)buffer1) = 0; // end len
	buffer2[0] = buffer2[1] = 0;
//	offset1 = alloc_node(get_next_pm(offset1));
	d2->next_offset_ig = INIT_OFFSET;//e2o.no;
	meta2->next_offset_ig = 0;//e2o.no_32;//offset1;	
	meta2->size_l = buffer2-d2->buffer;					

	s2m->group_size+=meta2->size_l;
	pmem_memcpy(offset_to_node_data(offset2.no),d2,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	split3_point_update(meta2,old_vea2,new_vea2,temp_key2,vea2i);
	split3_point_update(meta2,NULL,new_vea2,temp_key2,vea2i);
//	meta2->ll_cnt = vea2i;
	meta2->size_r_cnt = (vea2i << 16) + meta2->size_l;


	s1m->end_offset = offset1.no_32;
	s2m->end_offset = offset2.no_32;

// write new node meta

	pmem_memcpy(&offset_to_node_data(s1o.no)->next_offset,(void*)(&s1m->next_offset),sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy(&offset_to_node_data(s1o.no)->continue_len,&s1m->continue_len,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy(&offset_to_node_data(s2o.no)->next_offset,(void*)(&s2m->next_offset),sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy(&offset_to_node_data(s2o.no)->continue_len,&s2m->continue_len,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);

	_mm_sfence(); // ready to link

	//connect the list
	Node* prev_data;
//	Node_offset_u temp_node_offset;
	temp_offset.no_32 = offset_to_node(start_offset)->prev_offset;
	prev_data = offset_to_node_data(temp_offset.no);
	pmem_memcpy(&prev_data->next_offset,&s1o,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	_mm_sfence();

	temp_meta = offset_to_node(temp_offset.no);
	temp_meta->next_offset = s1o.no_32;
//	temp_meta->state-=NODE_SPLIT_BIT; // unlock prev meta

	temp_offset.no_32 = start_meta->next_offset;
	offset_to_node(temp_offset.no)->prev_offset = s2o.no_32;

	_mm_sfence(); // persisted and write volatile


	//range hash

//	if (s1o.no.offset == 181)
//		printf("test22\n");

	insert_range_entry((unsigned char*)&fk,start_meta->continue_len,SPLIT_OFFSET);
	fk &= ~(bit);
	insert_range_entry((unsigned char*)&fk,start_meta->continue_len+1,s1o.no);
	fk += bit;
	insert_range_entry((unsigned char*)&fk,start_meta->continue_len+1,s2o.no);

	_mm_sfence();

	s1m->state-=NODE_SPLIT_BIT;
	s2m->state-=NODE_SPLIT_BIT;
	//free node

	offset0.no = start_offset;
	while(offset0.no_32) // start offset is just recyele
	{
		meta0 = offset_to_node(offset0.no);
		start_offset = offset0.no;
		offset0.no_32 = meta0->next_offset_ig;
		free_node2(start_offset);
	}
	return 0;
}

int compact3(Node_offset start_offset)
{
//	printf("ocmpact\n");
	Node_meta* start_meta;
//	Node_meta* end_meta;
	Node_offset_u end_offset;

	start_meta = offset_to_node(start_offset);

	if (start_meta->state & NODE_SPLIT_BIT)
		return 0; // already

	if ((start_meta->state & NODE_SR_BIT) == 0)
		return 0;


	end_offset.no_32 = start_meta->end_offset;
	if (end_offset.no_32 == 0)
		return 0;
//	end_meta = offset_to_node(end_offset.no);

	// start offset should not be end offset!!!

	Node_meta* temp_meta;
	Node_offset_u temp_offset;

	int rv;
	rv = check_lock(start_offset);
	if (rv == 0)
		return 0; // fail
	else if (rv == -1)
		return 1; // retry

	_mm_sfence();

//#ifdef split_thread
//	while((start_meta->state & ~(NODE_SPLIT_BIT | NODE_SR_BIT)) != 0); // wiat until end
//#else
//	while((start_meta->state != (NODE_SPLIT_BIT)) ); // wiat until end
//#endif

//	_mm_sfence();

	//set split

	Node_offset_u s1o,e1o;

	Node_meta* s1m;
	Node_meta* e1m;

	s1o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	s1m = offset_to_node(s1o.no);
	
	e1o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	e1m = offset_to_node(e1o.no);

	split_node_init(s1o.no);
	s1m->state = NODE_SR_BIT;
	s1m->start_offset = s1o.no;
	s1m->prev_offset = start_meta->prev_offset;
	s1m->next_offset = start_meta->next_offset;
	s1m->continue_len = start_meta->continue_len;
	s1m->tf = 0;
	s1m->end_offset = e1o.no_32;

	split_node_init(e1o.no);
	e1m->start_offset = s1o.no;
//	e1m->next_offset = e2o.no_32; // for crash

	_mm_sfence();

// end to e1m
// e1m to w2m

	pmem_memcpy(&offset_to_node_data(end_offset.no)->next_offset,&e1o.no_32,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(s1o)->next_offset,&s1m->next_offset,16/*sizeof(uint32_t)*/,PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(s2o)->next_offset,&s2m->next_offset,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(e1o.no)->next_offset,&e2o.no_32,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);

	_mm_sfence();

	start_meta->end_offset1 = s1o.no_32;
	start_meta->end_offset2 = s1o.no_32;

	_mm_sfence();

	while((start_meta->state & ~(NODE_SPLIT_BIT | NODE_SR_BIT)) != 0); // wiat until end

//	start_meta->end_offset = 0;

	// do split

	_mm_sfence();

	Node_offset_u offset0,offset1;
	Node_meta *meta0,*meta1;
//	Node data0,data1;

	offset0.no = start_offset;
	offset1.no = s1o.no;
	meta1 = s1m;

	unsigned char* buffer0,*buffer_end0,*buffer1;
	uint16_t len,aligned_size;
	uint64_t key,fk;

	buffer1 = d1->buffer;

	int vea1i,vea0i;
	vea1i = 0;
	vea0i = 0;

//	uint16_t new_ts;
//	new_ts = 0; // flip


// valid move inv?

	buffer0 = NULL;

	Node_offset_u new_offset;

	int node_cnt = 0;
	int i,cf[sizeof(Node)/256];

	fk = *((uint64_t*)(offset_to_node_data(start_offset)->buffer+PH_LEN_SIZE+PH_TS_SIZE));

	while(offset0.no_32)
	{
		node_cnt++;
		meta0 = offset_to_node(offset0.no);

		if (meta0->size_l == meta0->local_inv)
		{
			offset0.no_32 = meta0->next_offset_ig;
//			printf("pass\n");
			continue;
		}
#if 0
		memcpy(&data0,offset_to_node_data(offset0.no),sizeof(Node));
		for (i=0;i<sizeof(Node)/256;i++)
			cf[i] = 1;
#else
		for (i=0;i<sizeof(Node)/256;i++)
			cf[i] = 0;
#endif		
//		if (buffer0 == NULL)
//			fk = *((uint64_t*)(data0.buffer+PH_LEN_SIZE+PH_TS_SIZE));

		buffer0 = d0->buffer;
		buffer_end0 = buffer0+meta0->size_l;
		vea0i = 0;

		while(buffer0 < buffer_end0)
		{
			len = get_ll(meta0->ll,vea0i);
			aligned_size = PH_LTK_SIZE+(len & ~(INV_BIT));
			if (aligned_size % KV_ALIGN)
				aligned_size+= (KV_ALIGN-aligned_size%KV_ALIGN);
			if (len & INV_BIT)
			{
				unsigned char* uci = buffer0;
				int cfi = (uci-(unsigned char*)d0)/256;

				while(1)
				{
					if (cf[cfi] == 0)
					{
						cf[cfi] = 1;
						memcpy((unsigned char*)d0+256*cfi,(unsigned char*)offset_to_node_data(offset0.no)+256*cfi,256);
					}
					uci+=256;
					cfi++;
					if (uci >= buffer0+aligned_size || uci-d0->buffer >= NODE_BUFFER)
						break;

				}

				key = *((uint64_t*)(buffer0+PH_LEN_SIZE+PH_TS_SIZE));
				{
					if (buffer1 + aligned_size+PH_LEN_SIZE > d1->buffer+NODE_BUFFER)
					{
						*((uint16_t*)buffer1) = 0; // end len

						new_offset.no = alloc_node2(get_next_pm(offset1.no));
						d1->next_offset_ig = new_offset.no;
						meta1->next_offset_ig = new_offset.no_32;
						meta1->size_l = buffer1-d1->buffer;
						s1m->group_size+=meta1->size_l;

						pmem_memcpy(offset_to_node_data(offset1.no),d1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//						memcpy(offset_to_node_data(offset1.no),&data1,sizeof(Node));

						_mm_sfence();

						split3_point_update(meta1,old_vea1,new_vea1,temp_key1,vea1i);
//						meta1->ll_cnt = vea1i;
						meta1->size_r_cnt = (vea1i << 16) + meta1->size_l;


						meta1 = offset_to_node(new_offset.no);
						meta1->start_offset = s1o.no;
						meta1->next_offset = 0;
						meta1->local_inv=0;
//						data1 = offset_to_node_data(offset1);

//						meta1->part = temp_meta->part+1;
//						meta1->size_l = 0;
//						if (vea1i >= NODE_ENTRY_MAX)
//							printf("nem1 %d\n",vea1i);
						vea1i = 0;
						buffer1 = d1->buffer;
						offset1 = new_offset;

					//alloc new
					}

					old_vea1[vea1i].node_offset = offset0.no;
					old_vea1[vea1i].kv_offset = buffer0-(unsigned char*)d0;
					old_vea1[vea1i].index = vea0i;
					old_vea1[vea1i].ts = 0;//*((uint16_t*)(buffer0+PH_LEN_SIZE)); //timestamp

					new_vea1[vea1i].node_offset = offset1.no;
					new_vea1[vea1i].kv_offset = buffer1-(unsigned char*)d1;
					new_vea1[vea1i].index = vea1i;
					new_vea1[vea1i].ts = 0;//new_ts; //timestamp

					temp_key1[vea1i] = key;

//					*((uint16_t*)(buffer0+PH_LEN_SIZE)) = 0; //timestamp

					memcpy(buffer1,buffer0,aligned_size);
					buffer1+=aligned_size;

					set_ll(meta1->ll,vea1i,len);
					vea1i++;
					
				}
			}
			vea0i++;
			buffer0+=aligned_size;
		}
		offset0.no_32 = meta0->next_offset_ig;
	}

//	if (buffer0 == 0)
//		fk = *((uint64_t*)(offset_to_node_data(start_offset)->buffer+PH_LEN_SIZE+PH_TS_SIZE));

//	static int cc=0;
//	cc++;
//	if (node_cnt > 200)
//		printf("cmp node_cnt %d gs0 %d gs1 %d\n",node_cnt,start_meta->group_size.load(),s1m->group_size.load());

//	*((uint16_t*)buffer1) = 0; // end len
	buffer1[0] = buffer1[1] = 0;
//	offset1 = alloc_node(get_next_pm(offset1));
	d1->next_offset_ig = e1o.no;
	meta1->next_offset_ig = e1o.no_32;//offset1;
	meta1->size_l = buffer1-d1->buffer;						

	s1m->group_size+=meta1->size_l;

//	s1m->group_size+=NODE_BUFFER-meta1->size_l;
//	s1m->invalidated_size+=NODE_BUFFER-meta1->size_l;
	meta1->local_inv+=NODE_BUFFER-meta1->size_l;


	pmem_memcpy(offset_to_node_data(offset1.no),d1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	memcpy(offset_to_node_data(offset1.no),&data1,sizeof(Node));
	split3_point_update(meta1,old_vea1,new_vea1,temp_key1,vea1i);
//	meta1->ll_cnt = vea1i;
	meta1->size_r_cnt = (vea1i << 16) + meta1->size_l;

	//lock free
//	s1m->end_offset = offset1.no_32;

// write new node meta

	pmem_memcpy(&offset_to_node_data(s1o.no)->next_offset,(void*)(&s1m->next_offset),sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy(&offset_to_node_data(s1o.no)->continue_len,&s1m->continue_len,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);

	_mm_sfence(); // ready to link

	//connect the list
	Node* prev_data;
//	Node_offset_u temp_offset;
	temp_offset.no_32 = offset_to_node(start_offset)->prev_offset;
	prev_data = offset_to_node_data(temp_offset.no);
	pmem_memcpy(&prev_data->next_offset,&s1o,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	_mm_sfence();

	temp_meta = offset_to_node(temp_offset.no);
	temp_meta->next_offset = s1o.no_32;
//	temp_meta->state-=NODE_SPLIT_BIT; // unlock prev meta

	temp_offset.no_32 = start_meta->next_offset;
	offset_to_node(temp_offset.no)->prev_offset = s1o.no_32;

	_mm_sfence(); // persisted and write volatile

	//range hash

	insert_range_entry((unsigned char*)&fk,start_meta->continue_len,s1o.no);

	//free node

	_mm_sfence();

	s1m->state-=NODE_SR_BIT;//NODE_SPLIT_BIT;

	offset0.no = start_offset;
	while(offset0.no_32) // start offset is just recyele
	{
		meta0 = offset_to_node(offset0.no);
		start_offset = offset0.no;
		offset0.no_32 = meta0->next_offset_ig;
		free_node2(start_offset);
	}

	return 0; // done
}

int compact3_lock(Node_offset start_offset)
{
//	printf("ocmpact\n");
	Node_meta* start_meta;
//	Node_meta* end_meta;
	Node_offset_u end_offset;

	start_meta = offset_to_node(start_offset);

	if (start_meta->state & NODE_SPLIT_BIT)
		return 0; // already

	if ((start_meta->state & NODE_SR_BIT) == 0)
		return 0;


	end_offset.no_32 = start_meta->end_offset;
	if (end_offset.no_32 == 0)
		return 0;
//	end_meta = offset_to_node(end_offset.no);

	// start offset should not be end offset!!!

	Node_meta* temp_meta;
	Node_offset_u temp_offset;

	int rv;
	rv = check_lock(start_offset);
	if (rv == 0)
		return 0; // fail
	else if (rv == -1)
		return 1; // retry

	_mm_sfence();

//#ifdef split_thread
	while((start_meta->state & ~(NODE_SPLIT_BIT | NODE_SR_BIT)) != 0); // wiat until end
//#else
//	while((start_meta->state != (NODE_SPLIT_BIT)) ); // wiat until end
//#endif

	_mm_sfence();


	//set split

	Node_offset_u s1o;//,e1o;

	Node_meta* s1m;
//	Node_meta* e1m;

	s1o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	s1m = offset_to_node(s1o.no);
/*	
	e1o.no = alloc_node2(part_rotation);
	part_rotation = (part_rotation+1)%PM_N;
	e1m = offset_to_node(e1o.no);
*/
	split_node_init(s1o.no);
	s1m->state = NODE_SPLIT_BIT;
	s1m->start_offset = s1o.no;
	s1m->prev_offset = start_meta->prev_offset;
	s1m->next_offset = start_meta->next_offset;
	s1m->continue_len = start_meta->continue_len;
	s1m->tf = 0;
	s1m->end_offset = 0;//e1o.no_32;
/*
	split_node_init(e1o.no);
	e1m->start_offset = s1o.no;
//	e1m->next_offset = e2o.no_32; // for crash
*/
//	_mm_sfence();

// end to e1m
// e1m to w2m

//	pmem_memcpy(&offset_to_node_data(end_offset.no)->next_offset,&e1o.no_32,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(s1o)->next_offset,&s1m->next_offset,16/*sizeof(uint32_t)*/,PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(s2o)->next_offset,&s2m->next_offset,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
//	pmem_memcpy(&offset_to_node_data(e1o.no)->next_offset,&e2o.no_32,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);

//	_mm_sfence();

//	temp_offset.no = start_offset;
//	start_meta->end_offset1 = temp_offset.no_32;
//	start_meta->end_offset2 = temp_offset.no_32;

//	_mm_sfence();

//	start_meta->end_offset = 0;

	// do split

	Node_offset_u offset0,offset1;
	Node_meta *meta0,*meta1;
//	Node data0,data1;

	offset0.no = start_offset;
	offset1.no = s1o.no;
	meta1 = s1m;

	unsigned char* buffer0,*buffer_end0,*buffer1;
	uint16_t len,aligned_size;
	uint64_t key,fk;

	buffer1 = d1->buffer;

	int vea1i,vea0i;
	vea1i = 0;
	vea0i = 0;

//	uint16_t new_ts;
//	new_ts = 0; // flip


// valid move inv?

	buffer0 = NULL;

	Node_offset_u new_offset;

	int node_cnt = 0;
	int i,cf[sizeof(Node)/256];

	fk = *((uint64_t*)(offset_to_node_data(start_offset)->buffer+PH_LEN_SIZE+PH_TS_SIZE));

	while(offset0.no_32)
	{
		node_cnt++;
		meta0 = offset_to_node(offset0.no);

		if (meta0->size_l == meta0->local_inv)
		{
			offset0.no_32 = meta0->next_offset_ig;
//			printf("pass\n");
			continue;
		}
#if 0
		memcpy(&data0,offset_to_node_data(offset0.no),sizeof(Node));
		for (i=0;i<sizeof(Node)/256;i++)
			cf[i] = 1;
#else
		for (i=0;i<sizeof(Node)/256;i++)
			cf[i] = 0;
#endif		
//		if (buffer0 == NULL)
//			fk = *((uint64_t*)(data0.buffer+PH_LEN_SIZE+PH_TS_SIZE));

		buffer0 = d0->buffer;
		buffer_end0 = buffer0+meta0->size_l;
		vea0i = 0;

		while(buffer0 < buffer_end0)
		{
			len = get_ll(meta0->ll,vea0i);
			aligned_size = PH_LTK_SIZE+(len & ~(INV_BIT));
			if (aligned_size % KV_ALIGN)
				aligned_size+= (KV_ALIGN-aligned_size%KV_ALIGN);
			if (len & INV_BIT)
			{
				unsigned char* uci = buffer0;
				int cfi = (uci-(unsigned char*)d0)/256;

				while(1)
				{
					if (cf[cfi] == 0)
					{
						cf[cfi] = 1;
						memcpy((unsigned char*)d0+256*cfi,(unsigned char*)offset_to_node_data(offset0.no)+256*cfi,256);
					}
					uci+=256;
					cfi++;
					if (uci >= buffer0+aligned_size || uci-d0->buffer >= NODE_BUFFER)
						break;

				}

				key = *((uint64_t*)(buffer0+PH_LEN_SIZE+PH_TS_SIZE));
				{
					if (buffer1 + aligned_size+PH_LEN_SIZE > d1->buffer+NODE_BUFFER)
					{
						*((uint16_t*)buffer1) = 0; // end len

						new_offset.no = alloc_node2(get_next_pm(offset1.no));
						d1->next_offset_ig = new_offset.no;
						meta1->next_offset_ig = new_offset.no_32;
						meta1->size_l = buffer1-d1->buffer;
						s1m->group_size+=meta1->size_l;

						pmem_memcpy(offset_to_node_data(offset1.no),d1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//						memcpy(offset_to_node_data(offset1.no),&data1,sizeof(Node));

						_mm_sfence();

						split3_point_update(meta1,NULL,new_vea1,temp_key1,vea1i);
//						meta1->ll_cnt = vea1i;
						meta1->size_r_cnt = (vea1i << 16) + meta1->size_l;


						meta1 = offset_to_node(new_offset.no);
						meta1->start_offset = s1o.no;
						meta1->next_offset = 0;
						meta1->local_inv=0;
//						data1 = offset_to_node_data(offset1);

//						meta1->part = temp_meta->part+1;
//						meta1->size_l = 0;
//						if (vea1i >= NODE_ENTRY_MAX)
//							printf("nem1 %d\n",vea1i);
						vea1i = 0;
						buffer1 = d1->buffer;
						offset1 = new_offset;

					//alloc new
					}
/*
					old_vea1[vea1i].node_offset = offset0.no;
					old_vea1[vea1i].kv_offset = buffer0-(unsigned char*)&data0;
					old_vea1[vea1i].index = vea0i;
					old_vea1[vea1i].ts = 0;//*((uint16_t*)(buffer0+PH_LEN_SIZE)); //timestamp
*/
					new_vea1[vea1i].node_offset = offset1.no;
					new_vea1[vea1i].kv_offset = buffer1-(unsigned char*)d1;
					new_vea1[vea1i].index = vea1i;
					new_vea1[vea1i].ts = 0;//new_ts; //timestamp

					temp_key1[vea1i] = key;

//					printf("key %llx file %d offset %d kv %d\n",key,offset1.no.file,offset1.no.offset,new_vea1[vea1i].kv_offset);

//					*((uint16_t*)(buffer0+PH_LEN_SIZE)) = 0; //timestamp

					memcpy(buffer1,buffer0,aligned_size);
					buffer1+=aligned_size;

					set_ll(meta1->ll,vea1i,len);
					vea1i++;
					
				}
			}
			vea0i++;
			buffer0+=aligned_size;
		}
		offset0.no_32 = meta0->next_offset_ig;
	}

//	if (buffer0 == 0)
//		fk = *((uint64_t*)(offset_to_node_data(start_offset)->buffer+PH_LEN_SIZE+PH_TS_SIZE));

//	static int cc=0;
//	cc++;
//	if (node_cnt > 200)
//		printf("cmp node_cnt %d gs0 %d gs1 %d\n",node_cnt,start_meta->group_size.load(),s1m->group_size.load());

//	*((uint16_t*)buffer1) = 0; // end len
	buffer1[0] = buffer1[1] = 0;
//	offset1 = alloc_node(get_next_pm(offset1));
	d1->next_offset_ig = INIT_OFFSET;//e1o.no;
	meta1->next_offset_ig = 0;//e1o.no_32;//offset1;
	meta1->size_l = buffer1-d1->buffer;						

	s1m->group_size+=meta1->size_l;
	pmem_memcpy(offset_to_node_data(offset1.no),d1,sizeof(Node),PMEM_F_MEM_NONTEMPORAL);
//	memcpy(offset_to_node_data(offset1.no),&data1,sizeof(Node));
	split3_point_update(meta1,NULL,new_vea1,temp_key1,vea1i);
//	meta1->ll_cnt = vea1i;
	meta1->size_r_cnt = (vea1i << 16) + meta1->size_l;

	//lock free
	s1m->end_offset = offset1.no_32;
//	s1m->end_offset.store(offset1.no_32,std::memory_order_seq_cst);

// write new node meta

	pmem_memcpy(&offset_to_node_data(s1o.no)->next_offset,(void*)(&s1m->next_offset),sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	pmem_memcpy(&offset_to_node_data(s1o.no)->continue_len,&s1m->continue_len,sizeof(uint16_t),PMEM_F_MEM_NONTEMPORAL);

	_mm_sfence(); // ready to link

	//connect the list
	Node* prev_data;
//	Node_offset_u temp_offset;
	temp_offset.no_32 = offset_to_node(start_offset)->prev_offset;
	prev_data = offset_to_node_data(temp_offset.no);
	pmem_memcpy(&prev_data->next_offset,&s1o,sizeof(uint32_t),PMEM_F_MEM_NONTEMPORAL);
	_mm_sfence();

	temp_meta = offset_to_node(temp_offset.no);
	temp_meta->next_offset = s1o.no_32;
//	temp_meta->state-=NODE_SPLIT_BIT; // unlock prev meta

	temp_offset.no_32 = start_meta->next_offset;
	offset_to_node(temp_offset.no)->prev_offset = s1o.no_32;

	_mm_sfence(); // persisted and write volatile

	//range hash

	insert_range_entry((unsigned char*)&fk,start_meta->continue_len,s1o.no);

	//free node

	_mm_sfence();

	s1m->state-=NODE_SPLIT_BIT;

//	start_meta->end_offset1 = start_meta->end_offset2 = s1o.no_32;

	offset0.no = start_offset;
	while(offset0.no_32) // start offset is just recyele
	{
		meta0 = offset_to_node(offset0.no);
		start_offset = offset0.no;
		offset0.no_32 = meta0->next_offset_ig;
		free_node2(start_offset);
	}

	return 0; // done
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

uint16_t get_length_from_ve(ValueEntry& ve)
{
	Node_meta* meta;
	meta = offset_to_node(ve.node_offset);
	return get_ll(meta->ll,ve.index);
}

//#define INVALID_MASK (1<<16)-1

void invalidate_kv2(ValueEntry& ve)
{
	Node_meta* meta;
	meta = offset_to_node(ve.node_offset);
//	meta->length_array[ve.index]&=INVALID_MASK;

	uint16_t len;
	len = (inv_ll(meta->ll,ve.index) & (~INV_BIT)) + PH_LTK_SIZE;
	if (len % KV_ALIGN)
		len+=(KV_ALIGN-len%KV_ALIGN);
	meta->local_inv+=len;
	meta = offset_to_node(meta->start_offset);
	meta->invalidated_size+=len; // invalidated
}



//-------------------------------------------------------------

//scan


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

#if 0
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
//	Node_offset node_offset;// = query->scan_offset;
	Node_meta* node_meta;// = offset_to_node(node_offset);
//	Node_offset end_offset;// = node_meta->end_offset;
	uint32_t end_offset_32;
	int i = 0;
	int part;
	int node_size[100*PART_MAX];
	Node_offset_u node_offset;
//	nou.no_32 = query->scan_offset;
	node_offset.no_32 = query->scan_offset;
//	node_offset = query->scan_offset;
	node_meta = offset_to_node(node_offset.no);
	end_offset_32 = node_meta->end_offset;
	int meta_size = (unsigned char*)node_data->buffer-(unsigned char*)node_data;
	while(1)
	{
//		node_data[i] = *offset_to_node_data(node_offset); // copy
		node_size[i] = offset_to_node(node_offset.no)->size;
		memcpy(&node_data[i],offset_to_node_data(node_offset.no),node_size[i]+meta_size);

		if (node_offset.no_32 == end_offset_32)
			break;
		node_offset.no = node_data[i].next_offset_ig;
//		node_offset = *(Node_offset*)(&offset_to_node(node_offset)->next_offset_ig);
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
	if (node_meta->la_max == 0)
		node_meta->length_array = 0;
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
			else if (kv-(unsigned char*)node_data == node_meta->length_array[j])
			{
				j++;
				if (j == node_meta->la_max)
					j = 0;
			}
			else
				query->sorted_kv[(query->sorted_kv_max)++] = kv;
			kv+=PH_LTK_SIZE+v_size;
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

void invalidate_kv(ValueEntry& ve) // old version!!!
{
//	const int kl_size = LK_SIZE;//PH_KEY_SIZE+PH_LEN_SIZE;//key_size+len_size;
	Node_meta* meta;
	uint16_t len = get_length_from_ve(ve);

	meta = offset_to_node(ve.node_offset);
	if (meta->la_cnt+1 == meta->la_max)
	{
		meta->la_max=meta->la_max*2;
		meta->length_array = (uint16_t*)realloc(meta->length_array,sizeof(uint16_t)*meta->la_max);
	}
	
	meta->length_array[meta->la_cnt++] = ve.kv_offset;
	offset_to_node(meta->start_offset)->invalidated_size+=len+PH_LTK_SIZE;//kl_size; // kv_len is value len
	if (len%KV_ALIGN)
		offset_to_node(meta->start_offset)->invalidated_size+=(KV_ALIGN-len%KV_ALIGN);

//	delete_kv((unsigned char*)offset_to_node_data(ve.node_offset)+ve.kv_offset); // test
	
}

#endif


int split_or_compact(Node_offset node_offset)
{
	return 0;
	/*
	if (file_num >= FILE_LIMIT || offset_to_node(node_offset)->ll_cnt <= 1)
		return 0;
	else
		return 1;
		*/
	Node_meta* meta;
	meta = offset_to_node(node_offset);
//	if (meta->continue_len < 60) // test
//		return 1;
	if (offset_to_node(node_offset)->group_size-offset_to_node(node_offset)->invalidated_size >= NODE_BUFFER*MAX_VAL)
		return 1;
	else
		return 0;

	return meta->invalidated_size == 0;
//	if (meta->group_size-meta->invalidated_size >= PART_MAX*NODE_BUFFER)
//		return 1;
//	return meta->group_size > meta->invalidated_size*2;//*2;//2;//4;
//	return meta->group_size-meta->invalidated_size >= NODE_BUFFER*4;
}













//-----------------------------------------------------------------------------

//DOUBLE LOG DEAD

//--------------------------------------------------------------------


#ifdef idle_thread

extern thread_local PH_Thread* my_thread;
#define THREAD_RUN my_thread->running=1;
#define THREAD_IDLE my_thread->running=0;

#else

#define THREAD_RUN
#define THREAD_IDLE

#endif

#ifdef split_thread

std::atomic<int> split_queue_start[SPLIT_NUM];
std::atomic<int> split_queue_end[SPLIT_NUM];
//std::atomic<Node_offset> split_queue[SPLIT_NUM][SPLIT_QUEUE_LEN];
std::atomic<uint32_t> split_queue[SPLIT_NUM][SPLIT_QUEUE_LEN];
//std::atomic<uint8_t> split_queue_lock[SPLIT_NUM];

void* split_work(void* iid)
{
	int i,id = *((int*)iid);
	Node_offset_u node_offset;
	int prev_en = 0;
//	Node_meta* meta;
//	update_free_cnt();
	while(1)
	{
		update_free_cnt();
		while(split_queue_start[id] < split_queue_end[id])
		{
			update_free_cnt0();
	THREAD_RUN

			i = split_queue_start[id]%SPLIT_QUEUE_LEN;
			while(split_queue[id][i] == 0);
			node_offset.no_32 = split_queue[id][i];
			split_queue[id][i] = 0;
			_mm_sfence();
			split_queue_start[id]++;
//			i = split_queue_start[id].fetch_add(1) % SPLIT_QUEUE_LEN;

//			meta = offset_to_node(node_offset);
//			if (try_split(node_offset) == 0)
//				continue;
	
			int is;
			is = offset_to_node(node_offset.no)->invalidated_size;

			if (split_or_compact(node_offset.no))
			{
				split3(node_offset.no);
//				while (split3(node_offset.no) == 0);
//				if (split2p(node_offset) < 0)
//					dec_ref(node_offset);
			}
			else
			{
//				compact3(node_offset.no);
				while ( compact3(node_offset.no)); // lock fail retry
//			while ( compact3_lock(node_offset.no)); // lock fail retry

//				if (compact2p(node_offset) < 0)
//					dec_ref(node_offset);
			}
			// if fail do not try again

			if (split_queue_end[id] % 10 == 0)
			{
				int en = split_queue_end[id]-split_queue_start[id];
//				if (split_queue_end[id] % 10000 == 0)
//				printf("queue %d / %d ct %d is %d\n",en,SPLIT_QUEUE_LEN,compact_threshold,is);
				if (en < SPLIT_QUEUE_LEN / 2)
				{
					if (compact_threshold > 0 && prev_en > en)
						compact_threshold--;
				}
				else if (en >= SPLIT_QUEUE_LEN/2+SPLIT_QUEUE_LEN/10 && prev_en < en)
					compact_threshold++;
				prev_en = en;
			}


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
	int qe;
	uint32_t z=0;
	Node_offset_u nou;
	nou.no = node_offset;

	Node_meta* meta;
	meta = offset_to_node(node_offset);
	uint8_t state = meta->state;
	if ((state & NODE_SR_BIT) || (state & NODE_SPLIT_BIT))
		return -2;
	if (meta->state.compare_exchange_strong(state,state|NODE_SR_BIT) == false)
		return -2;

	for (j=0;j<num_of_split;j++)
	{
		split_index++;
		if (split_index>=num_of_split)
			split_index = 0;
//	for (;i<num_of_split;i++)
//	{
		qe = split_queue_end[split_index];
		if (qe < split_queue_start[split_index] + SPLIT_QUEUE_LEN-10)// && try_at_lock(split_queue_lock[split_index]))
		{
			if (split_queue_end[split_index].compare_exchange_strong(qe,qe+1))
			{
				split_queue[split_index][qe%SPLIT_QUEUE_LEN] = nou.no_32;
				return 1;
			}
		/*	
			if (split_queue[split_index][qe%SPLIT_QUEUE_LEN].compare_exchange_strong(z,nou.no_32))
			{
//			j = split_queue_end[i].fetch_add(1);
//			j = split_queue_end[i];
//			Node_meta* meta = offset_to_node(node_offset);
//			meta->state = 2;
//			split_queue[split_index][split_queue_end[split_index]%SPLIT_QUEUE_LEN] = node_offset;
			split_queue_end[split_index]++;
//			split_queue_lock[split_index] = 0;
//			split_index++;
			return 1;
			}
			*/
		}
//	}
//	i = 0;
	}
	printf("queue full %d\n",compact_threshold);
//	meta->state-=NODE_SPLIT_BIT;
	meta->state-=NODE_SR_BIT;
	return -1;
}

void init_split()
{
	int i;
	for(i=0;i<num_of_split;i++)
		/*split_queue_lock[i] =*/ split_queue_start[i] = split_queue_end[i] = 0;

}
void clean_split()
{
	int i;
	for (i=0;i<num_of_split;i++)
	{
		printf("split thread %d qs %d qe %d\n",i,(int)split_queue_start[i],(int)split_queue_end[i]);
		split_queue_end[i] = -1;
	}
}
#endif

#if 1

thread_local uint64_t temp_key[100*PART_MAX2],temp_sb[100*PART_MAX2];
thread_local unsigned char* temp_offset[100*PART_MAX2];
thread_local unsigned char* temp_sb2[100*PART_MAX2];
thread_local int tc,tdc;


// ab 0 sb -> temp
// ab 1 temp -> sb
void merge_sort(int ab,int loc,int size)
{
	if (size <= 1)
	{
		if (ab == 1)
		{
			temp_sb[loc] = temp_key[loc];
			temp_sb2[loc] = temp_offset[loc];
		}
		/*
		else
		{
			temp_key[loc] = temp_sb[loc];
			temp_offset[loc] = temp_sb2[loc];
		}
		return 1-ab;
		*/
		return;
	}

	int h2 = size/2,h1 = size-h2;
	merge_sort(1-ab,loc,h1);
	merge_sort(1-ab,loc+h1,h2);

	uint64_t *src_k,*dst_k;
	unsigned char **src_o;
	unsigned char **dst_o;

	if (ab == 1)
	{
		src_k = temp_key;
		dst_k = temp_sb;
		src_o = temp_offset;
		dst_o = temp_sb2;
	}
	else
	{
		dst_k = temp_key;
		src_k = temp_sb;
		dst_o = temp_offset;
		src_o = temp_sb2;
	}

	int i,l1,l2;
	l1 = loc;
	l2 = loc+h1;
	for (i=loc;i<loc+size;i++)
	{
		if (l1 < loc+h1 && (l2 == loc+size || src_k[l1] < src_k[l2]))
		{
			dst_k[i] = src_k[l1];
			dst_o[i] = src_o[l1++];
		}
		else
		{
			dst_k[i] = src_k[l2];
			dst_o[i] = src_o[l2++];
		}
	}
/*
	for (i=loc+1;i<loc+size;i++)
	{
		if (dst_k[i-1] > dst_k[i])
			printf("???\n");
	}
*/
//	return 1-rv;

}

int scan_node(Node_offset offset,unsigned char* key,int result_req,std::string* scan_result)
{
//fail:
//	unsigned char temp_buffer[NODE_BUFFER];
//	uint16_t temp_len[100],tl;

	uint64_t start_key = *(uint64_t*)key;


	unsigned char* buffer;
	unsigned char* buffer_end;

	uint16_t len,aligned_size;
	int i;

	Node* node_data;
	Node_meta* node_meta;

//	node_meta = offset_to_node(offset);
//	node_data = offset_to_node_data(offset);

	// here, we have ref and ve.node_offset and we should copy it and def it

//	sort_inv(node_meta->inv_cnt,node_meta->inv_kv);


//	Node d0[PART_MAX];
//	const int meta_size = (unsigned char*)temp_data[0].buffer-(unsigned char*)&temp_data[0];//24;
	const int meta_size = 16;
	tc = 0;
	tdc = 0;

	Node_offset_u node_offset,node_offset2;//,addtional_node;
	node_offset.no = offset;

	// prefetch failed
	//Node_offset node_offset = offset;
	//node_offset = offset;

//	while(node_offset.no_32)

	{
		node_offset2 = node_offset;
		while(node_offset2.no_32) // in group
		{
			node_meta = offset_to_node(node_offset2.no);

			if (node_meta->invalidated_size != node_meta->size_l)
			{

			node_data = offset_to_node_data(node_offset2.no);
			cp256((unsigned char*)&temp_data[tdc],(unsigned char*)node_data,node_meta->size_l + meta_size);
//			memcpy((unsigned char*)&temp_data[tdc],(unsigned char*)node_data,node_meta->size_l + meta_size);

//		memcpy((unsigned char*)&d0[part0],(unsigned char*)node_data,node_meta->size + meta_size);

			tdc++;
			}

			node_offset2.no_32 = node_meta->next_offset_ig;
		}
//		printf("scan node %d\n",tdc+1);
	}
	tdc = 0;

unsigned char* bug;
uint16_t ll_cnt;
	{
		node_offset2 = node_offset;
		while(node_offset2.no_32) // in group
		{
			node_meta = offset_to_node(node_offset2.no);

			if (node_meta->invalidated_size != node_meta->size_l)
			{

			buffer = temp_data[tdc].buffer;
			tdc++;
			ll_cnt = node_meta->size_r_cnt >> 16;
			for (i=0;i<ll_cnt;i++)
			{
				len = get_ll(node_meta->ll,i);
				aligned_size = (len & 0x7fff)+PH_LTK_SIZE;
				if (aligned_size % KV_ALIGN)
					aligned_size+=(KV_ALIGN-aligned_size%KV_ALIGN);
				if (len & 0x8000)
				{
					temp_offset[tc] = buffer;
					bug = (buffer+PH_LEN_SIZE+PH_TS_SIZE);
					temp_key[tc] = *(uint64_t*)bug;

//					temp_key[tc] = *(uint64_t*)((buffer+PH_LEN_SIZE+PH_TS_SIZE));
					tc++;
				}
				buffer+=aligned_size;
			}

			}

			node_offset2.no_32 = node_meta->next_offset_ig;
		}
	}
/*
	if (part0 > 4) //tset
	{
		printf("part 22 %d\n",part0);
		scanf("%d",&part0);
	}
*/

	int j,k;
	uint16_t size;
	uint64_t sk;
	unsigned char* so;
	int cnt0=0;

//	const int kls = key_size+len_size;
#if 0
		//sort node
		for (j=0;j<tc;j++)
		{
			for (k=j+1;k<tc;k++)
			{
				if (temp_key[j] > temp_key[k])
				{
					sk = temp_key[j];
					temp_key[j] = temp_key[k];
					temp_key[k] = sk;
					so = temp_offset[j];
					temp_offset[j] = temp_offset[k];
					temp_offset[k] = so;
				}
			}
		}
#endif

/*
		if (merge_sort(0,0,tc) == 0)
		{
			dst_k = temp_key;
			dst_o = temp_offset;
		}
		else
		{
			dst_k = temp_sb;
			dst_o = temp_sb2;
		}
*/
		merge_sort(0,0,tc);
/*
		for (i=1;i<tc;i++)
		{
			if (dst_k[i] < dst_k[i-1])
			{
				printf("se\n");
				scanf("%d");
			}
		}
*/
		for (i=0;i<tc;i++)
		{
			if (start_key > temp_key[i])
				continue;
			len = *(uint16_t*)temp_offset[i] & 0x7fff;
			scan_result[cnt0].assign((char*)(temp_offset[i]),PH_LTK_SIZE + len);
			cnt0++;
			if (result_req <= cnt0)
				break;
		}

//		printf("cnt0 %d\n",cnt0);
//cnt_sum+=cnt;
	return cnt0; //temp

}

#endif

}


