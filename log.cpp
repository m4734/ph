#include <libpmem.h>
#include <string.h>
#include <x86intrin.h>
#include <stdio.h> // test
#include <sys/mman.h> // mmap
#include <unistd.h> //usleep

#include "log.h"
#include "data2.h"
#include "thread2.h"
#include "global2.h"
#include "cceh.h"
#include "skiplist.h"
//#include "recovery.h"

//#include "hash.h"

//using namespace PH;
namespace PH
{

//extern void recover_counter(uint64_t key,uint64_t version);

extern CCEH* hash_index;
extern Skiplist* skiplist;

//const size_t ble_len = sizeof(BaseLogEntry);

size_t log_size;
int log_max;
DoubleLog* doubleLogList; // should be private

//DoubleLog* WDLL;

size_t HARD_EVICT_SPACE;
size_t SOFT_EVICT_SPACE;

extern thread_local PH_Thread* my_thread;

//#define INTERLEAVE

void recover_log()
{
	int i;
	for (i=0;i<log_max;i++)
	{
		doubleLogList[i].recover();
	}
}

void init_log(int num_pmem, int num_log)
{

	printf("init log\n");

//	log_max = num_pmem*num_log;
	log_max = num_log;

	doubleLogList = new DoubleLog[log_max];

//	log_size = LOG_SIZE_PER_PMEM/size_t(num_log);
	log_size = TOTAL_DATA_SIZE/10/(log_max); // TOTAL DATA SIZE / HOT RATIO / LOG NUM
	log_size/=1024;
	log_size*=1024;

#if 0
	if (log_size < 1024*1024*1024) // minimum
		log_size = 1024*1024*1024;
#endif

#if 1
	HARD_EVICT_SPACE = log_size/20; // 5% 5%
//	HARD_EVICT_SPACE = log_size/100; // 5% 1%
	SOFT_EVICT_SPACE = log_size/2; // 10% 50%
//	SOFT_EVICT_SPACE = log_size/10; // 10% 50%
#else
	HARD_EVICT_SPACE = (ENTRY_SIZE * 1000 * 50) * 2; // 5MB
	SOFT_EVICT_SPACE = (ENTRY_SIZE * 1000 * 100) * 2; // (10MB) * x
#endif
	printf("HARD EVICT SPACE %lu\n",HARD_EVICT_SPACE);
	printf("SOFT EVICT SPACE %lu\n",SOFT_EVICT_SPACE);

	if (SOFT_EVICT_SPACE > log_size)
		printf("SOFT EVICT TOO BIG\n");

#if 0
	if (SOFT_EVICT_SPACE < 1024*1024*1024)
	{
		SOFT_EVICT_SPACE = 1024*1024*1024;
		if (SOFT_EVICT_SPACE > log_size)
			SOFT_EVICT_SPACE = log_size;

	}
#endif
//	printf("LOG NUM %d LOG SIZE %lfGB SUM %lfGB\n",num_log,double(log_size)/1024/1024/1024,double(log_size)*num_log*num_pmem/1024/1024/1024);

	int i,j,cnt=0;
	for (i=0;i<num_log;i++)
	{
//		for (j=0;j<num_pmem;j++) // inner loop pmem
		{
			j = i % num_pmem;
			char path[100];
			int len;
#ifdef INTERLEAVE
			sprintf(path,"/mnt/pmem0/log%d",cnt);
#else
			sprintf(path,"/mnt/pmem%d/log%d",j+1,i+1); // 1~
#endif
			len = strlen(path);
			path[len] = 0;
			doubleLogList[cnt].log_num = cnt;
			doubleLogList[cnt++].init(path,log_size,HARD_EVICT_SPACE,SOFT_EVICT_SPACE);
		}
	}
}

void clean_log()
{

	printf("clean log\n");
	printf("LOG MAX %d LOG SIZE %lfGB SUM %lfGB\n",log_max,double(log_size)/1024/1024/1024,double(log_size)*log_max/1024/1024/1024);

	int i;
	for (i=0;i<log_max;i++)
	{
		doubleLogList[i].clean();
	}

	delete[] doubleLogList; // array

	printf("clean now\n");
}

#define SKIP_MEMSET

void DoubleLog::init(char* filePath, size_t req_size,size_t hes,size_t ses)
{
//	tail_offset = LOG_BLOCK_SIZE;
//	head_offset = LOG_BLOCK_SIZE;

//	printf("size %lu\n",req_size);

	int is_pmem;
	pmemLogAddr = (unsigned char*)pmem_map_file(filePath,req_size,PMEM_FILE_CREATE,0777,&my_size,&is_pmem);
	if (my_size != req_size)
		printf("my_size %lu is not req_size %lu\n",my_size,req_size);
	if (is_pmem == 0)
		printf("is not pmem\n");
#if 0
#ifdef SKIP_MEMSET
	printf("----------------skip memset----------------------\n");
#else
	memset(pmemLogAddr,0,my_size);
#endif
#endif

#if 0
	head_p = pmemLogAddr;
	tail_p = pmemLogAddr;
	end_p = pmemLogAddr + my_size;

	dram_list_pool_max = LIST_POOL_UNIT;
	dram_list_pool = (Dram_List**)malloc(sizeof(Dram_List*)*dram_list_pool_max);
	dram_list_pool_cnt = 0;
	alloc_new_dram_pool();
//	dram_list_pool_alloced = 0;

	free_dram_list_head = NULL;
	dram_list_head = dram_list_tail = NULL;
#endif

	head_sum = tail_sum = 0;

#ifdef USE_DRAM_CACHE
	dramLogAddr = (unsigned char*)mmap(NULL,req_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);
	if (!dramLogAddr)
		printf("dram mmap error\n");
#endif


	use = 0;
	evict_alloc = 0;
	min_tail_sum = 0;

	soft_adv_offset = 0;

	hard_evict_space = hes;
	soft_evict_space = ses;

	block_cnt = 0;
}

void DoubleLog::recover() // should be last...
{
#if 0
	memcpy(dramLogAddr,pmemLogAddr,my_size);

	size_t offset;
	uint64_t key,v1,v2;
	EntryHeader* header1;
	EntryHeader* header2;
	offset = 0;
	unsigned char* addr = dramLogAddr;

	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;
	EntryAddr ea1,ea2;

	ea1.loc = HOT_LOG;
	ea1.file_num = log_num;

	bool update;

	SkiplistNode* skiplistNode;
	SkipAddr prev_sa_list[MAX_LEVEL+1];
	SkipAddr next_sa_list[MAX_LEVEL+1];

	while(offset < my_size)
	{
//do 
		header1 = (EntryHeader*)addr;

		if (header1->version > 0)
		{
			update = true;

			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

//			if (key == 15307025541213771458UL)
//				debug_error("hot key error here\n");

			kvp_p = hash_index->insert(key,&seg_lock,my_thread->read_lock);
			v1 = header1->version;
			if (kvp_p->key == key)
			{
				ea2.value = kvp_p->value;
				header2 = (EntryHeader*)get_entry(ea2);
				v2 = header2->version;
				if (v1 < v2)
				{
					update = false;
					header1->valid = false;// if it is on HOTLOG
				}
				else
					invalidate_entry(ea2);
			}
			else
				ea2.loc = NONE;

			if (update)
			{
				recover_counter(key,v1);
				kvp_p->key = key;
				ea1.offset = offset;
				kvp_p->value = ea1.value;
				if (ea2.loc != HOT_LOG) // USE WARM CACHE
				{
					skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
					skiplistNode->key_list[skiplistNode->key_list_size++] = key;
				}
			}

			hash_index->unlock_entry2(seg_lock,my_thread->read_lock);

		}


		addr += LOG_ENTRY_SIZE;
		offset += LOG_ENTRY_SIZE;
	}

	head_sum = my_size-(my_size%LOG_ENTRY_SIZE);
	tail_sum = 0;
#endif
}

void DoubleLog::clean()
{
//	printf("%lu %lu\n",my_size,log_size);
#if 0
	int i;
	for (i=0;i<dram_list_pool_cnt;i++)
		free(dram_list_pool[i]);
	free(dram_list_pool);
#endif
//printf(" my size %lu\n",my_size);
#ifdef USE_DRAM_CACHE
	munmap(dramLogAddr,my_size);
#endif
	pmem_unmap(pmemLogAddr,my_size);
}

void DoubleLog::log_check()
{
	if (head_sum < tail_sum)
		debug_error("xxx\n");
}

void DoubleLog::ready_log(uint64_t value_size8)
{
	size_t offset = head_sum % my_size;
	uint32_t required_size;

	required_size = LOG_ENTRY_SIZE_WITHOUT_VALUE + value_size8 + JUMP_SIZE;
//	if (offset+required_size >= my_size) // check the space
	if (offset+NODE_SIZE > my_size)
		head_sum+=(my_size-offset);

//	if (tail_sum + my_size < head_sum + ENTRY_SIZE)
//		block_cnt++;
	while(tail_sum + my_size < head_sum + required_size)
	{
		usleep(1);// sleep
//		asm("nop");
	}

}
void DoubleLog::insert_pmem_log(uint64_t key,uint64_t value_size, unsigned char *value)
{
	// use checksum or write twice

	EntryHeader jump;
	jump.valid_bit = 0;
	jump.delete_bit = 0;
	jump.version = tail_sum;

	uint64_t value_size8 = get_v8(value_size);

	// 1 write kv
	unsigned char* head_p = pmemLogAddr + head_sum%my_size;
	memcpy(head_p+ENTRY_HEADER_SIZE, &key, KEY_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE, &value_size, SIZE_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE, value, value_size);
// dont write warm cache
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE, &jump, JUMP_SIZE);

	pmem_persist(head_p+ENTRY_HEADER_SIZE,KEY_SIZE+SIZE_SIZE+value_size+WARM_CACHE_SIZE+JUMP_SIZE);
	_mm_sfence();
}
#if 0
const size_t CACHE_MASK = 0xffffffffffffffc0; // 64 // 1111...11000000
void clwb(unsigned char* addr,size_t len)
{
	int i;
	unsigned char* start = (addr & CACHE_MASK);
	if (len % 64 > 0)
		len+=64;
	for (i=0;i<len;i+=64)
		_mm_clwb((void*)(start+i));
}
#endif
void DoubleLog::insert_dram_log(uint64_t version, uint64_t key, uint64_t value_size, unsigned char *value)
{
	printf("not here\n");
	// use checksum or write twice
	EntryHeader jump;
	jump.valid_bit = 0;
	jump.delete_bit = 0;
	jump.version = tail_sum;

	uint64_t value_size8 = get_v8(value_size);

	// 1 write kv
	//memcpy(head_p+header_size ,src+header_size ,ble_len-header_size);
	unsigned char* head_p = dramLogAddr + head_sum%my_size;
	memcpy(head_p,&version,ENTRY_HEADER_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE, &key, KEY_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE, &value_size, SIZE_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE, value, value_size);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8, &jump, JUMP_SIZE);

//	_mm_sfence();
}

void DoubleLog::insert_dram_log(uint64_t version, uint64_t key, uint64_t value_size, unsigned char *value,NodeAddr* warm_cache)
{
	// use checksum or write twice
	EntryHeader jump;
	jump.valid_bit = 0;
	jump.delete_bit = 0;
	jump.version = tail_sum;

	// 1 write kv
	uint64_t value_size8 = get_v8(value_size);

	unsigned char* head_p = dramLogAddr + head_sum%my_size;
	memcpy(head_p,&version,ENTRY_HEADER_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE, &key, KEY_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE, &value_size, SIZE_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE, value, value_size);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8, warm_cache, WARM_CACHE_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE, &jump, JUMP_SIZE);

//	_mm_sfence();
}

void DoubleLog::copy_to_pmem_log(uint64_t value_size)
{
	unsigned char* pmem_head_p = pmemLogAddr + head_sum%my_size;
	unsigned char* dram_head_p = dramLogAddr + head_sum%my_size;
	uint64_t value_size8 = get_v8(value_size);

	memcpy(pmem_head_p+ENTRY_HEADER_SIZE,dram_head_p+ENTRY_HEADER_SIZE,KEY_SIZE + SIZE_SIZE + value_size8 + WARM_CACHE_SIZE + JUMP_SIZE);

	pmem_persist(pmem_head_p+ENTRY_HEADER_SIZE,KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE+JUMP_SIZE);
	_mm_sfence();

}

void DoubleLog::write_version(uint64_t version)
{
	memcpy(pmemLogAddr+head_sum%my_size ,&version, ENTRY_HEADER_SIZE);
	pmem_persist(pmemLogAddr + head_sum%my_size , ENTRY_HEADER_SIZE);
	_mm_sfence();
}

}

