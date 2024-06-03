
#ifndef PH_LOG
#define PH_LOG


#include <atomic>

#include "global2.h"
//#include "data2.h"

namespace PH
{

#define USE_DRAM_CACHE

//#define LOG_SIZE_PER_PMEM (12*1024*1024*1024) // 128GB / 10% / threads
const size_t LOG_SIZE_PER_PMEM = (size_t(12)*1024*1024*1024);
//const size_t DRAM_LIST_UNIT = 1024*1024;
//const size_t LIST_POOL_UNIT = 1024;
//#define LOG_BLOCK_SIZE 4096

//const size_t HARD_EVICT_SPACE = LOG_SIZE_PER_PMEM/20; // 5% // 300MB / 3GB
//const size_t SOFT_EVICT_SPACE = LOG_SIZE_PER_PMEM/10; // 5% // 600MB / 3GB


const size_t header_size = sizeof(uint64_t);


void init_log(int num_pmem,int num_log);
void clean_log();
/*
struct Dram_List
{
	struct Dram_List* prev=NULL;
	struct Dram_List* next=NULL;
	BaseLogEntry ble;
};
*/

class PH_Thread;

class DoubleLog
{


	public: // lazy

//	size_t log_size;
	size_t my_size;
	size_t soft_adv_offset;
//	size_t head_offset,tail_offset;

	unsigned char* pmemLogAddr;
//	unsigned char* head_p;
//	unsigned char* tail_p;
//	unsigned char* end_p;

	unsigned char* dramLogAddr;
//	unsigned char* dram_head_p;


//	volatile size_t head_offset;
//	volatile size_t tail_offset;

	volatile size_t head_sum;
	volatile size_t tail_sum;

//	size_t end_offset;

	size_t min_tail_sum;

#if 0
	Dram_List** dram_list_pool = NULL; //1024*1024
	int dram_list_pool_max;
	int dram_list_pool_cnt;
	int dram_list_pool_alloced;
	Dram_List* free_dram_list_head = NULL;
//	int free_dram_list_max;
//	int free_dram_list_cnt;

//	Dram_List* dram_list_head;
//	Dram_List* dram_list_tail;

//	Dram_List* alloc_new_dram_list();
//	void alloc_new_dram_pool();
#endif
	public:
	void init(char* filePath,size_t size);
	void clean();
//	void insert_log(unsigned char* addr, int len);
	void ready_log();
//	void check_turn(size_t &sum, size_t len);

//	void insert_log(struct BaseLogEntry *baseLogEntry_p);
	void insert_pmem_log(uint64_t key,unsigned char* value);
	void insert_dram_log(uint64_t version, uint64_t key,unsigned char* value);
	void write_version(uint64_t version);

//	inline unsigned char* get_pmem_head_p() { return pmemLogAddr+head_offset; }
//	inline unsigned char* get_dram_head_p() { return dramLogAddr+head_offset; }
//	inline size_t get_empty_space() { return (tail_offset+my_size-head_offset) % my_size; }
//	inline unsigned char* get_dram_tail_p() { return dramLogAddr+tail_offset; }

//	inline size_t get_head_sum() { return head_sum; }
//	inline size_t get_tail_sum() { return tail_sum; }
#if 0
	Dram_List* append_new_dram_list(uint64_t version,uint64_t key,unsigned char* value);
	void remove_dram_list(Dram_List* dl);
#endif
	int log_num;

	std::atomic<uint8_t> use=0;
	std::atomic<uint8_t> evict_alloc=0;
};

extern DoubleLog* doubleLogList;

inline unsigned char* value_to_log_addr(uint64_t value)
{
	return doubleLogList[(value & VALUE_SECOND_MASK)>>VALUE_SECOND_SHIFT].dramLogAddr + (value & VALUE_THIRD_MASK);
}

inline uint64_t log_addr_to_value(int log_num,size_t offset)
{
	return size_t(1) << VALUE_FIRST_SHIFT + size_t(log_num) << VALUE_SECOND_SHIFT + offset;
}


}

#endif
