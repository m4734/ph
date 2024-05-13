
#ifndef PH_LOG
#define PH_LOG


#include <atomic>

#include "global2.h"
#include "data2.h"

namespace PH
{

//#define LOG_SIZE_PER_PMEM (12*1024*1024*1024) // 128GB / 10% / threads
const size_t LOG_SIZE_PER_PMEM = (size_t(12)*1024*1024*1024);
const size_t DRAM_LIST_UNIT = 1024*1024;
const size_t LIST_POOL_UNIT = 1024;
//#define LOG_BLOCK_SIZE 4096

void init_log(int num_pmem,int num_log);
void clean_log();

struct Dram_List
{
	struct Dram_List* prev=NULL;
	struct Dram_List* next=NULL;
	BaseLogEntry ble;
};

class DoubleLog
{
//	size_t log_size;
	size_t my_size;
//	size_t head_offset,tail_offset;

	unsigned char* pmemLogAddr;
	unsigned char* head_p;
	unsigned char* tail_p;
	unsigned char* end_p;

	Dram_List** dram_list_pool = NULL; //1024*1024
	int dram_list_pool_max;
	int dram_list_pool_cnt;
	int dram_list_pool_alloced;
	Dram_List* free_dram_list_head = NULL;
//	int free_dram_list_max;
//	int free_dram_list_cnt;

	Dram_List* dram_list_head;
	Dram_List* dram_list_tail;

	Dram_List* alloc_new_dram_list();
	void alloc_new_dram_pool();

	public:
	void init(char* filePath,size_t size);
	void clean();
//	void insert_log(unsigned char* addr, int len);
	void ready_log();
	void insert_log(struct BaseLogEntry *baseLogEntry_p);
	void insert_log(uint64_t key,unsigned char* value);
	void write_version(uint64_t version);

	inline unsigned char* get_head_p() { return head_p; }

	Dram_List* append_new_dram_list(uint64_t version,uint64_t key,unsigned char* value);
	void remove_dram_list(Dram_List* dl);

	std::atomic<uint8_t> use=0; 
};
}

#endif
