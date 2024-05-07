#include <atomic>

#include "global2.h"
#include "data2.h"

namespace PH
{

//#define LOG_SIZE_PER_PMEM (12*1024*1024*1024) // 128GB / 10% / threads
const size_t LOG_SIZE_PER_PMEM = (size_t(12)*1024*1024*1024);
//#define LOG_BLOCK_SIZE 4096

void init_log(int num_pmem,int num_log);
void clean_log();

class DoubleLog
{
//	size_t log_size;
	size_t my_size;
//	size_t head_offset,tail_offset;

	unsigned char* pmemLogAddr;
	unsigned char* head_p;
	unsigned char* tail_p;
	unsigned char* end_p;

	public:
	void init(char* filePath,size_t size);
	void clean();
//	void insert_log(unsigned char* addr, int len);
	void insert_log(struct BaseLogEntry *baseLogEntry_p);
};
}
