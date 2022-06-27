#include <pthread.h>

#include "log.h"

namespace PH
{

struct PH_Thread
{
	public:
	pthread_t tid;
	unsigned int free_cnt;
	unsigned int seg_free_cnt;
//	unsigned char* pmem_log_addr;
//	unsigned char* dram_log_addr;

	LOG* log;

//	void init_log();
};

void reset_thread();
void exit_thread();
void update_free_cnt();
unsigned int min_free_cnt();
unsigned int min_seg_free_cnt();
void init_thread();
void clean_thread();

void print_thread_info();
int check_slow();

}
