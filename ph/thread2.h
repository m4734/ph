//#include "log.h"

#include <cstdint>
#include <atomic>

namespace PH
{

#define QUERY_THREAD_MAX 100
//thread_local Thread my_thread;

class DoubleLog;

class PH_Query_Thread
{
	private:
	DoubleLog* my_log;

	void update_free_cnt();
	int op_cnt;


	public:
	void init();
	void clean();

int insert_op(uint64_t key,unsigned char* value);
int read_op(uint64_t key,unsigned char* buf);
int delete_op(uint64_t key);
int scan_op(uint64_t start_key,uint64_t end_key);
int next_op(unsigned char* buf);

int local_seg_free_cnt=0;
int run = 0;
std::atomic<uint8_t> lock=0;

//	std::atomic<uint8_t> read_lock=0;
	volatile uint8_t read_lock = 0;

	unsigned char padding[64];
};

unsigned int min_seg_free_cnt();

}
