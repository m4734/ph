#include <atomic>

#include "global.h"

namespace PH
{
std::atomic<uint32_t> global_ts=0;
std::atomic<uint8_t> log_file_cnt=0;

void init_log();
void clean_log();

class LOG
{
	int file_max;
	int file_index;
	ValueEntry kv_in_offset;
	ValueEntry kv_out_offset;
	unsigned char* dram_array[100];
	unsigned char* pmem_array[100];
	// use pmem_addr

	public:
	void init();
	void clean();
	void insert_log(unsigned char* &key_p, unsigned char* &value_p);
	private:
	void new_log_file();
};
}
