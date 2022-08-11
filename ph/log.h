#include <atomic>

#include "global.h"

namespace PH
{
//std::atomic<uint32_t> global_ts=0;
extern std::atomic<uint8_t> log_file_cnt;

void init_log();
void clean_log();

struct LogOffset
{
//	uint16_t file;
	unsigned int file;
	unsigned int offset;
};

class LOG
{
	int file_max;
	int file_index;
//	Node_offset kv_in;
//	Node_offset kv_out;
	LogOffset kv_in;
	LogOffset kv_out;
//	unsigned char* dram_array[100];
	int dram_num[100];
	unsigned char* pmem_array[100];
	// use pmem_addr

	public:
	void init();
	void clean();
	void insert_log(Node_offset node_offset,unsigned char* &key_p, unsigned char* &value_p);
	void insert_log(Node_offset node_offset,unsigned char* &key_p, unsigned char* &value_p,int value_len);
	void ready(int value_len);
	private:
	void new_log_file();
//	void flush(unsigned char* kvp);
};
}
