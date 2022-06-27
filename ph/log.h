

// hash lock
// global timestamp
// dram write
// pmem write
// hash update


namespace PH
{

class LOG
{
	int num=0;

	unsigned char* pmem_log_addr;
	unsigned char* dram_log_addr;

	unsigned int log_start;
	unsigned int log_end;
	unsigned int log_max;
	unsigned int log_flush;


	public:
	int init_log();
	void clean_log();
	void set_num(int i);
	unsigned char* write_log(unsigned char* key_p,unsigned char* value_p,int value_len);
};
}
