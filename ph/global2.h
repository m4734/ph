
#ifndef GLOBAL2
#define GLOBAL2

#include <cstdint>

namespace PH
{

class PH_Interface
{

private:
int num_thread;
int num_pmem;
int num_log;

public:
void global_init(int num_thread,int num_pmem,int num_log);
void global_clean();

//--------------------------------------------------------------
int insert_op(uint64_t key,unsigned char* value);
int read_op(uint64_t key,unsigned char* buf);
int delete_op(uint64_t key);
int scan_op(uint64_t start_key,uint64_t end_key);
int next_op(unsigned char* buf);

};

}

#endif
