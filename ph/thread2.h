//#include "log.h"

#include <cstdint>

namespace PH
{

//thread_local Thread my_thread;

class DoubleLog;

class PH_Query_Thread
{
	private:
	DoubleLog* my_log;

	public:
	void init();
	void clean();

int insert_op(uint64_t key,unsigned char* value);
int read_op(uint64_t key,unsigned char* buf);
int delete_op(uint64_t key);
int scan_op(uint64_t start_key,uint64_t end_key);
int next_op(unsigned char* buf);


};

}
