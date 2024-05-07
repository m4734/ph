#include "log.h"

namespace PH
{

//thread_local Thread my_thread;

class Thread
{
	private:
	DoubleLog* my_log;

	public:
	void init();
	void clean();

	void insert_op();
	void read_op();
	void delete_op();
	void scan_op();
};

}
