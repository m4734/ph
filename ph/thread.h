#include <threads.h>

struct PH_Thread
{
	thrd_t tid;
	unsigned int free_cnt;
};

void update_free_cnt();
unsigned int min_free_cnt();
void init_thread();
void clean_thread();
