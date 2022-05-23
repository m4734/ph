#include <pthread.h>

namespace PH
{

struct PH_Thread
{
	pthread_t tid;
	unsigned int free_cnt;
};

void reset_thread();
void exit_thread();
void update_free_cnt();
unsigned int min_free_cnt();
void init_thread();
void clean_thread();

}
