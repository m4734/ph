#include <pthread.h>
#include "log.h"

#define idle_thread

namespace PH
{

#define FREE_SEG_LEN 100000
#define FREE_QUEUE_LEN 100000

class PH_Thread
{
	public:
	volatile unsigned int local_free_cnt[PM_N];
	volatile unsigned int local_free_index[PM_N];

	volatile unsigned int local_seg_free_cnt;

//	pthread_t tid;

#ifdef idle_thread
//	volatile int running; // overhead??
	int running;
#endif
	unsigned int op_cnt;

	void init();
	void clean();
};

extern thread_local PH_Thread* my_thread;
//thread_local unsigned int op_cnt = 0;

void reset_thread();
void exit_thread();
void update_free_cnt();
unsigned int get_min_free_cnt(int part);
unsigned int get_min_free_index(int part);
unsigned int min_seg_free_cnt();
void init_thread();
void clean_thread();

void print_thread_info();
//int check_slow();

void update_idle();

void at_update_free();

//inline void thread_run();
//inline void thread_idle();
/*
inline void thread_run()
{
#ifdef idle_thread
	my_thread->running = 1;
#endif
}
inline void thread_idle()
{
#ifdef idle_thread
	my_thread->running = 0;
#endif
}
*/
}
