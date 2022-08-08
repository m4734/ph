#include <pthread.h>

#define idle_thread

namespace PH
{

struct PH_Thread
{
	pthread_t tid;
	unsigned int free_cnt;
	unsigned int seg_free_cnt;

#ifdef idle_thread
//	volatile int running; // overhead??
	int running;
#endif
};

extern thread_local PH_Thread* my_thread;
//thread_local unsigned int op_cnt = 0;

void reset_thread();
void exit_thread();
void update_free_cnt();
unsigned int min_free_cnt();
unsigned int min_seg_free_cnt();
void init_thread();
void clean_thread();

void print_thread_info();
int check_slow();

void update_idle();

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
