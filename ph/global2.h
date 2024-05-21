
#ifndef GLOBAL2
#define GLOBAL2

#include <cstdint>
#include <pthread.h>

//#include "skiplist.h"
//#include "thread2.h"

namespace PH
{
 // 2 // 10 // 52
const size_t VALUE_FIRST_MASK = 0xc000000000000000; // 11000000 ...
const int VALUE_FIRST_SHIFT = 62;
const size_t VALUE_SECOND_MASK = 0x3ff0000000000000; // 001111 ... 00
const int VALUE_SECOND_SHIFT = 52;
const size_t VALUE_THIRD_MASK = 0x000fffffffffffff;

class PH_Interface
{

private:

// moved to global but not good
/*
int num_thread;
int num_pmem;
int num_log;
*/


//thread_local PH_Query_Thread my_thread;
void new_query_thread();
void clean_query_thread();

void new_evict_thread();
void clean_evict_thread();

void exit_threads();

pthread_t evict_pthreads[100];

//void *run_evict(void* p);

public:
void global_init(int num_thread,int num_pmem,int num_evict);
void global_clean();

//--------------------------------------------------------------
int insert_op(uint64_t key,unsigned char* value);
int read_op(uint64_t key,unsigned char* buf);
int delete_op(uint64_t key);
int scan_op(uint64_t start_key,uint64_t end_key);
int next_op(unsigned char* buf);

int end_op();

void run_evict_direct();
void run_evict_thread();

//Skiplist* skiplist;
//PH_List* list;

};

}

#endif
