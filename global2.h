
#ifndef GLOBAL2
#define GLOBAL2

#include <cstdint>
#include <pthread.h>
#include <string>

//#include "skiplist.h"
//#include "thread2.h"

namespace PH
{

	/*
#define VALUE_SIZE 100
#define KEY_RANGE (size_t(100)*1000*1000)
#define TOTAL_OPS (size_t(1000)*1000*1000)
#define ENTRY_SIZE (8+8+VALUE_SIZE)
#define TOTAL_DATA_SIZE (ENTRY_SIZE*KEY_RANGE)
	 */
	void recover_counter(uint64_t key,uint64_t version);

	extern size_t VALUE_SIZE0;

	//#define VALUE_SIZE 100

	extern size_t KEY_RANGE;
	//size_t TOTAL_OPS;
	extern size_t ENTRY_SIZE;
	extern size_t LOG_ENTRY_SIZE;
	extern size_t TOTAL_DATA_SIZE;

	const int COUNTER_MAX = 1024;

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

			void init_threads();
			void exit_threads();

			pthread_t evict_pthreads[100];

			//void *run_evict(void* p);

		public:
			void global_init(size_t VS,size_t KR,int num_thread,int num_pmem,int num_evict, int recover);
			void global_clean();

			//--------------------------------------------------------------
			int insert_op(uint64_t key,unsigned char* value);
			int read_op(uint64_t key,unsigned char* buf);
			int read_op(uint64_t key,std::string *value);
			int delete_op(uint64_t key);
			int scan_op(uint64_t start_key,uint64_t end_key);
			int next_op(unsigned char* buf);

			int end_op();

			void run_evict_direct();
			void run_evict_thread();

			void global_reset_test();

			//Skiplist* skiplist;
			//PH_List* list;

	};

}

#endif
