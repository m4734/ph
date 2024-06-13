//#include "log.h"
//#include "global2.h"

#include <cstdint>
#include <atomic>
#include <string>

//#include "skiplist.h"
#include "shared.h"

namespace PH
{

#define QUERY_THREAD_MAX 100
#define EVICT_THREAD_MAX 100
//thread_local Thread my_thread;

class DoubleLog;
class SkiplistNode;
class ListNode;

//class Skiplist;
//class PH_List;

class PH_Thread
{
	public:
	PH_Thread() : lock(0),read_lock(0),run(0),exit(0),op_cnt(0),update_request(0) {}

	void update_free_cnt();
	void update_tail_sum();
	void op_check();
	void sync_thread();

	std::atomic<uint8_t> lock; // thread alloc lock
	volatile uint8_t read_lock; // cceh read lock
	volatile uint8_t run;
	volatile uint8_t exit;
//	volatile size_t local_seg_free_head;
	size_t op_cnt;
	int update_request;

	size_t recent_log_tails[64];

	unsigned char padding[64]; // for cache line
};

class PH_Query_Thread : public PH_Thread
{
	private:
	DoubleLog* my_log;

	public:
	void init();
	void clean();

int insert_op(uint64_t key,unsigned char* value);
int read_op(uint64_t key,unsigned char* buf,std::string *value);
//int read_op(uint64_t key,std::string *value);
int delete_op(uint64_t key);
int scan_op(uint64_t start_key,uint64_t end_key);
int next_op(unsigned char* buf);

};

class PH_Evict_Thread : public PH_Thread
{
	private:

	int evict_log(DoubleLog* dl);
	int try_soft_evict(DoubleLog* dl);
	int try_hard_evict(DoubleLog* dl);
	int try_push(DoubleLog* dl);
	void hot_to_warm(SkiplistNode* node,bool force);
	void warm_to_cold(SkiplistNode* node);
	void split_listNode(ListNode* listNode,SkiplistNode* skiplistNode);

	DoubleLog** log_list;
	int log_cnt;

	DataNode temp_node;

	size_t sleep_time;

	public:
	void init();
	void clean();
	void evict_loop();

//	Skiplist* skiplist;
//	PH_List* list;

};

//unsigned int min_seg_free_cnt();
size_t get_min_tail(int log_num);

}
