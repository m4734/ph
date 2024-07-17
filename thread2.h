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

#define WARM_COLD_RATIO (10/2)

class DoubleLog;
class SkiplistNode;
class ListNode;
struct KVP;

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
	int thread_id;

	size_t recent_log_tails[64]; // remove this
	void* temp_seg;

	unsigned char padding[64]; // for cache line

	//check
	uint64_t log_write_cnt;
	uint64_t hot_to_warm_cnt;
	uint64_t warm_to_cold_cnt;
	uint64_t direct_to_cold_cnt;
	uint64_t hot_to_hot_cnt;
	uint64_t hot_to_cold_cnt;

	uint64_t soft_htw_cnt;
	uint64_t hard_htw_cnt;

	void reset_test();

	protected:
	void split_listNode_group(ListNode* listNode,SkiplistNode* skiplistNode);

	unsigned int seed_for_dtc;
};

class PH_Query_Thread : public PH_Thread
{
	private:
	DoubleLog* my_log;

	EntryAddr direct_to_cold(uint64_t key,unsigned char* value,KVP &kvp,std::atomic<uint8_t>* &seg_lock);
	void invalidate_entry(EntryAddr &ea);

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
	int test_inv_log(DoubleLog* dl);

	void hot_to_warm(SkiplistNode* node,bool force);
	void warm_to_cold(SkiplistNode* node);
//	bool try_evict_to_listNode(ListNode* listNode,uint64_t key,unsigned char* addr);
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
