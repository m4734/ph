//#include "log.h"
//#include "global2.h"

#include <cstdint>
#include <atomic>
#include <string>
#include <vector>

//#include "skiplist.h"
#include "shared.h"

namespace PH
{

#define QUERY_THREAD_MAX 100
#define EVICT_THREAD_MAX 100
	//thread_local Thread my_thread;

	//#define WARM_COLD_MAX_RATIO (20)


	// t = (M + m) / 2
	// M = 2m

	// we need 4/3 t //// t == 10 -> 2x = 40/3...
	const int WARM_COLD_MAX_RATIO = 14; // split when bigger than  // about 10%
//	const int WARM_COLD_MAX_RATIO = 20; // split when bigger than 
//	const int WARM_COLD_MAX_RATIO = 10; // split when bigger than 
	const int WARM_COLD_MIN_RATIO = 10; // merge when smaller than (after merge smaller than )

	class DoubleLog;
	class SkiplistNode;
	class ListNode;
	struct NodeMeta;
	struct KVP;

	//class Skiplist;
	//class PH_List;

	struct SecondOfPair
	{
		unsigned char* addr;
		EntryAddr ea;
		bool operator<(const SecondOfPair &sop) const
		{
			return addr<sop.addr;
		}
		bool operator==(const SecondOfPair &sop) const
		{
			return addr == sop.addr;
		}
	};

	class PH_Thread
	{
		protected:
			void buffer_init();
			void buffer_clean();

			void hot_to_warm(SkiplistNode* node,bool force);
			void warm_to_cold(SkiplistNode* node);
			//	bool try_evict_to_listNode(ListNode* listNode,uint64_t key,unsigned char* addr);
			void split_listNode(ListNode* listNode,SkiplistNode* skiplistNode);
			void split_warm_node(SkiplistNode* old_skipListNode, ListNode* half_listNode);
			void split_empty_warm_node(SkiplistNode* old_skiplistNode);
			int may_split_warm_node(SkiplistNode* node);
			void flush_warm_node(SkiplistNode* node);
			void try_cold_split(ListNode* listNode,SkiplistNode* node);
			//	void try_reduce_group(ListNode* listNode);

			//	void invalidate_entry(EntryAddr &ea);

			void check_end();
			//	NodeAddr get_warm_cache(EntryAddr ea);

			unsigned char *evict_buffer;//[WARM_BATCH_MAX_SIZE];
			unsigned char *entry_buffer;

			DataNode* sorted_buffer1;
			DataNode* sorted_buffer2;

			std::vector<std::pair<uint64_t,SecondOfPair>> split_key_list;
			std::vector<uint64_t> sorted_entry_size;



		public:
			PH_Thread();
			~PH_Thread();


			DataNode* split_buffer;

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
			uint64_t warm_log_write_cnt;
			uint64_t log_write_cnt;
			uint64_t hot_to_warm_cnt;
			uint64_t warm_to_warm_cnt;
			uint64_t warm_to_cold_cnt;
			uint64_t direct_to_cold_cnt;
			uint64_t hot_to_hot_cnt;
			uint64_t hot_to_cold_cnt;

			uint64_t soft_htw_cnt;
			uint64_t hard_htw_cnt;

			uint64_t htw_time,htw_cnt,wtc_time,wtc_cnt;
			uint64_t dtc_time;

			uint64_t reduce_group_cnt;
			uint64_t list_merge_cnt;

#ifdef WARM_STAT
			uint64_t warm_hit_cnt;
			uint64_t warm_miss_cnt;
			uint64_t warm_no_cnt;
#endif

#ifdef SCAN_TIME
			size_t main_time_sum;
			size_t first_time_sum;
			size_t second_time_sum;
			size_t third_time_sum;
			size_t t25_sum;
			size_t etc_time_sum;
#endif
			void reset_test();

		protected:
			void split_listNode_group(ListNode* listNode,SkiplistNode* skiplistNode);

			EntryAddr direct_to_cold(uint64_t key, uint64_t value_size,unsigned char* value,KVP &kvp,std::atomic<uint8_t>* &seg_lock, SkiplistNode* skiplist_from_warm, bool new_update);
			EntryAddr insert_to_cold(SkiplistNode* skiplistNode,unsigned char* src_addr, uint64_t key, int value_size8, std::atomic<uint8_t>* &seg_lock, EntryAddr old_ea); // have skiplist lock // return old ea // need invalidation and kv unlock

			//	void invalidate_entry(EntryAddr &ea);

			unsigned int seed_for_dtc;

			SkipAddr prev_sa_list[MAX_LEVEL+1];
			SkipAddr next_sa_list[MAX_LEVEL+1];

			int reset_test_cnt = 0;
	};

	const int DEFAULT_SCAN_POOL_SIZE = 1024*1024;

	class Scan_Result // signle thread
	{
		public:
			Scan_Result()
			{
				resultPool = 1;
				resultOffset = 0;
				pool_list.push_back((unsigned char*)malloc(DEFAULT_SCAN_POOL_SIZE));
			}
#if 0
			std::vector<DataNode*> listNode_dataNodeList; 
			std::vector<DataNode*> skiplistNode_dataNodeList; 
			std::vector<int> listNode_group_cnt;
			std::vector<int> skiplistNode_group_cnt;

			std::vector<unsigned char*> key_list_list;
			std::vector<int> key_list_cnt;
#else
			//1 data to mem
			//2 node to sort list insert and remove
			//3 to node res1
			//4 merge and res2
			/*
			   std::vector<unsigned char*> listNode_entryList; 
			   std::vector<unsigned char*> skiplistNode_entryList; 
			   std::vector<int> listNode_entry_cnt;
			   std::vector<int> skiplistNode_entry_cnt;

			   std::vector<unsigned char*> key_list_list;
			   std::vector<int> key_list_cnt;
			 */
//			void resize(uint64_t length);
			//	std::vector<unsigned char*> result;
			void insert(unsigned char* p,int size);

#endif

			//	void reserve_list(int size);
			//	void reserve_skiplist(int size);
			void empty();
			void clean();
			int getCnt();

		private:
			int resultSize;
			int resultCnt;
			int resultOffset;
			int resultPool;

			std::vector<unsigned char*> pool_list;
			std::vector<NodeAddr> entry_loc_list;

	};

	class PH_Query_Thread : public PH_Thread
	{
		private:
			DoubleLog* my_log;
			DoubleLog* my_warm_log;

			Scan_Result scan_result;

			std::vector<std::pair<uint64_t,unsigned char*>> skiplist_key_list;
			std::vector<std::pair<uint64_t,unsigned char*>> list_key_list;


		public:
			void init();
			void clean();

			int insert_op(uint64_t key,uint64_t value_size, unsigned char* value);
			int read_op(uint64_t key,unsigned char* buf,std::string *value);
			//int read_op(uint64_t key,std::string *value);
			int delete_op(uint64_t key);
			int scan_op(uint64_t start_key,uint64_t end_key);
			int next_op(unsigned char* buf);

	};

	//extern const size_t WARM_BATCH_MAX_SIZE;

	class PH_Evict_Thread : public PH_Thread
	{
		private:

			int evict_log(DoubleLog* dl);
			int try_soft_evict(DoubleLog* dl);
			int try_hard_evict(DoubleLog* dl);
			int try_push(DoubleLog* dl);
			int test_inv_log(DoubleLog* dl);


			DoubleLog** log_list;
			int log_cnt;

			int* child1_path;
			int* child2_path;

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
