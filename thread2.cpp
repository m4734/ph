#include <stdio.h>
#include <cstring>
#include <x86intrin.h> //fence
#include <unistd.h> //usleep
#include <libpmem.h> 
#include <queue>
#include <utility>
#include <algorithm>

#include "thread2.h"
#include "log.h"
#include "lock.h"
#include "cceh.h"
#include "skiplist.h"
#include "data2.h"
#include "global2.h"
#include "large.h"

namespace PH
{

#ifdef SYNCER
	extern std::atomic<int> evict_counter;
	extern std::atomic<int> query_counter;
#endif

	extern thread_local PH_Thread* my_thread;

	extern NodeAllocator* nodeAllocator;

	extern size_t HARD_EVICT_SPACE;
	extern size_t SOFT_EVICT_SPACE;

	//extern int num_thread;
	extern int num_query_thread;
	extern int num_evict_thread;
	extern int log_max;
	extern DoubleLog* doubleLogList;
	extern CCEH* hash_index;

	extern LargeAlloc* largeAlloc;

	// need to be private...

	PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];
	PH_Evict_Thread evict_thread_list[EVICT_THREAD_MAX];

	extern std::atomic<uint64_t> global_seq_num[COUNTER_MAX];

	//check
	extern std::atomic<uint64_t> warm_to_warm_sum;
	extern std::atomic<uint64_t> warm_log_write_sum;
	extern std::atomic<uint64_t> log_write_sum;
	extern std::atomic<uint64_t> hot_to_warm_sum;
	extern std::atomic<uint64_t> compact_sum;
	extern std::atomic<uint64_t> direct_to_cold_sum;
	extern std::atomic<uint64_t> dtc_fail_sum;
	extern std::atomic<uint64_t> hot_to_hot_sum;
	extern std::atomic<uint64_t> split_sum;
	extern std::atomic<uint64_t> hot_to_cold_sum;

	extern std::atomic<uint64_t> soft_htw_sum;
	extern std::atomic<uint64_t> hard_htw_sum;

	extern std::atomic<uint64_t> htw_cnt_sum;
	extern std::atomic<uint64_t> wtc_cnt_sum;

	extern std::atomic<uint64_t> dtc_time_sum;

	extern std::atomic<uint64_t> reduce_group_sum;
	extern std::atomic<uint64_t> list_merge_sum;

	extern std::atomic<uint64_t> data_sum_sum;
	extern std::atomic<uint64_t> ld_sum_sum;
	extern std::atomic<uint64_t> ld_cnt_sum;

#ifdef WARM_STAT
	extern std::atomic<uint64_t> warm_hit_sum;
	extern std::atomic<uint64_t> warm_miss_sum;
	extern std::atomic<uint64_t> warm_no_sum;
#endif

	extern Skiplist* skiplist;

	uint64_t test_the_index(KVP kvp)
	{
		printf(" not now\n");
		EntryAddr ea;
		unsigned char* addr;
		uint64_t key;

		ea.value = kvp.value;

		if (ea.loc == 1)
			addr = doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea);
		else
			addr = nodeAllocator->nodePoolList[ea.file_num]+ ea.offset;

		key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

		bool ex;
		KVP* kvp_p;
		KVP kvp2;
		volatile uint8_t *seg_depth_p;
		uint8_t seg_depth;
		ex = hash_index->read(key,&kvp2,&kvp_p,seg_depth,seg_depth_p);

		if (key != kvp.key || kvp2.key != kvp.key || kvp2.value != kvp.value)
			debug_error("kvp eee\n");

		return key;
	}

	void test_valid(EntryAddr ea)
	{
		printf("test vlaid not now\n");
#if 0
		unsigned char* addr;
		if (ea.loc == HOT_LOG)
		{
			addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
			EntryHeader* header;
			header = (EntryHeader*)addr;
			if (header->valid_bit == false)
				debug_error("valid test  fail1\n");
		}
		else
		{
			size_t offset_in_node;
			NodeMeta* nm;
			int cnt;
			int node_cnt;

			offset_in_node = ea.offset % NODE_SIZE;
			node_cnt = ea.offset/NODE_SIZE;
			nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
			if (ea.loc == 2)
			{
				int batch_num,offset_in_batch;
				batch_num = offset_in_node/WARM_BATCH_MAX_SIZE;
				offset_in_batch = offset_in_node%WARM_BATCH_MAX_SIZE;
				cnt = batch_num*WARM_BATCH_ENTRY_CNT + (offset_in_batch-NODE_HEADER_SIZE)/ENTRY_SIZE;
			}
			else
				cnt = (offset_in_node-NODE_HEADER_SIZE)/ENTRY_SIZE;
			if (nm->valid[cnt] == false)
				debug_error("false false\n");
		}
#endif
	}

	size_t get_min_tail(int log_num)
	{
		int i,mi;
		size_t min = 0xffffffffffffffff;
		for (i=0;i<num_query_thread;i++)
		{
			if (query_thread_list[i].run && min > query_thread_list[i].recent_log_tails[log_num])
			{
				min = query_thread_list[i].recent_log_tails[log_num];
				mi = i;
			}
		}
		query_thread_list[mi].update_request = 1;
		return min;
	}

	void PH_Thread::reset_test()
	{
		warm_log_write_cnt = log_write_cnt = hot_to_warm_cnt = compact_cnt = direct_to_cold_cnt = dtc_fail_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		split_cnt = 0;
		warm_to_warm_cnt = 0;
		soft_htw_cnt = hard_htw_cnt = 0;
#ifdef TIME_STAT
		dtc_time = 0;
#endif
		htw_cnt = wtc_cnt = 0;

		reduce_group_cnt = 0;
		list_merge_cnt = 0;
#ifdef WARM_STAT
		warm_hit_cnt = warm_miss_cnt = warm_no_cnt = 0;
#endif

		timeReset();

		reset_test_cnt++;
	}

	void PH_Thread::op_check()
	{
		return; // do nothing now
		++op_cnt;
		if (op_cnt % 128 == 0 || update_request) // 128?
			sync_thread();
	}

	void PH_Thread::sync_thread()
	{
		//	update_free_cnt();
		return;
		update_tail_sum();	
		update_request = 0;
	}

	void PH_Thread::update_tail_sum()
	{
		int i;
		for (i=0;i<log_max;i++)
			recent_log_tails[i] = doubleLogList[i].tail_sum;
	}

	//-------------------------------------------------------------------------------

	PH_Thread::PH_Thread() : lock(0),read_lock(0),run(0),exit(0),op_cnt(0),update_request(0),evict_buffer(NULL),split_buffer(NULL),sorted_buffer1(NULL),sorted_buffer2(NULL)
	{
		reset_test();
	}
	PH_Thread::~PH_Thread()
	{
	}

	void PH_Thread::buffer_init()
	{
		if (posix_memalign((void**)&evict_buffer,NODE_SIZE,NODE_SIZE) != 0)//WARM_BATCH_MAX_SIZE) != 0)
			printf("thread buffer alloc fail\n");
		if (posix_memalign((void**)&entry_buffer,NODE_SIZE,NODE_SIZE) != 0)
			printf("thread buffer alloc fail\n");
		if (posix_memalign((void**)&split_buffer,NODE_SIZE,NODE_SIZE*MAX_NODE_GROUP) != 0)
			printf("thread buffer alloc fail\n");
		if (posix_memalign((void**)&sorted_buffer1,NODE_SIZE,NODE_SIZE*MAX_NODE_GROUP) != 0)
			printf("thread buffer alloc fail\n");
		if (posix_memalign((void**)&sorted_buffer2,NODE_SIZE,NODE_SIZE*MAX_NODE_GROUP) != 0)
			printf("thread buffer alloc fail\n");
		if (posix_memalign((void**)&batch_read_buffer,NODE_SIZE,NODE_SIZE) != 0)
			printf("thread buffer alloc fail\n");
		if (posix_memalign((void**)&zero_buffer,NODE_SIZE,NODE_SIZE) != 0)
			printf("thread buffer alloc fail\n");
		memset(zero_buffer,0,NODE_SIZE);

	}
	void PH_Thread::buffer_clean()
	{
		free(evict_buffer);
		free(entry_buffer);

		free(split_buffer);
		free(sorted_buffer1);
		free(sorted_buffer2);

		free(batch_read_buffer);

		free(zero_buffer);

	}


	//-------------------------------------------------

	void PH_Query_Thread::init()
	{
		//		timeReset();
		buffer_init();

		int i;

		my_log = 0;
		exit = 0;

		for (i=0;i<log_max;i++)
		{
			while (doubleLogList[i].use == 0)
			{
				//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
				if (try_at_lock2(doubleLogList[i].use))
				{
					my_log = &doubleLogList[i];
				}
			}
			if (my_log)
				break;
		}

		if (my_log == 0)
			printf("new query thread no log!!!\n");
		//		else
		//			printf("log allocated\n");

		//	my_log->my_thread = this;

		//	local_seg_free_cnt = min_seg_free_cnt();
		//	local_seg_free_cnt = INV9;
		//	local_seg_free_head = seg_free_head;
		//		temp_seg = hash_index->thread_local_init();
		temp_seg = hash_index->ret_seg();

		//	recent_log_tails = new size_t[log_num];

		read_lock = 0;

		run = 1;

		seed_for_dtc = thread_id;

		new_version_for_insert.valid_bit = 1;
		new_version_for_insert.delete_bit = 0;
		jump_for_insert.valid_bit = 0;
		jump_for_insert.delete_bit = 0;
	}

	void PH_Thread::check_end()
	{
		//check
		warm_to_warm_sum+=warm_to_warm_cnt;
		warm_log_write_sum+=warm_log_write_cnt;
		log_write_sum+=log_write_cnt;
		hot_to_warm_sum+=hot_to_warm_cnt;
		compact_sum+=compact_cnt;
		//		printf("warm to cold cnt %lu\n",warm_to_cold_cnt);
		direct_to_cold_sum+=direct_to_cold_cnt;
		dtc_fail_sum+=dtc_fail_cnt;
		split_sum+=split_cnt;
		hot_to_hot_sum+=hot_to_hot_cnt;
		hot_to_cold_sum+=hot_to_cold_cnt;

		soft_htw_sum+=soft_htw_cnt;
		hard_htw_sum+=hard_htw_cnt;
#ifdef TIME_STAT
		dtc_time_sum+=dtc_time;
#endif
		htw_cnt_sum+=htw_cnt;
		wtc_cnt_sum+=wtc_cnt;

#ifdef WARM_STAT
		warm_hit_sum += warm_hit_cnt;
		warm_miss_sum += warm_miss_cnt;
		warm_no_sum += warm_no_cnt;
#endif

		data_sum_sum += data_sum;
		ld_sum_sum += ld_sum;
		ld_cnt_sum += ld_cnt;

#ifdef SCAN_TIME
		printf("%lu %lu %lu %lu %lu %lu\n",main_time_sum,first_time_sum,second_time_sum,t25_sum,third_time_sum,etc_time_sum);
#endif

		reduce_group_sum+=reduce_group_cnt;
		list_merge_sum+=list_merge_cnt;

		timeAgg();
	}

	void PH_Query_Thread::clean()
	{
		buffer_clean();

		my_log->use = 0;
		my_log = NULL;

		//		hash_index->thread_local_clean();
		//		free(temp_seg);
		run = 0;
		read_lock = 0;

		query_thread_list[thread_id].lock = 0;
		//		printf("query thread list %d end\n",thread_id);
		query_thread_list[thread_id].exit = 0;

		//	delete recent_log_tails;

		hash_index->remove_ts(temp_seg);
		//		scan_result.clean();

		check_end();
		reset_test();
	}	

	SkiplistNode* PH_Thread::get_skiplist_node_for_insert(uint64_t key,NodeAddr warm_cache) // need prev next sa list
	{
		SkiplistNode* skiplistNode;
		skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache); // with cache
		while(1)
		{
			if (try_at_lock2(skiplistNode->insert_lock) == false)
			{
				skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				continue;
			}
			if (skiplistNode->ver == 0 || skiplistNode->key > key || skiplist->find_next_node(skiplistNode)->key <= key)
			{
				at_unlock2(skiplistNode->insert_lock);
				skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				continue;
			}
			break;
		}
		return skiplistNode;
	}
#if 0
	SkiplistNode* PH_Thread::get_skiplist_node_for_evict(uint64_t key,NodeAddr warm_cache) // need prev next sa list
	{
		SkiplistNode* skiplistNode;
		skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache); // with cache
		while(1)
		{
			if (try_at_lock2(skiplistNode->evict_lock) == false)
			{
				skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				continue;
			}
			if (skiplistNode->ver == 0 || skiplistNode->key > key || skiplist->find_next_node(skiplistNode)->key <= key)
			{
				at_unlock2(skiplistNode->evict_lock);
				skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				continue;
			}
			break;
		}
		return skiplistNode;
	}
#endif
#if 0
	SkiplistNode* PH_Thread::get_skiplist_node_for_key_list(uint64_t key,NodeAddr warm_cache) // need prev next sa list
	{
		SkiplistNode* skiplistNode;
		skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache); // with cache
		while(1)
		{
			if (try_at_lock2(skiplistNode->key_list_lock) == false)
			{
				skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				continue;
			}
			if (skiplistNode->ver == 0 || skiplistNode->key > key || skiplist->find_next_node(skiplistNode)->key <= key)
			{
				at_unlock2(skiplistNode->key_list_lock);
				skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				continue;
			}
			break;
		}
		return skiplistNode;
	}
#endif

	int calc_th(DoubleLog* dl)
	{
		//		const size_t threshold = HARD_EVICT_SPACE;
		//				const size_t threshold = SOFT_EVICT_SPACE; // query thread is too busy 
#if 1
		const size_t min_threshold = dl->hard_evict_space/2;//HARD_EVICT_SPACE/2;
								    //		const size_t max_threshold = SOFT_EVICT_SPACE/2;
		const size_t max_threshold = dl->hard_evict_space;//HARD_EVICT_SPACE;
#else
		const size_t min_threshold = dl->hard_evict_space*1/4;//HARD_EVICT_SPACE/2;
								      //		const size_t max_threshold = SOFT_EVICT_SPACE/2;
		const size_t max_threshold = dl->hard_evict_space*2/4;//3/4;//HARD_EVICT_SPACE;

#endif

		const size_t threshold = max_threshold-min_threshold;

		//HARD 0 ~ SOFT 100

		//		size_t empty_space = (dl->tail_sum+dl->my_size-dl->head_sum)%dl->my_size;
		size_t empty_space = dl->my_size-(dl->head_sum-dl->tail_sum);

		if (empty_space < min_threshold)
			return 100; // always cold
		if (max_threshold < empty_space)
			return 0; // always hot
		return ((threshold)-(empty_space-min_threshold))*100/(threshold);
	}

	NodeAddr get_warm_cache(EntryAddr ea) // list or skiplist may changed // avoid error and check later
	{
		NodeAddr wc;
		if (ea.loc == HOT_LOG)
		{
			if (ea.offset < doubleLogList[ea.file_num].tail_sum)
				return emptyNodeAddr;
			//			uint64_t value_size8 = *(uint64_t*)(doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea) + ENTRY_HEADER_SIZE + KEY_SIZE);
			int value_size8 = ((EntryHeader*)(doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea)))->size;
			value_size8 = get_v8(value_size8);
			wc = *(NodeAddr*)(doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea) + ENTRY_HEADER_SIZE + KEY_SIZE /*+ SIZE_SIZE*/ + value_size8);
		}
		else if (ea.loc == WARM_LIST)
		{
			//ea is not warm cache // ea is data node and warm cahce is skiplist node
#if 1
			NodeMeta* nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[ea.file_num] + sizeof(NodeMeta) * (ea.offset/NODE_SIZE));
			if (nm->list_addr.loc != WARM_LIST)
				return emptyNodeAddr;
#if 1
			wc.pool_num = nm->list_addr.file_num;
			wc.node_offset = nm->list_addr.offset;
#else
			if (nm->list_addr.pool_num > skiplist->node_pool_list_cnt || (nm->list_addr.pool_num == skiplist->node_pool_list_cnt && nm->list_addr.node_offset > skiplist->node_pool_cnt)) // prevent access
				wc = emptyNodeAddr;
			else
				wc = nm->list_addr;
#endif
#endif
		}
		return wc;
	}

	void list_gc(SkiplistNode* skiplistNode) // need node lock
	{
		DoubleLog* dl;
		LogLoc ll;
		EntryHeader* header;
		unsigned char* addr;

		int i,dst;
		int li;
		dst = skiplistNode->list_head-1;
		for (i=skiplistNode->list_head-1;i>=skiplistNode->list_tail;i--)
		{
			li = i % WARM_LOG_LIST_MAX;
			ll = skiplistNode->entry_list[li];
			dl = &doubleLogList[ll.log_num];
			addr = dl->dramLogAddr + (ll.offset%dl->my_size);
			header = (EntryHeader*)addr;

			if (dl->tail_sum > ll.offset || header->valid_bit == false)
			{
				skiplistNode->list_size_sum-=ll.size;
				continue;
			}
			if (dst != i)
				skiplistNode->entry_list[dst%WARM_LOG_LIST_MAX] = skiplistNode->entry_list[li];
			dst--;
		}

		skiplistNode->list_tail = dst+1;

	}


#if 1
void skiplist_dtc_check(SkiplistNode *skiplist_node)
{
	if (skiplist_node->dtc_batch_num == -1)
		return;
	NodeMeta *nodeMeta;
	int i,j,k;

	i = skiplist_node->dtc_batch_num / WARM_BATCH_CNT;
	j = skiplist_node->dtc_batch_num % WARM_BATCH_CNT;

	nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(skiplist_node->data_node_addr[i]);


	k = nodeMeta->batch_info[j].el_cnt-1;//.size()-1; // was batch end

	if (nodeMeta->batch_info[j].el[k].offset % WARM_BATCH_MAX_SIZE != skiplist_node->dtc_batch_size)
		debug_error("missmathp\n");
}
#endif

#define INDEX

	int PH_Query_Thread::insert_op(uint64_t key, int value_size, unsigned char* value)
	{
		tes(INSERT);
		last_value_size = value_size;
		//	update_free_cnt();
		op_check();
		//#ifdef HASH_TEST
#if 0
		// hash test

		{
			KVP* kvp_p;
			std::atomic<uint8_t> *seg_lock;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			if (kvp_p->key == key)
			{
				EntryAddr ea;
				ea.value = kvp_p->value;
				unsigned char* addr;
				addr = get_entry(ea);
				uint64_t ea_key;
				ea_key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
				if (ea_key != key)
					debug_error("key dosne'st match\n");
				test_valid(ea);
			}
			/*
			   kvp_p->key = key;
			   kvp_p->value = *(uint64_t*)value;
			 */
			hash_index->unlock_entry2(seg_lock,read_lock);

			//			return 0;
		}
#endif
		NodeAddr warm_cache;
		EntryAddr new_ea;
		EntryAddr old_ea;

		//fixed size;

		// use checksum or write twice

		//	my_log->ready_log();
		//	head_p = my_log->get_head_p();

		//		my_log->ready_log(); // to prevent dead lock ( from entry lock -> ready log ) it should be (ready_log -> entry lock)


		// 2 lock index ------------------------------------- lock from here

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		//		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
		//	kvp_p = hash_index->insert_with_fail(key,&seg_lock,read_lock);

		KVP kvp;
		volatile uint8_t *seg_depth_p;
		uint8_t seg_depth;
		int ex;
		ex = hash_index->read(key,&kvp,&kvp_p,seg_depth,seg_depth_p);

		//		uint64_t old_version,new_version;
		bool new_key;	

		//		if (kvp_p->key != key) // new key
		if (ex == 0) // new key
		{
			//			if (kvp_p->key != INV0) // may test
			//				printf("ececption1===============\n");
			/*
			   new_version = 1;
			   new_key = true;
			   set_valid(new_version);
			   new_ea.value = 0;
			 */
			old_ea.value = 0;
		}
		else
		{
			//			old_ea.value = kvp_p->value;
			old_ea.value = kvp.value;
			//			old_version = kvp_p->version;
			/*
			   new_version = old_version+1;
			   set_valid(new_version);
			//			new_version = set_loc_hot(new_version);
			new_key = false;
			 */
		}

		// 1 write kv
		//	my_log->insert_log(&ble);

		//NEED INDEX LOCK TO PREVENT MOVING
		int rv;
		DoubleLog* dst_log = my_log;
		Loc dst_loc;
		bool large = false;
		bool dtc = false;

		int value_size8 = get_v8(value_size);
		const int entry_size = value_size8 + ENTRY_SIZE_WITHOUT_VALUE;

		if (value_size > LARGE_VALUE_THRESHOLD)
		{
//			debug_error("not large now\n");
#ifdef LARGE_ALLOC
			large = true;

			LargeAddr largeAddr = largeAlloc->insert(value_size,value);
			large_addr = largeAddr; // buffer for insert // src of value

			//			value_size = sizeof(LargeAddr); // 8
			value_size = LARGE_PTR_SIZE;
			//			value_size = INV64; // for dtc
			value = (unsigned char*)&large_addr;
#else
			//dtc = true;
#endif

			//			return 0;
			//			dtc = true;
		}

#ifdef USE_DTC
		if (old_ea.loc == WARM_LIST)
		{
			rv = rand_r(&seed_for_dtc);
			if (/*reset_test_cnt || */(rv % 100) <= calc_th(my_log) )// && false) // to cold // ratio condition
				dtc = true;
		}
#endif

		if (dtc)
		{
			tes(INSERT_DTC);
			//			dst_loc = COLD_LIST;
			//		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			//			kvp.value = 0;
			//			KVP test_kvp = kvp;
			//			if (large_value)
			//				value_size = LARGE_PTR_SIZE;

			SkiplistNode* skiplistNode;

#ifdef WARM_CACHE
			if (ex)
			{
				EntryAddr old_ea;
				old_ea.value = kvp.value;
				warm_cache = get_warm_cache(old_ea);
			}
			else
#endif
				warm_cache = emptyNodeAddr;

			skiplistNode = get_skiplist_node_for_insert(key,warm_cache); // was evict

			if (skiplistNode->dtc_batch_size + entry_size > WARM_BATCH_MAX_SIZE) // clsoe the batch
			{
				skiplistNode->dtc_batch_num = -1;
				skiplistNode->dtc_batch_size = 0;
			}

//while(skiplistNode->dtc_batch_num == -1) // find safe empty batch using split
if (skiplistNode->dtc_batch_num == -1)
{
			/*new_ea = */

			find_empty_batch(skiplistNode);

			if (skiplistNode->empty_batch == -1)
			{
//				if (may_split_warm_node(skiplistNode,2) == 1)
//					skiplistNode = get_skiplist_node_for_insert(key,warm_cache); // was evict
			}
			else
			{
				skiplistNode->dtc_batch_num = skiplistNode->empty_batch;

				if (skiplistNode->empty_batch % WARM_BATCH_CNT == 0)
					skiplistNode->dtc_batch_size = NODE_HEADER_SIZE;
				else
					skiplistNode->dtc_batch_size = 0;

			}
}
//skiplist_dtc_check(skiplistNode);
if (skiplistNode->dtc_batch_num >= 0)
			dtc = direct_to_cold(key,value_size,value,skiplistNode,large); // kvp becomes old one
			else
				dtc = false;
//			if (dtc) // success // fail == full node
//				invalidate_entry(old_ea,old_ea.large);

//skiplist_dtc_check(skiplistNode);

//			hash_index->unlock_entry2(seg_lock,read_lock);
			at_unlock2(skiplistNode->insert_lock);

			if (dtc)
				direct_to_cold_cnt++;
			else
				dtc_fail_cnt++;
#if 0 // moved to direct_to_cold...
			old_ea.value = kvp.value;
			//			if (ex)
			if (kvp.key == key)
				invalidate_entry(old_ea);
			_mm_sfence();
			hash_index->unlock_entry2(seg_lock,read_lock);
#endif

#if 0 // direct to cold will fail if there is no space
			//			if (may_split_warm_node(skiplistNode) == 0)
			//				at_unlock2(skiplistNode->lock);
			if (may_split_warm_node(skiplistNode,2) == false) // if the dtc made new cold node
				at_unlock2(skiplistNode->evict_lock);
#endif
			tee(INSERT_DTC);

		}

		if (dtc == false)
		{
			tes(INSERT_LOG);

			dst_log = my_log;
			dst_loc = HOT_LOG;

			dst_log->ready_log(value_size8);

			const uint64_t z = 0;
			jump_for_insert.version = dst_log->tail_sum;
			/*
			   memcpy(entry_buffer,&z,ENTRY_HEADER_SIZE);
			   memcpy(entry_buffer+ENTRY_HEADER_SIZE,&key,KEY_SIZE);
			   memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE,&value_size,SIZE_SIZE);
			   memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE,value,value_size8);//v8?
			   memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8,&z,WARM_CACHE_SIZE);//v8?
			   memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE,&jump_for_insert,JUMP_SIZE);//v8?
			 */
			//			dst_log->insert_pmem_log(key,value_size,value);
			//			dst_log->buffer_to_pmem(evict_buffer,LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8+JUMP_SIZE);


			unsigned char* pmem_head_p = dst_log->get_pmem_head_p();

			//			memcpy(pmem_head_p,entry_buffer,LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8+JUMP_SIZE); // pmem
			//			pmem_persist(pmem_head_p,LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8+JUMP_SIZE);

			//------------------------- PMEM WRITE
			memcpy(pmem_head_p,&z,ENTRY_HEADER_SIZE); // zzz
			memcpy(pmem_head_p+ENTRY_HEADER_SIZE,&key,KEY_SIZE);
			memcpy(pmem_head_p+ENTRY_HEADER_SIZE+KEY_SIZE,value,value_size8);//v8?
			memcpy(pmem_head_p+ENTRY_HEADER_SIZE+KEY_SIZE+value_size8,&z,WARM_CACHE_SIZE);//v8? //zzz
			memcpy(pmem_head_p+ENTRY_HEADER_SIZE+KEY_SIZE+value_size8+WARM_CACHE_SIZE,&jump_for_insert,JUMP_SIZE);//v8?
			pmem_persist(pmem_head_p,LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8+JUMP_SIZE);
			//----------------------------

			//			new_ea.loc = 1; // hot
			new_ea.loc = dst_loc;
			new_ea.large = large;
			new_ea.size = value_size;
			new_ea.file_num = dst_log->log_num;
			new_ea.offset = dst_log->head_sum;// % dst_log->my_size; // use head sum without mod because it distinguoish overwrite;
#ifdef HOT_KEY_LIST
			while(1) // add key list // if is in hot log skip it and update ref
			{
				if (ex == 0 || old_ea.loc != HOT_LOG)
				{
					/*
					   if (ex == 0)
					   kvp.value = 0;
					   else
					   kvp = *kvp_p;
					 */
					SkiplistNode* node;
					//					SkiplistNode* next_node; // what was it??

#ifdef WARM_CACHE
					if (ex)
						warm_cache = get_warm_cache(old_ea);
					else
#endif
						warm_cache = emptyNodeAddr;

//					node = get_skiplist_node_for_key_list(key,warm_cache);
					node = get_skiplist_node_for_insert(key,warm_cache);

					if (node->key_list.size() >= WARM_KEY_LIST_MAX) // need split
					{
						/*
						   if (try_at_lock2(node->insert_lock) == false)
						   {
						   at_unlock2(node->key_list);
						   continue;
						   }
						 */
						if (split_warm_node_by_key_list(node) == false) // split failed
							at_unlock2(node->insert_lock);
						continue;
					}


					kvp_p = hash_index->insert(key,&seg_lock,read_lock); // prevent htw evict
					old_ea.value = kvp_p->value;

					if (old_ea.loc != HOT_LOG) // alwyas ture...
					{
						/*
						   if (node->key_list_size >= WARM_KEY_LIST_MAX_TEMP)//WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT)
												     //							debug_error("over\n");
						 */
						//						at_lock2(node->key_list_lock);
						if (node->key_list.size() >= WARM_KEY_LIST_MAX)
						{
//							at_unlock2(node->key_list_lock);
							at_unlock2(node->insert_lock);
							hash_index->unlock_entry2(seg_lock,read_lock);
							continue; // try again and may split
						}
//						node->key_list[node->key_list_size++] = key;
						node->key_list.push_back(key);
					}

//					at_unlock2(node->key_list_lock);
					at_unlock2(node->insert_lock);
					//				_mm_sfence();
					//					at_unlock2(node->lock);//here we have entry lock
				}
				else
				{
					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					old_ea.value = kvp_p->value;
					if (old_ea.loc != HOT_LOG)
					{
						hash_index->unlock_entry2(seg_lock,read_lock);
						continue;
					}
				}
				break;
			}
#else
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			old_ea.value = kvp_p->value;
#endif

			//-------------- here has key index lock

			uint64_t new_ver; //valid dele size ver
			new_version_for_insert.version = global_seq_num[key%COUNTER_MAX].fetch_add(1);
			new_version_for_insert.large_bit = large;
			new_version_for_insert.size = value_size;
			new_ver = new_version_for_insert.value;

			// update version
			//			memcpy(entry_buffer,&new_ver,ENTRY_HEADER_SIZE);


			_mm_sfence();
			//			dst_log->write_version(new_version.value); // has fence
			//			dst_log->buffer_to_pmem(entry_buffer,ENTRY_HEADER_SIZE);
			//			dst_log->buffer_to_pmem((unsigned char*)&new_ver,ENTRY_HEADER_SIZE);

			//-------------------------------------- PMEM HEADER
			memcpy(pmem_head_p,&new_ver,ENTRY_HEADER_SIZE); // pmem header
			pmem_persist(pmem_head_p,ENTRY_HEADER_SIZE);
			//--------------------------------------------

			// 3 get and write new version <- persist

			if (kvp_p->key != key) // new key...
			{
				/*
				   data_sum+=value_size+KEY_SIZE;
				   if (value_size >= LARGE_VALUE_THRESHOLD)
				   {
				   ld_sum+=value_size+KEY_SIZE;
				   ld_cnt++;
				   }
				 */	
				ex = 0;
			}
			else
				ex = 1;


			// 4 add dram list
#ifdef USE_DRAM_CACHE
			//	new_addr = dram_head_p;
#ifdef WARM_CACHE
			if (ex)
				warm_cache = get_warm_cache(old_ea);
			else
				warm_cache = emptyNodeAddr;

			//--------------------------------------- DRAM WRITE
			dst_log->insert_dram_log(new_ver,key,value_size,value,&warm_cache);

			//			memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8,&warm_cache,WARM_CACHE_SIZE);
			//			dst_log->buffer_to_dram(entry_buffer,LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8);
			//			unsigned char* dram_head_p = dst_log->get_dram_head_p();
			//			memcpy(dram_head_p,entry_buffer,LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8); //dram
			//			memcpy(dram_head_p,entry_buffer,ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE); //dram

			//--------------------------------------------------------------------------
#else
			warm_cache = emptyNodeAddr;
			dst_log->insert_dram_log(new_ver,key,value_size,value,&warm_cache);
#endif

#endif
			//			dst_log->copy_to_pmem_log(value_size);
			//			dst_log->insert_pmem_log(key,value_size,value);

			//			dst_log->head_sum_log[dst_log->head_sum_cnt] = dst_log->head_sum;
			//			dst_log->head_sum_cnt++;
			dst_log->head_sum+=LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8;
			//			dst_log->head_sum+=ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE; // NO JUMP SIZE

			//check
			log_write_cnt++;
			_mm_sfence();
			kvp_p->value = new_ea.value;
			//			kvp_p->version = new_version.value;
			_mm_sfence(); // value first!

			// 6 update index
			//	kvp_p->value = (uint64_t)my_log->get_head_p();

			// try lock


			if (ex == 0) // not so good // what about direct to cold
			{
				kvp_p->key = key;
			}
			else
			{
				//				if (old_ea.loc == HOT_LOG)
				//					hot_to_hot_cnt++; // need warm to hot?
				invalidate_entry(old_ea);//,large); // log write test
				// it was not old_ea.large ... why??
			}
#ifdef DST_CHECK
			EA_test(key,new_ea);
#endif

			_mm_sfence();
			hash_index->unlock_entry2(seg_lock,read_lock);

			tee(INSERT_LOG);

		}
		tee(INSERT);
		return 0;
	}

	int PH_Query_Thread::read_op(uint64_t key,unsigned char* buf,std::string *value)
	{
		tes(READ);
		//	update_free_cnt();
		op_check();
#ifdef HASH_TEST
		//hash test
		{
			KVP* kvp_p;
			volatile int* split_cnt_p;
			int split_cnt;
			KVP kvp;
			int ex;
			while(true)
			{
				ex = hash_index->read(key,&kvp,&kvp_p,split_cnt,split_cnt_p);
				if (ex == 0)
				{
					printf("deson't eixsit-----------------------------------------\n"); // value test
					return -1;
				}
				if (buf)
					memcpy(buf,(unsigned char*)&kvp_p->value,sizeof(uint64_t));
				else
					value->assign(kvp_p->value,sizeof(uint64_t));
				if (split_cnt_p == NULL || split_cnt == *split_cnt_p)
					return 0;
			}
		}
#endif
		//	uint64_t ret;
		//	hash_index->read(key,&ret);

		EntryAddr ea;
		volatile uint8_t *seg_depth_p;
		uint8_t seg_depth;
		KVP* kvp_p;
		KVP kvp;
		uint64_t ret;
		int ex;
		int value_size;

		std::atomic<uint8_t>* seg_lock;

		// if we use hash seg lock, do not use nm lock because there will be dead lock in cold evict
		// we will not use seg lock

		while (true)
		{
			//			seg_depth = hash_index->read(key,&ret,&seg_depth_p);
			ex = hash_index->read(key,&kvp,&kvp_p,seg_depth,seg_depth_p);
			//			seg_depth = *seg_depth_p;

			if (ex == 0)
			{

#ifndef NO_EXIST
				debug_error("can't find key\n");
#endif
				return -1;
			}

			ea.value = kvp.value;

			unsigned char* addr; // entry addr
			unsigned char* value_addr;

			if (ea.loc == HOT_LOG)// || ea.loc == WARM_LOG) // hot or warm
			{
				//				doubleLogList[ea.file_num].log_check();
				//				size_t old_tail_sum,logical_tail,logical_offset,diff;
				//				old_tail_sum = doubleLogList[ea.file_num].tail_sum;
				/*
				   logical_tail = old_tail_sum % doubleLogList[ea.file_num].my_size;
				   if (old_tail_sum > ea.offset)
				   logical_offset = ea.offset + doubleLogList[ea.file_num].my_size;
				   else
				   logical_offset = ea.offset;
				 */
				if (ea.offset < doubleLogList[ea.file_num].tail_sum)
					continue;
				addr = doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea);
				if (ea.large)
				{
					unsigned char* vsp;
					vsp = get_large_from_addr(addr);
					value_size = *(uint64_t*)vsp;
					value_addr = vsp+SIZE_SIZE_L;
				}
				else
				{
					//					value_size = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
					value_size = ea.size;
					value_addr = addr+ENTRY_HEADER_SIZE+KEY_SIZE;//+SIZE_SIZE;
				}
				/*
				   if (value_size >= NODE_SIZE)
				   {
				   debug_error("invalid log addr\n");
				   continue;
				   }
				 */
				if (buf)
					memcpy(buf,value_addr,value_size);
				else
					value->assign((char*)(value_addr),value_size);
				_mm_sfence();
#if 0 
				uint64_t test_key;
				uint64_t test_value;

				test_key = *(uint64_t*)(addr+HEADER_SIZE);
				//			test_value = *(uint64_t*)(addr+HEADER_SIZE+KEY_SIZE);
				test_value = *(uint64_t*)buf;

				if (key+1 != test_value || test_key+1 != test_value)
				{
					debug_error("test fail1\n");
					ex = hash_index->read(test_key,&kvp,&kvp_p,&seg_depth,&seg_depth_p);
				}
#endif

				//				if (ea.value != kvp_p->value) // value is unique ?
				//					continue;

				/*
				   diff = doubleLogList[ea.file_num].tail_sum-old_tail_sum;
				   if (logical_tail+diff > logical_offset) // entry may be overwritten // try again
				   continue;
				 */

				if (doubleLogList[ea.file_num].tail_sum > ea.offset)
					continue;
				//				doubleLogList[ea.file_num].log_check();

			}
			else // warm cold
			{
				NodeMeta* nm;
				int node_cnt;//,offset;

				node_cnt = ea.offset/NODE_SIZE;
				nm = (NodeMeta*)((unsigned char*)nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
				//at_lock2(nm->rw_lock);
				if (try_at_lock2(nm->rw_lock) == false)
					continue;
				//				_mm_sfence();
				/*
				   if (ea.loc == 3) // cold list
				   offset = ((ea.offset-NODE_HEADER_SIZE)%NODE_SIZE)/ENTRY_SIZE;
				   else // warm list
				   {
				   offset = ((ea.offset%NODE_SIZE)/WARM_BATCH_MAX_SIZE)*WARM_BATCH_ENTRY_CNT
				   + ((ea.offset%WARM_BATCH_MAX_SIZE)-NODE_HEADER_SIZE)/ENTRY_SIZE;
				   }

				   if (nm->valid[offset] == false) // invaldidated
				   {
				   at_unlock2(nm->rw_lock);
				   continue;
				   }
				 */
				//				if (kvp_p->key != key || kvp_p->value != ea.value) // updated?
				if (kvp.key != key || kvp.value != ea.value)
				{
					at_unlock2(nm->rw_lock);
					continue;
				}

				addr = (unsigned char*)nodeAllocator->nodePoolList[ea.file_num]+ea.offset;
				//		if (key == *(uint64_t*)(addr+HEADER_SIZE))
				//			break;
				if (ea.large)
				{
					unsigned char* vsp;
					vsp = get_large_from_addr(addr);
					value_size = *(uint64_t*)vsp; // 8 bytes...
					value_addr = vsp+SIZE_SIZE_L;
				}
				else
				{
					//					value_size = 100;
					value_size = ea.size;

					value_addr = addr+ENTRY_HEADER_SIZE+KEY_SIZE;//+SIZE_SIZE;
				}

				//	hash_index->read(key,&ea.value);//retry
				if (buf)
					memcpy(buf,value_addr,value_size);
				else
					value->assign((char*)(value_addr),value_size);
				//		at_unlock2(nm->lock);
				_mm_sfence();

#if 0
				uint64_t test_key;
				uint64_t test_value;

				test_key = *(uint64_t*)(addr+HEADER_SIZE);
				//test_value = *(uint64_t*)(addr+HEADER_SIZE+KEY_SIZE);
				test_value = *(uint64_t*)buf;

				if (key+1 != test_value || test_key+1 != test_value)
					debug_error("test fail2\n");
#endif
				at_unlock2(nm->rw_lock);
			}

			// read address from hash -> lock the node -> copy value -> check hash again (there was the value anyway) -> unlock the node 

			// need fence?
			if (seg_depth_p == NULL || seg_depth == *seg_depth_p)// && ret == *ret_p)
			{
#ifdef KEY_CHECK
				uint64_t test_key;
				test_key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
				if (test_key != key)
					debug_error("key test failed\n"); // TODO 20 thread
#endif

				break;
			}
		}

		tee(READ);

		return 0;
	}
	int PH_Query_Thread::delete_op(uint64_t key)
	{
		//	update_free_cnt();
		op_check();

		return 0;
	}
	/*
	   void Scan_Result::reserve_list(int size)
	   {
	   listNode_dataNodeList.reserve(size);
	   listNode_group_cnt.resize(size);
	   while (size > listNode_dataNodeList.size())
	   listNode_dataNodeList.push_back(new DataNode[MAX_NODE_GROUP]);
	   }

	   void Scan_Result::reserve_skiplist(int size)
	   {
	   skiplistNode_dataNodeList.reserve(size);
	   skiplistNode_group_cnt.resize(size);
	   key_list_cnt.resize(size);
	   while (size > skiplistNode_dataNodeList.size()) // what if ...
	   {
	   skiplistNode_dataNodeList.push_back(new DataNode[WARM_MAX_NODE_GROUP]);
	   key_list_list.push_back(new unsigned char[WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT * ENTRY_SIZE]);
	   }
	   }
	 */
	void Scan_Result::clean()
	{
		int i;
		for (i=0;i<resultPool;i++)
			free(pool_list[i]);
	}
	void Scan_Result::setTarget(int target)
	{
		resultTarget = target;
	}
	void Scan_Result::insert(unsigned char* p,int size)
	{
		if (resultCnt >= resultTarget)
			return;
		if (resultOffset + size > DEFAULT_SCAN_POOL_SIZE)
		{
			pool_list.push_back((unsigned char*)malloc(DEFAULT_SCAN_POOL_SIZE));
			resultOffset = 0;
			resultPool++;
		}
		memcpy(pool_list[resultPool-1]+resultOffset,p,size);
		NodeAddr loc;
		loc.pool_num = resultPool-1;
		loc.node_offset = resultOffset;
		entry_loc_list.push_back(loc);
		resultCnt++;
		resultOffset+=size;
	}
	unsigned char* Scan_Result::get_buffer(int size)
	{
		if (resultCnt >= resultTarget)
			return NULL;
		if (resultOffset + size > DEFAULT_SCAN_POOL_SIZE)
		{
			pool_list.push_back((unsigned char*)malloc(DEFAULT_SCAN_POOL_SIZE));
			resultOffset = 0;
			resultPool++;
		}
		//		memcpy(pool_list[resultPool-1]+resultOffset,p,size);
		NodeAddr loc;
		loc.pool_num = resultPool-1;
		loc.node_offset = resultOffset;
		entry_loc_list.push_back(loc);
		resultCnt++;
		resultOffset+=size;

		return pool_list[resultPool-1]+resultOffset;
	}
	void Scan_Result::empty()
	{
		resultCnt = 0;
		resultOffset = 0;

		entry_loc_list.clear();
	}
	int Scan_Result::getCnt()
	{
		return resultCnt;
	}

	void Scan_Result::insert_from_header(unsigned char* header)
	{
		int value_size;
		unsigned char* large_addr;
		unsigned char* dst_addr;

		if (((EntryHeader*)header)->large_bit)
		{
			large_addr = get_large_from_addr(header);
			value_size = *(uint64_t*)large_addr;
			dst_addr = get_buffer(ENTRY_SIZE_WITHOUT_VALUE+value_size);
			if (dst_addr == NULL)
				return;
			memcpy(dst_addr,header,ENTRY_HEADER_SIZE+KEY_SIZE);
			memcpy(dst_addr+ENTRY_HEADER_SIZE+KEY_SIZE,large_addr+SIZE_SIZE_L,value_size);
		}
		else
		{
			value_size = ((EntryHeader*)header)->size;
			insert(header,ENTRY_SIZE_WITHOUT_VALUE+value_size);
		}

	}

	int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t length)
	{
		//	update_free_cnt();
		op_check();
#if 0 // not now
#ifdef SCAN_TIME
		struct timespec ts1,ts2;
		struct timespec ts3,ts4;
		_mm_sfence();
		clock_gettime(CLOCK_MONOTONIC,&ts1);
		_mm_sfence();

#endif

		volatile uint8_t *seg_depth_p;
		uint8_t seg_depth;
		KVP* kvp_p;
		KVP kvp;
		int ex;
		NodeAddr warm_cache;
		EntryAddr ea;
		SkiplistNode* skiplistNode;
		SkiplistNode* next_skiplistNode;
		ListNode* listNode;
		NodeMeta* nodeMeta;
		DataNode* dataNode;

		uint64_t next_key;
		//		uint64_t scan_count=0;

		int group_idx;
		int size;

		//		scan_result.resize(length);
		scan_result.empty();
		scan_result.setTarget(length);

		ex = hash_index->read(start_key,&kvp,&kvp_p,seg_depth,seg_depth_p);
#ifdef WARM_CACHE
		if (ex)
		{
			ea.value = kvp.value;
			warm_cache = get_warm_cache(ea);
		}
		else
#endif
			warm_cache = emptyNodeAddr;

#if 0
		skiplistNode = skiplist->find_node(start_key,prev_sa_list,next_sa_list,warm_cache);
		while(1) // find first
		{
			if (skiplistNode->ver == 0 || start_key < skiplistNode->key)
			{
				skiplistNode = skiplist->find_node(start_key,prev_sa_list,next_sa_list);
				continue;
			}
			if (try_at_lock2(skiplistNode->lock) == false)
				continue;
			if (skiplistNode->ver == 0 || start_key < skiplistNode->key)
			{
				at_unlock2(skiplistNode->lock);
				skiplistNode = skiplist->find_node(start_key,prev_sa_list,next_sa_list);
				continue;
			}
			break;
		}
#else
		//		skiplistNode = get_skiplist_node(start_key,warm_cache);
		skiplistNode = get_skiplist_node_for_insert(start_key,warm_cache);
#endif

		//		at_lock2(skiplistNode->insert_lock);
		at_lock2(skiplistNode->evict_lock);
//		at_lock2(skiplistNode->key_list_lock);

		// locked 

		int i,j;
		//		int key_list_index;
		unsigned char* addr;
		std::atomic<uint8_t> *seg_lock;
		uint64_t key,key1,key2;

		//		DataNode dram_dataNode;

		size_t offset;
#if 1
		skiplist_key_list.clear();
		list_key_list.clear();
		int sklt;
		int lklt;
#endif


#ifdef SCAN_TIME
		_mm_sfence();
		clock_gettime(CLOCK_MONOTONIC,&ts2);
		etc_time_sum+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
		clock_gettime(CLOCK_MONOTONIC,&ts1);
		_mm_sfence();
#endif

		int end_batch;

		while(scan_result.getCnt() < length && skiplistNode != skiplist->end_node) // still todo
		{
			while(1) // find next key
			{
				//				next_skiplistNode = skiplist->sa_to_node(skiplistNode->next[0]);
				next_skiplistNode = skiplist->find_next_node(skiplistNode);
				next_key = next_skiplistNode->key;

				_mm_sfence();
				//				if (next_skiplistNode != skiplist->sa_to_node(skiplistNode->next[0]))
				if (next_skiplistNode != skiplist->find_next_node(skiplistNode))
					continue;
				break;
			}

			nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(skiplistNode->data_node_addr[0]);
			group_idx = 0;

			//			skiplist_cnt++;
			//			scan_result.reserve_skiplist(skiplist_cnt);

			// hot key copy
			if (skiplist_key_list.size())
				debug_error("list not empty\n");

			//			key_list_index = 0;
#ifdef SCAN_TIME
			_mm_sfence();
			clock_gettime(CLOCK_MONOTONIC,&ts3);
			_mm_sfence();
#endif
			sklt = 0;
			size = skiplistNode->key_list.size();
			for (i=0;i<size;i++)
			{
				key = skiplistNode->key_list[i];
				if (key < start_key)
					continue;

				while(true)
				{
					ex = hash_index->read(skiplistNode->key_list[i],&kvp,&kvp_p,seg_depth,seg_depth_p);
					if (ex)
					{
						ea.value = kvp.value;
						if (ea.loc != HOT_LOG)
						{
#if 1
							debug_error("scan exception"); // inserted during scan // may possible...
#endif
							ex = 0;
							break;
						}
						if (ea.offset < doubleLogList[ea.file_num].tail_sum)
							continue;
						addr = doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea);
						//						memcpy(scan_result.key_list_list[skiplist_cnt-1]+ (ENTRY_SIZE * key_list_index),addr,ENTRY_SIZE);
						if (seg_depth_p != NULL && seg_depth != *seg_depth_p)
							continue;
						if (((EntryHeader*)addr)->valid_bit == false)
							continue;
						skiplist_key_list.push_back(std::make_pair(key,addr));
					}
					break;
				}
				//				if (ex)
				//					key_list_index++;
			}
			//			scan_count+=key_list_index;

#ifdef SCAN_TIME
			_mm_sfence();
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			first_time_sum+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
			clock_gettime(CLOCK_MONOTONIC,&ts3);
			_mm_sfence();
#endif

			while(nodeMeta) // skiplist node group copy
			{
				dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);

				//				at_lock2(nodeMeta->rw_lock); // don't need this only read thread can ...

				//				memcpy(&scan_result.skiplistNode_dataNodeList[skiplist_cnt-1][group_idx],dataNode,sizeof(NODE_SIZE)); // pmem to dram

				//				sorted_buffer1[group_idx] = *dataNode; // is not sorted
				memcpy(&sorted_buffer1[group_idx],dataNode,NODE_SIZE);
				//				at_unlock2(nodeMeta->rw_lock);

				addr = (unsigned char*)&sorted_buffer1[group_idx];
				end_batch = 0;
				for (i=0;i<WARM_BATCH_CNT;i++)
				{
					end_batch+=WARM_BATCH_MAX_SIZE;
					//					addr = (unsigned char*)&sorted_buffer1[group_idx] + WARM_BATCH_MAX_SIZE*i;
					for (j=i*WARM_BATCH_ENTRY_CNT;j<NODE_SLOT_MAX;j++) // 20?
					{
						if (nodeMeta->entryLoc[j].valid)
						{
							key = *(uint64_t*)(addr+nodeMeta->entryLoc[j].offset+ENTRY_HEADER_SIZE);
							if (key >= start_key)
								skiplist_key_list.push_back(std::make_pair(key,addr+nodeMeta->entryLoc[j].offset));
						}
						else if (nodeMeta->entryLoc[j].offset >= end_batch) // or ... +1024
							break;
						//						j++;
					}
				}
				//				scan_count+=nodeMeta->valid_cnt;
				nodeMeta = nodeMeta->next_node_in_group;
				group_idx++;
			}
			//			scan_count+=skiplist_key_list.size();
#ifdef SCAN_SORT
			std::sort(skiplist_key_list.begin(),skiplist_key_list.end());
#endif

#ifdef SCAN_TIME
			_mm_sfence();
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			second_time_sum+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
			clock_gettime(CLOCK_MONOTONIC,&ts3);
			_mm_sfence();
#endif


			// list
			if (list_key_list.size())
				debug_error("list somegint\n");
			//			listNode = skiplistNode->my_listNode; // do we need listNode lock?? we already locked skiplist...
			listNode = skiplistNode->cold_nodes[0];
			while (listNode->next->key < start_key)
				listNode = listNode->next;

			while (listNode->key < next_key && scan_result.getCnt() < length) // scan skiplist
			{
				nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
				group_idx = 0;

				//				list_cnt++;
				//				scan_result.reserve_list(list_cnt);
				lklt = 0;
				while(nodeMeta) // scan listnode
				{
					dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);

					//					at_lock2(nodeMeta->rw_lock);

					//					memcpy(&scan_result.listNode_dataNodeList[list_cnt-1][group_idx],dataNode,sizeof(NODE_SIZE)); // pmem to dram
					//					sorted_buffer2[group_idx] = *dataNode; // the name is
					memcpy(&sorted_buffer2[group_idx],dataNode,NODE_SIZE);

					addr = (unsigned char*)&sorted_buffer2[group_idx];

					i = 0;
					while(nodeMeta->entryLoc[i].offset < NODE_SIZE)
					{
						if (nodeMeta->entryLoc[i].valid)
						{
							key = *(uint64_t*)(addr+nodeMeta->entryLoc[i].offset+ENTRY_HEADER_SIZE);
							if (key >= start_key)
								list_key_list.push_back(std::make_pair(key,addr+nodeMeta->entryLoc[i].offset));
						}
						i++;
					}

					//					at_unlock2(nodeMeta->rw_lock);

					//					scan_count+=nodeMeta->valid_cnt;
					//					scan_count+=list_sort_list.size();
					nodeMeta = nodeMeta->next_node_in_group;
					group_idx++;
				}

				//				scan_count+=list_key_list.size();
#ifdef SCAN_SORT
				std::sort(list_key_list.begin(),list_key_list.end());
#endif

#ifdef SCAN_TIME
				_mm_sfence();
				clock_gettime(CLOCK_MONOTONIC,&ts4);
				t25_sum+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
				clock_gettime(CLOCK_MONOTONIC,&ts3);
				_mm_sfence();
#endif

				//pop
#if 1
				unsigned char* header;

				if (skiplist_key_list.size() > sklt && list_key_list.size() > lklt)
				{
					key1 = skiplist_key_list[sklt].first;
					key2 = list_key_list[lklt].first;
					while (scan_result.getCnt() < length)
					{
						if (key1 < key2)
						{
							scan_result.insert_from_header(skiplist_key_list[sklt].second);
							sklt++;
							if (skiplist_key_list.size() == sklt)
								break;
							key1 = skiplist_key_list[sklt].first;
						}
						else
						{
							scan_result.insert_from_header(list_key_list[lklt].second);
							lklt++;
							if (list_key_list.size() == lklt)
								break;
							key2 = list_key_list[lklt].first;
						}
					}
				}
				if (scan_result.getCnt() < length)
				{
					size = list_key_list.size();
					for (i=lklt;i<size;i++)
					{
						scan_result.insert_from_header(list_key_list[lklt].second);
						lklt++;
					}
				}
				list_key_list.clear();
#endif

#ifdef SCAN_TIME
				_mm_sfence();
				clock_gettime(CLOCK_MONOTONIC,&ts4);
				third_time_sum+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
				clock_gettime(CLOCK_MONOTONIC,&ts3);
				_mm_sfence();
#endif

				listNode = listNode->next;
			}
			if (scan_result.getCnt() < length)
			{
				size = skiplist_key_list.size();
				for (i=sklt;i<size;i++)
				{
#if 1
					scan_result.insert_from_header(skiplist_key_list[sklt].second);
					sklt++;
#endif
				}
			}

			skiplist_key_list.clear();
			//			scan_count+=size;
			while(1) // may need delete lock // move to next
			{
				next_skiplistNode = skiplist->find_next_node(skiplistNode);

				if (try_at_lock2(next_skiplistNode->insert_lock) == false)
					continue;
				if (next_skiplistNode != skiplist->find_next_node(skiplistNode))
				{
					at_unlock2(next_skiplistNode->insert_lock);
					continue;
				}
				break;
			}
//			at_unlock2(skiplistNode->key_list_lock);
			at_unlock2(skiplistNode->evict_lock);
			at_unlock2(skiplistNode->insert_lock);

			//			at_lock2(next_skiplistNode->insert_lock);
			at_lock2(next_skiplistNode->evict_lock);
//			at_lock2(next_skiplistNode->key_list_lock);

			skiplistNode = next_skiplistNode;
		}

		at_unlock2(skiplistNode->insert_lock);
		at_unlock2(skiplistNode->evict_lock);
//		at_unlock2(skiplistNode->key_list_lock);

#ifdef SCAN_TIME
		_mm_mfence();
		clock_gettime(CLOCK_MONOTONIC,&ts2);
		main_time_sum+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

		return scan_result.getCnt();
#endif
		return 0;
	}
	int PH_Query_Thread::next_op(unsigned char* buf)
	{
		//	update_free_cnt();
		op_check();

		return 0;
	}

	//--------------------------------------------------------------------------------

	void PH_Evict_Thread::init()
	{
		//		timeReset();
		buffer_init();

		sleep_time = 1000;

		exit = 0;

		int ln = (log_max-1) / num_evict_thread+1;

		log_cnt = 0;
		log_list = new DoubleLog*[ln];

		int i;
		for (i=0;i<log_max;i++)
		{
			while (doubleLogList[i].evict_alloc == 0)
			{
				//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
				if (try_at_lock2(doubleLogList[i].evict_alloc))
				{
					log_list[log_cnt] = &doubleLogList[i];
					log_cnt++;

					//					printf("evict thread %d -- log %d\n",thread_id,i);
					break;
				}
			}
			if (log_cnt >= ln)
				break;
		}

		if (log_cnt == 0)
			printf("evict thread can not find log\n");

		for (i=log_cnt;i<ln;i++)
			log_list[i] = NULL;

		child1_path = (int*)malloc(WARM_LOG_LIST_MAX * sizeof(int));
		child2_path = (int*)malloc(WARM_LOG_LIST_MAX * sizeof(int));

		temp_seg = hash_index->ret_seg();
		read_lock = 0;
		run = 1;

#ifdef SCAN_TIME
		main_time_sum = first_time_sum = second_time_sum = third_time_sum = etc_time_sum = 0;
#endif
	}

	void PH_Evict_Thread::clean()
	{
		//		hash_index->thread_local_clean();
		//		free(temp_seg); // ERROR!

		run = 0;
		read_lock = 0;
		delete[] log_list; // remove pointers....

		hash_index->remove_ts(temp_seg);

		buffer_clean();

		free(child1_path);
		free(child2_path);

		evict_thread_list[thread_id].lock = 0;
		evict_thread_list[thread_id].exit = 0;

		check_end();
		reset_test();
	}

#define SPLIT_WITH_LIST_LOCK
#if 0
	void PH_Thread::split_empty_warm_node(SkiplistNode *old_skiplistNode) // old node is deleted // split need lock....
	{
		//we will not reuse old skiplist

		uint64_t half_key;
		SkiplistNode* child1_sl_node;
		SkiplistNode* child2_sl_node;
		ListNode* half_listNode;
#if 0 // compile
#if 0 // find half 

		//		if (old_skiplistNode->half_listNode == NULL)
		/*
		   old_skiplistNode->half_listNode = find_halfNode(old_skiplistNode);
		 */
#ifdef SPLIT_WITH_LIST_LOCK // LIST LOCK
		while(1)
		{
			old_skiplistNode->find_half_listNode();
			if (try_at_lock2(old_skiplistNode->half_listNode->lock) == false)
				continue;
			break;
		}
		half_key = old_skiplistNode->half_listNode->key; // need skiplist node lock to access half listNode
#else // NO LOCK
		old_skiplistNode->find_half_listNode();
		half_key = old_skiplistNode->half_listNode->key; // need skiplist node lock to access half listNode

#endif
#if 0 // fix this bug fd TODO fix this
		if (old_skipListNode->key >= half_key)
		{
			split_listNode_group(old_skipListNode->half_listNode,old_skipListNode); // use try cold
			/*
			   uint64_t half1,half2; // overflow
			   half1 = (old_skipListNode->key)/2; // approx
			   half2 = (skiplist->sa_to_node(old_skipListNode->next[0])->key)/2; // approx
			   half_key = half1 + half2;
			   if (half_key <= old_skipListNode->key)
			   debug_error("half error\n");
			 */
			old_skipListNode->half_listNode = find_halfNode(old_skipListNode);
			half_key = old_skipListNode->half_listNode->key; 
		}

		if (half_key <= old_skipListNode->key)
		{
			debug_error("half error\n");
		}
#else
		while (old_skiplistNode->key >= half_key) // have to be once
		{
			//			uint64_t old_half_key = half_key;
			//			split_listNode_group(old_skipListNode->half_listNode,old_skipListNode);
			SkiplistNode* next_skn; // for debug
			next_skn = skiplist->find_next_node(old_skiplistNode);
			try_cold_split(old_skiplistNode->half_listNode,old_skiplistNode);
#ifdef SPLIT_WITH_LIST_LOCK
			at_unlock2(old_skiplistNode->half_listNode->lock);
#endif
			/*
			   uint64_t half1,half2; // overflow
			   half1 = (old_skipListNode->key)/2; // approx
			   half2 = (skiplist->sa_to_node(old_skipListNode->next[0])->key)/2; // approx
			   half_key = half1 + half2;
			   if (half_key <= old_skipListNode->key)
			   debug_error("half error\n");
			 */
			//			old_skiplistNode->half_listNode = find_halfNode(old_skiplistNode);
			//			half_key = old_skiplistNode->half_listNode->key; 
#ifdef SPLIT_WITH_LIST_LOCK
			while(1)
			{
				old_skiplistNode->find_half_listNode();
				if (try_at_lock2(old_skiplistNode->half_listNode->lock) == false)
					continue;
				break;
			}
			half_key = old_skiplistNode->half_listNode->key; // need skiplist node lock to access half listNode
#else
			old_skiplistNode->find_half_listNode();
			half_key = old_skiplistNode->half_listNode->key; // need skiplist node lock to access half listNode
#endif
			if (old_skiplistNode->key >= half_key)
				debug_error("half again\n");
		}
#endif
		half_listNode = old_skiplistNode->half_listNode;

#endif
		//---------------------------------------------------------------------

		if (old_skiplistNode->cold_cnt <= 1) // need at least two cold nodes
		{			

			try_cold_split(old_skiplistNode->cold_nodes[0],old_skiplistNode);// == false) // we have skiplist node lock do we need list node lock??

			// never faill...
		}
		int half_cold_index = old_skiplistNode->cold_cnt/2;
		half_listNode = old_skiplistNode->cold_nodes[half_cold_index];

		at_lock2(half_listNode->lock);

		half_key = half_listNode->key;

		child1_sl_node = skiplist->allocate_node();
		child2_sl_node = skiplist->allocate_node();

		if (child1_sl_node == NULL || child2_sl_node == NULL) // alloc fail
		{
			printf("alloc fail\n");
			return;
		}

		at_lock2(child1_sl_node->split_lock);
		at_lock2(child2_sl_node->split_lock);
		at_lock2(child1_sl_node->insert_lock);
		at_lock2(child2_sl_node->insert_lock);
		at_lock2(child1_sl_node->evict_lock);
		at_lock2(child2_sl_node->evict_lock);
//		at_lock2(child1_sl_node->key_list_lock);
//		at_lock2(child2_sl_node->key_list_lock);

		int z;
		for (z=0;z<half_cold_index;z++)
		{
			child1_sl_node->cold_keys[z] = old_skiplistNode->cold_keys[z];
			child1_sl_node->cold_nodes[z] = old_skiplistNode->cold_nodes[z];
			child1_sl_node->cold_nodes[z]->warm_cache = child1_sl_node->myAddr;
		}
		child1_sl_node->cold_cnt = half_cold_index;
		for (;z<old_skiplistNode->cold_cnt;z++)
		{
			child2_sl_node->cold_keys[z-half_cold_index] = old_skiplistNode->cold_keys[z];
			child2_sl_node->cold_nodes[z-half_cold_index] = old_skiplistNode->cold_nodes[z];
			child2_sl_node->cold_nodes[z-half_cold_index]->warm_cache = child2_sl_node->myAddr;
		}
		child2_sl_node->cold_cnt = old_skiplistNode->cold_cnt-half_cold_index;

		//		old_skiplistNode->half_listNode->hold = 1;
		half_listNode->hold = 1;

		child1_sl_node->key = old_skiplistNode->key;
		//		child1_sl_node->my_listNode = old_skiplistNode->my_listNode.load();
		//		child1_sl_node->my_listNode = old_skiplistNode->my_listNode;

		child2_sl_node->key = half_key;
		//		child2_sl_node->my_listNode = old_skiplistNode->half_listNode;

#ifdef HOT_KEY_LIST
		// key list split
		int i;
		for (i=0;i<old_skiplistNode->key_list_size;i++)
		{
			if (old_skiplistNode->key_list[i] < half_key)
				child1_sl_node->key_list[child1_sl_node->key_list_size++] = old_skiplistNode->key_list[i];
			else
				child2_sl_node->key_list[child2_sl_node->key_list_size++] = old_skiplistNode->key_list[i];
#endif
		}

		NodeMeta* nodeMeta;
		NodeMeta* new_nodeMeta;

		//		nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(old_skipListNode->data_node_addr);

		// set new nodes

		NodeMeta* child1_meta;
		NodeMeta* child2_meta;
		NodeMeta* next_meta;

		child1_meta = nodeAllocator->nodeAddr_to_nodeMeta(child1_sl_node->data_node_addr[0]);
		child2_meta = nodeAllocator->nodeAddr_to_nodeMeta(child2_sl_node->data_node_addr[0]);
		//		next_meta = nodeAllocator->nodeAddr_to_nodeMeta(skiplist->sa_to_node(old_skiplistNode->next[0])->data_node_addr[0]);
		next_meta = nodeAllocator->nodeAddr_to_nodeMeta(skiplist->find_next_node(old_skiplistNode)->data_node_addr[0]);


		nodeAllocator->linkNext(child2_meta,next_meta);
		nodeAllocator->linkNext(child1_meta,child2_meta);
		//		nodeAllocator->linkNext(child2_meta,skiplist->sa_to_node(old_skipListNode->next[0]));

		SkiplistNode* prev_skiplistNode = old_skiplistNode->prev;
		//		SkiplistNode* next_skiplistNode = skiplist->sa_to_node(old_skiplistNode->next[0]);
		SkiplistNode* next_skiplistNode = skiplist->find_next_node(old_skiplistNode);
		NodeMeta* prev_meta = nodeAllocator->nodeAddr_to_nodeMeta(prev_skiplistNode->data_node_addr[0]);

		child1_sl_node->prev = prev_skiplistNode;
		child2_sl_node->prev = child1_sl_node;
		next_skiplistNode->prev = child2_sl_node;

		_mm_sfence();
		nodeAllocator->linkNext(prev_meta,child1_meta);		//persiste htere------
		_mm_sfence();

		skiplist->insert_node(child1_sl_node,prev_sa_list,next_sa_list);
		skiplist->insert_node(child2_sl_node,prev_sa_list,next_sa_list);

		// prev - 0 - 1 - 2

		//		nodeAllocator->free_node(nodeMeta);

		// should be persist here

		//		child1_sl_node->list_head = child1_sl_node->list_tail = 0;
		//		child2_sl_node->list_head = child2_sl_node->list_tail = 0;

		//		child1_sl_node->update_wc(); // moved to upper
		//		child2_sl_node->update_wc();

		at_unlock2(child1_sl_node->split_lock);
		at_unlock2(child2_sl_node->split_lock);
		at_unlock2(child1_sl_node->insert_lock);
		at_unlock2(child2_sl_node->insert_lock);
		at_unlock2(child1_sl_node->evict_lock);
		at_unlock2(child2_sl_node->evict_lock);
//		at_unlock2(child1_sl_node->key_list_lock);
//		at_unlock2(child2_sl_node->key_list_lock);

		//entry list
		// next dataNodeHeader
		// my_listNode myAddr data_node_addr
		//lock

		//head tail remain_cnt

		//delete old here...

		skiplist->delete_node(old_skiplistNode); // delete duringn find node
#ifdef SPLIT_WITH_LIST_LOCK
		at_unlock2(half_listNode->lock);
#endif
#endif
	}
#endif

	inline bool need_hot_to_warm(SkiplistNode* node)
	{
		if (node->list_head - node->list_tail >= WARM_LOG_LIST_MAX)
			return true;
		return ((node->list_head-node->list_tail)*EXPECTED_ENTRY_SIZE >= WARM_BATCH_MAX_SIZE-NODE_HEADER_SIZE);// TEMP // TODO calc size // may use size sum

	}
#if 0
	bool PH_Thread::try_hot_to_warm(SkiplistNode* node)
	{
		debug_error("do not use now\n");
		//		try_warm_to_cold(node);
		while(true)
		{
			if (need_hot_to_warm(node) == false)
				return false;
			if (try_at_lock2(node->insert_lock))
			{
				if (need_hot_to_warm(node))
				{
					hot_to_warm(node);//,false);
					at_lock2(node->evict_lock);
					at_unlock2(node->insert_lock); // ------------------
								       //					try_warm_to_cold(node);
					if (need_warm_to_cold(node))
						warm_to_cold(node);
					at_unlock2(node->evict_lock);
				}
				else
				{
					at_unlock2(node->insert_lock);
					return false;
				}
			}
			else
				return false;
		}
		// impossible
		return false;
	}
#endif


	int PH_Evict_Thread::test_inv_log(DoubleLog* dl)
	{
		printf ("error not now\n");
#if 0
		unsigned char* addr;
		uint64_t header;
		int rv=0;

		//pass invalid
		while(dl->tail_sum+ENTRY_SIZE <= dl->head_sum)
		{
			addr = dl->dramLogAddr+(dl->tail_sum%dl->my_size);
			header = *(uint64_t*)addr;

			//			if (is_valid(header))
			//				break;
			dl->tail_sum+=ENTRY_SIZE;
			//		dl->check_turn(dl->tail_sum,ble_len);
			if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
				dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
			rv = 1;
		}
		return rv;
#endif
		return 0;
	}

	int PH_Evict_Thread::try_push(DoubleLog* dl)
	{
		unsigned char* addr;
		//		uint64_t header;
		EntryHeader header;
		int rv=0;

		int value_size8;
		int entry_size;

		//pass invalid
		while(dl->tail_sum+NODE_SIZE/*LOG_ENTRY_SIZE*/ <= dl->head_sum)
		{
			addr = dl->dramLogAddr+(dl->tail_sum%dl->my_size);
			header.value = *(uint64_t*)addr;

			if (header.valid_bit)
				break;
			//			value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			value_size8 = header.size;
			value_size8 = get_v8(value_size8);
			entry_size = LOG_ENTRY_SIZE_WITHOUT_VALUE + value_size8;

			dl->tail_sum+=entry_size;
			if (dl->tail_sum%dl->my_size + NODE_SIZE/*LOG_ENTRY_SIZE*/ > dl->my_size)
				dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
			rv = 1;
		}
		return rv;
	}


	int PH_Evict_Thread::try_hard_evict(DoubleLog* dl)
	{
		unsigned char* addr;
		EntryHeader header;
		uint64_t key;
		int rv=0;

		KVP kvp;
		EntryAddr old_ea;
		std::atomic<uint8_t>* seg_lock;

		NodeAddr warm_cache;

		int value_size8;

		//check
		//	if (dl->tail_sum + HARD_EVICT_SPACE > dl->head_sum)
		//	if (dl->tail_sum + ble_len + dl->my_size > dl->head_sum + HARD_EVICT_SPACE)
		//		return rv;
		while(dl->tail_sum + dl->my_size <= dl->head_sum + dl->hard_evict_space && dl->head_sum + dl->hard_evict_space < dl->soft_adv_offset + dl->my_size)//HARD_EVICT_SPACE)
																				   //		if (dl->tail_sum + dl->my_size <= dl->head_sum + HARD_EVICT_SPACE)
		{
			//need hard evict
			addr = dl->dramLogAddr + (dl->tail_sum % dl->my_size);
			header.value = *(uint64_t*)addr;

			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
			//			value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			value_size8 = ((EntryHeader*)addr)->size;
			value_size8 = get_v8(value_size8);
			// evict now

			if (header.valid_bit == false)
			{
				dl->tail_sum+=LOG_ENTRY_SIZE_WITHOUT_VALUE + value_size8;
				if (dl->tail_sum%dl->my_size + NODE_SIZE/*LOG_ENTRY_SIZE*/ > dl->my_size)
					dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
				continue;
			}


			if (false /*&& header.prev_loc == 3*/) // IT WAS HARD HOT TO COLD
			{
#if 0
				old_ea.loc = HOT_LOG;
				old_ea.large = header.large_bit;
				old_ea.file_num = dl->log_num;
				old_ea.offset = dl->tail_sum%dl->my_size;
				kvp.value = old_ea.value;

				direct_to_cold(key,addr + ENTRY_HEADER_SIZE + KEY_SIZE,kvp,seg_lock,NULL,large,false);

				if (old_ea.value == kvp.value) // ok
				{
					//					old_ea.value = kvp.value;

					invalidate_entry(old_ea);
					hash_index->unlock_entry2(seg_lock,read_lock);

					hot_to_cold_cnt++;
				}

				dl->tail_sum+=LOG_ENTRY_SIZE;
				if (dl->tail_sum%dl->my_size + LOG_ENTRY_SIZE > dl->my_size)
					dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
				rv = 1;
#endif
			}
			else
			{
				// use warm cache
				// find skiplist node
				// check split // append // continue
				// hot to warm

#ifdef WARM_CACHE
				warm_cache = *(NodeAddr*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE/*+SIZE_SIZE*/+value_size8);
#else
				warm_cache = emptyNodeAddr;
#endif

				//				tes(SKIP_LOCK);
				SkiplistNode* node;
				//				node = get_skiplist_node(key,warm_cache);
				node = get_skiplist_node_for_insert(key,warm_cache);

				//				tee(SKIP_LOCK);

				if (may_split_warm_node(node,2))
					continue;
				//if (target_batch < 0) // split suceesss
				//	continue;

				//NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
				//at_lock2(nodeMeta->rw_lock);
				//				hot_to_warm(node,false); // always partial...
				//				at_lock2(node->insert_lock);
				hot_to_warm(node);
//				at_lock2(node->evict_lock);
				at_unlock2(node->insert_lock);
				//				try_warm_to_cold(node);

				// may need warm split or something
//				if (need_warm_to_cold(node))
//					warm_to_cold(node);
//				split_warm_node = may_split_warm_node(node,2); // lock???
									       //				at_unlock2(node->evict_lock);
									       //	at_unlock2(nodeMeta->rw_lock);

#if 0 // becaus hard evict is always parital there is still space i think
				NodeMeta *nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);

				if (NODE_HEADER_SIZE + nodeMeta->written_size + ENTRY_SIZE > NODE_SIZE)
				{
					try_push(dl);
					warm_to_cold(node);
					at_unlock2(node->lock);
					rv = 1;
					continue;
				}
#endif
				//	dl->head_sum+=ble_len;

				// do we need flush?
				/* // list order may not sorted
				   dl->tail_sum+=ENTRY_SIZE;
				   if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
				   dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
				 */

				//check if we need warm merge before unlock the node
				/*
				if (false && node->recent_entry_cnt < WARM_NODE_ENTRY_CNT && node->ver > 3) // DO NOT DELETE  // still have lock
				{
					flush_warm_node(node);
					skiplist->delete_node(node); // it will be jumped during find_node
				}
				else
				{
					//					if (node->data_tail + (WARM_NODE_ENTRY_CNT-WARM_BATCH_ENTRY_CNT) <= node->data_head)
					//						warm_to_cold(node);
					if (split_warm_node == 0) // didn't split
						at_unlock2(node->evict_lock);
					//						at_unlock2(node->lock);
				}
				*/

				hard_htw_cnt++;

				rv = 1;
			}
		}

		return rv;
	}

	int PH_Evict_Thread::try_soft_evict(DoubleLog* dl) // need return???
	{
		unsigned char* addr;
		uint64_t key;
		EntryHeader header;
		int rv = 0;

		SkiplistNode* node;
		NodeMeta* nodeMeta;
		LogLoc ll;
		NodeAddr warm_cache;
		int value_size8;

		//	if (dl->tail_sum + SOFT_EVICT_SPACE > dl->head_sum)
		//		return rv;

		//	while(dl->tail_sum+adv_offset + ble_len <= dl->head_sum && adv_offset <= SOFT_EVICT_SPACE )
		//	while (adv_offset <= SOFT_EVICT_SPACE)

		//
		if (dl->soft_adv_offset < dl->tail_sum) // jump if passed
			dl->soft_adv_offset = dl->tail_sum;

		while(dl->soft_adv_offset + /*LOG_ENTRY_SIZE*/ NODE_SIZE + dl->my_size <= dl->head_sum + dl->soft_evict_space)//SOFT_EVICT_SPACE)
		{
			//		if (dl->tail_sum + dl->my_size <= dl->head_sum + dl->hard_evict_space && dl->head_sum + dl->hard_evict_space < dl->soft_adv_offset + dl->my_size)//HARD_EVICT_SPACE)
			//			break;

			//			if (dl->soft_adv_offset != dl->head_sum_log[dl->adv_cnt])
			//				debug_error("not match\n");

			addr = dl->dramLogAddr + ((dl->soft_adv_offset) % dl->my_size);
			header.value = *(uint64_t*)addr;

			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
			//			value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			value_size8 = header.size;
			value_size8 = get_v8(value_size8);

			if (header.valid_bit)// && header.prev_loc != 3)// && is_checked(header) == false)
			{
				rv = 1;

				// regist the log num and size_t
#ifdef WARM_CACHE 
				warm_cache = *(NodeAddr*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE+value_size8);
#else
				warm_cache = emptyNodeAddr;
#endif
				node = get_skiplist_node_for_insert(key,warm_cache);

				// need to traverse to confirm the range...
				//					nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
#if 0
				if (NODE_HEADER_SIZE + nodeMeta->written_size + node->entry_size_sum + ENTRY_SIZE > NODE_SIZE) // NODE_BUFFER_SIZE???
				{
					// warm is full
					hot_to_warm(node,true);
					soft_htw_cnt++;

					if (NODE_HEADER_SIZE + nodeMeta->written_size + ENTRY_SIZE > NODE_SIZE)
						warm_to_cold(node);
					at_unlock2(node->lock);
					continue; // retry
				}
#endif
				//					at_lock2(node->insert_lock);

				// need to try flush
				//			node->try_hot_to_warm();
				//	if (node->entry_size_sum >= SOFT_BATCH_SIZE)
				//					if (node->current_batch_size + node->list_size_sum > WARM_BATCH_MAX_SIZE || node->list_head - node->list_tail >= NODE_SLOT_MAX)
				//					if (node->list_head - node->list_tail >= 8)//NODE_SLOT_MAX) // temp
				if (need_hot_to_warm(node))
					//					if (node->list_head - node->list_tail >= NODE_SLOT_MAX) // temp
				{
					list_gc(node);
					if (need_hot_to_warm(node))
					{
//						hot_to_warm(node,target_batch);//,false);
/*
						int target_batch;
						target_batch = may_split_warm_node(node,2);
						if (target_batch < 0)
						{
							continue; // node split and retry
//							at_unlock2(node->evict_lock);
						}
						*/
						if (may_split_warm_node(node,2))
							continue;

						hot_to_warm(node);
//						at_lock2(node->evict_lock);
//						at_unlock2(node->insert_lock); // ------------------
									       //					try_warm_to_cold(node);


						soft_htw_cnt++;
						//							at_unlock2(node->insert_lock);
					}
//					else
//						at_unlock2(node->insert_lock);
					//						else
					//							at_unlock2(node->insert_lock);
#if 0
					//						if (node->current_batch_size + node->list_size_sum > WARM_BATCH_MAX_SIZE || node->list_head - node->list_tail >= NODE_SLOT_MAX)
					if (node->list_head - node->list_tail >= 8)//NODE_SLOT_MAX) // temp
										   //						if (node->list_head - node->list_tail >= NODE_SLOT_MAX) // temp
					{
						//	NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
						//	at_lock2(nodeMeta->rw_lock);
						hot_to_warm(node,false);
						at_unlock2(node->insert_lock);
						may_split_warm_node(node);
						//	at_unlock2(nodeMeta->rw_lock);
						//	if (node->data_tail + (WARM_NODE_ENTRY_CNT-WARM_BATCH_ENTRY_CNT) <= node->data_head)
						//	warm_to_cold(node); // in lock???
						soft_htw_cnt++;
					}
#endif
				}

				// anyway hot to warm or not , the node is locked and can be inserted
				//add entry
				ll.log_num = dl->log_num;
				ll.offset = dl->soft_adv_offset;
				ll.size = ENTRY_SIZE_WITHOUT_VALUE+value_size8;

				//try push
				//	if (node->tail + WARM_NODE_ENTRY_CNT <= node->head)
				//	printf("warm node full!!\n");
				//	else
				//					if (node->list_tail + WARM_NODE_ENTRY_CNT > node->list_head) // list has space
				//					if (node->list_head - node->list_tail < WARM_NODE_ENTRY_CNT)//WARM_BATCH_ENTRY_CNT)// WARM_LOG_MIN)
				//					if (node->list_head - node->list_tail < NODE_SLOT_MAX)// WARM_LOG_MIN)
				//					if (node->current_batch_size + node->list_size_sum <= WARM_BATCH_MAX_SIZE && node->list_head - node->list_tail < NODE_SLOT_MAX)
				{
					//						node->entry_list[node->list_head%WARM_NODE_ENTRY_CNT] = ll;

#ifdef ENTRY_LIST_CHECK
					if (node->list_head-node->list_tail >= WARM_LOG_LIST_MAX)//NODE_SLOT_MAX)
						debug_error("entry lsit full!!\n");
#endif
					node->entry_list[node->list_head%WARM_LOG_LIST_MAX] = ll;
					node->list_head++; // lock...
					node->list_size_sum+=ll.size;
				}

				at_unlock2(node->insert_lock);
			}

			// padding
			dl->soft_adv_offset+=LOG_ENTRY_SIZE_WITHOUT_VALUE + value_size8;//LOG_ENTRY_SIZE;
			if ((dl->soft_adv_offset)%dl->my_size  + NODE_SIZE/*LOG_ENTRY_SIZE*/ > dl->my_size)
				dl->soft_adv_offset+=(dl->my_size-(dl->soft_adv_offset%dl->my_size));
			//			dl->adv_cnt++;

			// don't move tail sum
		}
		return rv;
	}

	//#define TEST_INV

	int PH_Evict_Thread::evict_log(DoubleLog* dl)
	{
#ifdef TEST_INV
		if (test_inv_log(dl))
			return 1;
		return 0;
#else
		int diff=0;
		if (try_soft_evict(dl))
			diff = 1;
		if (try_push(dl))
			diff = 1;

		if (try_hard_evict(dl))
			diff = 1;
		if (try_push(dl))
			diff = 1;
		return diff;		
#endif
	}

	void PH_Evict_Thread::evict_loop()
	{
		int i,done;
		//	while(done == 0)
		//		printf("evict start\n");
		while(exit == 0)
		{
			//		update_free_cnt();
			op_check();
#ifdef SYNCER
			if (evict_counter >= query_counter)
			{
				usleep(1);
				continue;
			}
#endif

			done = 1;
			for (i=0;i<log_cnt;i++)
			{
				if (evict_log(log_list[i]))
					done = 0;
			}
			if (done)
			{
				usleep(1);
				//				asm("nop");
			}
#if 0
			if (done)
			{
				run = 0;
				if (sleep_time > 1000*1000)
				{
					printf("evict idle %d\n",thread_id);
					//				usleep(1000*1000);

					usleep(sleep_time);
				}
				/*
				   _mm_mfence();
				   sync_thread();
				   _mm_mfence();
				 */
				run = 1;
				if (sleep_time < 1000*1000)
					sleep_time*=1.5;
			}
			else
			{
				if (sleep_time > 1000*2)
					sleep_time*=0.5;
			}
#endif
#ifdef SYNCER
	evict_counter++;
#endif
		}
		run = 0;
		//		printf("evict end\n");
	}

}
