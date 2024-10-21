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

#define ADDR2

namespace PH
{
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

	//PH_Thread thred_list[QUERY_THREAD_MAX+EVICT_THREAD_MAX];
	PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];
	PH_Evict_Thread evict_thread_list[EVICT_THREAD_MAX];

	extern std::atomic<uint64_t> global_seq_num[COUNTER_MAX];

	//check
	extern std::atomic<uint64_t> warm_to_warm_sum;
	extern std::atomic<uint64_t> warm_log_write_sum;
	extern std::atomic<uint64_t> log_write_sum;
	extern std::atomic<uint64_t> hot_to_warm_sum;
	extern std::atomic<uint64_t> warm_to_cold_sum;
	extern std::atomic<uint64_t> direct_to_cold_sum;
	extern std::atomic<uint64_t> hot_to_hot_sum;
	extern std::atomic<uint64_t> hot_to_cold_sum;

	extern std::atomic<uint64_t> soft_htw_sum;
	extern std::atomic<uint64_t> hard_htw_sum;

	extern std::atomic<uint64_t> htw_time_sum;
	extern std::atomic<uint64_t> wtc_time_sum;
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

	//	extern const size_t WARM_BATCH_MAX_SIZE;
	//	extern size_t WARM_BATCH_MAX_SIZE;
	/*
	   extern size_t WARM_BATCH_ENTRY_CNT;
	   extern size_t WARM_GROUP_ENTRY_CNT;
	   extern size_t WARM_BATCH_CNT;
	   extern size_t WARM_NODE_ENTRY_CNT;
	   extern size_t WARM_LOG_MAX;
	   extern size_t WARM_LOG_MIN;
	   extern size_t WARM_LOG_THRESHOLD;
	 */
	extern Skiplist* skiplist;
	extern PH_List* list;
	//extern const size_t MAX_LEVEL;

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
		warm_log_write_cnt = log_write_cnt = hot_to_warm_cnt = warm_to_cold_cnt = direct_to_cold_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		warm_to_warm_cnt = 0;
		soft_htw_cnt = hard_htw_cnt = 0;

		dtc_time = htw_time = wtc_time = 0;
		htw_cnt = wtc_cnt = 0;

		reduce_group_cnt = 0;
		list_merge_cnt = 0;
#ifdef WARM_STAT
		warm_hit_cnt = warm_miss_cnt = warm_no_cnt = 0;
#endif
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

	}
	void PH_Thread::buffer_clean()
	{
		free(evict_buffer);
		free(entry_buffer);

		free(split_buffer);
		free(sorted_buffer1);
		free(sorted_buffer2);

	}


	//-------------------------------------------------

	void PH_Query_Thread::init()
	{
		buffer_init();

		//		evict_buffer = (unsigned char*)malloc(WARM_BATCH_MAX_SIZE);

		//		key_list_buffer = (uint64_t*)malloc(sizeof(uint64_t) * MAX_NODE_GROUP*(NODE_SIZE/ENTRY_SIZE));
		//		old_ea_list_buffer = (EntryAddr*)malloc(sizeof(EntryAddr) * MAX_NODE_GROUP*(NODE_SIZE/ENTRY_SIZE));

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
		//		printf("sfd %u\n",seed_for_dtc);
	}

	void PH_Thread::check_end()
	{
		//check
		warm_to_warm_sum+=warm_to_warm_cnt;
		warm_log_write_sum+=warm_log_write_cnt;
		log_write_sum+=log_write_cnt;
		hot_to_warm_sum+=hot_to_warm_cnt;
		warm_to_cold_sum+=warm_to_cold_cnt;
		direct_to_cold_sum+=direct_to_cold_cnt;
		hot_to_hot_sum+=hot_to_hot_cnt;
		hot_to_cold_sum+=hot_to_cold_cnt;

		soft_htw_sum+=soft_htw_cnt;
		hard_htw_sum+=hard_htw_cnt;

		htw_time_sum+=htw_time;
		wtc_time_sum+=wtc_time;
		htw_cnt_sum+=htw_cnt;
		wtc_cnt_sum+=wtc_cnt;

		dtc_time_sum+=dtc_time;
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
	}

	void PH_Query_Thread::clean()
	{
		buffer_clean();
		//		delete evict_buffer;

		//		delete key_list_buffer;
		//		delete old_ea_list_buffer;

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
	}	

	EntryAddr insert_entry_to_slot(NodeMeta* nodeMeta,unsigned char* src_addr, uint64_t value_size8) // need lock from outside
	{
		bool large_value;
		if (value_size8 == INV64)
		{
			value_size8 = 8;
			large_value = true;
		}
		else
			large_value = false;

		if (value_size8 > nodeMeta->max_empty)
			return emptyEntryAddr;


		// anyway we will scan the array
		// try best fit...

		// 1 scan and merge invalid entries
		// 2 insert..

		int entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
		EntryAddr new_ea;
		int bfv;	
		int bfi = nodeMeta->el_clean(entry_size,bfv);
		if (bfi < 0) // no space
			return emptyEntryAddr;

		bool fit;
		EntryHeader jump;

		if (bfv == entry_size) // fit
		{
			fit = true;
		}
		else
		{
			if (nodeMeta->el_cnt >= NODE_SLOT_MAX) // ... just pass
				return emptyEntryAddr;

			fit = false;
			jump.valid_bit = 0;
			jump.delete_bit = 0;
			jump.version = nodeMeta->entryLoc[bfi+1].offset;
		}

		// copy data first..

		new_ea.loc = 3; // cold
		new_ea.large = large_value;
		new_ea.file_num = nodeMeta->my_offset.pool_num;
		//		if (slot_idx < NODE_SLOT_MAX)
		{
			//			old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;
			new_ea.offset = nodeMeta->my_offset.node_offset*NODE_SIZE + nodeMeta->entryLoc[bfi].offset; //NODE_HEADER_SIZE + ENTRY_SIZE*slot_idx;

			DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
			if (fit)
			{
				pmem_entry_write((unsigned char*)dataNode + nodeMeta->entryLoc[bfi].offset , src_addr, entry_size);
				nodeMeta->entryLoc[bfi].valid = 1;
			}
			else // not fit need jump
			{
				// write entry
				unsigned char* dst = (unsigned char*)dataNode+nodeMeta->entryLoc[bfi].offset;
				memcpy(dst+ENTRY_HEADER_SIZE,src_addr+ENTRY_HEADER_SIZE,entry_size-ENTRY_HEADER_SIZE);
				memcpy(dst+entry_size,&jump,ENTRY_HEADER_SIZE);
				pmem_persist(dst+ENTRY_HEADER_SIZE,entry_size);
				_mm_sfence();
				memcpy(dst,src_addr,ENTRY_HEADER_SIZE); // write version
				pmem_persist(dst,ENTRY_HEADER_SIZE);
				_mm_sfence();

				int i;
				for (i=nodeMeta->el_cnt;i>bfi+1;i--)
					nodeMeta->entryLoc[i] = nodeMeta->entryLoc[i-1];
				nodeMeta->el_cnt++;
				nodeMeta->entryLoc[bfi].valid = 1;
				nodeMeta->entryLoc[bfi+1].valid = 0;
				nodeMeta->entryLoc[bfi+1].offset = nodeMeta->entryLoc[bfi].offset + entry_size;
			}

			//			nodeMeta->valid[slot_idx] = true; // validate
			//			++nodeMeta->valid_cnt;
			nodeMeta->size_sum+=entry_size;

			//			ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr.value);
			ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr);
			//			listNode->valid_cnt++;
			listNode->size_sum+=entry_size;

			// modify hash index here
			//					kvp_p = hash_index->insert(key,&seg_lock,read_lock);

			//					set_loc_cold(kvp_p->version);
			//				kvp_p->version = set_loc_cold(kvp_p->version);
			//					kvp_p->value = (uint64_t)addr;
			/*
			   kvp_p->value = new_ea.value;
			   _mm_sfence();
			   hash_index->unlock_entry2(seg_lock,read_lock);
			 */
			//					nodeMeta->valid[cnt] = false; //invalidate
			// not here...
			//			++cnt;
			//			src_offset+=ENTRY_SIZE;
			//			at_unlock2(list_nodeMeta->rw_lock);
		}

		return new_ea;
	}


	void PH_Thread::split_listNode_group(ListNode *listNode,SkiplistNode *skiplistNode) // MAKE MANY BUGS
	{
		// lock all the nodes
		// copy
		//scan all keys
		//sort them
		//relocate
		//memcpy to pmem ( USE NEW NODES!)
		// modify index
		// free---- need versio nnumber???

		split_key_list.clear();
		sorted_entry_size.clear();
		NodeMeta *list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
		//		NodeMeta *half_list_nodeMeta;
		DataNode *list_dataNode_p[MAX_NODE_GROUP];
		NodeMeta *old_nodeMeta[MAX_NODE_GROUP];

		//		if (list_nodeMeta->list_addr.value != nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr).value)
		//			debug_error("cold block bug\n");

		unsigned char* addr;
		int offset;
		int group0_idx=0;
		uint64_t key;
		SecondOfPair second;
		EntryAddr ea;

		ea.loc = 3;

		int i;

		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;

		int base_offset;
		size_t list_size_sum=listNode->size_sum;

		while(list_nodeMeta)
		{
			at_lock2(list_nodeMeta->rw_lock); // lock the node

			list_dataNode_p[group0_idx] = nodeAllocator->nodeAddr_to_node(list_nodeMeta->my_offset);
			split_buffer[group0_idx] = *list_dataNode_p[group0_idx]; // pmem to dram
			old_nodeMeta[group0_idx] = list_nodeMeta;

			//			addr = split_buffer[group0_idx].buffer;
			addr = (unsigned char*)&split_buffer[group0_idx];

			ea.file_num = list_nodeMeta->my_offset.pool_num;
			base_offset = list_nodeMeta->my_offset.node_offset * NODE_SIZE;

			for (i=0;i<list_nodeMeta->el_cnt;i++) // find valid entries
			{
				if (list_nodeMeta->entryLoc[i].valid)
				{
					offset = list_nodeMeta->entryLoc[i].offset;
					key = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE);
					second.addr = addr+offset;
					ea.large = ((EntryHeader*)(addr+offset))->large_bit;
					ea.offset = base_offset + offset;
					second.ea = ea;
					split_key_list.push_back(std::make_pair(key,second)); // addr in temp dram
				}
			}

			list_nodeMeta = list_nodeMeta->next_node_in_group;
			group0_idx++;
		}

		//sort them //pass
		std::sort(split_key_list.begin(),split_key_list.end());
		//relocate
		// fixed size here

		//1 copy and calc size
		//2 set half key

		// 1 check size
		// 2 alloc...

		size_t group1_size_sum = 0;
		int entry_size;
		EntryHeader end_jump;
		end_jump.value = 0;

		int j;
		unsigned char* src_addr;
		uint64_t value_size8;
		//-------------------------------------------------------------------------------
		//		if (key_list.size() > 0)
		//			printf("test here\n");

		int group1_idx = 0;
		memset(&sorted_buffer1[0],0,NODE_SIZE);
		addr = (unsigned char*)&sorted_buffer1[0];
		offset = NODE_HEADER_SIZE;
		i = 0;
		j = 0;


		int size = split_key_list.size();

		ListNode* new_listNode;
		new_listNode = list->alloc_list_node();


		int split_type; // only for debug
		if (list_size_sum > NODE_SIZE)
		{
			split_type = 1; // based size

			while(i < size-1) // size == 1 will go right...
			{
				src_addr = split_key_list[i].second.addr;
				value_size8 = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE+KEY_SIZE);
				value_size8 = get_v8(value_size8);
				entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
				sorted_entry_size.push_back(entry_size);

				if (offset+entry_size + ENTRY_HEADER_SIZE > NODE_SIZE || j >= NODE_SLOT_MAX-1) // + JUMP
				{
					memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);
					group1_idx++;
					j = 0;
					memset(&sorted_buffer1[group1_idx],0,NODE_SIZE);
					addr = (unsigned char*)&sorted_buffer1[group1_idx];
					offset = NODE_HEADER_SIZE;
				}
				memcpy(addr+offset,split_key_list[i].second.addr,entry_size);
				offset+=entry_size;
				i++;
				j++;
				group1_size_sum+=entry_size;
				if (group1_size_sum*2 >= list_size_sum) // split based size
					break;
			}
			new_listNode->key = split_key_list[i].first;

		}
		else
		{
			split_type = 2; // based key
			uint64_t half1,half2;
			half1 = listNode->key/2;
			half2 = listNode->next->key/2;
			new_listNode->key = half1+half2;

			while(i < size && split_key_list[i].first < new_listNode->key)
			{
				src_addr = split_key_list[i].second.addr;
				value_size8 = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE+KEY_SIZE);
				value_size8 = get_v8(value_size8);
				entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
				sorted_entry_size.push_back(entry_size);

				if (offset+entry_size + ENTRY_HEADER_SIZE > NODE_SIZE || j >= NODE_SLOT_MAX-1) // + JUMP
				{
					memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);
					group1_idx++;
					j = 0;
					memset(&sorted_buffer1[group1_idx],0,NODE_SIZE);
					addr = (unsigned char*)&sorted_buffer1[group1_idx];
					offset = NODE_HEADER_SIZE;
				}
				memcpy(addr+offset,split_key_list[i].second.addr,entry_size);
				offset+=entry_size;
				i++;
				j++;
			}
		}
		memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);

		int ih = i; // i half

		j = 0;
		int group2_idx = 0;
		memset(&sorted_buffer2[0],0,NODE_SIZE);
		addr = (unsigned char*)&sorted_buffer2[0];
		offset = NODE_HEADER_SIZE;
		for (;i<size;i++)
		{
			src_addr = split_key_list[i].second.addr;
			value_size8 = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			value_size8 = get_v8(value_size8);
			entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
			sorted_entry_size.push_back(entry_size);

			if (offset+entry_size + ENTRY_HEADER_SIZE > NODE_SIZE || j >= NODE_SLOT_MAX-1) 
			{
				memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);
				group2_idx++;
				j = 0;
				memset(&sorted_buffer2[group2_idx],0,NODE_SIZE);
				addr = (unsigned char*)&sorted_buffer2[group2_idx];
				offset = NODE_HEADER_SIZE;
			}
			memcpy(addr+offset,split_key_list[i].second.addr,entry_size);
			offset+=entry_size;
			j++;
		}

		//alloc dst 
		NodeAddr new_nodeAddr1[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta1[MAX_NODE_GROUP];
		NodeAddr new_nodeAddr2[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta2[MAX_NODE_GROUP];

		// do not change listNode just link new nodemeta

		listNode->warm_cache = skiplistNode->myAddr;
		new_listNode->warm_cache = skiplistNode->myAddr;

		for (i=0;i<=group1_idx;i++)
		{
			new_nodeAddr1[i] = nodeAllocator->alloc_node(COLD_LIST);
			new_nodeMeta1[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr1[i]);
			new_nodeMeta1[i]->group_cnt = i+1;
			new_nodeMeta1[i]->list_addr = nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr);
			//			new_nodeMeta1[i]->list_addr = listNode->myAddr;
			at_lock2(new_nodeMeta1[i]->rw_lock); // ------------------------------- lock here!!!
		}
		for (i=0;i<=group2_idx;i++)
		{
			new_nodeAddr2[i] = nodeAllocator->alloc_node(COLD_LIST);
			new_nodeMeta2[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr2[i]);
			new_nodeMeta2[i]->group_cnt = i+1;
			new_nodeMeta2[i]->list_addr = nodeAddr_to_listAddr(COLD_LIST,new_listNode->myAddr);
			//			new_nodeMeta2[i]->list_addr = new_listNode->myAddr;
			at_lock2(new_nodeMeta2[i]->rw_lock); // ------------------------------- lock here!!!
		}

		for (i=0;i<group1_idx;i++) // connect
		{
			new_nodeMeta1[i]->next_node_in_group = new_nodeMeta1[i+1];
			new_nodeMeta1[i]->next_addr_in_group = new_nodeMeta1[i+1]->my_offset;
			sorted_buffer1[i].next_offset = emptyNodeAddr;
			sorted_buffer1[i].next_offset_in_group = new_nodeMeta1[i+1]->my_offset;
		}
		for (i=0;i<group2_idx;i++) // connect
		{
			new_nodeMeta2[i]->next_node_in_group = new_nodeMeta2[i+1];
			new_nodeMeta2[i]->next_addr_in_group = new_nodeMeta2[i+1]->my_offset;
			sorted_buffer2[i].next_offset = emptyNodeAddr;
			sorted_buffer2[i].next_offset_in_group = new_nodeMeta2[i+1]->my_offset;
		}

		sorted_buffer1[group1_idx].next_offset_in_group = emptyNodeAddr;
		sorted_buffer2[group2_idx].next_offset_in_group = emptyNodeAddr;

		new_nodeMeta2[0]->next_p = old_nodeMeta[0]->next_p;
		new_nodeMeta2[0]->next_addr = old_nodeMeta[0]->next_addr;
		new_nodeMeta1[0]->next_p = new_nodeMeta2[0];
		new_nodeMeta1[0]->next_addr = new_nodeMeta2[0]->my_offset;

		// 0 -> half -> next
		// [0 -> 1 ..] / [half -> half+1 ..]

		//memcpy to pmem

		// link!?
		//		sorted_temp_dataNode[MAX_NODE_GROUP/2-1].next_offset_in_group = emptyNodeAddr;
		sorted_buffer2[0].next_offset = old_nodeMeta[0]->next_p->my_offset; // thread bug here old_nodeMeta[0]->next is null // TODO fix this // may fixed..
		sorted_buffer1[0].next_offset = new_nodeMeta2[0]->my_offset;

		// fill new nodes
		//		EntryAddr ea;

		DataNode* new_dataNode;
		for (i=0;i<=group1_idx;i++)
		{
			new_dataNode = nodeAllocator->nodeAddr_to_node(new_nodeMeta1[i]->my_offset);
			pmem_nt_write((unsigned char*)new_dataNode,(unsigned char*)&sorted_buffer1[i],NODE_SIZE);
		}
		for (i=0;i<=group2_idx;i++)
		{
			new_dataNode = nodeAllocator->nodeAddr_to_node(new_nodeMeta2[i]->my_offset);
			pmem_nt_write((unsigned char*)new_dataNode,(unsigned char*)&sorted_buffer2[i],NODE_SIZE);
		}

		size_t start_offset;
		EntryAddr dst_ea;

		group1_idx = 0;
		addr = (unsigned char*)&sorted_buffer1[0];
		offset = NODE_HEADER_SIZE;
		j = 0;
		start_offset = new_nodeMeta1[0]->my_offset.node_offset * NODE_SIZE;
		dst_ea.file_num = new_nodeMeta1[0]->my_offset.pool_num;
		dst_ea.loc = COLD_LIST;

		for (i=0;i<ih;i++) // mvoing kvp
		{
			entry_size = sorted_entry_size[i];
			if (offset + entry_size + ENTRY_HEADER_SIZE > NODE_SIZE || j >= NODE_SLOT_MAX-1)
			{
				new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
				new_nodeMeta1[group1_idx]->entryLoc[j].offset = offset;
				j++;

				new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
				new_nodeMeta1[group1_idx]->entryLoc[j].offset = NODE_SIZE;
				new_nodeMeta1[group1_idx]->el_cnt = j+1;

				group1_idx++;
				addr = (unsigned char*)&sorted_buffer1[group1_idx];
				offset = NODE_HEADER_SIZE;
				j = 0;
				start_offset = new_nodeMeta1[group1_idx]->my_offset.node_offset * NODE_SIZE;
				dst_ea.file_num = new_nodeMeta1[group1_idx]->my_offset.pool_num;
			}

			//			key = key_list_buffer[i];
			key = split_key_list[i].first;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.value = kvp_p->value;
#ifdef SPLIT_KEY_TEST
			if (key >= new_listNode->key)
				debug_error("split erorrr1\n");
#endif
			new_nodeMeta1[group1_idx]->entryLoc[j].offset = offset;

			//			if (ea.value == old_ea_list_buffer[i].value)
			if (ea.value == split_key_list[i].second.ea.value)
			{
				new_nodeMeta1[group1_idx]->entryLoc[j].valid = 1;
				new_nodeMeta1[group1_idx]->size_sum+=entry_size;

				//				new_nodeMeta1[group1_idx]->valid[j] = true;
				//				new_nodeMeta1[group1_idx]->valid_cnt++;

				dst_ea.large = ea.large;
				dst_ea.offset = start_offset + offset;
				kvp_p->value = dst_ea.value;
#ifdef DST_CHECK
				EA_test(key,dst_ea);
#endif
			}
			else
			{
				new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
				//				new_nodeMeta1[group1_idx]->valid[j] = false;
			}
			hash_index->unlock_entry2(seg_lock,read_lock);

			j++;
			offset+=entry_size;
		}

		new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
		new_nodeMeta1[group1_idx]->entryLoc[j].offset = offset;
		j++;

		new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
		new_nodeMeta1[group1_idx]->entryLoc[j].offset = NODE_SIZE;
		new_nodeMeta1[group1_idx]->el_cnt = j+1;

		group2_idx = 0;
		addr = (unsigned char*)&sorted_buffer2[0];
		offset = NODE_HEADER_SIZE;
		j = 0;
		start_offset = new_nodeMeta2[0]->my_offset.node_offset * NODE_SIZE;
		dst_ea.file_num = new_nodeMeta2[0]->my_offset.pool_num;
		dst_ea.loc = COLD_LIST;

		for (;i<size;i++)
		{
			entry_size = sorted_entry_size[i];
			if (offset + entry_size + ENTRY_HEADER_SIZE  > NODE_SIZE || j >= NODE_SLOT_MAX-1)
			{
				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
				new_nodeMeta2[group2_idx]->entryLoc[j].offset = offset;
				j++;

				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
				new_nodeMeta2[group2_idx]->entryLoc[j].offset = NODE_SIZE;
				new_nodeMeta2[group2_idx]->el_cnt = j+1;

				group2_idx++;
				addr = (unsigned char*)&sorted_buffer2[group2_idx];
				offset = NODE_HEADER_SIZE;
				j = 0;
				start_offset = new_nodeMeta2[group2_idx]->my_offset.node_offset * NODE_SIZE;
				dst_ea.file_num = new_nodeMeta2[group2_idx]->my_offset.pool_num;
			}

			//			key = key_list_buffer[i];
			key = split_key_list[i].first;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.value = kvp_p->value;

#ifdef SPLIT_KEY_TEST
			if (key < new_listNode->key)
				debug_error("split erorrr2\n");
#endif

			new_nodeMeta2[group2_idx]->entryLoc[j].offset = offset;

			//			if (ea.value == old_ea_list_buffer[i].value)
			if (ea.value == split_key_list[i].second.ea.value)
			{
				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 1;
				new_nodeMeta2[group2_idx]->size_sum+=entry_size;

				//				new_nodeMeta2[group2_idx]->valid[j] = true;
				//				new_nodeMeta2[group2_idx]->valid_cnt++;

				dst_ea.large = ea.large;
				dst_ea.offset = start_offset + offset;
				kvp_p->value = dst_ea.value;
#ifdef DST_CHECK
				EA_test(key,dst_ea);
#endif
			}
			else
			{
				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
				//				new_nodeMeta2[group2_idx]->valid[j] = false;
			}
			hash_index->unlock_entry2(seg_lock,read_lock);

			j++;
			offset+=entry_size;
		}

		new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
		new_nodeMeta2[group2_idx]->entryLoc[j].offset = offset;
		j++;

		new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
		new_nodeMeta2[group2_idx]->entryLoc[j].offset = NODE_SIZE;
		new_nodeMeta2[group2_idx]->el_cnt = j+1;

		_mm_sfence();

		// link the list
		// link pmem first then dram...

		// free the nodes...
		for (i=0;i<group0_idx;i++)
			nodeAllocator->free_node(old_nodeMeta[i]);

		// don't alloc new node...
		listNode->data_node_addr = new_nodeMeta1[0]->my_offset;
		new_listNode->data_node_addr = new_nodeMeta2[0]->my_offset;


		if (new_listNode->key > listNode->next->key)
			debug_error("reverse key\n");
		if (listNode->key == new_listNode->key)
			debug_error("split key error\n");
		if (new_listNode->key == 0)
			debug_error("eku0\n");

		new_listNode->prev = listNode;

		listNode->block_cnt = group1_idx+1;
		new_listNode->block_cnt = group2_idx+1;

		_mm_sfence();

		listNode->next->prev = new_listNode;
		new_listNode->next = listNode->next;
		listNode->next = new_listNode;

		_mm_sfence();
#if 0
		ListNode* prev;
		while(1)
		{
			prev = listNode->prev;
			at_lock2(prev->lock);
			if (prev->next != listNode)
			{
				at_unlock2(prev->lock);
				continue;
			}
			break;
		}

		NodeMeta* prev_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(prev->data_node_addr);
		nodeAllocator->linkNext(prev_nodeMeta,new_nodeMeta1[0]);

		at_unlock2(prev->lock);
		_mm_sfence();
#else
		ListNode* prev = listNode->prev;
		NodeMeta* prev_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(prev->data_node_addr);
		nodeAllocator->linkNext(prev_nodeMeta,new_nodeMeta1[0]);

		_mm_sfence();

#endif
#if 0
		// listNode is never deleted just split
		SkiplistNode* next_skiplistNode;
		while(true)
		{
			next_skiplistNode = skiplist->sa_to_node(skiplistNode->next[0]);
			if (try_at_lock2(next_skiplistNode->lock) == false)
				continue;
			if (skiplistNode->next[0].value != next_skiplistNode->my_sa.value)
			{
				at_unlock2(next_skiplistNode->lock);
				continue;
			}
			break;
		}
		if (next_skiplistNode->key >= new_listNode->key)
			next_skiplistNode->my_listNode = new_listNode;
		at_unlock2(next_skiplistNode->lock);
#endif
		// unlock

		listNode->size_sum = 0;
		for (i=0;i<=group1_idx;i++)
			listNode->size_sum+=new_nodeMeta1[i]->size_sum;
		new_listNode->size_sum = 0;
		for (i=0;i<=group2_idx;i++)
			new_listNode->size_sum+=new_nodeMeta2[i]->size_sum;

		for (i=0;i<=group1_idx;i++)
			at_unlock2(new_nodeMeta1[i]->rw_lock);		
		for (i=0;i<=group2_idx;i++)
			at_unlock2(new_nodeMeta2[i]->rw_lock);		

#if 0 // test old

		group0_idx = 0;
		//		list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
		list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(test_addr1);
		while (list_nodeMeta)
		{
			list_dataNode_p[group0_idx] = nodeAllocator->nodeAddr_to_node(list_nodeMeta->my_offset);
			split_buffer[group0_idx] = *list_dataNode_p[group0_idx]; // pmem to dram
			old_nodeMeta[group0_idx] = list_nodeMeta;

			addr = split_buffer[group0_idx].buffer;
			offset = 0;

			ea.file_num = list_nodeMeta->my_offset.pool_num;
			ea.offset = list_nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

			i = 0;
			while(offset+ENTRY_SIZE <= NODE_BUFFER_SIZE) // node is full
			{
				if (list_nodeMeta->valid[i])
				{
					key = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE);
					second.addr = addr+offset;
					second.ea = ea;
					key_list.push(std::make_pair(key,second)); // addr in temp dram

#if 0
					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					if (kvp_p->value == ea.value)
						debug_error("not here\n");
					hash_index->unlock_entry2(seg_lock,read_lock);
#endif


				}
				ea.offset+=ENTRY_SIZE;
				offset+=ENTRY_SIZE;
				i++;
			}

			list_nodeMeta = list_nodeMeta->next_node_in_group;
			group0_idx++;

		}


#endif

	}
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

	void PH_Thread::try_cold_split(ListNode *listNode,SkiplistNode* node) // have cold but may warm lock or not
	{ // unlock the rws
	  //split cold here
	  //find half

	  // this node is protecxted by rw lock of warm node
	  // must prevent conflict with other split and delete

	  //ListNode* next_listNode;
		ListNode* prev_listNode;

		while(true) // try lock before split
		{
			prev_listNode = listNode->prev;
			at_lock2(prev_listNode->lock);
			if (listNode->prev != prev_listNode)
			{
				at_unlock2(prev_listNode->lock);
				continue;
			}
			//at_lock2(listNode->lock);
			/*
			   next_listNode = listNode->next;
			   at_lock2(next_listNode->lock);
			   if (listNode->next != next_listNode)
			   {
			   at_unlock2(next_listNode->lock);
			   at_unlock2(prev_listNode->lock);
			   continue;
			   }
			 */
			break;
		}

		//		at_unlock2(list_nodeMeta->rw_lock); // what?
		split_listNode_group(listNode,node); // we have the lock
						     //at_unlock2(next_listNode->lock);
						     //at_unlock2(listNode->lock);
		at_unlock2(prev_listNode->lock);

		//2-1-3 deadlock
	}

	NodeAddr get_warm_cache(EntryAddr ea) // list or skiplist may changed // avoid error and check later
	{
		NodeAddr wc;
		if (ea.loc == HOT_LOG)
		{
			if (ea.offset < doubleLogList[ea.file_num].tail_sum)
				return emptyNodeAddr;
			uint64_t value8 = *(uint64_t*)(doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea) + ENTRY_HEADER_SIZE + KEY_SIZE);
			value8 = get_v8(value8);
			wc = *(NodeAddr*)(doubleLogList[ea.file_num].dramLogAddr + get_log_offset(ea) + ENTRY_HEADER_SIZE + KEY_SIZE + SIZE_SIZE + value8);
		}
		else if (ea.loc == WARM_LIST)
		{
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
		}
		else
		{
			NodeMeta* nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[ea.file_num] + sizeof(NodeMeta) * (ea.offset/NODE_SIZE));
			if (nm->list_addr.loc != COLD_LIST)
				return emptyNodeAddr;
#if 1
			ListNode* listNode = (ListNode*)&list->node_pool_list[nm->list_addr.file_num][nm->list_addr.offset];
			wc = listNode->warm_cache;
#else
			if (nm->list_addr.pool_num > list->node_pool_list_cnt || (nm->list_addr.pool_num == list->node_pool_list_cnt && nm->list_addr.node_offset > list->node_pool_cnt))
				wc = emptyNodeAddr;
			else
			{
				ListNode* listNode = (ListNode*)&list->node_pool_list[nm->list_addr.pool_num][nm->list_addr.node_offset];
				wc = listNode->warm_cache;
			}
#endif
		}

		return wc;
	}

	EntryAddr PH_Thread::insert_to_cold(SkiplistNode* skiplistNode,unsigned char* src_addr, uint64_t key, int value_size8, std::atomic<uint8_t>* &seg_lock, EntryAddr old_ea) // have skiplist lock // return old ea // need invalidation and kv unlock
	{
		EntryAddr new_ea;
		EntryAddr real_old_ea;
		KVP* kvp_p;

		while(true) // always success unless re updated
		{
			ListNode* listNode = skiplistNode->my_listNode;
			while (key >= listNode->next->key)
				listNode = listNode->next;

			at_lock2(listNode->lock);//-------------------------------------lock dst cold
			if (key > listNode->next->key) // split??
			{
				at_unlock2(listNode->lock);
				continue;
			}

#if 1
			// lock here
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			//					if (kvp_p->value != (uint64_t)addr) // moved
			if (old_ea.value != emptyEntryAddr.value && kvp_p->value != old_ea.value) //by new update // warm to cold can finish
			{ // this is not new update and the key is re inserted
				hash_index->unlock_entry2(seg_lock,read_lock); // unlock
				at_unlock2(listNode->lock);
				//				new_ea.value = kvp_p->value; 
				return emptyEntryAddr; // no invalidation
			}

			// What happen if we failed once and retry here...

			//			if (kvp_p->key == key)
			if (kvp_p->value != INV0)
				real_old_ea.value = kvp_p->value;
			else // first update
			{
				real_old_ea = emptyEntryAddr;
				kvp_p->key = key;
			}
			//------------------------------ entry locked!!
#endif

			if (old_ea.value == emptyEntryAddr.value) // it is new update get new version now
			{
				EntryHeader new_version;
				new_version.valid_bit = 1;
				new_version.delete_bit = 0;
				if (value_size8 == INV64)
					new_version.large_bit = true;
				else
					new_version.large_bit = false;
				new_version.version = global_seq_num[key%COUNTER_MAX].fetch_add(1);

				memcpy(src_addr,&new_version,ENTRY_HEADER_SIZE);
			}

			NodeMeta* list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
			new_ea.value = 0;
			while (true) //list_nodeMeta) // try block group
			{
				at_lock2(list_nodeMeta->rw_lock);

				new_ea = insert_entry_to_slot(list_nodeMeta,src_addr,value_size8);

				if (new_ea.value != 0)
				{
					if (kvp_p->value == INV0)
					{
						if (value_size8 == INV64)
						{
							value_size8 = last_value_size;
							ld_sum+=last_value_size+KEY_SIZE;
							ld_cnt++;
						}
						data_sum+=value_size8+KEY_SIZE;
						if (value_size8 >= LARGE_VALUE_THRESHOLD)
						{
							ld_sum+=value_size8+KEY_SIZE;
							ld_cnt++;
						}
						else
							debug_error("???\n");
					}
					new_ea.large = old_ea.large;//...
					kvp_p->value = new_ea.value;
#ifdef DST_CHECK
					EA_test(key,new_ea);
#endif
					_mm_sfence();	

					//					list_nodeMeta->entryLoc[i].valid = 0; // src inv  not here
					//					nodeMeta->valid_cnt--;

					//need invalidation before unlock...
					//					hash_index->unlock_entry2(seg_lock,read_lock);
					at_unlock2(list_nodeMeta->rw_lock); // unlock dst rw lock
					at_unlock2(listNode->lock);

					//check
					//					warm_to_cold_cnt++; // to cold
					return real_old_ea; // still have kv lock
					break;
				}

				at_unlock2(list_nodeMeta->rw_lock);
				if (list_nodeMeta->next_node_in_group == NULL)
					break;
				list_nodeMeta = list_nodeMeta->next_node_in_group;
			}
			//			if (i >= NODE_SLOT_MAX) // need split
			if (new_ea.value == 0)
				//			if (list_nodeMeta == NULL) // cold split
			{
				hash_index->unlock_entry2(seg_lock,read_lock);
				//				at_unlock2(list_nodeMeta->rw_lock);

				if (list_nodeMeta->group_cnt < MAX_NODE_GROUP) // append
									       //				if (listNode->block_cnt < MAX_NODE_GROUP)
				{
					listNode->block_cnt++;
					NodeMeta* append_nodeMeta = append_group(list_nodeMeta,COLD_LIST); // have to be last
					append_nodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr);
					//					append_nodeMeta->list_addr = listNode->myAddr;
					//					if (append_nodeMeta->list_addr.loc == 0)
					//						debug_error("loc1\n");
				}
				else // split
				{
					try_cold_split(listNode,skiplistNode);
					//					cold_split_cnt++;
					skiplistNode->find_half_listNode();

				}
			}

			at_unlock2(listNode->lock);
		}
		return real_old_ea;
	}

	EntryAddr PH_Thread::direct_to_cold(uint64_t key,uint64_t value_size,unsigned char* value,KVP &kvp, std::atomic<uint8_t>* &seg_lock, SkiplistNode* skiplist_from_warm, bool new_update) // may lock from outside // have to be exist
	{ // ALWAYS NEW
	  //		printf("not now\n");
	  //NO UNLOCK IN HERE!

	  // 1 find skiplist node
	  // 2 find listnode
	  // 3 insert kv
	  // 4 return addr

		timespec ts1,ts2;
		clock_gettime(CLOCK_MONOTONIC,&ts1);

		//		int cold_split_cnt = 0;

		EntryAddr new_ea,old_ea;

		SkiplistNode* skiplist_node;

		ListNode* list_node;
		NodeMeta* list_nodeMeta;

		uint64_t value_size8;

		if (value_size == INV64)
			value_size8 = 8; // sizeof(large addr)
		else
			value_size8 = get_v8(value_size);

		const uint64_t z = 0;
		memcpy(entry_buffer,&z,ENTRY_HEADER_SIZE); // need to be zero for persist
		memcpy(entry_buffer+ENTRY_HEADER_SIZE,&key,KEY_SIZE);
		memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE,&value_size,SIZE_SIZE);
		memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE,value,value_size8);//v8?

		while(1)
		{
			if (skiplist_from_warm)
				skiplist_node = skiplist_from_warm;
			else
			{
#ifdef WARM_CACHE
				if (new_update) // use kvp // always new for now...
				{
					//					skiplist_node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock,kvp);
					EntryAddr old_ea;
					old_ea.value = kvp.value;
					NodeAddr warm_cache = get_warm_cache(old_ea);
					skiplist_node = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache);

				}
				else // from evict can't use kvp
#endif
					skiplist_node = skiplist->find_node(key,prev_sa_list,next_sa_list);
				if (try_at_lock2(skiplist_node->lock) == false)
					continue;
				if (skiplist_node->key > key || skiplist->find_next_node(skiplist_node)->key < key)
				{
					at_unlock2(skiplist_node->lock);
					continue;
				}
				//-------------------------------------------------------- skiplist
			}

			old_ea = insert_to_cold(skiplist_node,entry_buffer,key,value_size8,seg_lock,emptyEntryAddr);

			if (old_ea.loc == HOT_LOG)
			{
				skiplist_node->remove_key_from_list(key);
			}

			if (skiplist_from_warm == NULL)
				at_unlock2(skiplist_node->lock);

			if (old_ea.value != emptyEntryAddr.value)
				invalidate_entry(old_ea);

			hash_index->unlock_entry2(seg_lock,read_lock);

			break;
		}

		//		if (cold_split_cnt > 0)
		//			skiplist_node->find_half_listNode();

		clock_gettime(CLOCK_MONOTONIC,&ts2);
		dtc_time+=(ts2.tv_sec-ts1.tv_sec)*1000000000+(ts2.tv_nsec-ts1.tv_nsec);

		return new_ea;
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
			li = i % WARM_NODE_ENTRY_CNT;
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
				skiplistNode->entry_list[dst%WARM_NODE_ENTRY_CNT] = skiplistNode->entry_list[li];
			dst--;
		}

		skiplistNode->list_tail = dst+1;

	}

#define INDEX

	int PH_Query_Thread::insert_op(uint64_t key, uint64_t value_size, unsigned char* value)
	{
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
		EntryHeader new_version;
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
		bool dtc;
		DoubleLog* dst_log;
		Loc dst_loc;
		bool large_value = false;

		dtc = false;

		if (value_size > LARGE_VALUE_THRESHOLD)
		{
#ifdef LARGE_ALLOC
			large_value = true;

			LargeAddr largeAddr = largeAlloc->insert(value_size,value);
			large_addr = largeAddr; // buffer for insert // src of value

			value_size = sizeof(LargeAddr); // 8
							//			value_size = INV64; // for dtc
			value = (unsigned char*)&large_addr;
#else
			dtc = true;
#endif

			//			return 0;
			//			dtc = true;
		}

#ifdef USE_DTC
		if (ex == 0 && false)
			dtc = true;
		else if(ex && old_ea.loc == COLD_LIST)
		{
			rv = rand_r(&seed_for_dtc);
			if (/*reset_test_cnt || */(rv % 100) <= calc_th(my_log) )// && false) // to cold // ratio condition
				dtc = true;
		}
#endif

		if (dtc)
		{
			//			dst_loc = COLD_LIST;
			//		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			//			kvp.value = 0;
			//			KVP test_kvp = kvp;
			if (large_value)
				value_size = 8;

			SkiplistNode* skiplistNode;
			while (1)
			{
#ifdef WARM_CACHE
				if (ex)
				{
					EntryAddr old_ea;
					old_ea.value = kvp.value;
					NodeAddr warm_cache = get_warm_cache(old_ea);
					skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache);
				}
				else
#endif
					skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
				if (try_at_lock2(skiplistNode->lock) == false)
					continue;
				if (skiplistNode->key > key || skiplist->find_next_node(skiplistNode)->key < key)
				{
					at_unlock2(skiplistNode->lock);
					continue;
				}
				break;
			}
			new_ea = direct_to_cold(key,value_size,value,kvp,seg_lock,skiplistNode,true); // kvp becomes old one
			direct_to_cold_cnt++;
#if 0 // moved to direct_to_cold...
			old_ea.value = kvp.value;
			//			if (ex)
			if (kvp.key == key)
				invalidate_entry(old_ea);
			_mm_sfence();
			hash_index->unlock_entry2(seg_lock,read_lock);
#endif
			if (may_split_warm_node(skiplistNode) == 0)
				at_unlock2(skiplistNode->lock);

		}
		else // to log
		{

			dst_log = my_log;
			dst_loc = HOT_LOG;

			uint64_t value_size8 = get_v8(value_size);
			dst_log->ready_log(value_size8);

			//			new_ea.loc = 1; // hot
			new_ea.loc = dst_loc;
			new_ea.large = large_value;
			new_ea.file_num = dst_log->log_num;
			new_ea.offset = dst_log->head_sum;// % dst_log->my_size;

#ifdef HOT_KEY_LIST
			while(1)
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
					SkiplistNode* next_node;
					while(true)
					{

#ifdef WARM_CACHE
						if (ex)
						{	
							warm_cache = get_warm_cache(old_ea);
							node = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache);						
							//						node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock,kvp);
						}
						else
#endif
							node = skiplist->find_node(key,prev_sa_list,next_sa_list);
						if (try_at_lock2(node->lock) == false)
							continue;

						next_node = skiplist->find_next_node(node);
						if (next_node->key <= key || node->key > key) // may split
						{
							at_unlock2(node->lock);
							continue;
						}

						if (may_split_warm_node(node))
						{

							continue;
						}
						break;
					}

					kvp_p = hash_index->insert(key,&seg_lock,read_lock); // prevent htw evict
					old_ea.value = kvp_p->value;

					if (old_ea.loc != HOT_LOG)
					{
						if (node->key_list_size >= WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT)
							debug_error("over\n");
						node->key_list[node->key_list_size++] = key;
					}
					//				_mm_sfence();
					at_unlock2(node->lock);//here we have entry lock
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

			uint64_t new_ver;
			new_ver = global_seq_num[key%COUNTER_MAX].fetch_add(1);
			new_version.version = new_ver;
			new_version.valid_bit = 1;
			new_version.delete_bit = 0;
			new_version.large_bit = large_value;

			if (kvp_p->key != key) // new key...
			{
				data_sum+=value_size+KEY_SIZE;
				if (value_size >= LARGE_VALUE_THRESHOLD)
				{
					ld_sum+=value_size+KEY_SIZE;
					ld_cnt++;
				}
				ex = 0;
			}
			else
				ex = 1;

			// update version

			// 3 get and write new version <- persist

			// 4 add dram list
#ifdef USE_DRAM_CACHE
			//	new_addr = dram_head_p;
#ifdef WARM_CACHE
			if (ex)
				warm_cache = get_warm_cache(old_ea);
			else
				warm_cache = emptyNodeAddr;

			dst_log->insert_dram_log(new_version.value,key,value_size,value,&warm_cache);
#else
			dst_log->insert_dram_log(new_version.value,key,value_size,value);
#endif

#endif
			dst_log->copy_to_pmem_log(value_size);
			//			dst_log->insert_pmem_log(key,value_size,value);
			dst_log->write_version(new_version.value); // has fence

			//			dst_log->head_sum_log[dst_log->head_sum_cnt] = dst_log->head_sum;
			//			dst_log->head_sum_cnt++;
			dst_log->head_sum+=LOG_ENTRY_SIZE_WITHOUT_VALUE+value_size8;//ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8+WARM_CACHE_SIZE; // NO JUMP SIZE

			//check
			log_write_cnt++;

			kvp_p->value = new_ea.value;
			//			kvp_p->version = new_version.value;
			_mm_sfence(); // value first!

			// 6 update index
			//	kvp_p->value = (uint64_t)my_log->get_head_p();

			// try lock


			if (ex == 0) // not so good // what about direct to cold
			{
				kvp_p->key = key;
				_mm_sfence();

				//				hash_index->unlock_entry2(seg_lock,read_lock);
				//				return 0;
			}
			//			hash_index->unlock_entry2(seg_lock,read_lock);
			if (ex) //  no entry lock
			{
				if (old_ea.loc == HOT_LOG)
					hot_to_hot_cnt++; // need warm to hot?
				invalidate_entry(old_ea);
			}
			_mm_sfence();
			hash_index->unlock_entry2(seg_lock,read_lock);

		}
		return 0;
	}

	int PH_Query_Thread::read_op(uint64_t key,unsigned char* buf,std::string *value)
	{
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
		uint64_t value_size;

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
				printf("entry desonst exist\n");
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
					value_addr = vsp+SIZE_SIZE;
				}
				else
				{
					value_size = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
					value_addr = addr+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE;
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
				if (kvp_p->key != key || kvp_p->value != ea.value) // updated?
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
					value_size = *(uint64_t*)vsp;
					value_addr = vsp+SIZE_SIZE;
				}
				else
				{
					value_size = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
					value_addr = addr+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE;
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
		uint64_t value_size;
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
			memcpy(dst_addr+ENTRY_HEADER_SIZE+KEY_SIZE,large_addr,SIZE_SIZE+value_size);
		}
		else
		{
			value_size = *(uint64_t*)(header+ENTRY_HEADER_SIZE+KEY_SIZE);
			insert(header,ENTRY_SIZE_WITHOUT_VALUE+value_size);
		}

	}

	int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t length)
	{
		//	update_free_cnt();
		op_check();

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

		while(1) // find first
		{
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

			skiplistNode = skiplist->find_node(start_key,prev_sa_list,next_sa_list,warm_cache);
			if (try_at_lock2(skiplistNode->lock) == false)
				continue;
			if (start_key < skiplistNode->key)
			{
				at_unlock2(skiplistNode->lock);
				continue;
			}
			break;
		}
		// locked 

		int i,j;
		//		int key_list_index;
		unsigned char* addr;
		std::atomic<uint8_t> *seg_lock;
		uint64_t key,key1,key2;

		//		DataNode dram_dataNode;

		size_t offset;
#if 1
		//		std::vector<std::pair<uint64_t,unsigned char*>> skiplist_key_list;
		//		std::vector<std::pair<uint64_t,unsigned char*>> list_key_list;
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

		while(scan_result.getCnt() < length && skiplistNode != skiplist->end_node)
		{
			while(1)
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
			for (i=0;i<skiplistNode->key_list_size;i++)
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
			listNode = skiplistNode->my_listNode; // do we need listNode lock?? we already locked skiplist...
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

			while(1) // may need delete lock
			{
				//				next_skiplistNode = skiplist->sa_to_node(skiplistNode->next[0]);
				next_skiplistNode = skiplist->find_next_node(skiplistNode);

				if (try_at_lock2(next_skiplistNode->lock) == false)
					continue;
				//				if (next_skiplistNode != skiplist->sa_to_node(skiplistNode->next[0]))
				if (next_skiplistNode != skiplist->find_next_node(skiplistNode))
				{
					at_unlock2(next_skiplistNode->lock);
					continue;
				}
				break;
			}

			at_unlock2(skiplistNode->lock);

			skiplistNode = next_skiplistNode;
		}
		at_unlock2(skiplistNode->lock);

#ifdef SCAN_TIME
		_mm_mfence();
		clock_gettime(CLOCK_MONOTONIC,&ts2);
		main_time_sum+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

		return scan_result.getCnt();
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

		child1_path = (int*)malloc(WARM_NODE_ENTRY_CNT * sizeof(int));
		child2_path = (int*)malloc(WARM_NODE_ENTRY_CNT * sizeof(int));

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
	}

	void PH_Thread::flush_warm_node(SkiplistNode* node) // lock from outside
	{
		while(node->data_tail<node->data_head || node->list_tail < node->list_head) // flush all
		{
			if (node->data_tail < node->data_head)
				warm_to_cold(node);
			if (node->list_tail < node->list_head)
			{
				//				at_lock2(nodeMeta->rw_lock);
				hot_to_warm(node,false);
				//				at_unlock2(nodeMeta->rw_lock);
			}
		}
		if (node->current_batch_size > 0) // flus hlast batch
			warm_to_cold(node);
	}

#define SPLIT_WITH_LIST_LOCK

	void PH_Thread::split_empty_warm_node(SkiplistNode *old_skiplistNode) // old node is deleted // split need lock....
	{
		/*
		   if (old_skipListNode->key == 3458764513820540926UL)
		   debug_error("split here\n");
		 */
		//we will not reuse old skiplist

		uint64_t half_key;
		SkiplistNode* child1_sl_node;
		SkiplistNode* child2_sl_node;
		ListNode* half_listNode;

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

		child1_sl_node = skiplist->allocate_node();
		child2_sl_node = skiplist->allocate_node();

		if (child1_sl_node == NULL || child2_sl_node == NULL) // alloc fail
		{
			printf("alloc fail\n");
			return;
		}

		at_lock2(child1_sl_node->lock);
		at_lock2(child2_sl_node->lock);

		old_skiplistNode->half_listNode->hold = 1;

		child1_sl_node->key = old_skiplistNode->key;
		child1_sl_node->my_listNode = old_skiplistNode->my_listNode.load();

		child2_sl_node->key = half_key;
		child2_sl_node->my_listNode = old_skiplistNode->half_listNode;

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

		child1_sl_node->update_wc();
		child2_sl_node->update_wc();

		at_unlock2(child1_sl_node->lock);
		at_unlock2(child2_sl_node->lock);

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
	}

	int PH_Thread::may_split_warm_node(SkiplistNode *node) // had warm node lock // didn't had rw_lock
	{
		//		node->find_half_listNode();
		if (node->cold_block_sum > WARM_COLD_MAX_RATIO * WARM_MAX_NODE_GROUP || node->key_list_size >= NODE_SLOT_MAX) //WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT) // (WARM / COLD) RATIO
		{

			// flush all
			flush_warm_node(node);

			SkiplistNode *next_node;
			while(1) // blocking next also blocks prev split // we can modify prev tooo
			{
				next_node = skiplist->find_next_node(node);
				//				at_lock2(next_node->lock);
				if (try_at_lock2(next_node->lock) == false)
					continue;
				if (next_node == skiplist->find_next_node(node))
					break;
				at_unlock2(next_node->lock);
			}
			{
				//				NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
				//				at_lock2(nodeMeta->rw_lock);
				//				hot_to_warm(node,true); // flush all
				split_empty_warm_node(node);
				//				at_unlock2(nodeMeta->rw_lock);
				at_unlock2(next_node->lock);
			}
			return 1;
		}
		return 0;

	}

	void PH_Thread::warm_to_cold(SkiplistNode* node) // lock form outside
	{

		//evict unit will be 1024 // batch evict

		timespec ts1,ts2;
		clock_gettime(CLOCK_MONOTONIC,&ts1);

		int node_num;
		node_num = (node->data_tail%WARM_GROUP_BATCH_CNT)/WARM_BATCH_CNT; // %16 / 4

		ListNode* listNode;
		NodeMeta *nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[node_num]);
		at_lock2(nodeMeta->rw_lock); // warm node lock..
		DataNode *dataNode = nodeAllocator->nodeAddr_to_node(node->data_node_addr[node_num]);
		size_t src_offset;
		uint64_t key;
		EntryAddr old_ea;//,new_ea;
		int i;
		int slot_idx;
		int cold_split_cnt = 0;
		uint64_t value_size8;

		std::atomic<uint8_t>* seg_lock;

		old_ea.loc = 2; // warm
		old_ea.file_num = node->data_node_addr[node_num].pool_num;
		//		old_ea.offset = node->data_node_addr.offset*NODE_SIZE + sizeof(NodeAddr);

		//		new_ea.loc = 3; // cold

		// flush in batch ... 
		int start_slot,end_slot,batch_num;

		batch_num = node->data_tail % WARM_BATCH_CNT; // % 4
							      //		start_slot = WARM_BATCH_ENTRY_CNT*batch_num;
							      //		end_slot = start_slot + WARM_BATCH_ENTRY_CNT;
							      //		start_slot = node->data_tail % WARM_NODE_ENTRY_CNT;
		start_slot = batch_num*WARM_BATCH_ENTRY_CNT;
		/*
		   if (node->data_tail / WARM_BATCH_ENTRY_CNT == node->data_head / WARM_BATCH_ENTRY_CNT) // in same batch
		   end_slot = node->data_head % WARM_NODE_ENTRY_CNT;
		   else
		   end_slot = batch_num*WARM_BATCH_ENTRY_CNT + WARM_BATCH_ENTRY_CNT;
		 */
		end_slot = (batch_num+1)*WARM_BATCH_ENTRY_CNT; // always sam batch
#if 0
		memcpy(evict_buffer,(unsigned char*)dataNode + batch_num*WARM_BATCH_MAX_SIZE,WARM_BATCH_MAX_SIZE);
#else
		reverse_memcpy(evict_buffer,(unsigned char*)dataNode + batch_num*WARM_BATCH_MAX_SIZE,WARM_BATCH_MAX_SIZE); // only 1024
#endif

		//		src_offset = batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE; // in batch...
		//		src_offset = NODE_HEADER_SIZE; // temp buffer size is changed // in evict buffer...

		i = start_slot;
		//		src_offset = NODE_HEADER_SIZE+(start_slot%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE; // in batch
		//		for (i=start_slot;i<end_slot;i++)
		int node_offset = nodeMeta->my_offset.node_offset*NODE_SIZE;
		int base_offset = batch_num*WARM_BATCH_MAX_SIZE;
		unsigned char* src_addr;

		bool large_value;

		while (i < end_slot)
		{
			if (nodeMeta->entryLoc[i].offset == 0)
				break;
			if (nodeMeta->entryLoc[i].valid == 0)
			{
				nodeMeta->entryLoc[i].offset = 0;
				i++;
				continue;
			}
			old_ea.offset = node_offset + nodeMeta->entryLoc[i].offset;//batch_num*WARM_BATCH_MAX_SIZE + src_offset;
										   //			old_ea.offset = dna.node_offset*NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + src_offset;

			src_addr = evict_buffer+(nodeMeta->entryLoc[i].offset - base_offset);

			large_value = ((EntryHeader*)src_addr)->large_bit;
			if (large_value)
				old_ea.large = 1;
			else
				old_ea.large = 0;

			key = *(uint64_t*)(src_addr +ENTRY_HEADER_SIZE);
			value_size8 =  *(uint64_t*)(src_addr +ENTRY_HEADER_SIZE + SIZE_SIZE);
			value_size8 = get_v8(value_size8);

#if 0
			// lock here
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			//					if (kvp_p->value != (uint64_t)addr) // moved
			if (kvp_p->value != old_ea.value)
			{
				//	EntryAddr test_entryAddr;
				//	test_entryAddr.value= kvp_p->value;
				hash_index->unlock_entry2(seg_lock,read_lock); // unlock
									       //	at_unlock2(list_nodeMeta->rw_lock);
									       //	at_unlock2(listNode->lock);
				++cnt;
				src_offset+=ENTRY_SIZE;
				continue; 
			}
			//------------------------------ entry locked!!
#endif
			if (insert_to_cold(node,src_addr,key,value_size8,seg_lock,old_ea) != emptyEntryAddr) // scucess
			{
				//need inv and unlock
				nodeMeta->entryLoc[i].offset = 0;//init
				nodeMeta->entryLoc[i].valid = 0;

				if (large_value)
					invalidate_large_from_addr(src_addr); // it is dram..

				hash_index->unlock_entry2(seg_lock,read_lock);

				warm_to_cold_cnt++;
			}

			//			else
			//				debug_error("empty ettr\n");
			i++; // have to sucess...

		}

		//		node->data_tail+=(end_slot-start_slot);
		node->data_tail++;

		// split or init warm
		//		at_lock2(nodeMeta->lock); // outer
#if 0
		if (nodeMeta->valid_cnt)
		{
			SkiplistNode* new_skipNode;
			new_skipNode = skiplist->alloc_sl_node();

			if (new_skipNode) // warm split
			{
				uint64_t half_key;
				half_key = find_half_in_node(nodeMeta,&dataNode);

				if (half_key > node->key)
				{
					new_skipNode->key = half_key;

					ListNode* list_node = node->my_listNode;
					while(list_node->next->key < half_key)
						list_node = list_node->next;
					new_skipNode->my_listNode = list_node;

					SkiplistNode* prev[MAX_LEVEL+1];
					SkiplistNode* next[MAX_LEVEL+1];
					skiplist->insert_node(new_skipNode,prev,next);
				}
				else
					skiplist->free_sl_node(new_skipNode);

			}
		}
#endif
		/*
		   if (cold_split_cnt > 0)
		   node->find_half_listNode();
		 */

#if 0 // not here
		ListNode* half_listNode;

		half_listNode = find_halfNode(node,node->list_cnt);
		if (cnt * MAX_NODE_GROUP > WARM_COLD_RATIO) // (WARM / COLD) RATIO
		{
			//			SkiplistNode* new_skipListNode = skiplist->alloc_sl_node();
			//			if (new_skipListNode)
			{
				NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
				//				at_lock2(nodeMeta->rw_lock);
				//				hot_to_warm(node,true); // flush all
				split_warm_node(node,half_listNode);
				//				at_unlock2(nodeMeta->rw_lock);
			}
		}
#endif
		// 2 decide split key
		// 3 split

		/*
		   else // warm init
		   {
		// may need memset...
		// hot->cold...
		}
		 */

		//init nodeMeta
		/*
		   for (i=0;i<nodeMeta->slot_cnt;i++) // vinalifd warm
		   nodeMeta->valid[i] = false;
		   nodeMeta->written_size = 0;
		   nodeMeta->slot_cnt = 0;
		   nodeMeta->valid_cnt = 0;
		 */
		at_unlock2(nodeMeta->rw_lock);

		clock_gettime(CLOCK_MONOTONIC,&ts2);
		wtc_time+=(ts2.tv_sec-ts1.tv_sec)*1000000000+(ts2.tv_nsec-ts1.tv_nsec);
		wtc_cnt++;
	}


	//	void PH_Evict_Thread::hot_to_warm(SkiplistNode* node,bool evict_all) // skiplist lock from outside
	void PH_Thread::hot_to_warm(SkiplistNode* node,bool evict_all) // skiplist lock from outside
	{

		struct timespec ts1,ts2;
		clock_gettime(CLOCK_MONOTONIC,&ts1);

		int node_num;

		unsigned char* dst_node;
		int i;
		unsigned char* addr;
		EntryHeader* header;
		DoubleLog* dl;
		LogLoc ll;
		int li;
		int ex_entry_cnt=0;
		uint64_t key;
		EntryAddr old_ea;
		EntryAddr new_ea;
		KVP kvp;
		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;
		volatile uint8_t* seg_depth_p;
		uint8_t seg_depth;

		int written_size,base_offset;
		int start_offset;
		int start_index;
		int end_offset;
		int write_cnt,base_index;
		int batch_num;
		int i_dst;

		uint64_t value_size8;

		int entry_size;

#ifdef HTW_KEY_CHECK
		uint64_t wk1,wk2;
		wk1 = node->key;
		wk2 = skiplist->find_next_node(node)->key;
#endif


		do
		{
			if ((node->data_head-node->data_tail) >= WARM_GROUP_BATCH_CNT-WARM_BATCH_CNT) // if no space // batch >= 4 * 4
				warm_to_cold(node);

			node_num = (node->data_head%WARM_GROUP_BATCH_CNT)/WARM_BATCH_CNT; // % 16 / 4
			dst_node = (unsigned char*)nodeAllocator->nodeAddr_to_node(node->data_node_addr[node_num]);
			NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[node_num]);
			at_lock2(nodeMeta->rw_lock);//------------------------------------------------lock here

			batch_num = (node->data_head)%WARM_BATCH_CNT; // % 4

			base_offset = batch_num*WARM_BATCH_MAX_SIZE; // batch * 1024
			base_index = batch_num*WARM_BATCH_ENTRY_CNT; // batch * 20
			start_index = base_index + node->current_batch_index;
			end_offset = base_offset + WARM_BATCH_MAX_SIZE;

			if (batch_num == 0)
			{
				//				written_size = NODE_HEADER_SIZE;
				start_offset = base_offset + node->current_batch_size+NODE_HEADER_SIZE; // < 1024
			}
			else
			{
				//				written_size = 0;
				start_offset = base_offset + node->current_batch_size;
			}

			memset(evict_buffer+base_offset,0,WARM_BATCH_MAX_SIZE); // 1024
			written_size = 0;
			write_cnt = 0;
			//			target_cnt = WARM_BATCH_ENTRY_CNT - node->data_head%WARM_BATCH_ENTRY_CNT;

			for (i=node->list_tail;i<node->list_head;i++)
			{
				li = i % NODE_SLOT_MAX;//WARM_NODE_ENTRY_CNT; // need list max
				ll = node->entry_list[li];
				dl = &doubleLogList[ll.log_num];
				addr = dl->dramLogAddr + (ll.offset%dl->my_size);

				header = (EntryHeader*)addr;
				value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
				value_size8 = get_v8(value_size8);
				entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;

				if (dl->tail_sum > ll.offset || header->valid_bit == false)
				{
					node->list_size_sum-=value_size8;
					node->entry_list[li].log_num = INV_LOG; // invalid
					continue;
				}

				//				else if (/*true ||*/ (/*header->prev_loc == 3 && */false)) // cold // hot to cold
				{
#if 0
					//					printf("impossible\n");
					// direct to cold here
					// set old ea
					// direct to cold
					// invalidate old ea
					// unlock index

					old_ea.loc = HOT_LOG;
					old_ea.file_num = ll.log_num;
					old_ea.offset = ll.offset%dl->my_size;

					key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

					//					bool ex = hash_index->read(key,&kvp,&kvp_p,&seg_depth,&seg_depth_p);
					kvp.value = old_ea.value;

					new_ea = direct_to_cold(key,addr + ENTRY_HEADER_SIZE + KEY_SIZE,kvp,seg_lock,node,false);
					if (kvp.value == old_ea.value) // ok
					{
						//					old_ea.value = kvp.value;
						invalidate_entry(old_ea);
						hash_index->unlock_entry2(seg_lock,read_lock);

						hot_to_cold_cnt++;
					}

					node->entry_list[li].log_num = -1;
#endif
				}

				if (start_offset + written_size + entry_size > end_offset) // batch 1024 full
					break; 
				if (node->current_batch_index + write_cnt >= WARM_BATCH_ENTRY_CNT-1-1)//20
					break;

				//				else
				{
					//					if ((start_index+write_cnt)/20 != (start_offset+written_size)/WARM_BATCH_MAX_SIZE)
					//						debug_error("mispamthc\n");
					nodeMeta->entryLoc[start_index+write_cnt].valid = 0;
					nodeMeta->entryLoc[start_index+write_cnt].offset = start_offset + written_size;

					node->list_size_sum-=value_size8;

					memcpy(evict_buffer + start_offset + written_size,addr,entry_size);

					written_size+=entry_size;
					//					if (header->prev_loc != 0)
					//						ex_entry_cnt++;

					write_cnt++;

					hot_to_warm_cnt++;
				}
			}

			if (write_cnt == 0)
			{
				node->list_tail = i; //restart from i
				node->data_head++;
				node->current_batch_size = 0;
				node->current_batch_index = 0;

				at_unlock2(nodeMeta->rw_lock);
				continue;
			}

			//			if ((start_index+write_cnt)/20 != (start_offset+written_size-1)/WARM_BATCH_MAX_SIZE)
			//				debug_error("mispamthc\n");

			nodeMeta->entryLoc[start_index+write_cnt].valid = 0;
			nodeMeta->entryLoc[start_index+write_cnt].offset = start_offset +written_size;
			nodeMeta->entryLoc[start_index+write_cnt+1].valid = 0;
			nodeMeta->entryLoc[start_index+write_cnt+1].offset = base_offset+WARM_BATCH_MAX_SIZE;

			i_dst = i;

			//------------------------------- evict buffer filled

			//evict current batch size (+ NODE_HEADER) ~ written size...

			if (false && start_offset == NODE_HEADER_SIZE) // need node head flush
			{
				memcpy(evict_buffer,&nodeMeta->next_addr,sizeof(NodeAddr)); // warm node must be fixed
				memcpy(evict_buffer+sizeof(NodeAddr),&nodeMeta->next_addr_in_group,sizeof(NodeAddr)); // warm node must be fixed
				pmem_nt_write(dst_node,evict_buffer,NODE_HEADER_SIZE+written_size+ENTRY_HEADER_SIZE);
			}
			else
			{
				// we need 256 align
#if 1
				int padding = end_offset-start_offset-written_size;
				padding%=256;
				uint64_t first_header = *(uint64_t*)(evict_buffer+start_offset);
				*(uint64_t*)(evict_buffer+start_offset) = 0;
				pmem_nt_write(dst_node+start_offset,evict_buffer+start_offset,written_size+padding);
				_mm_sfence();
				//				pmem_nt_write(dst_node+start_offset,evict_buffer+start_offset,ENTRY_HEADER_SIZE); // persist--------------------
				pmem_nt_write(dst_node+start_offset,(unsigned char*)&first_header,ENTRY_HEADER_SIZE); // persist--------------------
				_mm_sfence();
#else
				pmem_nt_write(dst_node+start_offset,evict_buffer+start_offset,written_size);
				_mm_sfence();
#endif

			}

			node->current_batch_size+=written_size;
			node->current_batch_index+=write_cnt;
#if 0
			if (node->data_head % WARM_BATCH_ENTRY_CNT) // not aligned // always algined...
				pmem_nt_write(dst_node + batch_num*WARM_BATCH_MAX_SIZE + start_offset,buffer_start,written_size);
			else
			{
#if 0
				pmem_nt_write(dst_node + batch_num*WARM_BATCH_MAX_SIZE,evict_buffer,WARM_BATCH_MAX_SIZE);
#else
				pmem_reverse_nt_write(dst_node + batch_num*WARM_BATCH_MAX_SIZE,evict_buffer,WARM_BATCH_MAX_SIZE);

#endif
			}
#endif

			//------------------------------- pmem write

			// invalidate from here

			_mm_sfence();

			EntryAddr dst_addr,src_addr;
			dst_addr.loc = 2; // warm	
			dst_addr.file_num = nodeMeta->my_offset.pool_num;
			int node_offset = nodeMeta->my_offset.node_offset * NODE_SIZE;
			//			dst_addr.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + start_offset; //nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE + (node->data_head%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE;

			//			src_addr.loc = 1; //hot
			int slot_index = start_index;//node->data_head%WARM_NODE_ENTRY_CNT; // have to in batch

			for (i=node->list_tail;i<i_dst;i++)
			{
				li = i % NODE_SLOT_MAX;//WARM_NODE_ENTRY_CNT;
				ll = node->entry_list[li];
				if (ll.log_num == INV_LOG)
					continue;

				dl = &doubleLogList[ll.log_num];
				addr = dl->dramLogAddr + (ll.offset%dl->my_size);
				header = (EntryHeader*)addr;
				key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

				src_addr.loc = HOT_LOG;
				src_addr.large = header->large_bit;
				src_addr.file_num = ll.log_num;
				src_addr.offset = ll.offset;//%dl->my_size; // dobuleloglist log num my size

				kvp_p = hash_index->insert(key,&seg_lock,read_lock);
				if (kvp_p->value == src_addr.value)
				{

#ifdef HTW_KEY_CHECK
					if (key < wk1 || key >= wk2)
						debug_error("HTW_KEY_CHECK FAIL\n");
#endif
					// just change location
					dst_addr.large = src_addr.large;
					dst_addr.offset = node_offset + nodeMeta->entryLoc[slot_index].offset;
					kvp_p->value = dst_addr.value;
#ifdef DST_CHECK
					EA_test(key,dst_addr);
#endif

					//					nodeMeta->valid[slot_index] = true; // validate
					nodeMeta->entryLoc[slot_index].valid = 1;
					//					++nodeMeta->valid_cnt;
					if (src_addr.large)
						invalidate_large_from_addr(addr);
					_mm_sfence();
					header->valid_bit = 0; // invalidate hot log entry
#ifdef HOT_KEY_LIST	
					node->remove_key_from_list(key);
#endif
				}
#if 1 // may do nothing and save space... // no we need to push slot cnt because memory is already copied
				else // inserted during hot to warm
				{
					//					debug_error("htw\n");
					//					nodeMeta->valid[slot_index] = false; // validate fail

				}
#endif
				slot_index++;
				_mm_sfence(); // need?
				hash_index->unlock_entry2(seg_lock,read_lock);
				//				dst_addr.offset+=ENTRY_SIZE;
			}

			node->list_tail = i_dst;
			//			node->data_head+= write_cnt;

			if (node->list_tail < node->list_head)
			{
				node->data_head++;
				node->current_batch_size = 0;
				node->current_batch_index = 0;
			}


			at_unlock2(nodeMeta->rw_lock);//--------------------------------------------- unlock here
		}while(false && node->list_head-node->list_tail >= WARM_BATCH_ENTRY_CNT);

		clock_gettime(CLOCK_MONOTONIC,&ts2);
		htw_time+=(ts2.tv_sec-ts1.tv_sec)*1000000000+(ts2.tv_nsec-ts1.tv_nsec);
		htw_cnt++;

		node->recent_entry_cnt = ex_entry_cnt;
		/*
		   if (node->recent_entry_cnt < WARM_LOG_MIN)
		   {
		   }
		   else if (node->recent_entry_cnt > WARM_LOG_MAX) // too much warm nodes...
		   {
		   ListNode* half_listNode;
		   int cnt;
		   half_listNode = find_halfNode(node,cnt);
		   split_warm_node(node,half_listNode);
		   }
		 */

	}

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

		uint64_t value_size8;
		int entry_size;

		//pass invalid
		while(dl->tail_sum+NODE_SIZE/*LOG_ENTRY_SIZE*/ <= dl->head_sum)
		{
			addr = dl->dramLogAddr+(dl->tail_sum%dl->my_size);
			header.value = *(uint64_t*)addr;

			if (header.valid_bit)
				break;
			value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
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
		int split_warm_node;

		KVP kvp;
		EntryAddr old_ea;
		std::atomic<uint8_t>* seg_lock;

		NodeAddr warm_cache;

		uint64_t value_size8;

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
			value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
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
				old_ea.file_num = dl->log_num;
				old_ea.offset = dl->tail_sum%dl->my_size;
				kvp.value = old_ea.value;

				direct_to_cold(key,addr + ENTRY_HEADER_SIZE + KEY_SIZE,kvp,seg_lock,NULL,false);

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
				warm_cache = *(NodeAddr*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8);

				SkiplistNode* node;
#ifdef WARM_CACHE
				node = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache);
#else
				node = skiplist->find_node(key,prev_sa_list,next_sa_list);
#endif
				if (try_at_lock2(node->lock) == false)
					continue;
				if (skiplist->find_next_node(node)->key < key)
				{
					at_unlock2(node->lock);
					continue;
				}
				hard_htw_cnt++;
				//NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
				//at_lock2(nodeMeta->rw_lock);
				hot_to_warm(node,false); // always partial...
				split_warm_node = may_split_warm_node(node); // lock???
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
				if (false && node->recent_entry_cnt < WARM_NODE_ENTRY_CNT && node->ver > 3) // DO NOT DELETE  // still have lock
				{
					flush_warm_node(node);
					skiplist->delete_node(node); // it will be jumped during find_node
				}
				else
				{
					//					if (node->data_tail + (WARM_NODE_ENTRY_CNT-WARM_BATCH_ENTRY_CNT) <= node->data_head)
					//						warm_to_cold(node);
					if (split_warm_node == 0)
						at_unlock2(node->lock);
				}
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
		uint64_t value_size8;

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
			value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			value_size8 = get_v8(value_size8);

			if (header.valid_bit)// && header.prev_loc != 3)// && is_checked(header) == false)
			{
				rv = 1;

				warm_cache = *(NodeAddr*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE+value_size8);
				// regist the log num and size_t
				while(true) // the log allocated to this evict thread
				{
#ifdef WARM_CACHE 
					node = skiplist->find_node(key,prev_sa_list,next_sa_list,warm_cache);
#else
					node = skiplist->find_node(key,prev_sa_list,next_sa_list);
#endif
					if (try_at_lock2(node->lock) == false)
					{
						// too busy???
						// may need split here
						continue;
					}

					if (skiplist->find_next_node(node)->key <= key || node->key > key) // may split
					{
						at_unlock2(node->lock);
						continue;
					}

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
					//add entry
					ll.log_num = dl->log_num;
					ll.offset = dl->soft_adv_offset;
					ll.size = value_size8;

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
						node->entry_list[node->list_head%NODE_SLOT_MAX] = ll;
						//	node->entry_list.push_back(ll);
						//	node->entry_size_sum+=ENTRY_SIZE;
						node->list_head++; // lock...
						node->list_size_sum+=value_size8;

						//						at_unlock2(node->lock);
						//						break; // in the list // 
					}

					//					set_checked((uint64_t*)addr);

					// need to try flush
					//			node->try_hot_to_warm();
					//	if (node->entry_size_sum >= SOFT_BATCH_SIZE)
					if (node->current_batch_size + node->list_size_sum > WARM_BATCH_MAX_SIZE || node->list_head - node->list_tail >= NODE_SLOT_MAX)
						list_gc(node);
					if (node->current_batch_size + node->list_size_sum > WARM_BATCH_MAX_SIZE || node->list_head - node->list_tail >= NODE_SLOT_MAX)
						//					if (node->list_head - node->list_tail >= WARM_BATCH_ENTRY_CNT)
					{
						//	NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
						//	at_lock2(nodeMeta->rw_lock);
						hot_to_warm(node,false);
						may_split_warm_node(node);
						//	at_unlock2(nodeMeta->rw_lock);
						//	if (node->data_tail + (WARM_NODE_ENTRY_CNT-WARM_BATCH_ENTRY_CNT) <= node->data_head)
						//	warm_to_cold(node); // in lock???
						soft_htw_cnt++;
					}
					at_unlock2(node->lock);
					break;
				}
			}

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
		}
		run = 0;
		//		printf("evict end\n");
	}

}
