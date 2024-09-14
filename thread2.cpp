#include <stdio.h>
#include <cstring>
#include <x86intrin.h> //fence
#include <unistd.h> //usleep
#include <libpmem.h> 
#include <queue>
#include <utility>

#include "thread2.h"
#include "log.h"
#include "lock.h"
#include "cceh.h"
#include "skiplist.h"
#include "data2.h"
#include "global2.h"

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
	//extern volatile unsigned int seg_free_cnt;
	//extern std::atomic<uint32_t> seg_free_head;
	extern CCEH* hash_index;

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

	//	extern const size_t WARM_BATCH_MAX_SIZE;
	//	extern size_t WARM_BATCH_MAX_SIZE;
	extern size_t WARM_BATCH_ENTRY_CNT;
	extern size_t WARM_GROUP_ENTRY_CNT;
	extern size_t WARM_BATCH_CNT;
	extern size_t WARM_NODE_ENTRY_CNT;
	extern size_t WARM_LOG_MAX;
	extern size_t WARM_LOG_MIN;
	extern size_t WARM_LOG_THRESHOLD;

	extern Skiplist* skiplist;
	extern PH_List* list;
	//extern const size_t MAX_LEVEL;

#if 0
	//---------------------------------------------- seg

	unsigned int min_seg_free_cnt()
	{
		int i,mi;
		unsigned int min=999999999;
		for (i=0;i<num_query_thread;i++)
		{
			if (query_thread_list[i].run && min > query_thread_list[i].local_seg_free_head)
			{
				min = query_thread_list[i].local_seg_free_head;
				mi = i;
			}
		}
		for (i=0;i<num_evict_thread;i++)
		{
			if (evict_thread_list[i].run && min > evict_thread_list[i].local_seg_free_head)
			{
				min = evict_thread_list[i].local_seg_free_head;
				mi = i + num_query_thread;
			}
		}
		if (min == 999999999)
			return seg_free_head; // what happend?
		if (mi < num_query_thread)
			query_thread_list[mi].update_request = 1;
		else
			evict_thread_list[mi-num_query_thread].update_request = 1;
		return min;
	}
#endif

	void EA_test(uint64_t key, EntryAddr ea)
	{
		printf("not now\n");
		if (key != *(uint64_t*)(nodeAllocator->nodePoolList[ea.file_num]+ea.offset+ENTRY_HEADER_SIZE))
			debug_error("ea error!\n");
	}


	uint64_t test_the_index(KVP kvp)
	{
		printf(" not now\n");
		EntryAddr ea;
		unsigned char* addr;
		uint64_t key;

		ea.value = kvp.value;

		if (ea.loc == 1)
			addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
		else
			addr = nodeAllocator->nodePoolList[ea.file_num]+ea.offset;

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
#if 0
	void PH_Thread::update_free_cnt()
	{
		local_seg_free_head = seg_free_head;
#ifdef wait_for_slow
		int min = min_seg_free_cnt();
		if (min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
		{
			printf("in2\n");
			while(min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
			{
				update_idle();
				min = min_seg_free_cnt();
			}
			printf("out2\n");
		}
#endif
#if 0
		int i;
		pthread_t pt;

		pt = pthread_self();
		for (i=0;i<num_of_thread;i++)
		{
			if (pthread_equal(thread_list[i].tid,pt))
			{
				thread_list[i].free_cnt = free_cnt;
				thread_list[i].seg_free_cnt = seg_free_cnt;
				return;
			}
		}
		new_thread();
#endif

	}
#endif
	//-------------------------------------------------
	ListNode* find_halfNode(SkiplistNode* node) // do we have lock?
	{
		uint64_t current_key,next_key;
		//		ListNode* listNode;
		ListNode* half_listNode;
		ListNode* listNode;

		current_key = node->key;
		next_key = (skiplist->sa_to_node(node->next[0]))->key;
		listNode = node->my_listNode;
		half_listNode = node->my_listNode;
		int cnt = 0;
		node->cold_block_sum = 0;
		while (next_key > listNode->key)
		{
			node->cold_block_sum+=listNode->block_cnt;
			listNode = listNode->next;
			++cnt;
			if (cnt%2 == 0)
				half_listNode = half_listNode->next;
		}
		return half_listNode;
	}


	void PH_Query_Thread::init()
	{

		evict_buffer = (unsigned char*)malloc(WARM_BATCH_MAX_SIZE);

		key_list_buffer = (uint64_t*)malloc(sizeof(uint64_t) * MAX_NODE_GROUP*(NODE_SIZE/ENTRY_SIZE));
		old_ea_list_buffer = (EntryAddr*)malloc(sizeof(EntryAddr) * MAX_NODE_GROUP*(NODE_SIZE/ENTRY_SIZE));

		int i;

		my_log = 0;
		my_warm_log = 0;
		exit = 0;

		for (i=0;i<log_max;i+=2)
		{
			while (doubleLogList[i].use == 0)
			{
				//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
				if (try_at_lock2(doubleLogList[i].use))
				{
					my_log = &doubleLogList[i];
					//					my_warm_log = &WDLL[i];
					my_warm_log = &doubleLogList[i+1]; // warm log
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

		//check
		warm_log_write_cnt = log_write_cnt = hot_to_warm_cnt = warm_to_cold_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		warm_to_warm_cnt = 0;
		direct_to_cold_cnt = 0;

		soft_htw_cnt = hard_htw_cnt=0;
		dtc_time = htw_time = wtc_time = 0;
		htw_cnt = wtc_cnt = 0;

		seed_for_dtc = thread_id;
		//		printf("sfd %u\n",seed_for_dtc);
	}

	void PH_Query_Thread::clean()
	{
		delete evict_buffer;

		delete key_list_buffer;
		delete old_ea_list_buffer;

		my_log->use = 0;
		my_log = NULL;

		//		hash_index->thread_local_clean();
		//		free(temp_seg);
		hash_index->remove_ts(temp_seg);
		run = 0;
		read_lock = 0;

		query_thread_list[thread_id].lock = 0;
		//		printf("query thread list %d end\n",thread_id);
		query_thread_list[thread_id].exit = 0;

		//	delete recent_log_tails;

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
	}	

	/*
	   void pmem_node_nt_write(DataNode* dst_node,DataNode* src_node, size_t offset, size_t len)
	   {
	   pmem_memcpy((unsigned char*)dst_node+offset,(unsigned char*)src_node+offset,len,PMEM_F_MEM_NONTEMPORAL);
	   _mm_sfence();
	   }
	 */
	void pmem_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len)
	{
		pmem_memcpy(dst_addr,src_addr,len,PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}


	void pmem_entry_write(unsigned char* dst, unsigned char* src, size_t len)
	{
		// need version clean - kv write - version write ...
		memcpy(dst,src,len);
		pmem_persist(dst,len);
		_mm_sfence();
	}
	void pmem_next_write(DataNode* dst_node,NodeAddr nodeAddr)
	{
		dst_node->next_offset = nodeAddr;
		pmem_persist(dst_node,NODE_HEADER_SIZE);
		_mm_sfence();
	}

	EntryAddr insert_entry_to_slot(NodeMeta* list_nodeMeta,unsigned char* src_addr) // need lock from outside
	{
		int slot_idx;
		EntryAddr new_ea;

		if (list_nodeMeta->valid_cnt < NODE_SLOT_MAX)
		{
			for (slot_idx=0;slot_idx<NODE_SLOT_MAX;slot_idx++)
			{
				if (list_nodeMeta->valid[slot_idx] == false)
					break;
			}
		}
		else
			return emptyEntryAddr;

		new_ea.loc = 3; // cold
		new_ea.file_num = list_nodeMeta->my_offset.pool_num;
		if (slot_idx < NODE_SLOT_MAX)
		{
			//			old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;
			new_ea.offset = list_nodeMeta->my_offset.node_offset*NODE_SIZE + NODE_HEADER_SIZE + ENTRY_SIZE*slot_idx;

			DataNode* list_dataNode = nodeAllocator->nodeAddr_to_node(list_nodeMeta->my_offset);
			pmem_entry_write(list_dataNode->buffer + ENTRY_SIZE*slot_idx , src_addr, ENTRY_SIZE);
			list_nodeMeta->valid[slot_idx] = true; // validate
			++list_nodeMeta->valid_cnt;

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
		else
			return emptyEntryAddr; // impossible??
		return new_ea;
	}

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

		//scan all keys
		//		std::vector<KA_Pair> key_list;
		//		std::priority_queue<std::pair<uint64_t,unsigned char*>> key_list;
		std::priority_queue<std::pair<uint64_t,SecondOfPair>,std::vector<std::pair<uint64_t,SecondOfPair>>,std::greater<std::pair<uint64_t,SecondOfPair>>> key_list;
		NodeMeta *list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
		//		NodeMeta *half_list_nodeMeta;
		DataNode *list_dataNode_p[MAX_NODE_GROUP];
		NodeMeta *old_nodeMeta[MAX_NODE_GROUP];

		unsigned char* addr;
		int offset;
		int group0_idx=0;
		uint64_t key;
		SecondOfPair second;
		EntryAddr ea;

		ea.loc = 3;

		int i;

		while(list_nodeMeta)
		{
			at_lock2(list_nodeMeta->rw_lock); // lock the node

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
				}
				ea.offset+=ENTRY_SIZE;
				offset+=ENTRY_SIZE;
				i++;
			}

			list_nodeMeta = list_nodeMeta->next_node_in_group;
			group0_idx++;
			//			if (group_idx == MAX_NODE_GROUP/2)
			//				half_list_nodeMeta = list_nodeMeta;
		}

		//sort them //pass
		//relocate
		// fixed size here

		//-------------------------------------------------------------------------------
		//		if (key_list.size() > 0)
		//			printf("test here\n");

		int size = key_list.size();
		int group1_idx = 0;
		addr = sorted_buffer1[0].buffer;
		offset = 0;
		for (i=0;i<(size+1)/2;i++)
		{
			if (offset+ENTRY_SIZE > NODE_BUFFER_SIZE)
			{
				group1_idx++;
				addr = sorted_buffer1[group1_idx].buffer;
				offset = 0;
			}
			memcpy(addr+offset,key_list.top().second.addr,ENTRY_SIZE);
			key_list_buffer[i] = (key_list.top().first);
			old_ea_list_buffer[i] = (key_list.top().second.ea);
			key_list.pop();
			offset+=ENTRY_SIZE;
		}

		int group2_idx = 0;
		addr = sorted_buffer2[0].buffer;
		offset = 0;
		for (;i<size;i++)
		{
			if (offset+ENTRY_SIZE > NODE_BUFFER_SIZE)
			{
				group2_idx++;
				addr = sorted_buffer2[group2_idx].buffer;
				offset = 0;
			}
			memcpy(addr+offset,key_list.top().second.addr,ENTRY_SIZE);
			key_list_buffer[i] = (key_list.top().first);
			old_ea_list_buffer[i] = (key_list.top().second.ea);
			key_list.pop();
			offset+=ENTRY_SIZE;
		}

		//alloc dst 
		NodeAddr new_nodeAddr1[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta1[MAX_NODE_GROUP];
		NodeAddr new_nodeAddr2[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta2[MAX_NODE_GROUP];

		for (i=0;i<=group1_idx;i++)
		{
			new_nodeAddr1[i] = nodeAllocator->alloc_node();
			new_nodeMeta1[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr1[i]);
			new_nodeMeta1[i]->group_cnt = i+1;
			at_lock2(new_nodeMeta1[i]->rw_lock); // ------------------------------- lock here!!!
		}
		for (i=0;i<=group2_idx;i++)
		{
			new_nodeAddr2[i] = nodeAllocator->alloc_node();
			new_nodeMeta2[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr2[i]);
			new_nodeMeta2[i]->group_cnt = i+1;
			at_lock2(new_nodeMeta2[i]->rw_lock); // ------------------------------- lock here!!!
		}

		for (i=0;i<group1_idx;i++) // connect
		{
			new_nodeMeta1[i]->next_node_in_group = new_nodeMeta1[i+1];
			sorted_buffer1[i].next_offset = emptyNodeAddr;
			sorted_buffer1[i].next_offset_in_group = new_nodeMeta1[i+1]->my_offset;
		}
		for (i=0;i<group2_idx;i++) // connect
		{
			new_nodeMeta2[i]->next_node_in_group = new_nodeMeta2[i+1];
			sorted_buffer2[i].next_offset = emptyNodeAddr;
			sorted_buffer2[i].next_offset_in_group = new_nodeMeta2[i+1]->my_offset;
		}

		//		new_nodeMeta1[MAX_NODE_GROUP/2-1]->next_node_in_group = NULL;
		new_nodeMeta2[0]->next_p = old_nodeMeta[0]->next_p;
		new_nodeMeta2[0]->next_addr = old_nodeMeta[0]->my_offset;
		new_nodeMeta1[0]->next_p = new_nodeMeta2[0];
		new_nodeMeta1[0]->next_addr = new_nodeMeta2[0]->my_offset;

		// 0 -> half -> next
		// [0 -> 1 ..] / [half -> half+1 ..]

		//memcpy to pmem

		// link!?
		//		sorted_temp_dataNode[MAX_NODE_GROUP/2-1].next_offset_in_group = emptyNodeAddr;
		sorted_buffer2[0].next_offset = old_nodeMeta[0]->next_p->my_offset;
		sorted_buffer1[0].next_offset = new_nodeMeta2[0]->my_offset;

		// fill new nodes
		int j;
		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;
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
		addr = sorted_buffer1[0].buffer;
		offset = 0;
		j = 0;
		start_offset = new_nodeMeta1[0]->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;
		dst_ea.file_num = new_nodeMeta1[0]->my_offset.pool_num;
		dst_ea.loc = COLD_LIST;

		for (i=0;i<(size+1)/2;i++)
		{
			if (offset+ENTRY_SIZE > NODE_BUFFER_SIZE)
			{
				group1_idx++;
				addr = sorted_buffer1[group1_idx].buffer;
				offset = 0;
				j = 0;
				start_offset = new_nodeMeta1[group1_idx]->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;
				dst_ea.file_num = new_nodeMeta1[group1_idx]->my_offset.pool_num;
			}

			key = key_list_buffer[i];
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.value = kvp_p->value;

			if (ea.value == old_ea_list_buffer[i].value)
			{
				new_nodeMeta1[group1_idx]->valid[j] = true;
				new_nodeMeta1[group1_idx]->valid_cnt++;

				dst_ea.offset = start_offset + offset;
				kvp_p->value = dst_ea.value;

				//				EA_test(key,dst_ea);
			}
			else
				new_nodeMeta1[group1_idx]->valid[j] = false;
			hash_index->unlock_entry2(seg_lock,read_lock);

			j++;
			offset+=ENTRY_SIZE;
		}

		group2_idx = 0;
		addr = sorted_buffer2[0].buffer;
		offset = 0;
		j = 0;
		start_offset = new_nodeMeta2[0]->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;
		dst_ea.file_num = new_nodeMeta2[0]->my_offset.pool_num;
		dst_ea.loc = COLD_LIST;

		for (;i<size;i++)
		{
			if (offset+ENTRY_SIZE > NODE_BUFFER_SIZE)
			{
				group2_idx++;
				addr = sorted_buffer2[group2_idx].buffer;
				offset = 0;
				j = 0;
				start_offset = new_nodeMeta2[group2_idx]->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;
				dst_ea.file_num = new_nodeMeta2[group2_idx]->my_offset.pool_num;

			}

			key = key_list_buffer[i];
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.value = kvp_p->value;

			if (ea.value == old_ea_list_buffer[i].value)
			{
				new_nodeMeta2[group2_idx]->valid[j] = true;
				new_nodeMeta2[group2_idx]->valid_cnt++;

				dst_ea.offset = start_offset + offset;
				kvp_p->value = dst_ea.value;

				//				EA_test(key,dst_ea);
			}
			else
				new_nodeMeta2[group2_idx]->valid[j] = false;
			hash_index->unlock_entry2(seg_lock,read_lock);

			j++;
			offset+=ENTRY_SIZE;
		}


		_mm_sfence();

		// link the list
		// link pmem first then dram...

		// do not change listNode just link new nodemeta
		ListNode* new_listNode;
		new_listNode = list->alloc_list_node();

		// don't alloc new node...
		listNode->data_node_addr = new_nodeMeta1[0]->my_offset;
		new_listNode->data_node_addr = new_nodeMeta2[0]->my_offset;

		if (size > 1)
			new_listNode->key = key_list_buffer[(size+1)/2];
		else // empty
		{
			uint64_t half1,half2;
			half1 = listNode->key/2;
			half2 = listNode->next->key/2;
			new_listNode->key = half1+half2;
		}

		new_listNode->prev = listNode;

		listNode->block_cnt = group1_idx+1;
		new_listNode->block_cnt = group2_idx+1;

		_mm_sfence();

		listNode->next->prev = new_listNode;
		new_listNode->next = listNode->next;
		listNode->next = new_listNode;

		_mm_sfence();
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
		for (i=0;i<=group1_idx;i++)
			at_unlock2(new_nodeMeta1[i]->rw_lock);		
		for (i=0;i<=group2_idx;i++)
			at_unlock2(new_nodeMeta2[i]->rw_lock);		

		// free the nodes...
		for (i=0;i<group0_idx;i++)
			nodeAllocator->free_node(old_nodeMeta[i]);

	}
#if 0
	void PH_Thread::split_listNode_group(ListNode *listNode,SkiplistNode *skiplistNode)
	{
		// lock all the nodes
		// copy
		//scan all keys
		//sort them
		//relocate
		//memcpy to pmem ( USE NEW NODES!)
		// modify index
		// free---- need versio nnumber???

		//scan all keys
		//		std::vector<KA_Pair> key_list;
		//		std::priority_queue<std::pair<uint64_t,unsigned char*>> key_list;
		std::priority_queue<std::pair<uint64_t,SecondOfPair>,std::vector<std::pair<uint64_t,SecondOfPair>>,std::greater<std::pair<uint64_t,SecondOfPair>>> key_list;
		NodeMeta *list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
		//		NodeMeta *half_list_nodeMeta;
		DataNode *list_dataNode_p[MAX_NODE_GROUP];
		DataNode temp_dataNode[MAX_NODE_GROUP];
		NodeMeta *old_nodeMeta[MAX_NODE_GROUP];

		unsigned char* addr;
		int offset;
		int group_idx=0;
		uint64_t key;
		SecondOfPair second;
		EntryAddr ea;

		ea.loc = 3;

		while(list_nodeMeta)
		{

			at_lock2(list_nodeMeta->rw_lock); // lock the node

			list_dataNode_p[group_idx] = nodeAllocator->nodeAddr_to_node(list_nodeMeta->my_offset);
			temp_dataNode[group_idx] = *list_dataNode_p[group_idx]; // pmem to dram
			old_nodeMeta[group_idx] = list_nodeMeta;


			addr = temp_dataNode[group_idx].buffer;
			offset = 0;

			ea.file_num = list_nodeMeta->my_offset.pool_num;
			ea.offset = list_nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

			while(offset+ENTRY_SIZE <= NODE_BUFFER_SIZE) // node is full
			{
				key = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE);
				second.addr = addr+offset;
				second.ea = ea;
				key_list.push(std::make_pair(key,second)); // addr in temp dram
				offset+=ENTRY_SIZE;
				ea.offset+=ENTRY_SIZE;
			}

			list_nodeMeta = list_nodeMeta->next_node_in_group;
			group_idx++;
			//			if (group_idx == MAX_NODE_GROUP/2)
			//				half_list_nodeMeta = list_nodeMeta;
		}

		//sort them //pass
		//relocate
		// fixed size here

		//alloc dst first
		NodeAddr new_nodeAddr[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta[MAX_NODE_GROUP];
		DataNode* new_dataNode;
		int i;
		for (i=0;i<MAX_NODE_GROUP;i++)
		{
			new_nodeAddr[i] = nodeAllocator->alloc_node();
			new_nodeMeta[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr[i]);
			at_lock2(new_nodeMeta[i]->rw_lock); // ------------------------------- lock here!!!
		}
		for (i=0;i<MAX_NODE_GROUP-1;i++) // connect
			new_nodeMeta[i]->next_node_in_group = new_nodeMeta[i+1];
		for (i=0;i<MAX_NODE_GROUP/2;i++)
		{
			new_nodeMeta[i]->group_cnt = i+1;
			new_nodeMeta[MAX_NODE_GROUP/2+i]->group_cnt = i+1;
		}

		new_nodeMeta[MAX_NODE_GROUP/2-1]->next_node_in_group = NULL;
		new_nodeMeta[MAX_NODE_GROUP/2]->next_p = old_nodeMeta[0]->next_p;
		new_nodeMeta[0]->next_p = new_nodeMeta[MAX_NODE_GROUP/2];
#if 0
		if (new_nodeMeta[0]->next_p == NULL) // didn't happen
			debug_error("next_p NULL");
		if (new_nodeMeta[MAX_NODE_GROUP/2]->next_p == NULL) // didn't happen
			debug_error("next_p NULL");
#endif

		//-------------------------------------------------------------------------------

		DataNode sorted_temp_dataNode[MAX_NODE_GROUP];
		std::vector<uint64_t> key_list_of_node[MAX_NODE_GROUP];
		std::vector<EntryAddr> old_ea_list[MAX_NODE_GROUP];
		int dst_group_idx;
		dst_group_idx = 0;
		addr = sorted_temp_dataNode[0].buffer;
		offset = 0;
		int size = key_list.size();
		for (i=0;i<size;i++)
		{
			memcpy(addr+offset,key_list.top().second.addr,ENTRY_SIZE);
			key_list_of_node[dst_group_idx].push_back(key_list.top().first);
			old_ea_list[dst_group_idx].push_back(key_list.top().second.ea);
			key_list.pop();
			offset+=ENTRY_SIZE;
			if (offset+ENTRY_SIZE > NODE_BUFFER_SIZE)
			{
				dst_group_idx++;
				addr = sorted_temp_dataNode[dst_group_idx].buffer;
				offset = 0;
			}
		}


		// 0 -> half -> next
		// [0 -> 1 ..] / [half -> half+1 ..]

		//memcpy to pmem


		// link!?
		for (i=0;i<MAX_NODE_GROUP-1;i++)
		{
			sorted_temp_dataNode[i].next_offset = emptyNodeAddr;
			sorted_temp_dataNode[i].next_offset_in_group = new_nodeMeta[i+1]->my_offset;
		}
		sorted_temp_dataNode[MAX_NODE_GROUP/2-1].next_offset_in_group = emptyNodeAddr;
		sorted_temp_dataNode[MAX_NODE_GROUP/2].next_offset = old_nodeMeta[0]->next_p->my_offset;
		sorted_temp_dataNode[0].next_offset = new_nodeMeta[MAX_NODE_GROUP/2]->my_offset;

		// fill new nodes
		int j;
		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;
		//		EntryAddr ea;
		for (i=0;i<MAX_NODE_GROUP;i++) // persist
		{
			new_dataNode = nodeAllocator->nodeAddr_to_node(new_nodeMeta[i]->my_offset);
			pmem_nt_write((unsigned char*)new_dataNode,(unsigned char*)&sorted_temp_dataNode[i],NODE_SIZE);
			addr = new_dataNode->buffer;
			offset = 0;
			for (j=0;j<key_list_of_node[i].size();j++) // index
			{
				key = key_list_of_node[i][j];
				kvp_p = hash_index->insert(key,&seg_lock,read_lock);
				ea.value = kvp_p->value;
				if (ea.value == old_ea_list[i][j].value) // didn't moved
				{
					new_nodeMeta[i]->valid[j] = true;
					new_nodeMeta[i]->valid_cnt++;

					//					ea.loc = 3; // cold
					ea.file_num = new_nodeMeta[i]->my_offset.pool_num;
					ea.offset = new_nodeMeta[i]->my_offset.node_offset*NODE_SIZE + NODE_HEADER_SIZE + offset;
#if 0
					//test check
					{
						if (nodeAllocator->node_cnt[ea.file_num] < ea.offset/NODE_SIZE)
							debug_error("errorrr111\n");
						uint64_t test_key;
						test_key = *(uint64_t*)(nodeAllocator->nodePoolList[ea.file_num]+ea.offset+HEADER_SIZE);
						if (test_key != key)
							debug_error("key error\n");
					}
#endif

					kvp_p->value = ea.value;

					//					EA_test(key,ea);

				}
#if 1
				else
					new_nodeMeta[i]->valid[j] = false; // doesn't need...
#endif

#if 0				//test
				uint64_t test_key = test_the_index(*kvp_p);
				if (test_key != kvp_p->key)
					debug_error("index error\n");
#endif
				hash_index->unlock_entry2(seg_lock,read_lock);
				offset+=ENTRY_SIZE;

			}
		}


		_mm_sfence();

		// link the list
		// link pmem first then dram...

		// do not change listNode just link new nodemeta
		ListNode* new_listNode;
		new_listNode = list->alloc_list_node();

		// don't alloc new node...
		listNode->data_node_addr = new_nodeMeta[0]->my_offset;
		new_listNode->data_node_addr = new_nodeMeta[MAX_NODE_GROUP/2]->my_offset;

		new_listNode->key = key_list_of_node[MAX_NODE_GROUP/2][0];
		new_listNode->prev = listNode;


		_mm_sfence();

		listNode->next->prev = new_listNode;
		new_listNode->next = listNode->next;
		listNode->next = new_listNode;

		_mm_sfence();

		// unlock
		for (i=0;i<MAX_NODE_GROUP;i++)
			at_unlock2(new_nodeMeta[i]->rw_lock);		
		// free the nodes...
		for (i=0;i<MAX_NODE_GROUP;i++)
			nodeAllocator->free_node(old_nodeMeta[i]);

	}
#endif
	int calc_th(DoubleLog* dl)
	{
		//		const size_t threshold = HARD_EVICT_SPACE;
		//				const size_t threshold = SOFT_EVICT_SPACE; // query thread is too busy 

		const size_t min_threshold = dl->hard_evict_space/2;//HARD_EVICT_SPACE/2;
								    //		const size_t max_threshold = SOFT_EVICT_SPACE/2;
		const size_t max_threshold = dl->hard_evict_space;//HARD_EVICT_SPACE;
		const size_t threshold = max_threshold-min_threshold;

		//HARD 0 ~ SOFT 100

		size_t empty_space = (dl->tail_sum+dl->my_size-dl->head_sum)%dl->my_size;
		if (empty_space < min_threshold)
			return 100; // always cold
		if (max_threshold < empty_space)
			return 0; // always hot
		return ((threshold)-(empty_space-min_threshold))*100/(threshold);
	}

	void PH_Thread::try_cold_split(ListNode *listNode,NodeMeta* list_nodeMeta,SkiplistNode* node)
	{
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

		at_unlock2(list_nodeMeta->rw_lock); // what?

		split_listNode_group(listNode,node); // we have the lock
						     //at_unlock2(next_listNode->lock);
						     //at_unlock2(listNode->lock);
		at_unlock2(prev_listNode->lock);

		//2-1-3 deadlock
	}

	EntryAddr PH_Thread::direct_to_cold(uint64_t key,unsigned char* value,KVP &kvp, std::atomic<uint8_t>* &seg_lock, SkiplistNode* skiplist_from_warm, bool new_update) // may lock from outside // have to be exist
	{
		//		printf("not now\n");
		//NO UNLOCK IN HERE!

		// 1 find skiplist node
		// 2 find listnode
		// 3 insert kv
		// 4 return addr

		timespec ts1,ts2;
		clock_gettime(CLOCK_MONOTONIC,&ts1);

		int cold_split_cnt = 0;

		EntryAddr new_ea;

		SkiplistNode* skiplist_node;

		ListNode* list_node;
		NodeMeta* list_nodeMeta;

		unsigned char entry[ENTRY_SIZE];
		const uint64_t z = 0;
		memcpy(entry,&z,ENTRY_HEADER_SIZE); // need to be zero for persist
		memcpy(entry+ENTRY_HEADER_SIZE,&key,KEY_SIZE);
		memcpy(entry+ENTRY_HEADER_SIZE+KEY_SIZE,value,VALUE_SIZE0);

		while(1)
		{
			if (skiplist_from_warm)
				skiplist_node = skiplist_from_warm;
			else
			{
				if (new_update) // use kvp
					skiplist_node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock,kvp);
				else // from evict can't use kvp
					skiplist_node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock);
				if (try_at_lock2(skiplist_node->lock) == false)
					continue;
				if (skiplist_node->key > key || (skiplist->sa_to_node(skiplist_node->next[0]))->key < key)
				{
					at_unlock2(skiplist_node->lock);
					continue;
				}
				//-------------------------------------------------------- skiplist
			}
			list_node = skiplist_node->my_listNode;

			while (list_node->next->key <= key)
				list_node = list_node->next;

			at_lock2(list_node->lock);
			if (list_node->key > key || list_node->next->key < key)
			{
				at_unlock2(list_node->lock);
				if (skiplist_from_warm == NULL)
					at_unlock2(skiplist_node->lock);
				continue;
			}

			//----------------------------------------------------------------------------- listnode

			list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(list_node->data_node_addr);
			while (list_nodeMeta->valid_cnt >= NODE_SLOT_MAX && list_nodeMeta->next_node_in_group != NULL) // find space in group
				list_nodeMeta = list_nodeMeta->next_node_in_group;

			// insert entry to listnode

			at_lock2(list_nodeMeta->rw_lock);

			//index ehrere;;;
			KVP* kvp_p;
			//			std::atomic<uint8_t> *seg_lock;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock); //index lock before write
			if (new_update == 0 && kvp.value != kvp_p->value) // changed before move
			{
				kvp.value = kvp_p->value; // return recent kvp
				new_ea.value = 0;
				hash_index->unlock_entry2(seg_lock,read_lock);

				at_unlock2(list_nodeMeta->rw_lock);
				at_unlock2(list_node->lock);
				if (skiplist_from_warm == NULL)
					at_unlock2(skiplist_node->lock);

				clock_gettime(CLOCK_MONOTONIC,&ts2);
				dtc_time+=(ts2.tv_sec-ts1.tv_sec)*1000000000+(ts2.tv_nsec-ts1.tv_nsec);
				return new_ea;
			}
			kvp = *kvp_p; // return old kvp

			new_ea = insert_entry_to_slot(list_nodeMeta,entry);


			if (new_ea.value != 0) // good
			{
				//				uint64_t new_version = kvp_p->version+1;
				EntryAddr old_ea;
				old_ea.value = kvp_p->value;
				EntryHeader new_version;
#ifdef KVP_VER
				new_version.value = kvp_p->version;
				if (new_update)
					new_version.version++;
#else					
				new_version.version = global_seq_num[key%COUNTER_MAX].fetch_add(1);
#endif
				//				new_version.prev_loc = old_ea.loc; // will be 3
				set_valid(new_version);

				// update version
				unsigned char* addr;
				addr = nodeAllocator->nodePoolList[new_ea.file_num]+new_ea.offset; // loc = 3
				*(uint64_t*)addr = new_version.value;
				pmem_persist(addr,ENTRY_HEADER_SIZE);
				_mm_sfence();

				kvp_p->value = new_ea.value;

				_mm_sfence();
				//write version after key value
#ifdef KVP_VER
				kvp_p->version = new_version.value; //???
				_mm_sfence();
#endif

				//				hash_index->unlock_entry2(seg_lock,read_lock); // unlock outer

				at_unlock2(list_nodeMeta->rw_lock);
				at_unlock2(list_node->lock);
				if (skiplist_from_warm == NULL)
					at_unlock2(skiplist_node->lock);

				//check
				//				direct_to_cold_cnt++;

				break;
			}
			else // cold split
			{
				hash_index->unlock_entry2(seg_lock,read_lock);

				// try append first
				if (list_nodeMeta->group_cnt < MAX_NODE_GROUP)
				{
					list_node->block_cnt++;
					//	ListNode* new_listNode = list->alloc_list_node();
					append_group(list_nodeMeta);
					at_unlock2(list_nodeMeta->rw_lock);
				}
				else
				{
					try_cold_split(list_node,list_nodeMeta,skiplist_node);
					cold_split_cnt++;
				}
			}
			// rw lock is unloced
			at_unlock2(list_node->lock);
			if (skiplist_from_warm == NULL)
				at_unlock2(skiplist_node->lock);
		}

		if (cold_split_cnt > 0)
			skiplist_node->half_listNode = find_halfNode(skiplist_node);

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

			if (dl->tail_sum > ll.offset || is_valid(header) == false)
				continue;
			if (dst != i)
				skiplistNode->entry_list[dst%WARM_NODE_ENTRY_CNT] = skiplistNode->entry_list[li];
			dst--;
		}

		skiplistNode->list_tail = dst+1;

	}


#define INDEX

	int PH_Query_Thread::insert_op(uint64_t key,unsigned char* value)
	{
		//	update_free_cnt();
		op_check();
#ifdef HASH_TEST
		// hash test

		{
			KVP* kvp_p;
			std::atomic<uint8_t> *seg_lock;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			kvp_p->key = key;
			kvp_p->value = *(uint64_t*)value;
			hash_index->unlock_entry2(seg_lock,read_lock);

			return 0;
		}
#endif
		EntryAddr new_ea;
		EntryAddr old_ea;
		unsigned char* pmem_head_p;// = my_log->get_pmem_head_p();

#if 0 
		unsigned char* checksum_i = (unsigned char*)&ble;
		int i,cnt=0;
		for (i=0;i<8+8+VALUE_SIZE;i++)
		{
			if (*checksum_i == 0)
				cnt++;
		}
		if (cnt == -1)
			printf("xxx\n");
#endif
		//fixed size;

		// use checksum or write twice

		//test---------------------------------
#if 0
		{
			unsigned char* dram_head_p;// = my_log->get_dram_head_p();
			dram_head_p = my_log->dramLogAddr + my_log->head_sum%my_log->my_size;

			uint64_t test_old_key;
			test_old_key = *(uint64_t*)(dram_head_p+HEADER_SIZE);

			int ex;
			KVP test_kvp;
			KVP* test_kvp_p;
			int test_seg_depth;
			volatile int* test_seg_depth_p;
			ex = hash_index->read(test_old_key,&test_kvp,&test_kvp_p,&test_seg_depth,&test_seg_depth_p);
			if (ex)
			{
				EntryAddr test_ea;
				test_ea.value = test_kvp.value;
				if (dram_head_p == doubleLogList[test_ea.file_num].dramLogAddr + test_ea.offset)
					printf("fiali43-------------------------------\n");
			}
		}
#endif
		//	my_log->ready_log();
		//	head_p = my_log->get_head_p();
		/*
		   BaseLogEntry *ble = (BaseLogEntry*)head_p;
		   ble->key = key;
		   memcpy(ble->value,value,VALUE_SIZE);
		 */


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
#ifdef NO_READ
		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
		kvp = *kvp_p;
		if (kvp.key == key)
			ex = 1;
		else
			ex = 0;
		hash_index->unlock_entry2(seg_lock,read_lock);
#else
		ex = hash_index->read(key,&kvp,&kvp_p,seg_depth,seg_depth_p);
#endif


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
		}
		else
		{
			old_ea.value = kvp_p->value;
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
		int rv,dtc;
		DoubleLog* dst_log;
		Loc dst_loc;
		size_t prev_head_sum;

		dtc = 0;

#ifdef USE_WARM_LOG

		if (ex == 0 || old_ea.loc == HOT_LOG) // promote warm
		{
			dst_log = my_log;
			dst_loc = HOT_LOG;
		}
		else if (old_ea.loc == WARM_LOG)
		{
			if (my_log->warm_per_hot > my_warm_log->head_sum-old_ea.offset)
			{
				dst_log = my_log;
				dst_loc = HOT_LOG;
			}
			else
			{
				dst_log = my_warm_log;
				dst_loc = WARM_LOG;
			}
		}
		else// if (old_ea.loc == WARM_LIST)
		{
			if (old_ea.loc == COLD_LIST) // dtc
			{
				rv = rand_r(&seed_for_dtc);
				if (/*reset_test_cnt || */(rv % 100) <= calc_th(my_warm_log) )// && false) // to cold // ratio condition
					dtc = 1;
				else
				{
					dst_log = my_warm_log;
					dst_loc = WARM_LOG;
				}
			}
			else
			{
				dst_log = my_warm_log;
				dst_loc = WARM_LOG;
			}
		}
#else
		if (ex == 1 && old_ea.loc == COLD_LIST)
		{
			rv = rand_r(&seed_for_dtc);
			if (/*reset_test_cnt || */(rv % 100) <= calc_th(my_log) )// && false) // to cold // ratio condition
				dtc = 1;
		}

		if (dtc == 0)
		{
			dst_log = my_log;
			dst_loc = HOT_LOG;
		}
#endif

		if (dtc)
		{
			dst_loc = COLD_LIST;
			//		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			//			kvp.value = 0;
			new_ea = direct_to_cold(key,value,kvp,seg_lock,NULL,true); // kvp becomes old one
			old_ea.value = kvp.value;

			direct_to_cold_cnt++;

			if (ex)
				invalidate_entry(old_ea);
			_mm_sfence();
			hash_index->unlock_entry2(seg_lock,read_lock);
		}
		else // to log
		{
			dst_log->ready_log();

			pmem_head_p = dst_log->pmemLogAddr + dst_log->head_sum % dst_log->my_size;
			//	dram_head_p = my_log->dramLogAddr + my_log->head_sum%my_log->my_size;

			//			new_ea.loc = 1; // hot
			new_ea.loc = dst_loc;
			new_ea.file_num = dst_log->log_num;
			new_ea.offset = dst_log->head_sum % dst_log->my_size;

			dst_log->insert_pmem_log(key,value);
			//--------------------------------------------------- make version
			//if (kvp_p)

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
					if (ex)
						node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock,kvp);
					else
						node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock);
					if (try_at_lock2(node->lock) == false)
						continue;
					next_node = skiplist->sa_to_node(node->next[0]);
					if (next_node->key <= key || node->key > key) // may split
					{
						at_unlock2(node->lock);
						continue;
					}
					/*
					if (next_node != skiplist->sa_to_node(node->next[0]))
					{
						at_unlock2(node->lock);
						continue;
					}
					*/

					if (may_split_warm_node(node))
					{
						at_unlock2(node->lock);
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

#ifdef KVP_VER

#else
			uint64_t new_ver;
			new_ver = global_seq_num[key%COUNTER_MAX].fetch_add(1);
#endif
			if (kvp_p->key != key) // new key...
			{
				//				new_version.prev_loc = 0;
				//				new_version.prev_loc = HOT_LOG;
				//				new_version = 1;
#ifdef KVP_VER
				new_version.version = 1;
#else
				new_version.version = new_ver;
#endif
				set_valid(new_version);
				ex = 0;
			}
			else
			{
#ifdef KVP_VER
				new_version.value = kvp_p->version;
				//				new_version.prev_loc = old_ea.loc;
				new_version.version++;
#else
				//				new_version.prev_loc = old_ea.loc;
				new_version.version = new_ver;
#endif
				set_valid(new_version);
				ex = 1;
			}

			// update version
			dst_log->write_version(new_version.value); // has fence

			// 3 get and write new version <- persist

			// 4 add dram list
#ifdef USE_DRAM_CACHE
			//	new_addr = dram_head_p;
			dst_log->insert_dram_log(new_version.value,key,value);
#else
			new_addr = pmem_head_p; // PMEM
#endif

			prev_head_sum = dst_log->head_sum;
			dst_log->head_sum+=ENTRY_SIZE;

			//check
			if (dst_loc == HOT_LOG)
				log_write_cnt++;
			else if (dst_loc == WARM_LOG)
				warm_log_write_cnt++;

			kvp_p->value = new_ea.value;
			kvp_p->version = new_version.value;
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
			if (ex) // violation!! // no entry lock
				invalidate_entry(old_ea);
			_mm_sfence();
			hash_index->unlock_entry2(seg_lock,read_lock);
		}
		//	if (kvp_p)
		// 5 add to key list if new key

		// 7 unlock index -------------------------------------- lock to here
		//		_mm_sfence();

		//		hash_index->unlock_entry2(seg_lock,read_lock);


		// 8 remove old dram list
#ifdef USE_DRAM_CACHE


#endif
		// 9 check GC

		return 0;
	}

	void PH_Thread::invalidate_entry(EntryAddr &ea) // need kv lock
	{
		unsigned char* addr;
		if (ea.loc == HOT_LOG || ea.loc == WARM_LOG) // hot log
		{
			addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
			set_invalid((EntryHeader*)addr); // invalidate
			hot_to_hot_cnt++; // log to hot
		}
		else // warm or cold
		{
			size_t offset_in_node;
			NodeMeta* nm;
			int cnt;
			int node_cnt;

			offset_in_node = ea.offset % NODE_SIZE;
			node_cnt = ea.offset/NODE_SIZE;
			nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
			//			at_lock2(nm->rw_lock);
			// it doesn't change value just invalidate with meta
#if 1 // we don't need batch... // no i need the batch
			if (ea.loc == 2)
			{
				int batch_num,offset_in_batch;
				batch_num = offset_in_node/WARM_BATCH_MAX_SIZE;
				offset_in_batch = offset_in_node%WARM_BATCH_MAX_SIZE;
				cnt = batch_num*WARM_BATCH_ENTRY_CNT + (offset_in_batch-NODE_HEADER_SIZE)/ENTRY_SIZE;
			}
			else
				cnt = (offset_in_node-NODE_HEADER_SIZE)/ENTRY_SIZE;
#else
			cnt = (offset_in_node-NODE_HEADER_SIZE)/ENTRY_SIZE;
#endif
#if 0
			if (nm->valid[cnt])
			{
				nm->valid[cnt] = false; // invalidate
				--nm->valid_cnt;
			}
			else
				debug_error("impossible\n");
#else
			nm->valid[cnt] = false; // invalidate
			--nm->valid_cnt;
#endif
			//			at_unlock2(nm->rw_lock);
		}

		// no nm rw lock but need hash entry lock...
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
					printf("deson't eixsit-----------------------------------------\n");
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

		std::atomic<uint8_t>* seg_lock;

		// if we use hash seg lock, do not use nm lock because there will be dead lock in cold evict
		// we will not use seg lock

		while (true)
		{
			//			seg_depth = hash_index->read(key,&ret,&seg_depth_p);
			ex = hash_index->read(key,&kvp,&kvp_p,seg_depth,seg_depth_p);
			//			seg_depth = *seg_depth_p;

			if (ex == 0)
				return -1;

			ea.value = kvp.value;

			unsigned char* addr;
			if (ea.loc == HOT_LOG || ea.loc == WARM_LOG) // hot or warm
			{
				size_t old_tail_sum,logical_tail,logical_offset,diff;
				old_tail_sum = doubleLogList[ea.file_num].tail_sum;
				logical_tail = old_tail_sum % doubleLogList[ea.file_num].my_size;
				if (old_tail_sum > ea.offset)
					logical_offset = ea.offset + doubleLogList[ea.file_num].my_size;
				else
					logical_offset = ea.offset;

				addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
				if (buf)
					memcpy(buf,addr+ENTRY_HEADER_SIZE+KEY_SIZE,VALUE_SIZE0);
				else
					value->assign((char*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE),VALUE_SIZE0);
				//				_mm_sfence();
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

				diff = doubleLogList[ea.file_num].tail_sum-old_tail_sum;
				if (logical_tail+diff > logical_offset) // entry may be overwritten // try again
					continue;
			}
			else // warm cold
			{
				NodeMeta* nm;
				int node_cnt,offset;

				node_cnt = ea.offset/NODE_SIZE;
				nm = (NodeMeta*)((unsigned char*)nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
				at_lock2(nm->rw_lock);
				_mm_sfence();
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

				if (kvp.key != key || kvp.value != ea.value) // updated?
				{
					at_unlock2(nm->rw_lock);
					continue;
				}

				//		at_lock2(nm->lock);
				addr = (unsigned char*)nodeAllocator->nodePoolList[ea.file_num]+ea.offset;
				//		if (key == *(uint64_t*)(addr+HEADER_SIZE))
				//			break;
				//		at_unlock2(nm->lock);

				//	hash_index->read(key,&ea.value);//retry
				if (buf)
					memcpy(buf,addr+ENTRY_HEADER_SIZE+KEY_SIZE,VALUE_SIZE0);
				else
					value->assign((char*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE),VALUE_SIZE0);
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
				break;
		}

		return 0;
	}
	int PH_Query_Thread::delete_op(uint64_t key)
	{
		//	update_free_cnt();
		op_check();

		return 0;
	}
	int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t end_key)
	{
		//	update_free_cnt();
		op_check();

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

		sleep_time = 1000;

		exit = 0;

		int ln = (log_max/2-1) / num_evict_thread+1;

		log_cnt = 0;
		log_list = new DoubleLog*[ln];
		warm_log_list = new DoubleLog*[ln];

		int i;
		for (i=0;i<log_max;i+=2)
		{
			while (doubleLogList[i].evict_alloc == 0)
			{
				//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
				if (try_at_lock2(doubleLogList[i].evict_alloc))
				{
					log_list[log_cnt] = &doubleLogList[i];
					//					warm_log_list[log_cnt] = &WDLL[i];
					warm_log_list[log_cnt] = &doubleLogList[i+1];
					log_cnt++;

					//					printf("evict thread %d -- log %d\n",thread_id,i);
					break;
				}
			}
			if (log_cnt >= ln)
				break;
		}

		if (log_cnt == 0)
			printf("can not find log\n");

		for (i=log_cnt;i<ln;i++)
			warm_log_list[i] = log_list[i] = NULL;

		child1_path = (int*)malloc(WARM_NODE_ENTRY_CNT * sizeof(int));
		child2_path = (int*)malloc(WARM_NODE_ENTRY_CNT * sizeof(int));

		evict_buffer = (unsigned char*)malloc(WARM_BATCH_MAX_SIZE);
		key_list_buffer = (uint64_t*)malloc(sizeof(uint64_t) * MAX_NODE_GROUP*(NODE_SIZE/ENTRY_SIZE));
		old_ea_list_buffer = (EntryAddr*)malloc(sizeof(EntryAddr) * MAX_NODE_GROUP*(NODE_SIZE/ENTRY_SIZE));

		//		hash_index->thread_local_init();
		temp_seg = hash_index->ret_seg();
		read_lock = 0;
		run = 1;

		//check
		warm_log_write_cnt = log_write_cnt = hot_to_warm_cnt = warm_to_cold_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		warm_to_warm_cnt = 0;
		direct_to_cold_cnt = 0;

		hard_htw_cnt = soft_htw_cnt=0;
		dtc_time = htw_time = wtc_time = 0;
		htw_cnt = wtc_cnt = 0;

	}

	void PH_Evict_Thread::clean()
	{
		//		hash_index->thread_local_clean();
		//		free(temp_seg); // ERROR!
		hash_index->remove_ts(temp_seg);

		run = 0;
		read_lock = 0;
		delete[] log_list; // remove pointers....
		delete[] warm_log_list;

		free(evict_buffer);
		free(key_list_buffer);
		free(old_ea_list_buffer);

		free(child1_path);
		free(child2_path);

		evict_thread_list[thread_id].lock = 0;
		evict_thread_list[thread_id].exit = 0;


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
	}

#if 0
	const size_t PMEM_BUFFER_SIZE = 256;

	void PH_Evict_Thread::split_listNode(ListNode *listNode,SkiplistNode *skiplistNode)
	{
		printf("don't use this now\n");

		DataNode *list_dataNode = nodeAddr_to_node(listNode->data_node_addr);
		NodeMeta *list_nodeMeta = nodeAddr_to_nodeMeta(listNode->data_node_addr);

		at_lock2(list_nodeMeta->rw_lock);
		if (list_nodeMeta->valid_cnt == 0)
		{
			at_unlock2(list_nodeMeta->rw_lock);
			return;
		}

		DataNode temp_node = *list_dataNode; // move to dram
		DataNode temp_new_node;
		uint64_t half_key;
		uint64_t key;

		size_t offset;
		size_t offset2;
		unsigned char* addr;
		addr = (unsigned char*)&temp_node;
		offset = NODE_HEADER_SIZE;
		offset2 = NODE_HEADER_SIZE;

		half_key = find_half_in_node(list_nodeMeta,&temp_node);
		//		half_key = pivot;
#if 0
		if (half_key == listNode->key)
		{
			at_unlock2(list_nodeMeta->rw_lock);
			return; // fail -- all same key?
		}
#endif
		ListNode* new_listNode = list->alloc_list_node();
		NodeMeta* new_nodeMeta = nodeAddr_to_nodeMeta(new_listNode->data_node_addr);
		DataNode* new_dataNode = nodeAddr_to_node(new_listNode->data_node_addr);

		new_listNode->key = half_key;
		new_listNode->next = listNode->next;
		new_listNode->prev = listNode;

		int moved_idx[NODE_SLOT_MAX];
		int moved_cnt=0;
		int i;
		for (i=0;i<NODE_SLOT_MAX;i++)
		{
			if (list_nodeMeta->valid[i] == false) // ??? invalidated??
			{
				//						new_nodeMeta->valid[i] = false;
				offset+=ENTRY_SIZE;
				continue;
			}
			key = *(uint64_t*)(addr+offset+HEADER_SIZE);
			if (key >= half_key)
			{
				//				new_nodeMeta->valid[moved_cnt] = true;
				memcpy((unsigned char*)&temp_new_node+offset2,(unsigned char*)&temp_node+offset,ENTRY_SIZE);
				//need invalicdation
				moved_idx[moved_cnt] = i;
				++moved_cnt;

				//						++new_nodeMeta->valid_cnt;
				//						--list_nodeMeta->valid_cnt;

				offset2+=ENTRY_SIZE;
			}
			offset+=ENTRY_SIZE;
		}

		temp_new_node.next_offset = list_nodeMeta->next_p->my_offset;
		//				pmem_node_nt_write(nodeAddr_to_node(new_listNode->data_node_addr),&temp_new_node,0,NODE_SIZE);
		pmem_nt_write((unsigned char*)new_dataNode,(unsigned char*)&temp_new_node,offset2);
		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		EntryAddr ea;
		for (i=0;i<moved_cnt;i++)
		{
			key = *(uint64_t*)(temp_new_node.buffer + ENTRY_SIZE*i + HEADER_SIZE);
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.loc = 3; // cold
			ea.file_num = list_nodeMeta->my_offset.pool_num;
			ea.offset = list_nodeMeta->my_offset.node_offset*NODE_SIZE + NODE_HEADER_SIZE + ENTRY_SIZE*moved_idx[i];
			if (kvp_p->value == ea.value) // doesn't moved
			{
				// ea cold
				ea.file_num = new_nodeMeta->my_offset.pool_num;
				ea.offset = new_nodeMeta->my_offset.node_offset*NODE_SIZE + NODE_HEADER_SIZE + ENTRY_SIZE*i;
				kvp_p->value = ea.value;
				kvp_p->version = set_loc_cold(kvp_p->version);
				new_nodeMeta->valid[i] = true;
				new_nodeMeta->valid_cnt++;
				list_nodeMeta->valid_cnt--;
			}
			else
				new_nodeMeta->valid[i] = false; // moved to hot
			_mm_sfence(); // need this?
			hash_index->unlock_entry2(seg_lock,read_lock);
			list_nodeMeta->valid[moved_idx[i]] = false;
		}

		//		new_nodeMeta->valid_cnt = moved_cnt;
		//		list_nodeMeta->valid_cnt-=moved_cnt;

		// link here
		new_nodeMeta->next_p = list_nodeMeta->next_p;

		//link
		pmem_next_write(list_dataNode,new_nodeMeta->my_offset);
		list_nodeMeta->next_p = new_nodeMeta;

		ListNode* old;
		while(skiplistNode->my_listNode.load()->key < half_key)// && skiplistNode->key >= half_key)
		{
			//			at_lock2(skiplistNode->lock);
			if (skiplistNode->key >= half_key)
			{
				old = skiplistNode->my_listNode;
				if (old->key >= half_key)
					break;
				if (skiplistNode->my_listNode.compare_exchange_strong(old,new_listNode) == false)
					continue;
				//				skiplistNode->my_listNode = new_listNode;
			}
			//			at_unlock2(skiplistNode->lock);
			skiplistNode = skiplistNode->next[0];
			// unlock here?
		}

		list->insert_node(listNode,new_listNode);

		at_unlock2(list_nodeMeta->rw_lock);

	}
#endif

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
	}

	void PH_Thread::split_empty_warm_node(SkiplistNode *old_skipListNode) // old node is deleted
	{
		//we will not reuse old skiplist

		uint64_t half_key;
		SkiplistNode* child1_sl_node;
		SkiplistNode* child2_sl_node;

		if (old_skipListNode->half_listNode == NULL)
			old_skipListNode->half_listNode = find_halfNode(old_skipListNode);
		half_key = old_skipListNode->half_listNode->key; // need skiplist node lock to access half listNode
		if (old_skipListNode->key >= half_key)
		{
			split_listNode_group(old_skipListNode->half_listNode,old_skipListNode);
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


		child1_sl_node = skiplist->allocate_node();
		child2_sl_node = skiplist->allocate_node();

		if (child1_sl_node == NULL || child2_sl_node == NULL) // alloc fail
		{
			printf("alloc fail\n");
			return;
		}

		at_lock2(child1_sl_node->lock);
		at_lock2(child2_sl_node->lock);

		child1_sl_node->key = old_skipListNode->key;
		child1_sl_node->my_listNode = old_skipListNode->my_listNode.load();

		child2_sl_node->key = half_key;
		child2_sl_node->my_listNode = old_skipListNode->half_listNode;

#ifdef HOT_KEY_LIST
		// key list split
		int i;
		for (i=0;i<old_skipListNode->key_list_size;i++)
		{
			if (old_skipListNode->key_list[i] < half_key)
				child1_sl_node->key_list[child1_sl_node->key_list_size++] = old_skipListNode->key_list[i];
			else
				child2_sl_node->key_list[child2_sl_node->key_list_size++] = old_skipListNode->key_list[i];
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
		next_meta = nodeAllocator->nodeAddr_to_nodeMeta(skiplist->sa_to_node(old_skipListNode->next[0])->data_node_addr[0]);

		nodeAllocator->linkNext(child1_meta,child2_meta);
		//		nodeAllocator->linkNext(child2_meta,skiplist->sa_to_node(old_skipListNode->next[0]));
		nodeAllocator->linkNext(child1_meta,next_meta);

		_mm_sfence();

		skiplist->insert_node(child1_sl_node,prev_sa_list,next_sa_list);
		skiplist->insert_node(child2_sl_node,prev_sa_list,next_sa_list);

		// prev - 0 - 1 - 2

		//		nodeAllocator->free_node(nodeMeta);

		// should be persist here

		child1_sl_node->list_head = child1_sl_node->list_tail = 0;
		child2_sl_node->list_head = child2_sl_node->list_tail = 0;

		at_unlock2(child1_sl_node->lock);
		at_unlock2(child2_sl_node->lock);

		//entry list
		// next dataNodeHeader
		// my_listNode myAddr data_node_addr
		//lock

		//head tail remain_cnt

		//delete old here...

		skiplist->delete_node(old_skipListNode); // delete duringn find node

	}

	int PH_Thread::may_split_warm_node(SkiplistNode *node) // had warm node lock // didn't had rw_lock
	{
		if (node->cold_block_sum > WARM_COLD_RATIO * WARM_MAX_NODE_GROUP || node->key_list_size >= WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT) // (WARM / COLD) RATIO
		{
			// flush all
			flush_warm_node(node);


			SkiplistNode *next_node;
			while(1)
			{
				next_node = skiplist->sa_to_node(node->next[0]);
				at_lock2(next_node->lock);
				if (next_node == skiplist->sa_to_node(node->next[0]))
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
		node_num = (node->data_tail%WARM_GROUP_ENTRY_CNT)/WARM_NODE_ENTRY_CNT;

		ListNode* listNode;
		NodeMeta *nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[node_num]);
		at_lock2(nodeMeta->rw_lock); // warm node lock..
		DataNode *dataNode = nodeAllocator->nodeAddr_to_node(node->data_node_addr[node_num]);
		size_t src_offset;
		int cnt = 0;
		uint64_t key;
		EntryAddr old_ea,new_ea;
		int i;
		int slot_idx;
		int cold_split_cnt = 0;

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;

		old_ea.loc = 2; // warm
		old_ea.file_num = node->data_node_addr[node_num].pool_num;
		//		old_ea.offset = node->data_node_addr.offset*NODE_SIZE + sizeof(NodeAddr);

		//		new_ea.loc = 3; // cold

		int start_slot,end_slot,batch_num;

		batch_num = (node->data_tail % WARM_NODE_ENTRY_CNT) / WARM_BATCH_ENTRY_CNT;
		start_slot = WARM_BATCH_ENTRY_CNT*batch_num;
		end_slot = start_slot + WARM_BATCH_ENTRY_CNT;

		memcpy(evict_buffer,(unsigned char*)dataNode + batch_num*WARM_BATCH_MAX_SIZE,WARM_BATCH_MAX_SIZE);

		//		src_offset = batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE; // in batch...
		src_offset = NODE_HEADER_SIZE; // temp buffer size is changed // in evict buffer...

		i = start_slot;
		//		for (i=start_slot;i<end_slot;i++)
		while (i < end_slot)
		{
			if (nodeMeta->valid[i] == false)
			{
				i++;
				src_offset+=ENTRY_SIZE;
				continue;
			}
			old_ea.offset = node->data_node_addr[node_num].node_offset*NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + src_offset;

			key = *(uint64_t*)(evict_buffer+src_offset+ENTRY_HEADER_SIZE);
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
			listNode = node->my_listNode;
			while (key >= listNode->next->key)
				listNode = listNode->next;

			NodeMeta* list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
			//			NodeMeta* first_nodeMeta_in_group = list_nodeMeta;
			//			new_ea.file_num = listNode->data_node_addr.pool_num;
			//			new_ea.offset = ln->data_node_addr.offset*NODE_SIZE;

			at_lock2(listNode->lock);//-------------------------------------lock dst cold
						 //			at_lock2(listNode->lock);
			if (key > listNode->next->key) // split??
			{
				at_unlock2(listNode->lock);
				//				hash_index->unlock_entry2(seg_lock,read_lock);
				continue;
			}

			while (list_nodeMeta->valid_cnt >= NODE_SLOT_MAX && list_nodeMeta->next_node_in_group != NULL) // find space in group
				list_nodeMeta = list_nodeMeta->next_node_in_group;

			at_lock2(list_nodeMeta->rw_lock);

#if 1
			// lock here
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			//					if (kvp_p->value != (uint64_t)addr) // moved
			if (kvp_p->value != old_ea.value) //by new update
			{
				//	EntryAddr test_entryAddr;
				//	test_entryAddr.value= kvp_p->value;
				hash_index->unlock_entry2(seg_lock,read_lock); // unlock
				at_unlock2(list_nodeMeta->rw_lock);
				at_unlock2(listNode->lock);
				++i;
				src_offset+=ENTRY_SIZE;
				continue; 
			}
			//------------------------------ entry locked!!
#endif


			new_ea = insert_entry_to_slot(list_nodeMeta,evict_buffer + src_offset);

#if 0
			if (list_nodeMeta->valid_cnt < NODE_SLOT_MAX)
			{
				for (slot_idx=0;slot_idx<NODE_SLOT_MAX;slot_idx++)
				{
					if (list_nodeMeta->valid[slot_idx] == false)
						break;
				}
			}
			else
				slot_idx = NODE_SLOT_MAX;

			new_ea.file_num = list_nodeMeta->my_offset.pool_num;
			if (slot_idx < NODE_SLOT_MAX)
			{
				old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;
				new_ea.offset = list_nodeMeta->my_offset.node_offset*NODE_SIZE + NODE_HEADER_SIZE + ENTRY_SIZE*slot_idx;

				//check
				warm_to_cold_cnt++;

				DataNode* list_dataNode = nodeAddr_to_node(list_nodeMeta->my_offset);
				pmem_entry_write(list_dataNode->buffer + ENTRY_SIZE*slot_idx , addr + src_offset, ENTRY_SIZE);
				list_nodeMeta->valid[slot_idx] = true; // validate
				++list_nodeMeta->valid_cnt;

				// modify hash index here
				//					kvp_p = hash_index->insert(key,&seg_lock,read_lock);

				//					set_loc_cold(kvp_p->version);
				//				kvp_p->version = set_loc_cold(kvp_p->version);
				//					kvp_p->value = (uint64_t)addr;
				kvp_p->value = new_ea.value;
				_mm_sfence();
				hash_index->unlock_entry2(seg_lock,read_lock);

				//					nodeMeta->valid[cnt] = false; //invalidate
				// not here...
				++cnt;
				src_offset+=ENTRY_SIZE;

				at_unlock2(list_nodeMeta->rw_lock);

			}
#endif
			//			if (i >= NODE_SLOT_MAX) // need split
			//			if (new_ea != emptyEntryAddr)
			if (new_ea.value != 0)
			{
				EntryHeader eh;
				eh.value = kvp_p->version;
				//				eh.prev_loc = 2;
				kvp_p->version = eh.value; // warm to cold ...
				kvp_p->value = new_ea.value;
				_mm_sfence();

				//				if (nodeMeta->valid[i] == false)
				//					debug_error("valid2\n");
				nodeMeta->valid[i] = false;
				nodeMeta->valid_cnt--;

				hash_index->unlock_entry2(seg_lock,read_lock);
				at_unlock2(list_nodeMeta->rw_lock);

				src_offset+=ENTRY_SIZE;
				i++;

				//check
				warm_to_cold_cnt++;
			}
			else // cold split
			{
				hash_index->unlock_entry2(seg_lock,read_lock);

				if (list_nodeMeta->group_cnt < MAX_NODE_GROUP) // append
				{
					listNode->block_cnt++;
					append_group(list_nodeMeta);
					at_unlock2(list_nodeMeta->rw_lock);
				}
				else // split
				{
					try_cold_split(listNode,list_nodeMeta,node);
					cold_split_cnt++;
				}
			}
			//	at_unlock2(list_nodeMeta->rw_lock);//-----------------------------------------unlock dst cold
			at_unlock2(listNode->lock);
		}

		node->data_tail+=(end_slot-start_slot);

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

		if (cold_split_cnt > 0)
			node->half_listNode = find_halfNode(node);

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

		// hot to warm should be in one node
		int node_num;
		// fixed size

		//target_cnt > 0 && may fit batch

		// want to make left + batch_num*batch = target_cnt

		//------------------------------------------------------------- calc cnt and make space

		unsigned char* dst_node;
		int i,j;
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

		size_t written_size;
		size_t start_offset;
		unsigned char* buffer_start;
		int write_cnt;
		int target_cnt;
		int batch_num;
		int i_dst;

		do
		{
			if (WARM_GROUP_ENTRY_CNT - (node->data_head-node->data_tail) < WARM_BATCH_ENTRY_CNT) // if no space
				warm_to_cold(node);

			node_num = (node->data_head%WARM_GROUP_ENTRY_CNT)/WARM_NODE_ENTRY_CNT;
			dst_node = (unsigned char*)nodeAllocator->nodeAddr_to_node(node->data_node_addr[node_num]);

			start_offset = NODE_HEADER_SIZE + (node->data_head % WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE; // always HEADER!
			buffer_start = evict_buffer+start_offset;
			batch_num = (node->data_head%WARM_NODE_ENTRY_CNT)/WARM_BATCH_ENTRY_CNT;

			written_size = 0;
			write_cnt = 0;
			target_cnt = WARM_BATCH_ENTRY_CNT - node->data_head%WARM_BATCH_ENTRY_CNT;

			for (i=node->list_tail;i<node->list_head && write_cnt < target_cnt;i++)
			{
				li = i%WARM_NODE_ENTRY_CNT; // need list max
				ll = node->entry_list[li];
				dl = &doubleLogList[ll.log_num];
				addr = dl->dramLogAddr + (ll.offset%dl->my_size);

				header = (EntryHeader*)addr;
				if (dl->tail_sum > ll.offset || is_valid(header) == false)
					node->entry_list[li].log_num = -1; // invalid
				else if (/*true ||*/ (/*header->prev_loc == 3 && */false)) // cold // hot to cold
				{
					//					printf("impossible\n");
					// direct to cold here
					// set old ea
					// direct to cold
					// invalidate old ea
					// unlock index

					if (ll.log_num%2 == 0)
						old_ea.loc = HOT_LOG;
					else
						old_ea.loc == WARM_LOG;
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
				}
				else
				{
					memcpy(buffer_start+written_size,addr,ENTRY_SIZE);

					written_size+=ENTRY_SIZE;
					//					if (header->prev_loc != 0)
					//						ex_entry_cnt++;

					write_cnt++;

					if (ll.log_num%2 == 0)
						hot_to_warm_cnt++;
					else
						warm_to_warm_cnt++;
				}
			}
			i_dst = i;

			//-----------------------------------------------

			NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[node_num]);
			at_lock2(nodeMeta->rw_lock);//------------------------------------------------lock here

			//------------------------------- evict buffer filled

			if (node->data_head % WARM_BATCH_ENTRY_CNT)
				pmem_nt_write(dst_node + batch_num*WARM_BATCH_MAX_SIZE + start_offset,buffer_start,written_size);
			else
			{
				if (batch_num == 0)
				{
					memcpy(evict_buffer,&nodeMeta->next_addr,sizeof(NodeAddr)); // warm node must be fixed
					memcpy(evict_buffer+sizeof(NodeAddr),&nodeMeta->next_addr_in_group,sizeof(NodeAddr)); // warm node must be fixed
				}
				pmem_nt_write(dst_node + batch_num*WARM_BATCH_MAX_SIZE,evict_buffer,WARM_BATCH_MAX_SIZE);

			}

			//------------------------------- pmem write

			// invalidate from here

			_mm_sfence();

			EntryAddr dst_addr,src_addr;
			dst_addr.loc = 2; // warm	
			dst_addr.file_num = nodeMeta->my_offset.pool_num;
			dst_addr.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + start_offset; //nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE + (node->data_head%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE;

			//			src_addr.loc = 1; //hot
			int slot_index = node->data_head%WARM_NODE_ENTRY_CNT; // have to in batch

			for (i=node->list_tail;i<i_dst;i++)
			{
				li = i % WARM_NODE_ENTRY_CNT;
				ll = node->entry_list[li];
				if (ll.log_num == -1)
					continue;

				dl = &doubleLogList[ll.log_num];
				addr = dl->dramLogAddr + (ll.offset%dl->my_size);
				header = (EntryHeader*)addr;
				key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

				if (ll.log_num%2 == 0)
					src_addr.loc = HOT_LOG;
				else
					src_addr.loc = WARM_LOG;

				src_addr.file_num = ll.log_num;
				src_addr.offset = ll.offset%dl->my_size; // dobuleloglist log num my size

				kvp_p = hash_index->insert(key,&seg_lock,read_lock);
				if (kvp_p->value == src_addr.value)
				{
					/*
					   EntryHeader eh;
					   eh.value = kvp_p->version;
					//					eh.prev_loc = 1;
					kvp_p->version = eh.value; // hot to warm... //set_prev_loc_warm(kvp_p->version);
					if (eh.prev_loc == 0 && eh.version > 1)
					debug_error("fefefe\n");

					//	kvp_p->version = set_loc_warm(kvp_p->version);
					 */
					// just change location
					kvp_p->value = dst_addr.value;
					/*
					   {
					   EntryAddr ta;
					   ta.value = kvp_p->value;
					   if (ta.loc != WARM_LIST || ((ta.offset%1024-NODE_HEADER_SIZE)%ENTRY_SIZE))
					   debug_error("warm offset error\n");
					   }
					 */
					nodeMeta->valid[slot_index] = true; // validate
					++nodeMeta->valid_cnt;
					_mm_sfence();
					set_invalid(header); // invalidate
#ifdef HOT_KEY_LIST	
					//remove the key from key list	
					uint64_t temp1,temp2=0;	
					for (j=node->key_list_size-1;j>=0;j--)
					{
						if (node->key_list[j] == key)
						{
							node->key_list[j] = temp2;
							break;
						}
						temp1 = node->key_list[j];
						node->key_list[j] = temp2;
						temp2 = temp1;
					}

					if (j < 0)
						debug_error("key not found\n");

					node->key_list_size--;
#endif
				}
#if 1 // may do nothing and save space... // no we need to push slot cnt because memory is already copied
				else // inserted during hot to warm
				{
					//					debug_error("htw\n");
					nodeMeta->valid[slot_index] = false; // validate fail
				}
#endif
				slot_index++;
				_mm_sfence(); // need?
				hash_index->unlock_entry2(seg_lock,read_lock);
				dst_addr.offset+=ENTRY_SIZE;
			}

			node->list_tail = i_dst;
			node->data_head+= write_cnt;

			at_unlock2(nodeMeta->rw_lock);//--------------------------------------------- unlock here
		}while(node->list_head-node->list_tail >= WARM_BATCH_ENTRY_CNT);

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

		//pass invalid
		while(dl->tail_sum+ENTRY_SIZE <= dl->head_sum)
		{
			addr = dl->dramLogAddr+(dl->tail_sum%dl->my_size);
			header.value = *(uint64_t*)addr;

			if (is_valid(header))
				break;
			dl->tail_sum+=ENTRY_SIZE;
			if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
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
			// evict now

			if (is_valid(header) == false)
			{
				dl->tail_sum+=ENTRY_SIZE;
				if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
					dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
				continue;
			}

			if (false /*&& header.prev_loc == 3*/)
			{
				if (dl->log_num%2 == 0)
					old_ea.loc = HOT_LOG;
				else
					old_ea.loc = WARM_LOG;
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

				dl->tail_sum+=ENTRY_SIZE;
				if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
					dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
				rv = 1;
			}
			else
			{
				SkiplistNode* node;
#ifdef ADDR2
				node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock);
#else
				node = skiplist->find_node(key,prev_sa_list,next_sa_list);
#endif
				if (try_at_lock2(node->lock) == false)
					continue;
				if ((skiplist->sa_to_node(node->next[0]))->key < key)
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

		//	if (dl->tail_sum + SOFT_EVICT_SPACE > dl->head_sum)
		//		return rv;

		//	while(dl->tail_sum+adv_offset + ble_len <= dl->head_sum && adv_offset <= SOFT_EVICT_SPACE )
		//	while (adv_offset <= SOFT_EVICT_SPACE)

		//
		if (dl->soft_adv_offset < dl->tail_sum) // jump if passed
			dl->soft_adv_offset = dl->tail_sum;

		while(dl->soft_adv_offset + ENTRY_SIZE + dl->my_size <= dl->head_sum + dl->soft_evict_space)//SOFT_EVICT_SPACE)
		{
			addr = dl->dramLogAddr + ((dl->soft_adv_offset) % dl->my_size);
			header.value = *(uint64_t*)addr;
			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

			if (is_valid(header))// && header.prev_loc != 3)// && is_checked(header) == false)
			{
				rv = 1;
				// regist the log num and size_t
				while(true) // the log allocated to this evict thread
				{
#ifdef ADDR2
					node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock);
#else
					node = skiplist->find_node(key,prev_sa_list,next_sa_list);
#endif
					if (try_at_lock2(node->lock) == false)
					{
						// too busy???
						// may need split here
						continue;
					}

					if ((skiplist->sa_to_node(node->next[0]))->key <= key || node->key > key) // may split
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

					//try push
					//	if (node->tail + WARM_NODE_ENTRY_CNT <= node->head)
					//	printf("warm node full!!\n");
					//	else
					//					if (node->list_tail + WARM_NODE_ENTRY_CNT > node->list_head) // list has space
					if (node->list_head - node->list_tail < WARM_BATCH_ENTRY_CNT)// WARM_LOG_MIN)
					{
						node->entry_list[node->list_head%WARM_NODE_ENTRY_CNT] = ll;
						//	node->entry_list.push_back(ll);
						//	node->entry_size_sum+=ENTRY_SIZE;
						node->list_head++; // lock...

						at_unlock2(node->lock);
						break;
					}
					list_gc(node);

					//					set_checked((uint64_t*)addr);

					// need to try flush
					//			node->try_hot_to_warm();
					//	if (node->entry_size_sum >= SOFT_BATCH_SIZE)
					if (node->list_head - node->list_tail >= WARM_BATCH_ENTRY_CNT)
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
					//					break;
				}
			}

			dl->soft_adv_offset+=ENTRY_SIZE;
			if ((dl->soft_adv_offset)%dl->my_size  + ENTRY_SIZE > dl->my_size)
				dl->soft_adv_offset+=(dl->my_size-((dl->soft_adv_offset)%dl->my_size));
			//		dl->check_turn(tail_sum,ble_len);


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
				if (evict_log(warm_log_list[i]))
					done = 0;
			}
			if (done)
			{
				usleep(1);
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
