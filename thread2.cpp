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

	extern size_t HARD_EVICT_SPACE;
	extern size_t SOFT_EVICT_SPACE;
	extern size_t SOFT_BATCH_SIZE;

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

	//check
	extern std::atomic<uint64_t> log_write_sum;
	extern std::atomic<uint64_t> hot_to_warm_sum;
	extern std::atomic<uint64_t> warm_to_cold_sum;
	extern std::atomic<uint64_t> direct_to_cold_sum;
	extern std::atomic<uint64_t> hot_to_hot_sum;
	extern std::atomic<uint64_t> hot_to_cold_sum;

	extern std::atomic<uint64_t> soft_htw_sum;
	extern std::atomic<uint64_t> hard_htw_sum;

	//	extern const size_t WARM_BATCH_MAX_SIZE;
	extern size_t WARM_BATCH_MAX_SIZE;
	extern size_t WARM_BATCH_ENTRY_CNT;
	extern size_t WARM_BATCH_CNT;
	extern size_t WARM_NODE_ENTRY_CNT;
	extern size_t WARM_LOG_MAX;

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
		printf(" not now\n");
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
		volatile int *seg_depth_p;
		int seg_depth;
		ex = hash_index->read(key,&kvp2,&kvp_p,&seg_depth,&seg_depth_p);

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
		log_write_cnt = hot_to_warm_cnt = warm_to_cold_cnt = direct_to_cold_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		soft_htw_cnt = hard_htw_cnt = 0;
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


	void PH_Query_Thread::init()
	{

		int i;

		my_log = 0;
		exit = 0;

		for (i=0;i<log_max;i++)
		{
			while (doubleLogList[i].use == 0)
			{
				//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
				if (try_at_lock2(doubleLogList[i].use))
					my_log = &doubleLogList[i];
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
		log_write_cnt = hot_to_warm_cnt = warm_to_cold_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		direct_to_cold_cnt = 0;

		soft_htw_cnt = hard_htw_cnt=0;

		seed_for_dtc = thread_id;
//		printf("sfd %u\n",seed_for_dtc);
	}

	void PH_Query_Thread::clean()
	{
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
		log_write_sum+=log_write_cnt;
		hot_to_warm_sum+=hot_to_warm_cnt;
		warm_to_cold_sum+=warm_to_cold_cnt;
		direct_to_cold_sum+=direct_to_cold_cnt;
		hot_to_hot_sum+=hot_to_hot_cnt;
		hot_to_cold_sum+=hot_to_cold_cnt;

		soft_htw_sum+=soft_htw_cnt;
		hard_htw_sum+=hard_htw_cnt;
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
	void pmem_next_in_group_write(DataNode* dst_node,NodeAddr nodeAddr)
	{
		dst_node->next_offset_in_group = nodeAddr;
		pmem_persist(dst_node,NODE_HEADER_SIZE);
		_mm_sfence();
	}

	void append_group(NodeMeta* list_nodeMeta)
	{
		NodeAddr new_nodeAddr = nodeAllocator->alloc_node();
		NodeMeta* new_nodeMeta = nodeAddr_to_nodeMeta(new_nodeAddr);
		DataNode* new_dataNode = nodeAddr_to_node(new_nodeAddr);
		pmem_next_in_group_write(nodeAddr_to_node(list_nodeMeta->my_offset),new_nodeAddr); // persist

		list_nodeMeta->next_node_in_group = new_nodeMeta;
		new_nodeMeta->group_cnt = list_nodeMeta->group_cnt+1;
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

			DataNode* list_dataNode = nodeAddr_to_node(list_nodeMeta->my_offset);
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
		NodeMeta *list_nodeMeta = nodeAddr_to_nodeMeta(listNode->data_node_addr);
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

			list_dataNode_p[group_idx] = nodeAddr_to_node(list_nodeMeta->my_offset);
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
			new_nodeMeta[i] = nodeAddr_to_nodeMeta(new_nodeAddr[i]);
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
			new_dataNode = nodeAddr_to_node(new_nodeMeta[i]->my_offset);
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

	int calc_th(DoubleLog* dl)
	{
		const size_t threshold = HARD_EVICT_SPACE;
		//		const size_t threshold = SOFT_EVICT_SPACE; // query thread is too busy 
		size_t empty_space = (dl->tail_sum+dl->my_size-dl->head_sum)%dl->my_size;
		if (empty_space < HARD_EVICT_SPACE/10)
			return 100;
		if (threshold < empty_space)
			return 0; // always hot
		return ((threshold)-empty_space)*100/(threshold);
	}

	EntryAddr PH_Thread::direct_to_cold(uint64_t key,unsigned char* value,KVP &kvp, std::atomic<uint8_t>* &seg_lock, SkiplistNode* skiplist_from_warm = NULL)
	{
//		printf("not now\n");
		//NO UNLOCK IN HERE!

		// 1 find skiplist node
		// 2 find listnode
		// 3 insert kv
		// 4 return addr

		EntryAddr new_ea;

		SkiplistNode* skiplist_node;
		SkiplistNode* prev[MAX_LEVEL+1];
		SkiplistNode* next[MAX_LEVEL+1];

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
				skiplist_node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock,kvp);
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

			list_nodeMeta = nodeAddr_to_nodeMeta(list_node->data_node_addr);
			while (list_nodeMeta->valid_cnt >= NODE_SLOT_MAX && list_nodeMeta->next_node_in_group != NULL) // find space in group
				list_nodeMeta = list_nodeMeta->next_node_in_group;

			// insert entry to listnode

			at_lock2(list_nodeMeta->rw_lock);

			//index ehrere;;;
			KVP* kvp_p;
			//			std::atomic<uint8_t> *seg_lock;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock); //index lock before write
			kvp = *kvp_p; // return old kvp

			new_ea = insert_entry_to_slot(list_nodeMeta,entry);


			if (new_ea.value != 0) // good
			{
				//				uint64_t new_version = kvp_p->version+1;
				EntryAddr old_ea;
				old_ea.value = kvp_p->value;
				EntryHeader new_version;
				new_version.value = kvp_p->version;
				new_version.prev_loc = old_ea.loc;
				new_version.version++;
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
				kvp_p->version = new_version.value; //???

//				test_the_index(*kvp_p);

				_mm_sfence();
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
					//	ListNode* new_listNode = list->alloc_list_node();
					append_group(list_nodeMeta);
					at_unlock2(list_nodeMeta->rw_lock);
				}
				else
				{

					// then split
					//split cold here
					//find half

					// this node is protecxted by rw lock of warm node
					// must prevent conflict with other split and delete

					ListNode* next_listNode;
					ListNode* prev_listNode;

					while(true) // lock prev and cur // next->prev only modifed by cur...
					{
						prev_listNode = list_node->prev;
						at_lock2(prev_listNode->lock);
						if (list_node->prev != prev_listNode || prev_listNode->next != list_node)
						{
							at_unlock2(prev_listNode->lock);
							continue;
						}
						//	at_lock2(listNode->lock);
						next_listNode = list_node->next;
						at_lock2(next_listNode->lock);
						if (list_node->next != next_listNode)
						{
							at_unlock2(next_listNode->lock);
							at_unlock2(prev_listNode->lock);
							continue;
						}
						break;
					}
					at_unlock2(list_nodeMeta->rw_lock);

					split_listNode_group(list_node,skiplist_node); // we have the lock

					at_unlock2(next_listNode->lock);
					//	at_unlock2(listNode->lock);
					at_unlock2(prev_listNode->lock);

					//2-1-3 deadlock
				}
			}
			// rw lock is unloced
			at_unlock2(list_node->lock);
			if (skiplist_from_warm == NULL)
				at_unlock2(skiplist_node->lock);
		}

		return new_ea;
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
		//	unsigned char* new_addr;
		//	unsigned char* old_addr;
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


		my_log->ready_log(); // to prevent dead lock ( from entry lock -> ready log ) it should be (ready_log -> entry lock)


		// 2 lock index ------------------------------------- lock from here

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		//		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
		//	kvp_p = hash_index->insert_with_fail(key,&seg_lock,read_lock);

		KVP kvp;
		volatile int *seg_depth_p;
		int seg_depth;
		int ex;

		ex = hash_index->read(key,&kvp,&kvp_p,&seg_depth,&seg_depth_p);

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
		rv = rand_r(&seed_for_dtc);
		if (ex == 1 && old_ea.loc > 1 && ((rv % 100) <= calc_th(my_log) ))// && false) // to cold // ratio condition
			dtc = 1;
		else
			dtc = 0;
		if (dtc)
		{
			//			printf("not now\n");

			//			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			new_ea = direct_to_cold(key,value,kvp,seg_lock); // kvp becomes old one
			old_ea.value = kvp.value;

			direct_to_cold_cnt++;
			/*
			   ea.loc = 3;
			   ea.file_num;
			   ea.offset;
			 */
#if 0
			//			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			if (kvp_p->key != key)
			{
				new_version = 1;
				set_valid(new_version);
				ex = 0;
			}
			else
			{
				new_version = kvp_p->version+1;
				set_valid(new_version);
				ex = 1;
			}
			//			new_version = kvp_p->version+1;

			// update version
			unsigned char* addr;
			addr = nodeAllocator->nodePoolList[new_ea.file_num]+new_ea.offset; // loc = 3
			*(uint64_t*)addr = new_version;
			pmem_persist(addr,HEADER_SIZE);
			_mm_sfence();
#endif
		}
		else // to log
		{

			//			my_log->ready_log();
			pmem_head_p = my_log->pmemLogAddr + my_log->head_sum%my_log->my_size;
			//	dram_head_p = my_log->dramLogAddr + my_log->head_sum%my_log->my_size;

			new_ea.loc = 1; // hot
			new_ea.file_num = my_log->log_num;
			new_ea.offset = my_log->head_sum%my_log->my_size;

			my_log->insert_pmem_log(key,value);
			//--------------------------------------------------- make version
			//if (kvp_p)

			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			old_ea.value = kvp_p->value;
			if (kvp_p->key != key)
			{
				new_version.prev_loc = 0;
				//				new_version = 1;
				set_valid(new_version);
				new_version.version = 1;
				ex = 0;
			}
			else
			{
				new_version.prev_loc = old_ea.loc;
				new_version.value = kvp_p->version;
				new_version.version++;
				set_valid(new_version);
				ex = 1;
			}

			// update version
			my_log->write_version(new_version.value); // has fence

			// 3 get and write new version <- persist

			// 4 add dram list
#ifdef USE_DRAM_CACHE
			//	new_addr = dram_head_p;
			my_log->insert_dram_log(new_version.value,key,value);
#else
			new_addr = pmem_head_p; // PMEM
#endif

			my_log->head_sum+=ENTRY_SIZE;

			//check
			log_write_cnt++;

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
#if 0				//test
				uint64_t test_key = test_the_index(*kvp_p);
				if (test_key != kvp_p->key)
					debug_error("index error\n");
#endif

				hash_index->unlock_entry2(seg_lock,read_lock);
				return 0;
			}
			//			hash_index->unlock_entry2(seg_lock,read_lock);

		}
		//	if (kvp_p)
		// 5 add to key list if new key

		// 7 unlock index -------------------------------------- lock to here
		//		_mm_sfence();
#if 0				//test
		uint64_t test_key = test_the_index(*kvp_p);
		if (test_key != kvp_p->key)
			debug_error("index error\n");
#endif

		//		hash_index->unlock_entry2(seg_lock,read_lock);


		// 8 remove old dram list
#ifdef USE_DRAM_CACHE

		invalidate_entry(old_ea);
		hash_index->unlock_entry2(seg_lock,read_lock);

#endif
		// 9 check GC

		return 0;
	}

	void PH_Thread::invalidate_entry(EntryAddr &ea)
	{
		unsigned char* addr;
		if (ea.loc == 1) // hot log
		{
			addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
			set_invalid((EntryHeader*)addr); // invalidate
			hot_to_hot_cnt++;
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
			if (ea.loc == 2)
			{
				int batch_num,offset_in_batch;
				batch_num = offset_in_node/WARM_BATCH_MAX_SIZE;
				offset_in_batch = offset_in_node%WARM_BATCH_MAX_SIZE;
				cnt = batch_num*WARM_BATCH_ENTRY_CNT + (offset_in_batch-NODE_HEADER_SIZE)/ENTRY_SIZE;
			}
			else
				cnt = (offset_in_node-NODE_HEADER_SIZE)/ENTRY_SIZE;

			//			if ( (offset_in_node-NODE_HEADER_SIZE)%ENTRY_SIZE != 0)
			//				debug_error("divde\n");


			if (nm->valid[cnt])
			{
				nm->valid[cnt] = false; // invalidate
				--nm->valid_cnt;
			}
			else
				debug_error("impossible\n");
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
				ex = hash_index->read(key,&kvp,&kvp_p,&split_cnt,&split_cnt_p);
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
		volatile int *seg_depth_p;
		int seg_depth;
		KVP* kvp_p;
		KVP kvp;
		uint64_t ret;
		int ex;
		while (true)
		{
			//			seg_depth = hash_index->read(key,&ret,&seg_depth_p);
			ex = hash_index->read(key,&kvp,&kvp_p,&seg_depth,&seg_depth_p);

			//			seg_depth = *seg_depth_p;

			if (ex == 0)
				return -1;

			ea.value = kvp.value;


			unsigned char* addr;
			if (ea.loc == 1) // hot
			{
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
				if (ea.value != kvp_p->value)
				{
					continue;
				}

			}
			else // warm cold
			{
				NodeMeta* nm;
				int node_cnt,offset;

				node_cnt = ea.offset/NODE_SIZE;
				nm = (NodeMeta*)((unsigned char*)nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
				at_lock2(nm->rw_lock);
				_mm_sfence();
				if (ea.loc == 3)
					offset = ((ea.offset-NODE_HEADER_SIZE)%NODE_SIZE)/ENTRY_SIZE;
				else
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
					log_list[log_cnt++] = &doubleLogList[i];
//					printf("evict thread %d -- log %d\n",thread_id,i);
					break;
				}
			}
			if (log_cnt >= ln)
				break;
		}

		for (i=log_cnt;i<ln;i++)
			log_list[i] = NULL;

		child1_path = (int*)malloc(WARM_NODE_ENTRY_CNT * sizeof(int));
		child2_path = (int*)malloc(WARM_NODE_ENTRY_CNT * sizeof(int));

		htw_evict_buffer = (unsigned char*)malloc(WARM_BATCH_MAX_SIZE);

//		hash_index->thread_local_init();
		temp_seg = hash_index->ret_seg();
		read_lock = 0;
		run = 1;

		//check
		log_write_cnt = hot_to_warm_cnt = warm_to_cold_cnt = hot_to_hot_cnt = hot_to_cold_cnt = 0;
		direct_to_cold_cnt = 0;

		hard_htw_cnt = soft_htw_cnt=0;

	}

	void PH_Evict_Thread::clean()
	{
		//		hash_index->thread_local_clean();
//		free(temp_seg); // ERROR!
		hash_index->remove_ts(temp_seg);

		run = 0;
		read_lock = 0;
		delete[] log_list; // remove pointers....

		free(htw_evict_buffer);

		free(child1_path);
		free(child2_path);

		evict_thread_list[thread_id].lock = 0;
		evict_thread_list[thread_id].exit = 0;


		//check
		log_write_sum+=log_write_cnt;
		hot_to_warm_sum+=hot_to_warm_cnt;
		warm_to_cold_sum+=warm_to_cold_cnt;
		direct_to_cold_sum+=direct_to_cold_cnt;
		hot_to_hot_sum+=hot_to_hot_cnt;
		hot_to_cold_sum+=hot_to_cold_cnt;

		soft_htw_sum+=soft_htw_cnt;
		hard_htw_sum+=hard_htw_cnt;

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
	/*
	   template <typename A,typename B>
	   std::pair<A,B> operator<(const std::pair<A,B> &l, const std::pair<A,B> &r)
	   {
	   return l.first < r.first;
	   }
	 */

	void PH_Evict_Thread::split_warm_node(SkiplistNode* old_skipListNode,ListNode* half_listNode)
	{
		uint64_t half_key;
		SkiplistNode* child1_sl_node;
		SkiplistNode* child2_sl_node;

		half_key = half_listNode->key;

		child1_sl_node = skiplist->alloc_sl_node();
		child2_sl_node = skiplist->alloc_sl_node();

		if (child1_sl_node == NULL || child2_sl_node == NULL) // alloc fail
			return;

		at_lock2(child1_sl_node->lock);
		at_lock2(child2_sl_node->lock);

		child1_sl_node->key = old_skipListNode->key;
		child2_sl_node->key = half_key;

		NodeMeta* nodeMeta;
		NodeMeta* new_nodeMeta;

		nodeMeta = nodeAddr_to_nodeMeta(old_skipListNode->data_node_addr);

		// move data

		DataNode parent_temp_dataNode;
		DataNode child1_temp_dataNode,child2_temp_dataNode;
		DataNode* parent_dataNode;

		parent_dataNode = nodeAddr_to_node(old_skipListNode->data_node_addr);

		parent_temp_dataNode = *parent_dataNode;
		memset(&child1_temp_dataNode,0,sizeof(DataNode));
		memset(&child2_temp_dataNode,0,sizeof(DataNode));
		// data head to tail
		int parent_batch_num;
		int i,j;
		int parent_index;
		int child1_batch_num,child2_batch_num;
		int child1_index_in_batch,child2_index_in_batch;
		int child1_index,child2_index;

		child1_batch_num = child2_batch_num = 0;
		child1_index = child2_index = 0;
		child1_index_in_batch = child2_index_in_batch = 0;

		unsigned char* src_addr;
		unsigned char* child1_dst_addr;
		unsigned char* child2_dst_addr;
		uint64_t key;

		// all valid from head
		// ---t------h------->

		// h-------------t---

		parent_batch_num = (old_skipListNode->data_head % WARM_NODE_ENTRY_CNT) / WARM_BATCH_ENTRY_CNT;

		child1_dst_addr = (unsigned char*)&child1_temp_dataNode + WARM_BATCH_MAX_SIZE * child1_batch_num + NODE_HEADER_SIZE + child1_index_in_batch * ENTRY_SIZE;
		child2_dst_addr = (unsigned char*)&child2_temp_dataNode + WARM_BATCH_MAX_SIZE * child2_batch_num + NODE_HEADER_SIZE + child2_index_in_batch * ENTRY_SIZE;

		for (i=0;i<WARM_BATCH_CNT;i++)
		{
			src_addr = (unsigned char*)&parent_temp_dataNode + WARM_BATCH_MAX_SIZE * parent_batch_num + NODE_HEADER_SIZE;
			parent_index = parent_batch_num*WARM_BATCH_ENTRY_CNT;
			for (j=0;j<WARM_BATCH_ENTRY_CNT;j++)
			{
				if (nodeMeta->valid[parent_index])
				{
					key = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE);
					if (key < half_key) // child1
					{
						memcpy(child1_dst_addr,src_addr,ENTRY_SIZE);
						child1_path[child1_index] = parent_index;
						child1_index_in_batch++;
						child1_index++;
						if (child1_index_in_batch >= WARM_BATCH_ENTRY_CNT)
						{
							child1_index_in_batch = 0;
							child1_batch_num++;
							child1_dst_addr = (unsigned char*)&child1_temp_dataNode + WARM_BATCH_MAX_SIZE * child1_batch_num + NODE_HEADER_SIZE + child1_index_in_batch * ENTRY_SIZE;
						}
						else
							child1_dst_addr+=ENTRY_SIZE;
					}
					else
					{
						memcpy(child2_dst_addr,src_addr,ENTRY_SIZE);
						child2_path[child2_index] = parent_index;
						child2_index_in_batch++;
						child2_index++;
						if (child2_index_in_batch >= WARM_BATCH_ENTRY_CNT)
						{
							child2_index_in_batch = 0;
							child2_batch_num++;
							child2_dst_addr = (unsigned char*)&child2_temp_dataNode + WARM_BATCH_MAX_SIZE * child2_batch_num + NODE_HEADER_SIZE + child2_index_in_batch * ENTRY_SIZE;
						}
						else
							child2_dst_addr+=ENTRY_SIZE;

					}
				}
				src_addr+=ENTRY_SIZE;
				parent_index++;
				parent_index%=WARM_NODE_ENTRY_CNT;
			}
			parent_batch_num++;
			parent_batch_num%=WARM_BATCH_CNT;
		}

		// set new nodes

		DataNode* child1_dataNode_p;
		DataNode* child2_dataNode_p;

		NodeAddr child1_addr,child2_addr;
		child1_addr = nodeAllocator->alloc_node();
		child2_addr = nodeAllocator->alloc_node();
		child1_dataNode_p = nodeAddr_to_node(child1_addr);
		child2_dataNode_p = nodeAddr_to_node(child2_addr);

		child1_temp_dataNode.next_offset = child2_addr;
		child2_temp_dataNode.next_offset = (skiplist->sa_to_node(old_skipListNode->next[0]))->data_node_addr;
		//		child2_temp_dataNode.next_offset = node->next[0].load()->data_node_addr; // do we have the lock?

		child1_sl_node->dataNodeHeader = child1_temp_dataNode.next_offset;
		child2_sl_node->dataNodeHeader = child2_temp_dataNode.next_offset;

		pmem_nt_write((unsigned char*)child1_dataNode_p,(unsigned char*)&child1_temp_dataNode,NODE_SIZE);
		pmem_nt_write((unsigned char*)child2_dataNode_p,(unsigned char*)&child2_temp_dataNode,NODE_SIZE);

		//		new_skipListNode->data_node_addr = child2_addr;
		//		new_nodeMeta = nodeAddr_to_nodeMeta(new_skipListNode->data_node_addr);

		_mm_sfence();

		// link

		// 1 build child1 and child2
		// 2 connect prev to child1
		// insert sl 1 2 
		// delete sl 0

		//		new_nodeMeta->next_p = nodeMeta->next_p;
		//		nodeMeta->next_p = new_nodeMeta;

		NodeMeta* child1_nodeMeta;
		NodeMeta* child2_nodeMeta;

		child1_nodeMeta = nodeAddr_to_nodeMeta(child1_addr);
		child2_nodeMeta = nodeAddr_to_nodeMeta(child2_addr);

		child1_nodeMeta->next_p = child2_nodeMeta;
		child2_nodeMeta->next_p = nodeMeta->next_p;

		child1_sl_node->data_node_addr = child1_addr;
		child2_sl_node->data_node_addr = child2_addr;

		//data nodes are already written
		child1_sl_node->my_listNode = old_skipListNode->my_listNode.load();
		child2_sl_node->my_listNode = half_listNode; // may need to lock the node

		SkiplistNode* prev[MAX_LEVEL+1];
		SkiplistNode* next[MAX_LEVEL+1];
		skiplist->insert_node(child1_sl_node,prev_sa_list,next_sa_list);
		skiplist->insert_node(child2_sl_node,prev_sa_list,next_sa_list);

		// prev - 0 - 1 - 2

		//		nodeAllocator->free_node(nodeMeta);

		// should be persist here



		//index here

		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;
		EntryAddr old_ea,new_ea;

		old_ea.loc = 2; //WARM
		old_ea.file_num = old_skipListNode->data_node_addr.pool_num;

		new_ea.loc = 2;
		new_ea.file_num = child1_addr.pool_num;

		for (i=0;i<child1_index;i++)
		{
			key = *(uint64_t*)((unsigned char*)&child1_temp_dataNode + WARM_BATCH_MAX_SIZE * (i/WARM_BATCH_ENTRY_CNT) + NODE_HEADER_SIZE + ENTRY_SIZE * (i%WARM_BATCH_ENTRY_CNT) + ENTRY_HEADER_SIZE);
			old_ea.offset = old_skipListNode->data_node_addr.node_offset*NODE_SIZE + WARM_BATCH_MAX_SIZE*(child1_path[i]/WARM_BATCH_ENTRY_CNT) + NODE_HEADER_SIZE + ENTRY_SIZE * (child1_path[i]%WARM_BATCH_ENTRY_CNT);
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);

			if (kvp_p->value != old_ea.value)
			{
				child1_nodeMeta->valid[i] = false;
				hash_index->unlock_entry2(seg_lock,read_lock);
				continue;
			}

			child1_nodeMeta->valid[i] = true;// valid...
			child1_nodeMeta->valid_cnt++;

			new_ea.offset = child1_addr.node_offset*NODE_SIZE + WARM_BATCH_MAX_SIZE*(i/WARM_BATCH_ENTRY_CNT) + NODE_HEADER_SIZE + ENTRY_SIZE * (i%WARM_BATCH_ENTRY_CNT);
			kvp_p->value = new_ea.value;

//			EA_test(key,new_ea);

			hash_index->unlock_entry2(seg_lock,read_lock);

		}
		child1_sl_node->data_tail = 0;
		child1_sl_node->data_head = child1_index;

		new_ea.file_num = child2_addr.pool_num;

		for (i=0;i<child2_index;i++)
		{
			key = *(uint64_t*)((unsigned char*)&child2_temp_dataNode + WARM_BATCH_MAX_SIZE * (i/WARM_BATCH_ENTRY_CNT) + NODE_HEADER_SIZE + ENTRY_SIZE * (i%WARM_BATCH_ENTRY_CNT) + ENTRY_HEADER_SIZE);
			old_ea.offset = old_skipListNode->data_node_addr.node_offset*NODE_SIZE + WARM_BATCH_MAX_SIZE*(child2_path[i]/WARM_BATCH_ENTRY_CNT) + NODE_HEADER_SIZE + ENTRY_SIZE * (child2_path[i]%WARM_BATCH_ENTRY_CNT);
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);

			if (kvp_p->value != old_ea.value)
			{
				child2_nodeMeta->valid[i] = false;
				hash_index->unlock_entry2(seg_lock,read_lock);
				continue;
			}

			// valid...

			child2_nodeMeta->valid[i] = true;
			child2_nodeMeta->valid_cnt++;

			new_ea.offset = child2_addr.node_offset*NODE_SIZE + WARM_BATCH_MAX_SIZE*(i/WARM_BATCH_ENTRY_CNT) + NODE_HEADER_SIZE + ENTRY_SIZE * (i%WARM_BATCH_ENTRY_CNT);
			kvp_p->value = new_ea.value;

//			EA_test(key,new_ea);

			hash_index->unlock_entry2(seg_lock,read_lock);
		}
		child2_sl_node->data_tail = 0;
		child2_sl_node->data_head = child2_index;


		child1_sl_node->list_head = child1_sl_node->list_tail = 0;
		child2_sl_node->list_head = child2_sl_node->list_tail = 0;

		LogLoc ll;
		DoubleLog *dl;

		for (i=old_skipListNode->list_tail;i<old_skipListNode->list_head;i++)
		{
			ll = old_skipListNode->entry_list[i%WARM_NODE_ENTRY_CNT];
			dl = &doubleLogList[ll.log_num];
			key = *(uint64_t*)(dl->dramLogAddr+(ll.offset%dl->my_size)+ENTRY_HEADER_SIZE);
			if (key < half_key)
			{
				child1_sl_node->entry_list[child1_sl_node->list_head%WARM_NODE_ENTRY_CNT] = ll;
				child1_sl_node->list_head++;
			}
			else
			{
				child2_sl_node->entry_list[child2_sl_node->list_head%WARM_NODE_ENTRY_CNT] = ll;
				child2_sl_node->list_head++;
			}
		}

		at_unlock2(child1_sl_node->lock);
		at_unlock2(child2_sl_node->lock);

		//entry list
		// next dataNodeHeader
		// my_listNode myAddr data_node_addr
		//lock

		//head tail remain_cnt

		//delete old here...

		skiplist->delete_node(old_skipListNode);

	}

	void PH_Evict_Thread::warm_to_cold(SkiplistNode* node)
	{
		//	printf("warm_to_cold\n");
		ListNode* listNode;
		NodeMeta *nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
		at_lock2(nodeMeta->rw_lock);
		//		DataNode dataNode = *nodeAddr_to_node(node->data_node_addr); // dram copy
		DataNode *dataNode = nodeAddr_to_node(node->data_node_addr);
		size_t src_offset;
		int cnt = 0;
		uint64_t key;
		//		unsigned char* addr = (unsigned char*)&dataNode;
		EntryAddr old_ea,new_ea;
		int i;
		int slot_idx;

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;

		old_ea.loc = 2; // warm
		old_ea.file_num = node->data_node_addr.pool_num;
		//		old_ea.offset = node->data_node_addr.offset*NODE_SIZE + sizeof(NodeAddr);

		//		new_ea.loc = 3; // cold

		//while (offset < NODE_SIZE)

		int start_slot,end_slot,batch_num;

		//		batch_num = ((node->head+WARM_BATCH_CNT)%WARM_NODE_ENTRY_CNT)/WARM_BATCH_ENTRY_CNT;
		batch_num = (node->data_tail % WARM_NODE_ENTRY_CNT) / WARM_BATCH_ENTRY_CNT;
		start_slot = WARM_BATCH_ENTRY_CNT*batch_num;
		end_slot = start_slot + WARM_BATCH_ENTRY_CNT;

		//		cnt = 0;
		//		while (cnt < nodeMeta->slot_cnt)

		unsigned char temp_buffer[WARM_BATCH_MAX_SIZE];
		memcpy(temp_buffer,(unsigned char*)dataNode + batch_num*WARM_BATCH_MAX_SIZE,WARM_BATCH_MAX_SIZE);

		//		src_offset = batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE;
		src_offset = NODE_HEADER_SIZE; // temp buffer size is changed

		cnt = start_slot;
		while (cnt < end_slot)
		{
			if (nodeMeta->valid[cnt] == false)
			{
				++cnt;
				src_offset+=ENTRY_SIZE;
				continue;
			}
			old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + src_offset;

			key = *(uint64_t*)(temp_buffer+src_offset+ENTRY_HEADER_SIZE);
//			EA_test(key,old_ea);
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

			NodeMeta* list_nodeMeta = nodeAddr_to_nodeMeta(listNode->data_node_addr);
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


#if 0
	uint64_t test_key = test_the_index(*kvp_p);
	if (test_key != kvp_p->key)
		debug_error("index error\n");
#endif

			if (kvp_p->value != old_ea.value) //by new update
			{
				//	EntryAddr test_entryAddr;
				//	test_entryAddr.value= kvp_p->value;
				hash_index->unlock_entry2(seg_lock,read_lock); // unlock
				at_unlock2(list_nodeMeta->rw_lock);
				at_unlock2(listNode->lock);
				++cnt;
				src_offset+=ENTRY_SIZE;
				continue; 
			}
			//------------------------------ entry locked!!
#endif


			new_ea = insert_entry_to_slot(list_nodeMeta,temp_buffer + src_offset);

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
				eh.prev_loc = 2;
				kvp_p->version = eh.value; // warm to cold ...
				kvp_p->value = new_ea.value;
				_mm_sfence();

				nodeMeta->valid[cnt] = false;
				nodeMeta->valid_cnt--;

				hash_index->unlock_entry2(seg_lock,read_lock);
				at_unlock2(list_nodeMeta->rw_lock);

				++cnt;
				src_offset+=ENTRY_SIZE;

				//check
				warm_to_cold_cnt++;
			}
			else // cold split
			{
				hash_index->unlock_entry2(seg_lock,read_lock);

				// try append first
				if (list_nodeMeta->group_cnt < MAX_NODE_GROUP)
				{
					// append
					//	ListNode* new_listNode = list->alloc_list_node();
					append_group(list_nodeMeta);
					at_unlock2(list_nodeMeta->rw_lock);
				}
				else
				{

					// then split
					//split cold here
					//find half

					// this node is protecxted by rw lock of warm node
					// must prevent conflict with other split and delete

					//					ListNode* next_listNode;
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
						//						at_lock2(listNode->lock);
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

					at_unlock2(list_nodeMeta->rw_lock);

					split_listNode_group(listNode,node); // we have the lock

					//					at_unlock2(next_listNode->lock);
					//						at_unlock2(listNode->lock);
					at_unlock2(prev_listNode->lock);

					//2-1-3 deadlock
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

		// 1 check cold length and count them
		//		int cnt;
		uint64_t current_key,next_key;
		//		ListNode* listNode;
		ListNode* half_listNode;
		current_key = node->key;
		next_key = (skiplist->sa_to_node(node->next[0]))->key;
		listNode = node->my_listNode;
		half_listNode = node->my_listNode;
		cnt = 0;
		while (next_key > listNode->key)
		{
			listNode = listNode->next;
			++cnt;
			if (cnt%2 == 0)
				half_listNode = half_listNode->next;
		}

		if (cnt * MAX_NODE_GROUP > WARM_COLD_RATIO) // (WARM / COLD) RATIO
		{
			//			SkiplistNode* new_skipListNode = skiplist->alloc_sl_node();
			//			if (new_skipListNode)
			{
				NodeMeta* nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
				//				at_lock2(nodeMeta->rw_lock);
//				hot_to_warm(node,true); // flush all
				split_warm_node(node,half_listNode);
				//				at_unlock2(nodeMeta->rw_lock);
			}
		}
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

	}

	void PH_Evict_Thread::hot_to_warm(SkiplistNode* node,bool evict_all)
	{
		int i;
		LogLoc ll;
		unsigned char* addr;
		EntryHeader* header;
		DoubleLog* dl;
		//	unsigned char node_buffer[NODE_SIZE]; 
		//		size_t entry_list_cnt;

		NodeMeta* nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
		//		at_lock2(nodeMeta->rw_lock);//------------------------------------------------lock here

		//	Node temp_node;
		//		DataNode* dst_node = nodeAddr_to_node(node->data_node_addr);
		unsigned char* dst_node = (unsigned char*)nodeAddr_to_node(node->data_node_addr);

		size_t write_size;
		//		unsigned char* buffer_write_start = temp_node.buffer+nodeMeta->written_size;
		//		unsigned char* buffer_write_start = htw_evict_buffer + ((node->tail%WARM_NODE_ENTRY_CNT)/WARM_BATCH_ETNRY_CNT)*WARM_BATCH_SIZE + NODE_HEADER_SIZE + (node->tail%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE;
		unsigned char* buffer_write_start;
		int batch_num;

		// buffer
		// header | start1 ~~~ end1 | pad |1024| empty header | start2 ~~~ end2 | pad |2048| ...

		//		std::atomic<uint8_t>* seg_lock;
		/*
		   if (node->my_node->entry_sum == 0) // impossible???
		   {
		   temp_node.next_offset = node->next_offset;
		   write_size+=sizeof(NodeOffset);
		   }
		 */
		//		unsigned char* old_addr[100]; //test ------------------------

		EntryAddr old_ea;
		uint64_t key;
		KVP kvp;
		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;
		volatile int* seg_depth_p;
		int seg_depth;
		int start_slot,end_slot;
		int slot_index;
		int list_index,li;
		//		bool parital = false;

		do
		{

			buffer_write_start = htw_evict_buffer + NODE_HEADER_SIZE + (node->data_head%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE;
			batch_num = (node->data_head % WARM_NODE_ENTRY_CNT)/WARM_BATCH_ENTRY_CNT;
			// write only one or less batch

			start_slot = node->data_head % WARM_NODE_ENTRY_CNT;
			end_slot = (start_slot + WARM_BATCH_ENTRY_CNT)-(start_slot%WARM_BATCH_ENTRY_CNT);

			memset(htw_evict_buffer,0,WARM_BATCH_MAX_SIZE);
			memcpy(htw_evict_buffer,&node->dataNodeHeader,sizeof(NodeAddr)); //if batch_num == 1

			if (start_slot % WARM_BATCH_ENTRY_CNT != 0) // read modfiy write
				memcpy(htw_evict_buffer,dst_node+batch_num*WARM_BATCH_MAX_SIZE,WARM_BATCH_MAX_SIZE);

#if 0
			if (node->head - node->tail >= WARM_BATCH_ENTRY_CNT) // full
			{
				memset(htw_evict_buffer,0,WARM_BATCH_MAX_SIZE);
				*(uint64_t*)htw_evict_buffer = *(uint64_t*)(&node->dataNodeHeader); // only first batch need this
				node->remain_cnt = WARM_BATCH_ENTRY_CNT;
			}
			else // lack
			{
				end_slot = node->head % WARM_NODE_ENTRY_CNT;
				//			partial = true;
				node->remain_cnt = WARM_BATCH_ENTRY_CNT-(end_slot%WARM_NODE_ENTRY_CNT);
			}
#endif

			write_size = 0;
			//		entry_list_cnt = node->entry_list.size();
			//		for (i=0;i<entry_list_cnt;i++)
			slot_index = start_slot;
			list_index = node->list_tail;
//			for (i=start_slot;i<end_slot;i++)

//			if (start_slot % WARM_BATCH_ENTRY_CNT || end_slot %WARM_BATCH_ENTRY_CNT) // from split
//				debug_error("staend\n");

			while(slot_index<end_slot)
			{
				if (list_index >= node->list_head)
					break;
				li = list_index%WARM_NODE_ENTRY_CNT;
				ll = node->entry_list[li];
				dl = &doubleLogList[ll.log_num];
				addr = dl->dramLogAddr + (ll.offset%dl->my_size);

				//			old_addr[i] = addr; // test--------------------

				header = (EntryHeader*)addr;
				if (dl->tail_sum > ll.offset || is_valid(header) == false)
				{
					node->entry_list[li].log_num = -1; // invalid
									  //		continue;
				}
				else if (header->prev_loc == 3 && false) // cold // hot to cold
				{
					// direct to cold here

					// set old ea
					// direct to cold
					// invalidate old ea
					// unlock index

					/*
					   old_ea.loc = 1; // HOT
					   old_ea.file_num = ll.log_num;
					   old_ea.offset = ll.offset%dl->my_size;
					 */
					key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
//					bool ex = hash_index->read(key,&kvp,&kvp_p,&seg_depth,&seg_depth_p);
					direct_to_cold(key,addr + ENTRY_HEADER_SIZE + KEY_SIZE,kvp,seg_lock,node);
					old_ea.value = kvp.value;

					invalidate_entry(old_ea);
					hash_index->unlock_entry2(seg_lock,read_lock);

					hot_to_cold_cnt++;

					node->entry_list[li].log_num = -1;
					//		continue;
				}
				else
				{
					memcpy(buffer_write_start+write_size,addr,ENTRY_SIZE);
					hot_to_warm_cnt++;
					slot_index++; // fill entry
					write_size+=ENTRY_SIZE;
				}
				list_index++;
				//check
			}
			
//			memset(buffer_write_start+write_size,0,ENTRY_SIZE*(WARM_BATCH_ENTRY_CNT)-write_size);
//			if (write_size >= WARM_BATCH_MAX_SIZE || ENTRY_SIZE*(WARM_BATCH_ENTRY_CNT) < write_size)
//				debug_error("sfsnflskefnslnef\n"); ??BUGGGG!!

			if (buffer_write_start - htw_evict_buffer + write_size > WARM_BATCH_MAX_SIZE)
				debug_error("1111111111");

			//		if (write_size > 0)
			//		if (parital)
			{
				pmem_nt_write(dst_node+WARM_BATCH_MAX_SIZE*batch_num,htw_evict_buffer,WARM_BATCH_MAX_SIZE);
			}

			// invalidate from here

			_mm_sfence();

			KVP* kvp_p;
			//		size_t key;
			//	unsigned char* dst_addr = (unsigned char*)dst_node + nodeMeta->size;
			EntryAddr dst_addr,src_addr;
			dst_addr.loc = 2; // warm	
			dst_addr.file_num = nodeMeta->my_offset.pool_num;
			//		dst_addr.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE + nodeMeta->written_size;
			dst_addr.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE + (node->data_head%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE;

			src_addr.loc = 1; //hot

			//		for (i=0;i<entry_list_cnt;i++)

			//		int slot_cnt = node->tail%WARM_NODE_ENTRY_CNT;

			slot_index = node->data_head;
			int si;
			for (i=node->list_tail;i<list_index;i++)
			{
				li = i % WARM_NODE_ENTRY_CNT;
				si = slot_index % WARM_NODE_ENTRY_CNT;
				ll = node->entry_list[li];
				if (ll.log_num == -1)
					continue;
				dl = &doubleLogList[ll.log_num];
				addr = dl->dramLogAddr + (ll.offset%dl->my_size);
				header = (EntryHeader*)addr;
				key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
				//		if (dl->tail_sum > ll.offset || is_valid(header) == false)
				//			continue;

				src_addr.file_num = ll.log_num;
				src_addr.offset = ll.offset%dl->my_size; // dobuleloglist log num my size

				kvp_p = hash_index->insert(key,&seg_lock,read_lock);
				if (kvp_p->value == src_addr.value)
				{
					EntryHeader eh;
					eh.value = kvp_p->version;
					eh.prev_loc = 1;
					kvp_p->version = eh.value; // hot to warm... //set_prev_loc_warm(kvp_p->version);
					kvp_p->value = dst_addr.value;
					//	kvp_p->version = set_loc_warm(kvp_p->version);

//					EA_test(key,dst_addr);

					nodeMeta->valid[si] = true; // validate
					++nodeMeta->valid_cnt;
					_mm_sfence();
					set_invalid(header); // invalidate

				}
#if 1 // may do nothing and save space... // no we need to push slot cnt because memory is already copied
				else // inserted during hot to warm
				{
//					debug_error("htw\n");
					nodeMeta->valid[si] = false; // validate fail
				}
				slot_index++;
#endif

				_mm_sfence(); // need?
				hash_index->unlock_entry2(seg_lock,read_lock);

				dst_addr.offset+=ENTRY_SIZE;
			}
//			node->tail+=(end_slot-start_slot);
			while (slot_index % WARM_BATCH_ENTRY_CNT) // fill empty space
			{
				slot_index++;
				nodeMeta->valid[slot_index%WARM_NODE_ENTRY_CNT] = false;
			}
			node->list_tail = list_index;
			node->data_head = slot_index;
			/*
			if (node->list_tail % WARM_BATCH_ENTRY_CNT)
				debug_error("lis t tail\n");
				*/
//			if (node->data_head % WARM_BATCH_ENTRY_CNT)
//				debug_error("alingne\n");

			//		nodeMeta->written_size+=write_size;

		} while(node->list_tail < node->list_head && evict_all);

		//		node->entry_list.clear();
		//		node->entry_size_sum = 0;

		//		at_unlock2(nodeMeta->rw_lock);//--------------------------------------------- unlock here

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
			//		dl->check_turn(dl->tail_sum,ble_len);
			if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
				dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
			//			rv = 1;
		}
		return rv;
	}

	int PH_Evict_Thread::try_hard_evict(DoubleLog* dl)
	{
		//	size_t head_sum = dl->get_head_sum();
		//	size_t tail_sum = dl->get_tail_sum();

		unsigned char* addr;
		//		uint64_t header;
		EntryHeader header;
		uint64_t key;
		int rv=0;

		//check
		//	if (dl->tail_sum + HARD_EVICT_SPACE > dl->head_sum)
		//	if (dl->tail_sum + ble_len + dl->my_size > dl->head_sum + HARD_EVICT_SPACE)
		//		return rv;

		while(dl->tail_sum + dl->my_size <= dl->head_sum + HARD_EVICT_SPACE)
			//		if (dl->tail_sum + dl->my_size <= dl->head_sum + HARD_EVICT_SPACE)
		{
			//need hard evict
			addr = dl->dramLogAddr + (dl->tail_sum % dl->my_size);
			header.value = *(uint64_t*)addr;
			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
			// evict now

			if (is_valid(header))
			{

				//				SkiplistNode* prev[MAX_LEVEL+1];
				//				SkiplistNode* next[MAX_LEVEL+1];
				SkiplistNode* node;
#ifdef ADDR2
				node = skiplist->find_node(key,prev_sa_list,next_sa_list,read_lock);
#else
				node = skiplist->find_node(key,prev_sa_list,next_sa_list);
#endif
				if (try_at_lock2(node->lock) == false)
					continue;
				//				return rv;
				if ((skiplist->sa_to_node(node->next[0]))->key < key)
				{
					at_unlock2(node->lock);
					continue;
					//				return rv;
				}
				hard_htw_cnt++;
				NodeMeta* nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
				at_lock2(nodeMeta->rw_lock);
				hot_to_warm(node,false); // always partial...
				at_unlock2(nodeMeta->rw_lock);

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

				dl->tail_sum+=ENTRY_SIZE;
				if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
					dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));

				if (node->data_tail + (WARM_NODE_ENTRY_CNT-WARM_BATCH_ENTRY_CNT) <= node->data_head)
					warm_to_cold(node);

				at_unlock2(node->lock);
				rv = 1;

			}
			else
			{
				dl->tail_sum+=ENTRY_SIZE;
				if (dl->tail_sum%dl->my_size + ENTRY_SIZE > dl->my_size)
					dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
			}

			//			try_push(dl);
			//			rv = 1;
		}

		return rv;
	}

	int PH_Evict_Thread::try_soft_evict(DoubleLog* dl) // need return???
	{
		unsigned char* addr;
		//	size_t adv_offset=0;
		uint64_t key;
		EntryHeader header;
		int rv = 0;
		//	addr = dl->dramLogAddr + (dl->tail_sum%dl->my_size);

		//		SkiplistNode* prev[MAX_LEVEL+1];
		//		SkiplistNode* next[MAX_LEVEL+1];
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

		while(dl->soft_adv_offset + ENTRY_SIZE + dl->my_size <= dl->head_sum + SOFT_EVICT_SPACE)
		{
			addr = dl->dramLogAddr + ((dl->soft_adv_offset) % dl->my_size);
			header.value = *(uint64_t*)addr;
			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

			//			if (dl->soft_adv_offset == 1500000240)
			//				debug_error("here!\n");

			if (is_valid(header))// && is_checked(header) == false)
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
						continue;

					if ((skiplist->sa_to_node(node->next[0]))->key < key) // may split
					{
						at_unlock2(node->lock);
						continue;
					}

					// need to traverse to confirm the range...
					nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
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
					if (node->list_head - node->list_tail < WARM_LOG_MAX)
					{
						node->entry_list[node->list_head%WARM_NODE_ENTRY_CNT] = ll;
						//	node->entry_list.push_back(ll);
						//	node->entry_size_sum+=ENTRY_SIZE;
						node->list_head++; // lock...

						at_unlock2(node->lock);
						break;
					}

					//					set_checked((uint64_t*)addr);

					// need to try flush
					//			node->try_hot_to_warm();
					//	if (node->entry_size_sum >= SOFT_BATCH_SIZE)
//					if (node->head - node->tail >= node->remain_cnt)
					{
						NodeMeta* nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
						at_lock2(nodeMeta->rw_lock);
						hot_to_warm(node,false);
						at_unlock2(nodeMeta->rw_lock);
						if (node->data_tail + (WARM_NODE_ENTRY_CNT-WARM_BATCH_ENTRY_CNT) <= node->data_head)
							warm_to_cold(node); // in lock???
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
