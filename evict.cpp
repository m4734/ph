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
	extern NodeAllocator* nodeAllocator;
	extern CCEH* hash_index;
	extern Skiplist* skiplist;

	int find_evict_target_batch(SkiplistNode* node) // return evict target batch num // or -1 to fail // i expect lock for node
	{
		// find empty batch and min batch // dst and src

		int target_batch=-1;
		int i,j;
		int cur_batch = -1;
		int min_size=WARM_BATCH_MAX_SIZE;
		NodeMeta* nodeMeta;

		node->empty_batch = -1;
		for (i=0;i<node->data_node_cnt;i++)
		{
			nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[i]);
			for (j = 0;j < WARM_BATCH_CNT;j++)
			 {
				 cur_batch++;
				 /*
				 if (node->empty_batch == cur_batch)
					 continue;
					 */
				if (node->empty_batch < 0 && nodeMeta->batch_info[j].size_sum == 0)
				{
					node->empty_batch = cur_batch;
					continue;
				}

				if (min_size > nodeMeta->batch_info[j].size_sum) 
				{
					min_size = nodeMeta->batch_info[j].size_sum;
					target_batch = cur_batch;
				}
			 }
		}

		if (min_size > WARM_EVICT_THRESHOLD || node->empty_batch < 0)
			return -1;
		return target_batch;

	}

	void PH_Thread::hot_to_warm(SkiplistNode* node)//, bool has_key_list_lock = false) // skiplist lock from outside
	{
		int target_batch = find_evict_target_batch(node);
		hot_to_warm(node,target_batch);
	}


	//	void PH_Evict_Thread::hot_to_warm(SkiplistNode* node,bool evict_all) // skiplist lock from outside
	void PH_Thread::hot_to_warm(SkiplistNode* node, int target_batch)//, bool has_key_list_lock = false) // skiplist lock from outside
	{

		// find evict batch
		// read the batch
		// append hot to warm
		// write new batch

#ifdef TIME_STAT
		struct timespec ts1,ts2;
#endif
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
		//		EntryAddr new_ea;
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

		int value_size8;

		int entry_size;

		bool same_batch;

#ifdef HTW_KEY_CHECK
		uint64_t wk1,wk2;
		wk1 = node->key;
		wk2 = skiplist->find_next_node(node)->key;
#endif
		// just flush a batch

		tes(HTW);

//		node_num = (node->data_head%WARM_GROUP_BATCH_CNT)/WARM_BATCH_CNT; // % 16 / 4
		node_num = node->empty_batch / WARM_BATCH_CNT;
		dst_node = (unsigned char*)nodeAllocator->nodeAddr_to_node(node->data_node_addr[node_num]);
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[node_num]);
		at_lock2(nodeMeta->rw_lock);//------------------------------------------------lock here

//		batch_num = (node->data_head)%WARM_BATCH_CNT; // % 4
		batch_num = node->empty_batch%WARM_BATCH_CNT;

		// src batch to read buffer

		int src_node_num;// = (node->data_tail)%WARM_BATCH_CNT; // tail +1 ?
		src_node_num = target_batch / WARM_BATCH_CNT;
		unsigned char *src_node;
		src_node = (unsigned char*)nodeAllocator->nodeAddr_to_node(node->data_node_addr[src_node_num]);
		NodeMeta* src_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[src_node_num]);
		if (node_num == src_node_num)
			same_batch = true;
		else
			same_batch = false;
		if (same_batch == false)
			at_lock2(src_nodeMeta->rw_lock);//------------------------------------------------lock here // dead lock when the src and dst node is same!!!

		int src_start_offset,src_base_offset; // need to check valid entries form start offset
		int src_batch_num;
		int src_start_index;
//		src_batch_num = (node->data->tail)%WARM_BATCH_CNT;
		src_batch_num = target_batch%WARM_BATCH_CNT;
		src_base_offset = src_batch_num*WARM_BATCH_MAX_SIZE;
		if (src_batch_num == 0)
			src_start_offset = src_base_offset + NODE_HEADER_SIZE;
		else
			src_start_offset = src_base_offset;
		memcpy(batch_read_buffer, src_node+src_base_offset, WARM_BATCH_MAX_SIZE);

		src_start_index = src_batch_num*WARM_BATCH_ENTRY_CNT;

		//------------------------------------------

		base_offset = batch_num*WARM_BATCH_MAX_SIZE; // batch * 1024
		base_index = batch_num*WARM_BATCH_ENTRY_CNT; // batch * 20
		start_index = base_index;
		end_offset = base_offset + WARM_BATCH_MAX_SIZE;

		if (batch_num == 0) // add header size
			start_offset = base_offset + NODE_HEADER_SIZE; // < 1024
		else
			start_offset = base_offset;

		// init buffers
		memset(evict_buffer+base_offset,0,WARM_BATCH_MAX_SIZE); // 1024
#if 1 
		//				node->el_cnt[batch_num] = 2; // will not need ...
		if (batch_num == 0)
			pmem_nt_write(dst_node+start_offset,evict_buffer+start_offset,WARM_BATCH_MAX_SIZE-NODE_HEADER_SIZE); // exccpt header
		else
			pmem_nt_write(dst_node+start_offset,evict_buffer+start_offset,WARM_BATCH_MAX_SIZE);
		//batch is clean now
#endif
		written_size = 0;
		write_cnt = 0;

		// read form src buffer and write to evict buffer if the entry is valid
		EntryLoc entryLoc;
		entryLoc.valid = 0;
		nodeMeta->batch_info[batch_num].el.clear();

		for (i=0;i<src_nodeMeta->batch_info[src_batch_num].el.size()-1;i++) // last is end and max
		{
			if (src_nodeMeta->batch_info[src_batch_num].el[i].valid) // the entry is valid shoud move
			{
				entry_size = src_nodeMeta->batch_info[src_batch_num].el[i+1].offset-src_nodeMeta->batch_info[src_batch_num].el[i].offset;
				memcpy(evict_buffer+written_size,batch_read_buffer+src_nodeMeta->batch_info[src_batch_num].el[i].offset,entry_size);
	
//				nodeMeta->batch_info[batch_num][write_cnt].valid = 0; // it is just init of dst node
//				nodeMeta->batch_info[batch_num][write_cnt].offset = start_offset + written_size; // 0~NODE_SIZE
				entryLoc.offset = start_offset + written_size;
				nodeMeta->batch_info[batch_num].el.push_back(entryLoc);

				written_size+=entry_size;
				write_cnt++;
				// need path information
			}
		}

		//			target_cnt = WARM_BATCH_ENTRY_CNT - node->data_head%WARM_BATCH_ENTRY_CNT;
		int entry_from_log_start_index = write_cnt;

		//flush each entry
		// fill evict buffer form hot log
		// 
		for (i=node->list_tail;i<node->list_head;i++)
		{
			li = i % NODE_SLOT_MAX;//WARM_NODE_ENTRY_CNT; // need list max

			ll = node->entry_list[li];
			dl = &doubleLogList[ll.log_num];
			addr = dl->dramLogAddr + (ll.offset%dl->my_size);

			header = (EntryHeader*)addr;
			//				value_size8 = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			//				value_size8 = ((EntryHeader*)addr)->size;
			value_size8 = header->size;
			value_size8 = get_v8(value_size8);
			entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;

			if (dl->tail_sum > ll.offset || header->valid_bit == false) // expired
			{
				//					node->list_size_sum-=value_size8;
				node->list_size_sum-=ll.size;
				node->entry_list[li].log_num = INV_LOG; // invalid
				continue;
			}

			if (start_offset + written_size + entry_size > end_offset) // batch 1024 full
				break; 
			if (write_cnt >= WARM_BATCH_ENTRY_CNT-1-1)//20
				break;

			// move to evict batch from hot log

			{
//				nodeMeta->batch_info[batch_num][write_cnt].valid = 0;
//				nodeMeta->batch_info[batch_num][write_cnt].offset = start_offset + written_size; // 0~NODE_SIZE
				entryLoc.offset = start_offset + written_size;
				nodeMeta->batch_info[batch_num].el.push_back(entryLoc);

				//					node->list_size_sum-=value_size8;
				node->list_size_sum-=ll.size;

				memcpy(evict_buffer + start_offset + written_size,addr,entry_size);

				written_size+=entry_size;
				//					if (header->prev_loc != 0)
				//						ex_entry_cnt++;

				write_cnt++;
				hot_to_warm_cnt++;

			}
		}

/*
		nodeMeta->entryLoc[start_index+write_cnt].valid = 0;
		nodeMeta->entryLoc[start_index+write_cnt].offset = start_offset +written_size;
		nodeMeta->entryLoc[start_index+write_cnt+1].valid = 0;
		nodeMeta->entryLoc[start_index+write_cnt+1].offset = base_offset+WARM_BATCH_MAX_SIZE;
		*/
		entryLoc.offset = start_offset + written_size;
		nodeMeta->batch_info[batch_num].el.push_back(entryLoc);
		entryLoc.offset = start_offset + base_offset + WARM_BATCH_MAX_SIZE;
		nodeMeta->batch_info[batch_num].el.push_back(entryLoc);

//		nodeMeta->el_cnt[batch_num] = start_index+write_cnt+1+1-base_index;

		i_dst = i; // end index of flushed entry

		//------------------------------- evict buffer filled

		//evict current batch size (+ NODE_HEADER) ~ written size...
		// start offset always header
		// write header zero write payload write header real

		//------------------------------- pmem write
		//buffer to pmem dst


		if (false && start_offset == NODE_HEADER_SIZE) // need node head flush
		{
			memcpy(evict_buffer,&nodeMeta->next_addr,sizeof(NodeAddr)); // warm node must be fixed
			memcpy(evict_buffer+sizeof(NodeAddr),&nodeMeta->next_addr_in_group,sizeof(NodeAddr)); // warm node must be fixed
			pmem_nt_write(dst_node,evict_buffer,NODE_HEADER_SIZE+written_size+ENTRY_HEADER_SIZE);
		}
		else
		{
			// we may need memset 0 
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

		_mm_sfence();

		//-------------------- ref change hash index

		EntryAddr dst_addr,src_addr;
		int slot_index = 0;
		int node_offset = nodeMeta->my_offset.node_offset * NODE_SIZE;

		// invalidate from here

		// invalidate warm
		// the node is locked and ....
		// kv can be newly inserted during migration - have to check hash...
		{
			int src_end_offset;
			int offset;
			src_end_offset = src_base_offset + WARM_BATCH_MAX_SIZE;
//			for(i=src_start_index;i < NODE_SLOT_MAX;i++)
			for (i=0;i<src_nodeMeta->batch_info[src_batch_num].el.size()-1;i++)
			{
				if (src_nodeMeta->batch_info[src_batch_num].el[i].valid) // the entry is valid shoud move
				{
					entry_size = src_nodeMeta->batch_info[src_batch_num].el[i+1].offset-src_nodeMeta->batch_info[src_batch_num].el[i].offset;
					written_size+=entry_size;
					write_cnt++;
					// need path information

					offset = src_base_offset+src_nodeMeta->batch_info[src_batch_num].el[i].offset;
					addr = src_node+offset;
					header = (EntryHeader*)(batch_read_buffer+offset); // entry header?? // it is in node
					key = *(uint64_t*)(batch_read_buffer+offset+ENTRY_HEADER_SIZE);

					src_addr.loc = WARM_LIST;
					src_addr.large = header->large_bit;
					//				src_addr.size = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
					src_addr.size = ((EntryHeader*)addr)->size;
					src_addr.file_num = src_nodeMeta->my_offset.pool_num;
					src_addr.offset = offset;//%dl->my_size; // WE NEED %dl->my_size // do not use mod we need to check - overwrite

					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					if (kvp_p->value == src_addr.value)
					{

#ifdef HTW_KEY_CHECK
						if (key < wk1 || key >= wk2)
							debug_error("HTW_KEY_CHECK FAIL\n");
#endif
						// just change location
						dst_addr.large = src_addr.large;
						dst_addr.size = src_addr.size;
						dst_addr.offset = node_offset + nodeMeta->batch_info[batch_num].el[slot_index].offset;
						kvp_p->value = dst_addr.value;
#ifdef DST_CHECK
						EA_test(key,dst_addr);
#endif

						//					nodeMeta->valid[slot_index] = true; // validate
						nodeMeta->batch_info[batch_num].el[slot_index].valid = 1;
						//					++nodeMeta->valid_cnt;
						//					if (src_addr.large) // not understand
						//						invalidate_large_from_addr(addr);
						//						_mm_sfence();
					} // else new kv is inserted during ....
					  // nothing happoend
					slot_index++;
					_mm_sfence(); // need?
					hash_index->unlock_entry2(seg_lock,read_lock);
					//				dst_addr.offset+=ENTRY_SIZE;
				}
			}
		}

		// invalidate log

		dst_addr.loc = 2; // warm	
		dst_addr.file_num = nodeMeta->my_offset.pool_num;
		//			dst_addr.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + start_offset; //nodeMeta->my_offset.node_offset * NODE_SIZE + batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE + (node->data_head%WARM_BATCH_ENTRY_CNT)*ENTRY_SIZE;

		//			src_addr.loc = 1; //hot
		//		int slot_index = start_index;//node->data_head%WARM_NODE_ENTRY_CNT; // have to in batch
		slot_index = entry_from_log_start_index;

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
			//				src_addr.size = *(uint64_t*)(addr+ENTRY_HEADER_SIZE+KEY_SIZE);
			src_addr.size = ((EntryHeader*)addr)->size;
			src_addr.file_num = ll.log_num;
			src_addr.offset = ll.offset;//%dl->my_size; // WE NEED %dl->my_size // do not use mod we need to check - overwrite

			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			if (kvp_p->value == src_addr.value)
			{

#ifdef HTW_KEY_CHECK
				if (key < wk1 || key >= wk2)
					debug_error("HTW_KEY_CHECK FAIL\n");
#endif
				// just change location
				dst_addr.large = src_addr.large;
				dst_addr.size = src_addr.size;
				dst_addr.offset = node_offset + nodeMeta->batch_info[batch_num].el[slot_index].offset;
				kvp_p->value = dst_addr.value;
#ifdef DST_CHECK
				EA_test(key,dst_addr);
#endif

				//					nodeMeta->valid[slot_index] = true; // validate
//				nodeMeta->entryLoc[slot_index].valid = 1;
				nodeMeta->batch_info[batch_num].el[slot_index].valid = 1;
				//					++nodeMeta->valid_cnt;
				//					if (src_addr.large) // not understand
				//						invalidate_large_from_addr(addr);
				_mm_sfence();
				header->valid_bit = 0; // invalidate hot log entry
#ifdef HOT_KEY_LIST	
#if 0
				if (has_key_list_lock)
					node->remove_key_from_list(key); // here has insert lock // key lock // key list lock
				else
#endif
				{
					//						at_lock2(node->key_list_lock);
					node->remove_key_from_list(key);
					//						at_unlock2(node->key_list_lock);
				}
#endif
			}
#if 1 // may do nothing and save space... // no we need to push slot cnt because memory is already copied
			else // inserted during hot to warm
			{
				//					debug_error("htw0\n");
				if (header->valid_bit) // may possible... // index -> invalidate // just pass it // it will be invalidated by query thread
				{
					/*
					   EntryAddr temp;
					   temp.value = kvp_p->value;
					   printf("%d %d\n",temp.size,src_addr.size);
					 */
					debug_error("htw\n");
				}
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

		node->data_head++; // alwyas next batch


		at_unlock2(nodeMeta->rw_lock);//--------------------------------------------- unlock here
		if (same_batch == false)
			at_unlock2(src_nodeMeta->rw_lock);

		tee(HTW);
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
		//		if ((node->data_head-node->data_tail) >= WARM_GROUP_BATCH_CNT-1) // if no space // batch >= 4 * 4
		//			warm_to_cold(node);

	}

#if 1 // split cold 

	void PH_Thread::split_warm_node(SkiplistNode *old_skiplistNode) // MAKE MANY BUGS
	{
		//split old-skiplistNode

//		warm_split_cnt++;
		cold_split_cnt++; // do not want change the name

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
//		NodeMeta *list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
		DataNode *old_dataNode_p[MAX_NODE_GROUP];
		NodeMeta *old_nodeMeta[MAX_NODE_GROUP];

		unsigned char* addr;
		unsigned char* addr2;
		int offset;
		int group0_idx=0;
		uint64_t key;
		SecondOfPair second;
		EntryAddr ea;

		ea.loc = WARM_LIST;

		int i,k;

		KVP* kvp_p;
		std::atomic<uint8_t>* seg_lock;

		int base_offset;
		int group0_size_sum=0; // for split type .. but why

		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(old_skiplistNode->data_node_addr[0]);
		while(nodeMeta)
		{
			at_lock2(nodeMeta->rw_lock); // lock the node

			old_dataNode_p[group0_idx] = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
			split_buffer[group0_idx] = *old_dataNode_p[group0_idx]; // pmem to dram
			old_nodeMeta[group0_idx] = nodeMeta;

			addr = (unsigned char*)&split_buffer[group0_idx];

			ea.file_num = nodeMeta->my_offset.pool_num;
			base_offset = nodeMeta->my_offset.node_offset * NODE_SIZE;

			for (k=0;k<WARM_BATCH_CNT;k++)
			{
				int el_size = nodeMeta->batch_info[k].el.size(); // what the bug;
//			for (i=0;i<nodeMeta->batch_info[k].el.size()-1;i++) // find valid entries
				for (i=0;i<el_size-1;i++)
			{
				if (nodeMeta->batch_info[k].el[i].valid)
				{
					offset = nodeMeta->batch_info[k].el[i].offset;
					addr2 = addr+offset;
					key = *(uint64_t*)(addr2+ENTRY_HEADER_SIZE);

					second.addr = addr2;
					ea.large = ((EntryHeader*)addr2)->large_bit;
					ea.size = ((EntryHeader*)addr2)->size;
					ea.offset = base_offset + offset;
					second.ea = ea;
					split_key_list.push_back(std::make_pair(key,second)); // addr in temp dram
					group0_size_sum+=ea.size;
				}
			}
			}
			nodeMeta = nodeMeta->next_node_in_group;
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

		int entry_size;
//		EntryHeader end_jump;
//		end_jump.value = 0;

		int j;
		unsigned char* src_addr;
		int value_size8;
		//-------------------------------------------------------------------------------
		//		if (key_list.size() > 0)
		//			printf("test here\n");

		SkiplistNode* new_skiplistNode1;
		SkiplistNode* new_skiplistNode2;

		new_skiplistNode1 = skiplist->allocate_node();
		new_skiplistNode2 = skiplist->allocate_node();

		if (new_skiplistNode1 == NULL || new_skiplistNode2 == NULL)
		{
			printf("alloc fail\n");
		} // alloced skiplist but node? // need append!!

		at_lock2(new_skiplistNode1->split_lock);
		at_lock2(new_skiplistNode2->split_lock);
		at_lock2(new_skiplistNode1->insert_lock);
		at_lock2(new_skiplistNode2->insert_lock);
//		at_lock2(new_skiplistNode1->split_lock); // may no evict lock...
//		at_lock2(new_skiplistNode2->split_lock);

		int split_type; // only for debug
		uint64_t m_key;
		int batch_cnt;

		size_t group1_size_sum = 0;
		int group1_idx = 0;
		memset(&sorted_buffer1[0],0,NODE_SIZE);
		addr = (unsigned char*)&sorted_buffer1[0];
		offset = NODE_HEADER_SIZE;
		i = 0; // entry iterator

		int entry_cnt = split_key_list.size();

		if (group0_size_sum > NODE_SIZE) // what does it mean? // it means split by key list // if key list is too large we will split the node by key range half
		{
			split_type = 1; // based size

			while(i < entry_cnt-1 && group1_size_sum*2 < group0_size_sum) // size == 1 will go right...
			{
				src_addr = split_key_list[i].second.addr;
				value_size8 = ((EntryHeader*)src_addr)->size;
				value_size8 = get_v8(value_size8);
				entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
				sorted_entry_size.push_back(entry_size);

				if (offset+entry_size > WARM_BATCH_MAX_SIZE)// || j >= NODE_SLOT_MAX-1) // + JUMP // 1024
				{
					//					memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);
					batch_cnt++;
					if (batch_cnt >= WARM_BATCH_CNT) // next node // 4
					{
						group1_idx++;
						memset(&sorted_buffer1[group1_idx],0,NODE_SIZE);
//						addr = (unsigned char*)&sorted_buffer1[group1_idx];
						offset = NODE_HEADER_SIZE;
						batch_cnt = 0;
					}
					else
						offset = 0;
					addr = (unsigned char*)&sorted_buffer1[group1_idx]+(batch_cnt)*WARM_BATCH_MAX_SIZE;
				}

				memcpy(addr+offset,split_key_list[i].second.addr,entry_size);
				offset+=entry_size;
				i++;
				group1_size_sum+=entry_size;
			}
			m_key = split_key_list[i].first;
		}
		else // key range based split for key list overflow
		{
			split_type = 2; // based key
			uint64_t half1,half2;
			half1 = old_skiplistNode->key/2;
			half2 = skiplist->find_next_node(old_skiplistNode)->key/2; // really?
			m_key = half1 + half2;	

			while(i < entry_cnt && split_key_list[i].first < m_key)
			{
				src_addr = split_key_list[i].second.addr;
				value_size8 = ((EntryHeader*)src_addr)->size;
				value_size8 = get_v8(value_size8);
				entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
				sorted_entry_size.push_back(entry_size);

				if (offset+entry_size > WARM_BATCH_MAX_SIZE)// || j >= NODE_SLOT_MAX-1) // + JUMP
				{
					//					memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);
					batch_cnt++;
					if (batch_cnt >= WARM_BATCH_CNT) // next node
					{
						group1_idx++;
						memset(&sorted_buffer1[group1_idx],0,NODE_SIZE);
//						addr = (unsigned char*)&sorted_buffer1[group1_idx];
						offset = NODE_HEADER_SIZE;
						batch_cnt = 0;
					}
					else
						offset = 0;
					addr = (unsigned char*)&sorted_buffer1[group1_idx]+(batch_cnt)*WARM_BATCH_MAX_SIZE;
				}

				memcpy(addr+offset,split_key_list[i].second.addr,entry_size);
				offset+=entry_size;
				i++;
			}
		}
		//		memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);

		// fill remain elements into right node
		int ih = i; // i half

		j = 0;
		int group2_idx = 0;
		memset(&sorted_buffer2[0],0,NODE_SIZE);
		addr = (unsigned char*)&sorted_buffer2[0];
		offset = NODE_HEADER_SIZE;
		batch_cnt = 0;

		for (;i<entry_cnt;i++)
		{
			src_addr = split_key_list[i].second.addr;
			value_size8 = ((EntryHeader*)src_addr)->size;
			value_size8 = get_v8(value_size8);
			entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
			sorted_entry_size.push_back(entry_size);

			if (offset+entry_size > WARM_BATCH_MAX_SIZE)// NODE_SIZE || j >= NODE_SLOT_MAX-1) 
			{
				/*
				   if (offset < NODE_SIZE) // memset 0 // we don't need this
				   memcpy(addr+offset,&end_jump,ENTRY_HEADER_SIZE);
				 */
				 batch_cnt++;
				 if (batch_cnt >= WARM_BATCH_CNT)
				 {
				group2_idx++;
				j = 0;
				memset(&sorted_buffer2[group2_idx],0,NODE_SIZE);
//				addr = (unsigned char*)&sorted_buffer2[group2_idx];
				offset = NODE_HEADER_SIZE;
				batch_cnt = 0;
				 }
				 else
					 offset = 0;
				addr = (unsigned char*)&sorted_buffer2[group2_idx] + batch_cnt*WARM_BATCH_MAX_SIZE;
				 
			}
			memcpy(addr+offset,split_key_list[i].second.addr,entry_size);
			offset+=entry_size;
			j++;
		}

		// set skiplist key
		new_skiplistNode1->key = old_skiplistNode->key;
		new_skiplistNode2->key = m_key;

		//alloc dst -------------------------------------------------
		NodeAddr new_nodeAddr1[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta1[MAX_NODE_GROUP];
		NodeAddr new_nodeAddr2[MAX_NODE_GROUP];
		NodeMeta* new_nodeMeta2[MAX_NODE_GROUP];

		// do not change listNode just link new nodemeta

		// first node is already allocated

		new_nodeAddr1[0] = new_skiplistNode1->data_node_addr[0];
		new_nodeMeta1[0] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr1[0]);
		at_lock2(new_nodeMeta1[0]->rw_lock);

		for (i=1;i<=group1_idx;i++)
		{
			new_nodeMeta1[i] = append_group(new_nodeMeta1[i-1],WARM_LIST);
			new_nodeAddr1[i] = new_nodeMeta1[i]->my_offset;
			/*
			new_nodeAddr1[i] = nodeAllocator->alloc_node(WARM_LIST);
			new_nodeMeta1[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr1[i]);
			new_nodeMeta1[i]->group_cnt = i+1;
			new_nodeMeta1[i]->list_addr = nodeAddr_to_listAddr(WARM_LIST,listNode->myAddr);
			//			new_nodeMeta1[i]->list_addr = listNode->myAddr;
			*/
			at_lock2(new_nodeMeta1[i]->rw_lock); // ------------------------------- lock here!!!
		}

		new_nodeAddr2[0] = new_skiplistNode2->data_node_addr[0];
		new_nodeMeta2[0] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr2[0]);
		at_lock2(new_nodeMeta2[0]->rw_lock);

		for (i=1;i<=group2_idx;i++)
		{
			new_nodeMeta2[i] = append_group(new_nodeMeta1[i-1],WARM_LIST);
			new_nodeAddr2[i] = new_nodeMeta2[i]->my_offset;
			/*
			new_nodeAddr2[i] = nodeAllocator->alloc_node(WARM_LIST);
			new_nodeMeta2[i] = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr2[i]);
			new_nodeMeta2[i]->group_cnt = i+1;
			new_nodeMeta2[i]->list_addr = nodeAddr_to_listAddr(WARM_LIST,new_listNode->myAddr);
			//			new_nodeMeta2[i]->list_addr = new_listNode->myAddr;
			*/
			at_lock2(new_nodeMeta2[i]->rw_lock); // ------------------------------- lock here!!!
		}

		for (i=0;i<group1_idx;i++) // connect // already appended in skiplist
		{
//			new_nodeMeta1[i]->next_node_in_group = new_nodeMeta1[i+1];
//			new_nodeMeta1[i]->next_addr_in_group = new_nodeMeta1[i+1]->my_offset;
			sorted_buffer1[i].next_offset = emptyNodeAddr;
			sorted_buffer1[i].next_offset_in_group = new_nodeMeta1[i+1]->my_offset;
		}
		for (i=0;i<group2_idx;i++) // connect
		{
//			new_nodeMeta2[i]->next_node_in_group = new_nodeMeta2[i+1];
//			new_nodeMeta2[i]->next_addr_in_group = new_nodeMeta2[i+1]->my_offset;
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
#if 1
		_mm_sfence();
		for (i=0;i<group0_idx;i++) // release old -- invalidation dead lock
			at_unlock2(old_nodeMeta[i]->rw_lock);		
#endif

//----------------------------  update ref

		size_t start_offset;
		EntryAddr dst_ea;
		EntryLoc el;

		group1_idx = 0;
		offset = NODE_HEADER_SIZE;
		start_offset = new_nodeMeta1[0]->my_offset.node_offset * NODE_SIZE;
		dst_ea.file_num = new_nodeMeta1[0]->my_offset.pool_num;
		dst_ea.loc = WARM_LIST;

		batch_cnt = 0;

		for (i=0;i<ih;i++) // mvoing kvp
		{
			entry_size = sorted_entry_size[i];
//			if (offset + entry_size > NODE_SIZE || j >= NODE_SLOT_MAX-1)
			if (offset + entry_size > WARM_BATCH_MAX_SIZE)
			{
				batch_cnt++;
//				if (offset < NODE_SIZE)
				{ // always write last length
//					new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
//					new_nodeMeta1[group1_idx]->entryLoc[j].offset = offset;
					el.valid = 0;
					el.offset = offset;
//					new_nodeMeta1[group1_idx]->batch_info[batch_cnt].el[j].valid = 0;
//					new_nodeMeta1[group1_idx]->batch_info[batch_cnt].el[j].offset = offset;
					new_nodeMeta1[group1_idx]->batch_info[batch_cnt].el.push_back(el);
				}

				if (batch_cnt >= WARM_BATCH_CNT)
				{
					/*
					new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
					new_nodeMeta1[group1_idx]->entryLoc[j].offset = NODE_SIZE;
					new_nodeMeta1[group1_idx]->el_cnt[0] = j+1;
					*/ // last element.. we may not need it
					group1_idx++;
					offset = NODE_HEADER_SIZE;
					start_offset = new_nodeMeta1[group1_idx]->my_offset.node_offset * NODE_SIZE;
					dst_ea.file_num = new_nodeMeta1[group1_idx]->my_offset.pool_num;
					batch_cnt = 0;
				}
			}
			//			key = key_list_buffer[i];
			key = split_key_list[i].first;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.value = kvp_p->value;
#ifdef SPLIT_KEY_TEST
			if (key >= new_listNode->key)
				debug_error("split erorrr1\n");
#endif
			el.offset = offset;
			el.valid = 0;
//			new_nodeMeta1[group1_idx]->entryLoc[j].offset = offset;

			//			if (ea.value == old_ea_list_buffer[i].value)
			if (ea.value == split_key_list[i].second.ea.value)
			{
				el.valid = 1;
//				new_nodeMeta1[group1_idx]->entryLoc[j].valid = 1;
//				new_nodeMeta1[group1_idx]->size_sum+=entry_size;
				new_nodeMeta1[group1_idx]->batch_info[batch_cnt].size_sum+=entry_size;
				
				dst_ea.large = ea.large;
				dst_ea.size = ea.size;
				dst_ea.offset = start_offset + offset;
				kvp_p->value = dst_ea.value;
#ifdef DST_CHECK
				EA_test(key,dst_ea);
#endif
			}
			/*
			else
			{
//				new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
				el.valid = 0;
				//				new_nodeMeta1[group1_idx]->valid[j] = false;
			}
			*/

			new_nodeMeta1[group1_idx]->batch_info[batch_cnt].el.push_back(el);

			hash_index->unlock_entry2(seg_lock,read_lock);

			offset+=entry_size;
		}

//		if (offset < NODE_SIZE)
		{
//			new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
//			new_nodeMeta1[group1_idx]->entryLoc[j].offset = offset;
			el.valid = 0;
			el.offset = offset;
			new_nodeMeta1[group1_idx]->batch_info[batch_cnt].el.push_back(el);
		}

/*
		new_nodeMeta1[group1_idx]->entryLoc[j].valid = 0;
		new_nodeMeta1[group1_idx]->entryLoc[j].offset = NODE_SIZE;
		new_nodeMeta1[group1_idx]->el_cnt[0] = j+1;
		*/
//-------------------------- right part
		group2_idx = 0;
		offset = NODE_HEADER_SIZE;
		start_offset = new_nodeMeta2[0]->my_offset.node_offset * NODE_SIZE;
		dst_ea.file_num = new_nodeMeta2[0]->my_offset.pool_num;
		dst_ea.loc = WARM_LIST;

		batch_cnt = 0;

		for (;i<entry_cnt;i++)
		{
			entry_size = sorted_entry_size[i];
//			if (offset + entry_size > NODE_SIZE || j >= NODE_SLOT_MAX-1)
			if (offset + entry_size > WARM_BATCH_MAX_SIZE)
			{
				batch_cnt++;

				el.valid = 0;
				el.offset = offset;
				new_nodeMeta2[group2_idx]->batch_info[batch_cnt].el.push_back(el);

/*
				if (offset < NODE_SIZE)
				{
					new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
					new_nodeMeta2[group2_idx]->entryLoc[j].offset = offset;
					j++;
				}
				*/

				if (batch_cnt >= WARM_BATCH_CNT)
				{
					/*
				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
				new_nodeMeta2[group2_idx]->entryLoc[j].offset = NODE_SIZE;
				new_nodeMeta2[group2_idx]->el_cnt[0] = j+1;
				*/

				group2_idx++;
				offset = NODE_HEADER_SIZE;
				start_offset = new_nodeMeta2[group2_idx]->my_offset.node_offset * NODE_SIZE;
				dst_ea.file_num = new_nodeMeta2[group2_idx]->my_offset.pool_num;
				batch_cnt = 0;
				}
			}

			//			key = key_list_buffer[i];
			key = split_key_list[i].first;
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);
			ea.value = kvp_p->value;

#ifdef SPLIT_KEY_TEST
			if (key < new_listNode->key)
				debug_error("split erorrr2\n");
#endif

//			new_nodeMeta2[group2_idx]->entryLoc[j].offset = offset;
			el.offset = offset;
			el.valid = 0;

			//			if (ea.value == old_ea_list_buffer[i].value)
			if (ea.value == split_key_list[i].second.ea.value)
			{
				el.valid = 1;
//				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 1;
//				new_nodeMeta2[group2_idx]->size_sum+=entry_size;
				new_nodeMeta2[group2_idx]->batch_info[batch_cnt].size_sum+=entry_size;

				dst_ea.large = ea.large;
				dst_ea.size = ea.size;
				dst_ea.offset = start_offset + offset;
				kvp_p->value = dst_ea.value;
#ifdef DST_CHECK
				EA_test(key,dst_ea);
#endif
			}
			/*
			else
			{
				new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
				//				new_nodeMeta2[group2_idx]->valid[j] = false;
			}
			*/
			new_nodeMeta2[group2_idx]->batch_info[batch_cnt].el.push_back(el);

			hash_index->unlock_entry2(seg_lock,read_lock);

			offset+=entry_size;
		}

/*
		if (offset < NODE_SIZE)
		{
			new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
			new_nodeMeta2[group2_idx]->entryLoc[j].offset = offset;
			j++;
		}
		*/
		el.valid = 0;
		el.offset = offset;
		new_nodeMeta2[group2_idx]->batch_info[batch_cnt].el.push_back(el);

/*
		new_nodeMeta2[group2_idx]->entryLoc[j].valid = 0;
		new_nodeMeta2[group2_idx]->entryLoc[j].offset = NODE_SIZE;
		new_nodeMeta2[group2_idx]->el_cnt[0] = j+1;
		*/

		_mm_sfence();

//----------------------------------------------------------

		new_skiplistNode1->data_node_cnt = group1_idx+1;
		new_skiplistNode2->data_node_cnt = group2_idx+1; // do we use this?

#if 0

		// link the list
		// link pmem first then dram...

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
		/*
		   listNode->size_sum = 0;
		   for (i=0;i<=group1_idx;i++)
		   listNode->size_sum+=new_nodeMeta1[i]->size_sum;
		   new_listNode->size_sum = 0;
		   for (i=0;i<=group2_idx;i++)
		   new_listNode->size_sum+=new_nodeMeta2[i]->size_sum;
		 */

//warm list key list redistribution --------------------------

for (i=0;i<old_skiplistNode->key_list_size;i++)
{
			if (old_skiplistNode->key_list[i] < m_key)
				new_skiplistNode1->key_list[new_skiplistNode1->key_list_size++] = old_skiplistNode->key_list[i];
			else
				new_skiplistNode2->key_list[new_skiplistNode2->key_list_size++] = old_skiplistNode->key_list[i];

}

//skiplist link -------------------------------------------

		NodeMeta* child1_meta;
		NodeMeta* child2_meta;
		NodeMeta* next_meta;
		SkiplistNode* child1_sl_node = new_skiplistNode1;
		SkiplistNode* child2_sl_node = new_skiplistNode2;

		child1_meta = nodeAllocator->nodeAddr_to_nodeMeta(new_skiplistNode1->data_node_addr[0]);
		child2_meta = nodeAllocator->nodeAddr_to_nodeMeta(new_skiplistNode2->data_node_addr[0]);
		next_meta = nodeAllocator->nodeAddr_to_nodeMeta(skiplist->find_next_node(old_skiplistNode)->data_node_addr[0]);

		nodeAllocator->linkNext(child2_meta,next_meta);
		nodeAllocator->linkNext(child1_meta,child2_meta);

		SkiplistNode* prev_skiplistNode = old_skiplistNode->prev;
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

		skiplist->delete_node(old_skiplistNode); // delete duringn find node

//----------------- unlock and delete

		for (i=0;i<=group1_idx;i++)
			at_unlock2(new_nodeMeta1[i]->rw_lock);		
		for (i=0;i<=group2_idx;i++)
			at_unlock2(new_nodeMeta2[i]->rw_lock);		

		// free the nodes...
		/* // skiplist node will be freed by delete node ...
		for (i=0;i<group0_idx;i++)
			nodeAllocator->free_node(old_nodeMeta[i]);
			*/

		at_unlock2(new_skiplistNode1->split_lock);
		at_unlock2(new_skiplistNode2->split_lock);
		at_unlock2(new_skiplistNode1->insert_lock);
		at_unlock2(new_skiplistNode2->insert_lock);


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

#endif

	bool PH_Thread::split_warm_node_by_key_list(SkiplistNode *node)
	{
			if (try_at_lock2(node->split_lock) == false) // somone split this node
				return false;

			// need lock order insert - evict
#if 0
			if (has_lock == 2)
				at_unlock2(node->evict_lock); // htw wtc dead lock
			if (has_lock != 1)
				at_lock2(node->insert_lock);
			//			if (has_lock != 2)
			at_lock2(node->evict_lock);
//			if (has_lock != 3)
//				at_lock2(node->key_list_lock); // conflict in hot to warm

#endif
			SkiplistNode *next_node;
			while(1) // blocking next also blocks prev split // we can modify prev tooo
			{
				next_node = skiplist->find_next_node(node);
				//				at_lock2(next_node->lock);
				//				if (try_at_lock2(next_node->lock) == false)
				//					continue;
				if (try_at_lock2(next_node->split_lock) == false)
					continue;
				if (next_node == skiplist->find_next_node(node))
					break;
				//				at_unlock2(next_node->lock);
			}
			// split non empty node now // may refer cold split
			{
				//				NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
				//				at_lock2(nodeMeta->rw_lock);
				//				hot_to_warm(node,true); // flush all
//				split_empty_warm_node(node); // always success...
				split_warm_node(node);
							     //				at_unlock2(nodeMeta->rw_lock);
							     //				at_unlock2(next_node->lock);
				at_unlock2(next_node->split_lock);
			}
			return true;
	}


// need split // do not need split // node is invalid? -- have lock
	int PH_Thread::may_split_warm_node(SkiplistNode *node, const int has_lock) // had warm node lock // didn't had rw_lock // has insert lock // return target batch
	{
#if 1
		//		node->find_half_listNode();
		//		if (node->cold_block_sum > WARM_COLD_MAX_RATIO * WARM_MAX_NODE_GROUP || node->key_list_size >= WARM_KEY_LIST_MAX) //WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT) // (WARM / COLD) RATIO
//		if (node->cold_cnt > WARM_COLD_MAX_RATIO || node->key_list_size >= WARM_KEY_LIST_MAX_TEMP) //WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT) // (WARM / COLD) RATIO
		// if // valid ratio

		int target_batch;
		target_batch = find_evict_target_batch(node);

		if (target_batch >= 0)
			return target_batch; // no split

		if (node->data_node_cnt < WARM_MAX_NODE_GROUP) // append
		{
			//target batch 0
			//empty batch 1
			NodeMeta* last_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[node->data_node_cnt-1]);
			NodeMeta* append_nodeMeta = append_group(last_nodeMeta,WARM_LIST);
			node->data_node_addr[node->data_node_cnt] = append_nodeMeta->my_offset;
			// what is list addr??
//			nodeMeta->list_addr = nodeAddr_to_listAddr(WARM_LIST,node->myAddr); // from other place
			node->data_node_cnt++;

			node->empty_batch = (node->data_node_cnt-1)*WARM_BATCH_CNT+1;
			return node->empty_batch;//(node->data_node_cnt-1)*WARM_BATCH_CNT;
		}
		else // split
		{
			if (try_at_lock2(node->split_lock) == false) // somone split this node
				return -1;

			// need lock order insert - evict
#if 0
			if (has_lock == 2)
				at_unlock2(node->evict_lock); // htw wtc dead lock
			if (has_lock != 1)
				at_lock2(node->insert_lock);
			//			if (has_lock != 2)
			at_lock2(node->evict_lock);
//			if (has_lock != 3)
//				at_lock2(node->key_list_lock); // conflict in hot to warm

#endif
			SkiplistNode *next_node;
			while(1) // blocking next also blocks prev split // we can modify prev tooo
			{
				next_node = skiplist->find_next_node(node);
				//				at_lock2(next_node->lock);
				//				if (try_at_lock2(next_node->lock) == false)
				//					continue;
				if (try_at_lock2(next_node->split_lock) == false)
					continue;
				if (next_node == skiplist->find_next_node(node))
					break;
				//				at_unlock2(next_node->lock);
			}

			// split non empty node now // may refer cold split

			{
				//				NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
				//				at_lock2(nodeMeta->rw_lock);
				//				hot_to_warm(node,true); // flush all
//				split_empty_warm_node(node); // always success...
				split_warm_node(node);
							     //				at_unlock2(nodeMeta->rw_lock);
							     //				at_unlock2(next_node->lock);
				at_unlock2(next_node->split_lock);
			}
			return -1; // it is split and needs new target batch in new node
		}
#endif
	}


}
