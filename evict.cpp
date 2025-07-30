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

	int find_evict_target_batch(SkiplistNode* node) // return evict target batch num // or -1 to fail // i expect lock for node
	{
		int target_batch;
		int i,j;
		int cur_batch = -1;
		int min_size=WARM_BATCH_MAX_SIZE;
		NodeMeta* nodeMeta;

		for (i=0;i<node->data_node_cnt;i++)
		{
			nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[i]);
			for (j = 0;j < WARM_BATCH_CNT;j++)
			 {
				 cur_batch++;
				 if (node->empty_batch == cur_batch)
					 continue;

				if (min_size > nodeMeta->batch_info[j].size_sum) 
				{
					min_size = nodeMeta->batch_info[j].size_sum;
					target_batch = cur_batch;
				}
			 }
		}

		if (min_size > WARM_EVICT_THRESHOLD)
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

		for (i=0;i<src_nodeMeta->batch_info[src_batch_num].el.size()-2;i++) // last is end and max
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

		//

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
			for (i=0;i<src_nodeMeta->batch_info[src_batch_num].el.size()-2;i++)
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


}
