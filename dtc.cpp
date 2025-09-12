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

	extern thread_local PH_Thread* my_thread;

	extern std::atomic<uint64_t> global_seq_num[COUNTER_MAX];

	EntryAddr insert_entry_to_slot(NodeMeta* nodeMeta,unsigned char* src_addr, int value_size8,int j,int k) // need lock from outside
	{
		/*
		   bool large_value;
		   if (value_size8 == INV64)
		   {
		   value_size8 = 8;
		   large_value = true;
		   }
		   else
		   large_value = false;
		 */

		const int entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;

		// anyway we will scan the array
		// try best fit...

		// 1 scan and merge invalid entries
		// 2 insert..

		EntryAddr new_ea;
#if 1
		new_ea.loc = WARM_LIST;
		new_ea.large = ((EntryHeader*)src_addr)->large_bit;
		new_ea.size = ((EntryHeader*)src_addr)->size;
		new_ea.file_num = nodeMeta->my_offset.pool_num;
		new_ea.offset = nodeMeta->my_offset.node_offset*NODE_SIZE + nodeMeta->batch_info[j].el[k].offset; //NODE_HEADER_SIZE + ENTRY_SIZE*slot_idx;
		//-------------------------------------------- //try next fit


		int space = nodeMeta->batch_info[j].el[k+1].offset - nodeMeta->batch_info[j].el[k].offset;
		if (space == entry_size)
		{
			EntryHeader jump;
			jump.value = 0;
			jump.version = nodeMeta->batch_info[j].el[k+1].offset;
			//			old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;

			DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
			my_thread->tes(TEMP3);
			pmem_entry_write((unsigned char*)dataNode + nodeMeta->batch_info[j].el[k].offset , src_addr, entry_size,(unsigned char*)&jump);
			//				pmem_entry_write((unsigned char*)dataNode + nodeMeta->entryLoc[nfi].offset , src_addr, ENTRY_SIZE_WITHOUT_VALUE);

			my_thread->tee(TEMP3);
			nodeMeta->batch_info[j].el[k].valid = 1;
			nodeMeta->batch_info[j].size_sum+=entry_size;
		}
		else if (space > entry_size)
		{
			// not now
			new_ea.value = 0;
		}
		else // fail
		{
			new_ea.value = 0;
		}
		return new_ea;

#if 0
		else if(nodeMeta->entryLoc[nodeMeta->el_cnt[0]-2].valid == false && nodeMeta->entryLoc[nodeMeta->el_cnt[0]-1].offset-nodeMeta->entryLoc[nodeMeta->el_cnt[0]-2].offset >= entry_size) // test // only for append in empty jump will be zero
		{
			nodeMeta->entryLoc[nodeMeta->el_cnt[0]].offset = nodeMeta->entryLoc[nodeMeta->el_cnt[0]-1].offset;
			nodeMeta->entryLoc[nodeMeta->el_cnt[0]].valid = 0;
			nodeMeta->entryLoc[nodeMeta->el_cnt[0]-1].offset = nodeMeta->entryLoc[nodeMeta->el_cnt[0]-2].offset+entry_size;
			nfi = nodeMeta->el_cnt[0]-2;
			nodeMeta->el_cnt[0]++;
			/*
			   new_ea.loc = 3; // cold
			   new_ea.large = large_value;
			   new_ea.file_num = nodeMeta->my_offset.pool_num;
			 */
			//		if (slot_idx < NODE_SLOT_MAX)
			//			{

			jump.version = nodeMeta->entryLoc[nfi+1].offset;

			//			old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;
			new_ea.offset = nodeMeta->my_offset.node_offset*NODE_SIZE + nodeMeta->entryLoc[nfi].offset; //NODE_HEADER_SIZE + ENTRY_SIZE*slot_idx;

			DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
			my_thread->tes(TEMP3);
			pmem_entry_write((unsigned char*)dataNode + nodeMeta->entryLoc[nfi].offset , src_addr, entry_size,(unsigned char*)&jump);
			//				pmem_entry_write((unsigned char*)dataNode + NODE_HEADER_SIZE + nfi*128 , src_addr, 128);
			my_thread->tee(TEMP3);
			nodeMeta->entryLoc[nfi].valid = 1;

			nodeMeta->size_sum+=entry_size;

			//				ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr);
			//				listNode->size_sum+=entry_size;
			//			}

			return new_ea;
		}

		//		return emptyEntryAddr; // fixed size only
#endif
#endif

#if 0
		//-------------------------------------------------------
		//		debug_error("try fial\n");
		int bfv;	
		int bfi = nodeMeta->el_clean(entry_size,bfv);
		if (bfi < 0) // no space
			return emptyEntryAddr;

		//		debug_error("unexpected sccuess\n"); // it doesn't happen after fill
		bool fit;
		//		EntryHeader jump;

		if (bfv == entry_size) // fit
		{
			fit = true;
			jump.valid_bit = 0;
			jump.delete_bit = 0;
			jump.version = nodeMeta->entryLoc[bfi+1].offset;
		}
		else
		{
			if (nodeMeta->el_cnt[0] >= NODE_SLOT_MAX) // ... just pass
				return emptyEntryAddr;

			fit = false;
			jump.valid_bit = 0;
			jump.delete_bit = 0;
			jump.version = nodeMeta->entryLoc[bfi+1].offset;
		}

		// copy data first..

		// duplicated if use first part
		new_ea.loc = 3; // cold
		new_ea.large = ((EntryHeader*)src_addr)->large_bit;
		new_ea.size = ((EntryHeader*)src_addr)->size;
		new_ea.file_num = nodeMeta->my_offset.pool_num;
		//		if (slot_idx < NODE_SLOT_MAX)
		{
			//			old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;
			new_ea.offset = nodeMeta->my_offset.node_offset*NODE_SIZE + nodeMeta->entryLoc[bfi].offset; //NODE_HEADER_SIZE + ENTRY_SIZE*slot_idx;

			DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
			if (fit)
			{

				pmem_entry_write((unsigned char*)dataNode + nodeMeta->entryLoc[bfi].offset , src_addr, entry_size,(unsigned char*)&jump);
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
				for (i=nodeMeta->el_cnt[0];i>bfi+1;i--)
					nodeMeta->entryLoc[i] = nodeMeta->entryLoc[i-1];
				nodeMeta->el_cnt[0]++;
				nodeMeta->entryLoc[bfi].valid = 1;
				nodeMeta->entryLoc[bfi+1].valid = 0;
				nodeMeta->entryLoc[bfi+1].offset = nodeMeta->entryLoc[bfi].offset + entry_size;
			}

			//			nodeMeta->valid[slot_idx] = true; // validate
			//			++nodeMeta->valid_cnt;
			nodeMeta->size_sum+=entry_size;

			//			ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr.value);
			//			ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr);
			//			listNode->valid_cnt++;
			//			listNode->size_sum+=entry_size;

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
#endif
	}



	bool PH_Thread::direct_to_cold(uint64_t key,int value_size,unsigned char* value, SkiplistNode* skiplist_node, bool large) // may lock from outside // have to be exist // node lock
	{ // ALWAYS NEW
	  //NO UNLOCK IN HERE!

	  // 1 find skiplist node
	  // 2 find listnode
	  // 3 insert kv
	  // 4 return addr

#ifdef TIME_STAT
		timespec ts1,ts2;
		clock_gettime(CLOCK_MONOTONIC,&ts1);
#endif
		tes(DIRECT_TO_COLD);

		bool rv;

		//		uint64_t value_size8 = get_v8(value_size);

		const uint64_t z = 0;
		memcpy(entry_buffer,&z,ENTRY_HEADER_SIZE); // need to be zero for persist
		memcpy(entry_buffer+ENTRY_HEADER_SIZE,&key,KEY_SIZE);
		//		memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE,&value_size,SIZE_SIZE);
		//		memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE/*+SIZE_SIZE*/,value,value_size8);//v8?
		memcpy(entry_buffer+ENTRY_HEADER_SIZE+KEY_SIZE/*+SIZE_SIZE*/,value,value_size);//v8?

		//		EntryAddr src_ea;
		std::atomic<uint8_t>* seg_lock; // need for invalidation

		//		tee(INSERT_TO_COLD_OF_DTC);

		//hash_index->unlock_entry2(seg_lock,read_lock);

		//			at_unlock2(skiplist_node->lock);
		// thre waws cod that find skplist node and lock and unlock in this funcion
		//		tes(INSERT_TO_COLD);

		EntryAddr new_ea,old_ea;
		KVP* kvp_p;

		int value_size8 = get_v8(value_size);

		//		while(true) // always success unless re updated // split retry loop
		{
#if 0
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

			// What happen if we failed once and retry here...

			//------------------------------ entry locked!!
#endif

			//			NodeMeta* list_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(listNode->data_node_addr);
			//			new_ea.value = 0;
			//			while (true)//list_nodeMeta) // try block group // group loop

			EntryHeader* header = (EntryHeader*)entry_buffer;
			header->valid_bit = 1;
			header->delete_bit = 0;
			header->large_bit = large;
			header->size = value_size;

			NodeMeta *nodeMeta;
			int i,j,k;
			int el_size;
			const int entry_size = ENTRY_SIZE_WITHOUT_VALUE + value_size8;
			for (i=0;i<skiplist_node->data_node_cnt;i++) // try insert among data nodes
			{
				nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(skiplist_node->data_node_addr[i]);

				for (j=0;j<WARM_BATCH_CNT;j++)
				{
					if (nodeMeta->batch_info[j].size_sum + entry_size > NODE_SIZE-NODE_HEADER_SIZE)
						continue;
					el_size = nodeMeta->batch_info[j].el.size()-1;
					for (k=0;k<el_size;k++)
					{
						if (nodeMeta->batch_info[j].el[k].valid)
							continue;
						if (nodeMeta->batch_info[j].el[k+1].offset - nodeMeta->batch_info[j].el[k].offset < entry_size)
							continue;
						// found pos // already has skiplist node lock
						// do we need rw lock? // src dst rw lock will safe
						// all write needs skiplist node lock
						// we dont need src rw lock
						// entryLoc may change .. need dst lock...

						//skiplist lock -> key lock


						kvp_p = hash_index->insert(key,&seg_lock,read_lock);
						old_ea.value = kvp_p->value;
						if (old_ea.loc != WARM_LIST) // it is inserted
						{
							hash_index->unlock_entry2(seg_lock,read_lock);
							return false;
						}

						//alwyas new update //get new version
						header->version = global_seq_num[key%COUNTER_MAX].fetch_add(1);

						at_lock2(nodeMeta->rw_lock);

						tes(INSERT_ENTRY_TO_SLOT);
						new_ea = insert_entry_to_slot(nodeMeta,entry_buffer,value_size8,j,k);
						tee(INSERT_ENTRY_TO_SLOT);

						at_unlock2(nodeMeta->rw_lock);


						if (new_ea.value != 0)
						{

							//success from here

							// What happen if we failed once and retry here...

							//			if (kvp_p->key == key)
							if (kvp_p->value != INV0)
								old_ea.value = kvp_p->value;
							else // first update
							{
								old_ea = emptyEntryAddr;
								kvp_p->key = key;
							}
							//------------------------------ entry locked!!
							/*
							   if (kvp_p->value == INV0) // just statistics
							   {
							//						if (value_size8 == LARGE_SIZE)
							if (new_ea.large)
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
							//						else
							//							debug_error("???\n");
							}
							 */

							//					new_ea.large = old_ea.large;//... did in insert entyr to slot
							kvp_p->value = new_ea.value;
#ifdef DST_CHECK
							EA_test(key,new_ea);
#endif
							_mm_sfence();	

							//need invalidation before unlock...
							if (old_ea != emptyEntryAddr)
								invalidate_entry(old_ea,old_ea.large);
							hash_index->unlock_entry2(seg_lock,read_lock);

							//check
							//					warm_to_cold_cnt++; // to cold
							tee(DIRECT_TO_COLD);
							return true;
						}
						else
						{
							//failed
							//				debug_error("fail here\n");
							hash_index->unlock_entry2(seg_lock,read_lock);

						}


					}
				}


			}

		}

#ifdef TIME_STAT
		clock_gettime(CLOCK_MONOTONIC,&ts2);
		dtc_time+=(ts2.tv_sec-ts1.tv_sec)*1000000000+(ts2.tv_nsec-ts1.tv_nsec);
#endif
		tee(DIRECT_TO_COLD);
		return false;
	}



}
