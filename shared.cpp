#include <libpmem.h> 
#include <x86intrin.h> // fence
#include <string.h> // memcpy
#include <stdio.h>

#include "shared.h"
#include "log.h"
#include "data2.h"
#include "skiplist.h"

namespace PH
{
	extern DoubleLog* doubleLogList;
	extern NodeAllocator* nodeAllocator;
	extern size_t WARM_BATCH_ENTRY_CNT;
	extern size_t ENTRY_SIZE;
	extern PH_List* list;
	extern Skiplist* skiplist;

void debug_error(const char* msg)
{
	printf("error----------------------------------------\n");
	printf("%s\n",msg);
}

	unsigned char* get_entry(EntryAddr &ea)
	{
		if (ea.loc == HOT_LOG)
			return  doubleLogList[ea.file_num].dramLogAddr + ea.offset;
		else
			return (unsigned char*)nodeAllocator->nodePoolList[ea.file_num]+ea.offset;
	}

	void EA_test(uint64_t key, EntryAddr ea)
	{
//		printf("not now\n");
		if (ea.loc == HOT_LOG)
		{
			if (key != *(uint64_t*)(doubleLogList[ea.file_num].dramLogAddr+ea.offset+ENTRY_HEADER_SIZE))
				debug_error("ea error!\n");

		}
		else
		{
			if (key != *(uint64_t*)(nodeAllocator->nodePoolList[ea.file_num]+ea.offset+ENTRY_HEADER_SIZE))
				debug_error("ea error!\n");
		}
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
	void pmem_reverse_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len) //need len align
	{
		int offset;
		offset = len-256;
		while(offset >= 0)
		{
			pmem_memcpy(dst_addr+offset,src_addr+offset,256,PMEM_F_MEM_NONTEMPORAL);
			offset-=256;
		}
		_mm_sfence();
	}
	void reverse_memcpy(unsigned char* dst_addr,unsigned char* src_addr, size_t len) //need len align
	{
		int offset;
		offset = len-256;
		while(offset >= 0)
		{
			memcpy(dst_addr+offset,src_addr+offset,256);
			offset-=256;
		}
		_mm_sfence();
	}
	void pmem_entry_write(unsigned char* dst, unsigned char* src, size_t len)
	{
		// need version clean - kv write - version write ...
		memcpy(dst+ENTRY_HEADER_SIZE,src+ENTRY_HEADER_SIZE,len-ENTRY_HEADER_SIZE);
		pmem_persist(dst+ENTRY_HEADER_SIZE,len-ENTRY_HEADER_SIZE);
		_mm_sfence();
		memcpy(dst,src,ENTRY_HEADER_SIZE); // write version
		pmem_persist(dst,ENTRY_HEADER_SIZE);
		_mm_sfence();

	}
	void pmem_next_write(DataNode* dst_node,NodeAddr nodeAddr)
	{
		dst_node->next_offset = nodeAddr;
		pmem_persist(dst_node,NODE_HEADER_SIZE);
		_mm_sfence();
	}

#if 1
// dtc kv skip
// hth kv
// htw kv skip

	void invalidate_entry(EntryAddr &ea,bool try_merge) // need kv lock
	{
		unsigned char* addr;
		if (ea.loc == HOT_LOG)// || ea.loc == WARM_LOG) // hot log
		{
			addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
			set_invalid((EntryHeader*)addr); // invalidate
							 //			hot_to_hot_cnt++; // log to hot
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

			nm->invalidate(ea);

#if 1 // we don't need batch... // no i need the batch
			{
#if 0 // reducd not now
				ListNode* listNode = list->addr_to_listNode(nm->list_addr);
//				ListNode* listNode = list->addr_to_listNode(nm->list_addr);
				listNode->valid_cnt--;

				if (try_merge)
				{
					bool rv = false;
					if (listNode->valid_cnt * 2 <= NODE_SLOT_MAX) // try destory list node // must not be head
					{
						ListNode* prev_listNode = listNode->prev;
						if (listNode->hold == 0 && prev_listNode->valid_cnt + listNode->valid_cnt <= NODE_SLOT_MAX)
						{
							rv = try_merge_listNode(prev_listNode,listNode);
						}
						else // else??
						{
							ListNode* next_listNode = listNode->next;
							if (next_listNode->hold == 0 && next_listNode->valid_cnt + listNode->valid_cnt <= NODE_SLOT_MAX)
							{
								rv = try_merge_listNode(listNode,next_listNode); // need more test
							}
						}
					}
//					else if (listNode->valid_cnt + NODE_SLOT_MAX*2 < listNode->block_cnt * NODE_SLOT_MAX) // try shorten group
					if (need_reduce(listNode))
					{
						rv = try_reduce_group(listNode);// need more test
					}
					if (rv)
					{
						SkiplistNode* skiplistNode;
						NodeAddr warm_cache = listNode->warm_cache;
						skiplistNode = &skiplist->node_pool_list[warm_cache.pool_num][warm_cache.node_offset];	
						skiplistNode->cold_block_sum--;
					}
				}
#endif
			}
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
			//			nm->valid[cnt] = false; // invalidate
			//			--nm->valid_cnt;
#endif
			//			at_unlock2(nm->rw_lock);
		}

		// no nm rw lock but need hash entry lock...
	}
#endif

}
