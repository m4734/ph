#include <stdio.h>
#include <sys/mman.h> //mmap
#include <stdlib.h> //malloc
#include <x86intrin.h> // fence
#include <libpmem.h>
#include <string.h> //memset

#include "lock.h"
#include "data2.h"
#include "global2.h"
#include "recovery.h"

namespace PH
{

	size_t NODE_SLOT_MAX;

	NodeAllocator* nodeAllocator;

	extern int num_pmem;

	void pmem_next_in_group_write(DataNode* dst_node,NodeAddr nodeAddr)
	{
#if 0
		pmem_memcpy_persist(&dst_node->next_offset_in_group,&nodeAddr,sizeof(NodeAddr));
#else
		dst_node->next_offset_in_group = nodeAddr;
		pmem_persist(dst_node,NODE_HEADER_SIZE);
#endif
		_mm_sfence();
	}

	NodeMeta* append_group(NodeMeta* list_nodeMeta)
	{
		NodeAddr new_nodeAddr = nodeAllocator->alloc_node();
		NodeMeta* new_nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(new_nodeAddr);
		DataNode* new_dataNode = nodeAllocator->nodeAddr_to_node(new_nodeAddr);
		if (list_nodeMeta)
		{
			pmem_next_in_group_write(nodeAllocator->nodeAddr_to_node(list_nodeMeta->my_offset),new_nodeAddr); // persist
			list_nodeMeta->next_node_in_group = new_nodeMeta;
			list_nodeMeta->next_addr_in_group = new_nodeMeta->my_offset;
			new_nodeMeta->group_cnt = list_nodeMeta->group_cnt+1;

			//			new_nodeMeta->list_addr.value = list_nodeMeta->list_addr.value;
		}
		else
		{
			new_nodeMeta->group_cnt = 1;
		}
		return new_nodeMeta;
	}


#if 0
	void NodeAllocator::linkNext(NodeAddr nodeAddr)
	{
		NodeMeta* nm = nodeAddr_to_nodeMeta(nodeAddr);
		Node* pmem_node = nodeAddr_to_node(nodeAddr);
		memset(pmem_node,0,NODE_SIZE);
		printf("efefe\n");
		pmem_node->next_offset = nm->next_p->my_offset;
		pmem_persist(&pmem_node->next_offset,sizeof(NodeAddr));
		_mm_sfence();
		printf("xxxxx\n");
		//		nm->size = sizeof(NodeAddr);

	}
#endif
	void NodeAllocator::linkNext(NodeMeta* nm1,NodeMeta* nm2)
	{
		//		forced_sync(nm1->my_offset);

		nm1->next_p = nm2;
		nm1->next_addr = nm2->my_offset;
		DataNode* pmem_node = nodeAddr_to_node(nm1->my_offset);
		//		memset(pmem_node,0,NODE_SIZE);
		pmem_node->next_offset = nm2->my_offset;
		pmem_persist(&pmem_node->next_offset,sizeof(NodeAddr));//NODE_HEADER_SIZE);
		_mm_sfence();
		//		forced_sync(nm1->my_offset);
		//		nm->size = sizeof(NodeAddr);
	}

	void NodeAllocator::init()
	{
		//		NODE_SLOT_MAX = NODE_BUFFER_SIZE / ENTRY_SIZE;
		NODE_SLOT_MAX = 80; // 4096 / 50

		nodeMetaPoolList = (unsigned char**)malloc(sizeof(unsigned char*)*POOL_MAX);
		nodePoolList = (unsigned char**)malloc(sizeof(unsigned char*)*POOL_MAX);

		pool_cnt = 0;
		node_cnt = (int*)malloc(sizeof(int)*POOL_MAX);

		free_head_p = NULL;
		alloc_cnt = 0;

		//		free_head = 0;
		//		free_tail = 0;

		alloc_pool();

		//		start_node = alloc_node();
		//		end_node = alloc_node();

		//		start_node->next_p = end_node;
		//		linkNext(start_node);

	}
	void NodeAllocator::clean()
	{
		size_t sum = 0;
		int i,j;
		NodeMeta* nodeMeta;
		for (i=0;i<pool_cnt;i++)
		{
			sum+=node_cnt[i];
			for (j=0;j<node_cnt[i];j++)
			{
				nodeMeta = (NodeMeta*)(nodeMetaPoolList[i]+sizeof(NodeMeta)*j);
				free(nodeMeta->entryLoc);
			}
			munmap(nodeMetaPoolList[i],sizeof(NodeMeta)*POOL_NODE_MAX);
			pmem_unmap(nodePoolList[i],POOL_SIZE);
		}
		free(nodeMetaPoolList);
		free(nodePoolList);

		printf("node cnt %ld sum %ld size %lfGB\n",alloc_cnt.load(),sum,double(sum)*NODE_SIZE/1024/1024/1024);
	}

	void NodeAllocator::expand(NodeAddr nodeAddr)
	{
#if 0
		int i;
		if (nodeAddr.pool_num > pool_cnt)
		{
			for (i=node_cnt[pool_cnt];i<POOL_NODE_MAX;i++)
			{

			}
			node_cnt[pool_cnt] = POOL_NODE_MAX;
		}
#endif
		while (nodeAddr.pool_num >= pool_cnt)
		{
			alloc_pool(true);
		}
		//		if (nodeAddr.node_offset > node_cnt[nodeAddr.pool_num])
		//			node_cnt[nodeAddr.pool_num] = nodeAddr.node_offset;
	}

	void NodeAllocator::alloc_pool(bool fill)
	{
		if (pool_cnt + num_pmem > POOL_MAX)
			printf("alloc pool max\n");
		int i,j;
		int is_pmem;
		char path[100];
		size_t req_size,my_size;
		req_size = POOL_SIZE;
		for(i=0;i<num_pmem;i++)
		{
			sprintf(path,"/mnt/pmem%d/data%d",i+1,pool_cnt+i); // 1~
			nodeMetaPoolList[pool_cnt + i] = (unsigned char*)mmap(NULL,sizeof(NodeMeta)*POOL_NODE_MAX,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);  // for free recoever...
			if (!nodeMetaPoolList[pool_cnt + i])
				printf("alloc_pool error1----------------------------------------------\n");
			nodePoolList[pool_cnt + i] = (unsigned char*)pmem_map_file(path,POOL_SIZE,PMEM_FILE_CREATE,0777,&my_size,&is_pmem);
			if (!nodePoolList[pool_cnt +i])
				printf("alloc_pool error2----------------------------------------------\n");

			if (is_pmem == 0)
				printf("is not pmem\n");
			if (my_size != req_size)
				printf("my size is not req size\n");
			for (j=0;j<POOL_NODE_MAX;j++)
				((NodeMeta*)(nodeMetaPoolList[pool_cnt+i] + sizeof(NodeMeta)*j))->entryLoc = NULL;
			if (fill)
				node_cnt[pool_cnt+i] = POOL_NODE_MAX;
			else
				node_cnt[pool_cnt+i] = 0;
		}
		pool_cnt += num_pmem;
	}

	void NodeAllocator::collect_free_node()
	{
		//		free_head_p = NULL;
		int i,j;
		NodeMeta* nodeMeta;
		for (i=0;i<pool_cnt;i++)
		{
			for (j=0;j<POOL_NODE_MAX;j++)
			{
				nodeMeta = ((NodeMeta*)(nodeMetaPoolList[i]+sizeof(NodeMeta)*j));
				if (nodeMeta->entryLoc == NULL)
				{
					nodeMeta->my_offset.pool_num = i;
					nodeMeta->my_offset.node_offset = j;
					nodeMeta->entryLoc = (EntryLoc*)malloc(sizeof(EntryLoc) * NODE_SLOT_MAX);
					free_node(nodeMeta);
				}
			}
		}
	}

	NodeAddr NodeAllocator::alloc_node()
	{
		at_lock2(lock);
		NodeMeta *nm;
		if (free_head_p)
		{
			nm = (NodeMeta*)free_head_p;
			free_head_p = free_head_p->next_p;
			at_unlock2(lock);
		}
		else
		{

			size_t pool_num = pool_cnt - num_pmem + alloc_cnt % num_pmem;
			if (node_cnt[pool_num] >= POOL_NODE_MAX)
			{
				alloc_pool();
				pool_num = pool_cnt - num_pmem + alloc_cnt % num_pmem;
			}

			nm = (NodeMeta*)(nodeMetaPoolList[pool_num]+sizeof(NodeMeta)*node_cnt[pool_num]);
			nm->my_offset.pool_num = pool_num;
			nm->my_offset.node_offset = node_cnt[pool_num];
			++node_cnt[pool_num];
			++alloc_cnt;

			nm->entryLoc = (EntryLoc*)malloc(sizeof(EntryLoc) * NODE_SLOT_MAX);

			at_unlock2(lock);
			//			nm->alloc_cnt_for_test = 0;
		}
		//		nm->pool_num = pool_cnt-PMEM_NUM + alloc_cnt%PMEM_NUM;
		//		nm->node = (Node*)nodePoolList[node_cnt[pool_num]];
		//		nm->written_size = 0;
		//		nm->slot_cnt = 0;
		//		nm->valid.resize(NODE_SLOT_MAX);
		/*
		   int i;
		   for (i=0;i<NODE_SLOT_MAX;i++) // or WARM_NODE_ENTRY_CNT
		   nm->valid[i] = false;
		 */
		nm->size_sum = 0; // init

		nm->group_cnt = 1;

		nm->next_p = NULL;
		nm->next_node_in_group = NULL;

		nm->next_addr = nm->next_addr_in_group = emptyNodeAddr;

		//pmem memset
		DataNode* dataNode = nodeAddr_to_node(nm->my_offset);
		memset(dataNode,0,NODE_SIZE);
		pmem_persist(dataNode,NODE_SIZE);

		_mm_sfence();
		nm->rw_lock = 0;
		//		forced_sync(nm->my_offset);

		//		nm->test = 0; // test

		return nm->my_offset;
	}

	extern Skiplist* skiplist; // fot test
	extern PH_List* list;

	void NodeAllocator::free_node(NodeMeta* nm)
	{
		at_lock2(lock);
		nm->next_p = free_head_p;
		free_head_p = nm;
		at_unlock2(lock);
	}

	uint64_t find_half_in_node(NodeMeta* nm,DataNode* node) // USE DRAM NODE!!!!
	{
		int i,j;
		uint64_t keys[NODE_SLOT_MAX];
		uint64_t new_key;
		int cnt = 0;
		size_t offset = NODE_HEADER_SIZE;
		unsigned char* addr = (unsigned char*)node;

		for (i=0;i<NODE_SLOT_MAX;i++) // always full?
		{
			if (nm->valid[i] == false)
				continue;
			new_key = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE);
			offset+=ENTRY_SIZE;
			for (j=cnt;j>0;j--)
			{
				if (new_key < keys[j-1])
					keys[j] = keys[j-1];
				else
					break;
			}
			keys[j] = new_key;
			cnt++;
		}
		//		if (cnt == 0)
		//			printf("can not find half\n");
		return keys[cnt/2];
	}

	void NodeMeta::init_warm_el() // only for 4KB
	{
		int i,offset;
		for (i=0;i<NODE_SLOT_MAX;i++)
			entryLoc[i].valid = entryLoc[i].offset = 0;

		for (i=0;i<WARM_BATCH_CNT;i++) // 4
		{
			offset = i * WARM_BATCH_ENTRY_CNT;

			entryLoc[offset].valid = 0;
			entryLoc[offset].offset = WARM_BATCH_MAX_SIZE*i;

			entryLoc[offset+1].valid = 1;
			entryLoc[offset+1].offset = WARM_BATCH_MAX_SIZE*(i+1);
		}
		entryLoc[0].offset = NODE_HEADER_SIZE;
	}
	void NodeMeta::init_cold_el()
	{
		int i;
		for (i=0;i<NODE_SLOT_MAX;i++)
			entryLoc[i].valid = entryLoc[i].offset = 0;

		entryLoc[0].valid = 0;
		entryLoc[0].offset = NODE_HEADER_SIZE;
		entryLoc[1].valid = 1;
		entryLoc[1].offset = NODE_SIZE;
		max_empty = NODE_SIZE - NODE_HEADER_SIZE;
		need_clean = false;
		el_cnt = 2;
	}
	int NodeMeta::invalidate(EntryAddr ea)
	{
		int offset = ea.offset%NODE_SIZE;
		int start_index,end_index;
		if (ea.loc == WARM_LIST)
		{
			start_index = (offset / WARM_BATCH_MAX_SIZE) * WARM_BATCH_ENTRY_CNT; // batch * 20
			end_index = start_index+WARM_BATCH_ENTRY_CNT;
			for (i=start_index;i<end_index;i++)
			{
				if (offset == entryLoc[i].offset)
				{
#ifdef INV_TEST
					if (entryLoc[i].valid == 0)
						debug_error("inv test fail\n");
#endif
					entryLoc[i].valid = 0;
					//			size_sum-=(entryLoc[i+1].offset-entryLoc[i].offset);
					return 0;
				}
			}

		}
		else // COLD_LIST NEED MERGE and max empty
		{
			int size;
			start_index = 0;
			end_index = NODE_MAX_SLOT;
			at_lock2(rw_lock);
			for (i=start_index;i<end_index;i++) // track first invalid
			{
				if (offset == entryLoc[i].offset)
				{
#ifdef INV_TEST
					if (entryLoc[i].valid == 0)
						debug_error("inv test fail\n");
#endif
					entryLoc[i].valid = 0;
					size=(entryLoc[i+1].offset-entryLoc[i].offset);
					size_sum-=size; // who need this?
							//			max_empty = NODE_SIZE;
					need_clean = true;
					at_unlock2(rw_lock);
					return size;
				}
			}
			at_unlock2(rw_lock);
		}
		debug_error("inv can't find\n");
		return 0;
	}

	int NodeMeta::el_clean(int entry_size,int &bfv) // return best fit
	{
		int src_idx,dst_idx;
		int bfi=-1;
		bfv=NODE_SIZE;

		src_idx = 1;
		dst_idx = 1;

		//		while(nodeMeta->entryLoc[src_idx].offset > 0)
		while(nodeMeta->entryLoc[src_idx].offset < NODE_SIZE)
		{
			if (nodeMeta->entryLoc[src_idx].valid)
			{
				//				if (src_idx != dst_idx)
				nodeMeta->entryLoc[dst_idx] = nodeMeta->entryLoc[src_idx];
				if (nodeMeta->entryLoc[dst_idx-1].valid == 0)
				{
					int es = nodeMeta->entryLoc[dst_idx].offset - nodeMeta->entryLoc[dst_idx-1].offset;
					if (es >= entry_size && bfv > es)
					{
						bfv = es;
						bfi = dst_idx-1;
					}
				}
				dst_idx++;
			}
			else if (nodeMeta->entryLoc[src_idx-1].valid)
			{
				//				if (src_idx != dst_idx)
				nodeMeta->entryLoc[dst_idx] = nodeMeta->entryLoc[src_idx];
				dst_idx++;
			}
			src_idx++;
		}
		nodeMeta->entryLoc[src_idx].offset = 0;

		nodeMeta->entryLoc[dst_idx].offset = NODE_SIZE;
		nodeMeta->entryLoc[dst_idx].valid = 0;

		if (nodeMeta->entryLoc[dst_idx-1].valid == 0)
		{
			int es = nodeMeta->entryLoc[dst_idx].offset - nodeMeta->entryLoc[dst_idx-1].offset;
			if (es >= entry_size && bfv > es)
			{
				bfv = es;
				bfi = dst_idx-1;
			}
		}

		dst_idx++;
		el_cnt = dst_idx;

		return bfi;
	}


}
