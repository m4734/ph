#include <stdio.h>
#include <sys/mman.h> //mmap
#include <stdlib.h> //malloc
#include <x86intrin.h> // fence
#include <libpmem.h>
#include <string.h> //memset

#include "lock.h"
#include "data2.h"

namespace PH
{

	NodeAllocator* nodeAllocator;

	extern int num_pmem;
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
		nm1->next_p = nm2;
		DataNode* pmem_node = nodeAddr_to_node(nm1->my_offset);
		memset(pmem_node,0,NODE_SIZE);
		pmem_node->next_offset = nm2->my_offset;
		pmem_persist(&pmem_node->next_offset,sizeof(NodeAddr));
		_mm_sfence();
//		nm->size = sizeof(NodeAddr);
	}

	void NodeAllocator::init()
	{
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
		int i;
		for (i=0;i<pool_cnt;i++)
		{
			munmap(nodeMetaPoolList[i],sizeof(NodeMeta)*POOL_NODE_MAX);
			pmem_unmap(nodePoolList[i],POOL_SIZE);
		}
		free(nodeMetaPoolList);
		free(nodePoolList);
	}

	void NodeAllocator::alloc_pool()
	{
		if (pool_cnt + num_pmem > POOL_MAX)
			printf("alloc pool max\n");
		int i;
		int is_pmem;
		char path[100];
		size_t req_size,my_size;
		req_size = POOL_SIZE;
		for(i=0;i<num_pmem;i++)
		{
			sprintf(path,"/mnt/pmem%d/data%d",i,pool_cnt+i);
			nodeMetaPoolList[pool_cnt + i] = (unsigned char*)mmap(NULL,sizeof(NodeMeta)*POOL_NODE_MAX,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0); 
			if (!nodeMetaPoolList[pool_cnt + i])
				printf("alloc_pool error1----------------------------------------------\n");
			nodePoolList[pool_cnt + i] = (unsigned char*)pmem_map_file(path,POOL_SIZE,PMEM_FILE_CREATE,0777,&my_size,&is_pmem);
			if (!nodePoolList[pool_cnt +i])
				printf("alloc_pool error2----------------------------------------------\n");

			if (is_pmem == 0)
				printf("is not pmem\n");
			if (my_size != req_size)
				printf("my size is not req size\n");
		}
		pool_cnt += num_pmem;
	}

//	NodeMeta* NodeAllocator::alloc_node()
	NodeAddr NodeAllocator::alloc_node()
	{
		at_lock2(lock);
		NodeMeta *nm;
		if (free_head_p)
		{
			nm = (NodeMeta*)free_head_p;
			free_head_p = free_head_p->next_p;
			at_unlock2(lock);
//			return nm;
			return nm->my_offset;
		}
		
		if (node_cnt[pool_cnt - num_pmem + alloc_cnt % num_pmem] >= POOL_NODE_MAX)
			alloc_pool();

		size_t pool_num = pool_cnt - num_pmem + alloc_cnt % num_pmem;

		nm = (NodeMeta*)(nodeMetaPoolList[pool_num]+sizeof(NodeMeta)*node_cnt[pool_num]);
//		nm->pool_num = pool_cnt-PMEM_NUM + alloc_cnt%PMEM_NUM;
//		nm->node = (Node*)nodePoolList[node_cnt[pool_num]];
		nm->my_offset.pool_num = pool_num;
		nm->my_offset.node_offset = node_cnt[pool_num];
		nm->written_size = 0;
		nm->slot_cnt = 0;
		int i;
		for (i=0;i<NODE_SLOT_MAX;i++)
			nm->valid[i] = false;
		nm->valid_cnt = 0;

		++node_cnt[pool_num];
		++alloc_cnt;

		at_unlock2(lock);
//		return nm;
		return nm->my_offset;
	}

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
		size_t offset = sizeof(NodeAddr);
		unsigned char* addr = (unsigned char*)node;

		for (i=0;i<NODE_SLOT_MAX;i++) // always full?
		{
			if (nm->valid[i] == false)
				continue;
			new_key = *(uint64_t*)(addr+offset+HEADER_SIZE);
			offset+=ble_len;
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


}
