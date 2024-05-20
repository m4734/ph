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

	void linkNext(NodeMeta* nm)
	{
		memset(nm->node,0,NODE_SIZE);
		nm->node->next_offset = nm->my_offset;
		pmem_persist(&nm->node->next_offset,sizeof(NodeOffset));
		_mm_sfence();
		nm->size = sizeof(NodeOffset);
	}

	void NodeAllocator::init()
	{
		nodeMetaPoolList = (unsigned char**)malloc(sizeof(unsigned char*)*POOL_MAX);
		nodePoolList = (unsigned char**)malloc(sizeof(unsigned char*)*POOL_MAX);

		pool_cnt = 0;
		node_cnt = (int*)malloc(sizeof(int)*POOL_MAX);

		free_head_p = NULL;
		alloc_cnt = 0;

		alloc_pool();

		start_node = alloc_node();
		end_node = alloc_node();

		start_node->next_p = end_node;
		linkNext(start_node);

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
			nodeMetaPoolList[i] = (unsigned char*)mmap(NULL,sizeof(NodeMeta)*POOL_NODE_MAX,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0); 
			if (!nodeMetaPoolList[i])
				printf("alloc_pool error1\n");
			nodePoolList[i] = (unsigned char*)pmem_map_file(path,POOL_SIZE,PMEM_FILE_CREATE,0777,&my_size,&is_pmem);
			if (is_pmem == 0)
				printf("is not pmem\n");
			if (my_size != req_size)
				printf("my size is not req size\n");


		}
		pool_cnt += num_pmem;
	}

	NodeMeta* NodeAllocator::alloc_node()
	{
		at_lock2(lock);
		NodeMeta *nm;
		if (free_head_p)
		{
			nm = (NodeMeta*)free_head_p;
			free_head_p = free_head_p->next_p;
			at_unlock2(lock);
			return nm;
		}
		
		if (node_cnt[pool_cnt - num_pmem + alloc_cnt % num_pmem] >= POOL_NODE_MAX)
			alloc_pool();

		size_t pool_num = pool_cnt - num_pmem + alloc_cnt % num_pmem;

		nm = (NodeMeta*)nodeMetaPoolList[node_cnt[pool_num]];
//		nm->pool_num = pool_cnt-PMEM_NUM + alloc_cnt%PMEM_NUM;
		nm->node = (Node*)nodePoolList[node_cnt[pool_num]];
		nm->my_offset.file_num = pool_num;
		nm->my_offset.offset = node_cnt[pool_num];
		nm->size = 0;
		nm->slot_cnt = 0;
		int i;
		for (i=0;i<NODE_SLOT_MAX;i++)
			nm->valid[i] = false;

		++node_cnt[pool_num];
		++alloc_cnt;

		at_unlock2(lock);
		return nm;
	}

	void NodeAllocator::free_node(NodeMeta* nm)
	{
		at_lock2(lock);
		nm->next_p = free_head_p;
		free_head_p = nm;
		at_unlock2(lock);
	}
}
