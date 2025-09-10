
#include <atomic>
#include <stdlib.h> // rand
#include <stdio.h>
#include <x86intrin.h> //fence
#include <string.h> // memcpy
#include <libpmem.h>

#include "global2.h"
#include "skiplist.h"
#include "lock.h"
#include "data2.h"
#include "cceh.h"
#include "recovery.h"
#include "shared.h"

namespace PH
{
	extern thread_local PH_Thread* my_thread;

	extern CCEH* hash_index;

	//extern size_t HARD_EVICT_SPACE;
	//extern size_t SOFT_EVICT_SPACE;

	//const size_t DATA_SIZE = 100*1000*1000 * 100;//100M * 100B = 10G

	//const size_t NODE_POOL_LIST_SIZE = 1024;

	//NODE_POOL[NODE_POOL_LIST_SIZE][NODE_POOL_SIZE]
	// NODE_POOL size = NODE_POOL_LIST_SIZE * NODE_POOL_SIZE * NODE_SIZE = 1024*1024*1024*4096 4TB?

	//const size_t NODE_POOL_LIST_SIZE = 1024*1024; // 4GB?
	//const size_t NODE_POOL_SIZE = 1024; //4MB?

	const size_t NODE_POOL_LIST_SIZE = 1024*64; // 16bit
	const size_t NODE_POOL_SIZE = 1024*64;

	//size_t SKIPLIST_NODE_POOL_LIMIT = 1024 * 4*3;//DATA_SIZE/10/(NODE_SIZE*NODE_POOL_SIZE);
	//4MB * 1024 * 4 * 3 = 4GB * 12GB

	//const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?

	//const size_t WARM_BATCH_MAX_SIZE = 1024; // 1KB
	//size_t WARM_BATCH_MAX_SIZE = 1024;
	//#define WARM_BATCH_MAX_SIZE 1024
	//	size_t WARM_BATCH_ENTRY_CNT; // 8-9
	//	size_t WARM_BATCH_CNT; // 4096/1024
	//size_t WARM_BATCH_SIZE; // 120 * 8-9
	//	size_t WARM_NODE_ENTRY_CNT; // 8-9 * 4
	//	size_t WARM_GROUP_BATCH_CNT; // BATCH_CNT * MAX_GROUP

	/*
	   const size_t PMEM_UNIT = 256;
	   const size_t WARM_BATCH_CNT_IN_NODE = NODE_SIZE/PMEM_UNIT; // 4096 / 256 = 16
	   const size_t WARM_BATCH_CNT_IN_GROUP = WARM_BATCH_CNT_IN_NODE*MAX_NODE_GROUP; // 16*4 = 64
	 */

	// should be private
	Skiplist* skiplist;

	extern NodeAllocator* nodeAllocator;

	size_t getRandomLevel()
	{
		size_t level = 0;
		while(level < MAX_LEVEL)
		{
			if (rand()%2 == 0)
				break;
			++level;
		}
		return level;
	}
#if 0
	inline unsigned char* SkiplistNode::get_entry(int index)
	{
		int group_num = index/WARM_NODE_ENTRY_CNT;
		int batch_num = (index%WARM_NODE_ENTRY_CNT)/WARM_BATCH_ENTRY_CNT;
		int offset = index%WARM_BATCH_ENTRY_CNT;
		return group_node_p[group_num] + batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE + offset*ENTRY_SIZE;
	}
#endif

	void SkiplistNode::remove_key_from_list(uint64_t key)
	{
		//remove the key from key list	
		int j;
		uint64_t temp1,temp2=0;	
		int size = key_list.size();
		for (j=size-1;j>=0;j--)
		{
			if (key_list[j] == key)
			{
				key_list[j] = temp2;
				break;
			}
			temp1 = key_list[j];
			key_list[j] = temp2;
			temp2 = temp1;
		}

		if (j < 0)
			debug_error("key is not found\n");
//		key_list_size--;
		key_list.pop_back();
	}
	/*
	   void SkiplistNode::find_half_listNode() // do we have lock?
	   {
	   SkiplistNode* node = this;
	   uint64_t current_key,next_key;
	//		ListNode* listNode;
	ListNode* hl;
	ListNode* listNode;

	current_key = node->key;
	//		next_key = (skiplist->sa_to_node(node->next[0]))->key;
	next_key = skiplist->find_next_node(node)->key;
	listNode = node->my_listNode;
	hl = node->my_listNode;
	int cnt = 0;
	node->cold_block_sum = 0;
	while (next_key > listNode->key)
	{
	node->cold_block_sum+=listNode->block_cnt;
	listNode = listNode->next;
	++cnt;
	if (cnt%2 == 0)
	hl = hl->next;
	}
	half_listNode = hl;
	}
	 */
	 
	 #if 0 // no third
	void SkiplistNode::update_wc() // do we have lock?
	{
		SkiplistNode* node = this;
		uint64_t current_key,next_key;
		//		ListNode* listNode;
		ListNode* listNode;

		current_key = node->key;
		//		next_key = (skiplist->sa_to_node(node->next[0]))->key;
		next_key = skiplist->find_next_node(node)->key;
		int i;
		for (i=0;i<cold_cnt;i++)
			node->cold_nodes[i]->warm_cache = node->myAddr;
		//		listNode = node->my_listNode;
		/*
		   while (next_key > listNode->key)
		   {
		   listNode->warm_cache = node->myAddr;
		   listNode = listNode->next;
		   }
		 */
	}
#endif
	void SkiplistNode::setLevel(size_t l)
	{
		level = l;
		//	delete next;
		//	next.clear();
		//	next = new std::atomic<SkiplistNode*>[l+1];
		//	next.resize(l+1);
		if (next_size < l+1)
		{
			SkipAddr *old_next,*new_next;
			old_next = next;
			new_next = new SkipAddr[l+1];
//			next = new std::atomic<uint64_t>[l+1];
			next_size = l+1;
	
			int i;
			for (i=0;i<=l;i++)
				new_next[i].value = 0;
			next = new_next;

			delete old_next;
		}
		built = 0;
	}

	void SkiplistNode::setLevel()
	{
		setLevel(getRandomLevel());
	}

	SkiplistNode* Skiplist::allocate_node()
	{
		SkiplistNode* node;
		NodeMeta* nodeMeta;
		node = alloc_sl_node();
		nodeMeta = NULL;
//		for (i=0;i<WARM_MAX_NODE_GROUP;i++) // append full
		{
			nodeMeta = append_group(nodeMeta,WARM_LIST);
			node->data_node_addr[0] = nodeMeta->my_offset;
			nodeMeta->list_addr = nodeAddr_to_listAddr(WARM_LIST,node->myAddr);
			//			nodeMeta->list_addr = node->myAddr;
		}

// already initalized in alloc_sl_node
//		for (i=1;i<WARM_MAX_NODE_GROUP;i++)
//			node->data_node_addr[i] = emptyNodeAddr;

		node->data_node_cnt = 1;

		return node;
	}

	void const_init()
	{
		//		WARM_BATCH_CNT = 4;//NODE_SIZE/(WARM_BATCH_MAX_SIZE-NODE_HEADER_SIZE);
		//		WARM_BATCH_ENTRY_CNT = 20;//(WARM_BATCH_MAX_SIZE-NODE_HEADER_SIZE)/ENTRY_SIZE;
		//		WARM_NODE_ENTRY_CNT = WARM_BATCH_ENTRY_CNT*(WARM_BATCH_CNT);//(NODE_SIZE/(WARM_BATCH_SIZE+NODE_HEADER_SIZE)); //8-9 * 4
		//		WARM_GROUP_ENTRY_CNT = WARM_NODE_ENTRY_CNT*WARM_MAX_NODE_GROUP; // 32*4 
		//		WARM_GROUP_BATCH_CNT = WARM_BATCH_CNT * WARM_MAX_NODE_GROUP; // 4*4 = 16
		//	WARM_BATCH_SIZE = WARM_BATCH_ENTRY_CNT*ENTRY_SIZE
	}

	void Skiplist::recover_init(size_t size)
	{
		const_init();

		setLimit(size);
		//	node_pool_list = (Tree_Node**)malloc(sizeof(SkiplistNode*) * NODE_POOL_LIST_SIZE);
		node_pool_list = new SkiplistNode*[NODE_POOL_LIST_SIZE];

		//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);

		node_pool_list[0] = new SkiplistNode[NODE_POOL_SIZE];
		//	node_pool_list.push_back(new SkiplistNode[NODE_POOL_SIZE]);
		node_pool_cnt=0;
		node_pool_list_cnt = 0;
		node_free_head = NULL;

		node_alloc_lock = 0;
		node_counter = 1; // 0 should be del

		NodeMeta* nodeMeta;
		int i;
		NodeAddr nodeAddr;

		//		empty_node = allocate_node();
		empty_node = alloc_sl_node();
		empty_node->setLevel(MAX_LEVEL);
		empty_node->key = KEY_MIN;
		//		empty_node->my_listNode = list->empty_node;
		nodeAddr = {3,0};
		empty_node->data_node_addr[0] = nodeAddr;
		empty_node->data_node_cnt = 1;


		//		start_node = allocate_node();
		start_node = alloc_sl_node();
		start_node->setLevel(MAX_LEVEL);
		start_node->key = KEY_MIN;
		//		start_node->my_listNode = list->start_node;
		nodeAddr = {1,1};
		start_node->data_node_addr[0] = nodeAddr;
		start_node->data_node_cnt = 1;

		//		end_node = allocate_node();
		end_node = alloc_sl_node();
		end_node->setLevel(MAX_LEVEL);
		end_node->key = KEY_MAX;
		//		end_node->my_listNode = list->end_node;
		nodeAddr = {3,1};
		end_node->data_node_addr[0] = nodeAddr;
		empty_node->data_node_cnt = 1;

		for (i=0;i<=MAX_LEVEL;i++)
		{
			empty_node->next[i] = start_node->my_sa;//.value.load();
			start_node->next[i] = end_node->my_sa;//.value.load();
		}
		start_node->dst_cnt = start_node->level+1; // empty and end can not be freed...
		start_node->built = MAX_LEVEL;
		//	start_node->dataNodeHeader = start_node->data_node_addr[0];
		empty_node->built = MAX_LEVEL;
		//	empty_node->dataNodeHeader = empty_node->data_node_addr[0];

		empty_node->prev = NULL;
		start_node->prev = empty_node;// 
		/*
		   NodeMeta* nm_empty = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr[0]);
		   NodeMeta* nm_start = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr[0]);
		   NodeMeta* nm_end = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr[0]);
		 */
		//		nodeAllocator->linkNext(nm_empty,nm_start);
		//		nodeAllocator->linkNext(nm_start,nm_end);

		//	nodeAllocator->linkNext(empty_node->data_node_addr);
		//	nodeAllocator->linkNext(start_node->data_node_addr);

	}

	void Skiplist::init(size_t size)
	{
		const_init();

		setLimit(size);
		//	node_pool_list = (Tree_Node**)malloc(sizeof(SkiplistNode*) * NODE_POOL_LIST_SIZE);
		node_pool_list = new SkiplistNode*[NODE_POOL_LIST_SIZE];

		//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);

		node_pool_list[0] = new SkiplistNode[NODE_POOL_SIZE];
		//	node_pool_list.push_back(new SkiplistNode[NODE_POOL_SIZE]);
		node_pool_cnt=0;
		node_pool_list_cnt = 0;
		node_free_head = NULL;

		node_alloc_lock = 0;
		node_counter = 1; // 0 should be del

		NodeMeta* nodeMeta;
		int i;

		//empty - start - end

		empty_node = allocate_node();
		empty_node->setLevel(MAX_LEVEL);
		empty_node->key = KEY_MIN;
		//		empty_node->my_listNode = list->empty_node;

		start_node = allocate_node();
		start_node->setLevel(MAX_LEVEL);
		start_node->key = KEY_MIN;
		//		start_node->my_listNode = list->start_node;

		end_node = allocate_node();
		end_node->setLevel(MAX_LEVEL);
		end_node->key = KEY_MAX;
		//		end_node->my_listNode = list->end_node;

		for (i=0;i<=MAX_LEVEL;i++)
		{
			empty_node->next[i] = start_node->my_sa;//.value.load();
			start_node->next[i] = end_node->my_sa;//.value.load();
		}

		empty_node->prev = NULL;
		start_node->prev = empty_node;
		end_node->prev = start_node;

		start_node->built = MAX_LEVEL;
		start_node->dst_cnt = start_node->level+1; // empty and end can not be freed...

		//	start_node->dataNodeHeader = start_node->data_node_addr[0];
		empty_node->built = MAX_LEVEL;
		//	empty_node->dataNodeHeader = empty_node->data_node_addr[0];

		NodeMeta* nm_empty = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr[0]);
		NodeMeta* nm_start = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr[0]);
		NodeMeta* nm_end = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr[0]);

		nodeAllocator->linkNext(nm_empty,nm_start);
		nodeAllocator->linkNext(nm_start,nm_end);

		//nodeAllocator->linkNext(empty_node->data_node_addr);
		//nodeAllocator->linkNext(start_node->data_node_addr);

	}

#if 0
	uint64_t find_warm_min(NodeAddr nodeAddr)
	{
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
		DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
		DataNode dram_dataNode = *dataNode;

		EntryHeader* header;
		int offset = 0;
		int i,j,k;
		unsigned char* addr;
		uint64_t key;
		addr = dram_dataNode.buffer;

		int ow;

		uint64_t rv = KEY_MAX;

		k = 0;

		for (i=0;i<WARM_BATCH_CNT;i++)
		{
			addr = (unsigned char*)&dram_dataNode + i*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE;
			for (j=0;j<WARM_BATCH_ENTRY_CNT;j++)
			{
				if ( true || nodeMeta->valid[k])
				{
					header = (EntryHeader*)addr;
					if (header->version > 0)
					{
						ow = 1;
						key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
						if (ow)
						{
							if (rv > key)
								rv = key;
						}
					}
				}
				k++;
				addr+=ENTRY_SIZE;
			}
		}
		return rv;
	}

	uint64_t find_skip_min(SkiplistNode* node)
	{
		uint64_t rv = KEY_MAX,v;
		int i;
		for (i=0;i<WARM_MAX_NODE_GROUP;i++)
		{
			v = find_warm_min(node->data_node_addr[i]);
			if (v < rv)
				rv = v;
		}
		return rv;
	}
#endif
	void Skiplist::clean()
	{
		//	printf("skiplist cnt %ld\n",node_pool_list_cnt);
#ifdef SKIPLIST_TRAVERSE_TEST
		uint64_t k1,k2;
		int cnt0=0;
		int node_cnt=0;
		SkiplistNode* node;
		NodeAddr dataNode_addr;
		NodeAddr next_dataNode_addr;
		node = empty_node;

		dataNode_addr = node->data_node_addr[0];
		next_dataNode_addr = nodeAllocator->nodeAddr_to_node(dataNode_addr)->next_offset;

		while(node != end_node)
		{
			node = find_next_node(node);
			cnt0++;

			node_cnt+=node->data_node_cnt;
			dataNode_addr = node->data_node_addr[0];
			if (dataNode_addr != next_dataNode_addr)
				debug_error("link brok\n");
			//---------------
			next_dataNode_addr = nodeAllocator->nodeAddr_to_node(dataNode_addr)->next_offset;

		}
		printf("warm list cnt %d\n",cnt0);
		printf("warm node cnt %d = %lfGB\n",node_cnt,(double)node_cnt*NODE_SIZE/1024/1024/1024);
#endif
		int i,j;
		for (i=0;i<=node_pool_list_cnt;i++)
		{
			//		free(node_pool_list[i]);
			//		for (j=0;j<NODE_POOL_SIZE;j++)
			delete[] node_pool_list[i];
		}
		//	free(node_pool_list);
		delete[] node_pool_list;

		//	printf("skiplist is cleaned\n");
	}

	SkiplistNode* Skiplist::alloc_sl_node()
	{
		//just use lock

		if (node_pool_cnt >= NODE_POOL_SIZE && node_pool_list_cnt >= SKIPLIST_NODE_POOL_LIMIT)
		{
			printf("nos pacese for node skiplilit\n");
			debug_error("alloc sl node space erorr\n");
			return NULL;
		}

		while(node_alloc_lock);
		at_lock2(node_alloc_lock);

		SkiplistNode* node;

		if (node_free_head) // no pmem alloc...
		{
			node = node_free_head;
			//			node_free_head = sa_to_node(node_free_head->next[0]);
			node_free_head = node->prev;
			at_unlock2(node_alloc_lock);
		}
		else
		{
			if (node_pool_cnt >= NODE_POOL_SIZE) // alloc new pool
			{

				if (node_pool_list_cnt >= SKIPLIST_NODE_POOL_LIMIT)//NODE_POOL_LIST_SIZE)
				{
					printf("no space for node1!\n");
					debug_error("no space\n");
					at_unlock2(node_alloc_lock);
					return NULL;
				}

				++node_pool_list_cnt;
				node_pool_list[node_pool_list_cnt] = new SkiplistNode[NODE_POOL_SIZE];//(SkiplistNode*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
												      //			node_pool_list.push_back(new SkiplistNode[NODE_POOL_SIZE]);
				node_pool_cnt = 0;
			}

			node = &node_pool_list[node_pool_list_cnt][node_pool_cnt]; // first alloc // need init
			node->myAddr.pool_num = node_pool_list_cnt;
			node->myAddr.node_offset = node_pool_cnt;
			//		node->next = NULL;
			//		node->next_size = 0;
			node->my_sa.pool_num = node_pool_list_cnt;
			node->my_sa.offset = node_pool_cnt;

			//			node->key_list.resize(WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT);
			node->key_list.resize(WARM_KEY_LIST_DEFAULT);
			node->entry_list.resize(WARM_LOG_LIST_MAX);//NODE_SLOT_MAX);

			node_pool_cnt++;

			at_unlock2(node_alloc_lock);
		}

		//common init
		//	node->next = NULL;
		node->setLevel();
		node->dst_cnt = node->level+1;
		node->recent_entry_cnt = 0;

		node->list_head = node->list_tail = 0;
		node->list_size_sum = 0;
		//	node->data_node_addr = nodeAllocator->alloc_node();

		node->key_list.clear();


		node->empty_batch = -1;//0;

		node->data_node_cnt = 0;
		int i;
		for (i=0;i<WARM_MAX_NODE_GROUP;i++)
			node->data_node_addr[i] = emptyNodeAddr;

		_mm_sfence();
		node->ver = node_counter.fetch_add(1);
		node->my_sa.ver = node->ver;
		_mm_sfence();

//		node->key_list_lock = 0;
		node->insert_lock = 0;
//		node->evict_lock = 0;
		node->split_lock = 0;
		//		node->rw_lock = 0;

		return node;
	}

	void Skiplist::free_sl_node(SkiplistNode* node)
	{
		//		return; // do nothing
		at_lock2(node_alloc_lock);
		node->prev = node_free_head;
		node_free_head = node;
		at_unlock2(node_alloc_lock);
	}
	/*
	   inline SkiplistNode* Skiplist::sa_to_node(SkipAddr &sa)
	   {
#if 0
SkiplistNode* node = node_pool_list[sa.pool_num][sa.offset];
if (node->ver != sa.ver)
return NULL;
return node;
#endif
return node_pool_list[sa.pool_num][sa.offset];
}
	 */

SkiplistNode* Skiplist::find_next_node(SkiplistNode* node) // what if max
{
	SkiplistNode* next_node;
	int j;
	SkipAddr sa;
	uint64_t v;
	while(true)
	{
		sa.value = node->next[0].value.load();
		if (sa.ver == 0)
			continue;
		next_node = &node_pool_list[sa.pool_num][sa.offset];
		if (next_node->ver != sa.ver)
		{
			if (next_node->ver == 0)
			{
				v = sa.value;
				if (node->next[0].value.compare_exchange_strong(v,next_node->next[0].value.load()))
				{
					//						next_node->dst_cnt--; // passed cas
					int rv = next_node->dst_cnt.fetch_sub(1);
					if (rv == 1)
					{
						/* // no lazy delete
						for (j=0;j<next_node->data_node_cnt;j++)
						{
							//								if (nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j])->list_addr.value != nodeAddr_to_listAddr(WARM_LIST,next_node->myAddr).value)
							nodeAllocator->free_node(nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j]));
						}
						*/
						free_sl_node(next_node);
					}
				}
			}
//			else // may temporally possible
//				debug_error("skiplist impossible\n");
			continue;
		}
		break;
	}
	return next_node;
}



SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next) // what if max
{
	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
//	node = start_node;
	node = empty_node;
	int i,j;
	SkipAddr sa;
	uint64_t v;
	for (i=MAX_LEVEL;i>=0;i--)
	{
		while(true)
		{
			sa.value = node->next[i].value.load();
			//			next_node = sa_to_node(sa);
			next_node = &node_pool_list[sa.pool_num][sa.offset];
			if (next_node->ver != sa.ver)
			{
				if (next_node->ver == 0) // delete node need free
				{
					v = sa.value;
					if (node->next[i].value.compare_exchange_strong(v,next_node->next[i].value.load()))
					{
						//						next_node->dst_cnt--; // passed cas
						int rv = next_node->dst_cnt.fetch_sub(1);
						if (rv == 1)
						{
							/* no lazy delete
							for (j=0;j<next_node->data_node_cnt;j++)
							{
								//								if (nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j])->list_addr.value != nodeAddr_to_listAddr(WARM_LIST,next_node->myAddr).value)
								nodeAllocator->free_node(nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j]));
							}
							*/
							free_sl_node(next_node);
						}
					}
				}
				continue;
			}
			if (next_node->key <= key) // == for split // will find last node if key == key
				node = next_node;
			else
				break;
		}
		prev[i] = node->my_sa;
		next[i] = node->next[i].value.load();
	}
	return node;
}
#if 0
SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next,volatile uint8_t &read_lock) // what if max
{
	printf("ff2\n");
	return find_node(key,prev,next);
#if 0
	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
	// addr2
	KVP kvp; // is not inited
	KVP* kvp_p;
	uint8_t split_cnt;
	int ex;
	volatile uint8_t* split_cnt_p;
	SkipAddr sa,next_sa;

#ifdef NO_READ
	std::atomic<uint8_t>* seg_lock;
	kvp_p = hash_index->insert(key,&seg_lock,read_lock);
	kvp = *kvp_p;
	hash_index->unlock_entry2(seg_lock,read_lock);
#else
	ex = hash_index->read(key,&kvp,&kvp_p,split_cnt,split_cnt_p);
#endif


	if (ex && kvp.padding != INV0)
	{
		sa.value = kvp.padding;
		node = sa_to_node(sa);
		next_sa = node->next[0];
		next_node = sa_to_node(next_sa);
		if (node->ver == sa.ver && node->key <= key && next_node->ver == next_sa.ver && key < next_node->key)
		{
#ifdef STAT
			addr2_hit++;
#endif
			return node;
		}
#ifdef STAT
		else
			addr2_miss++;
#endif
	}
#ifdef STAT
	else
		addr2_no++;
#endif

	//addr2
	node = find_node(key,prev,next);

	//-------------------------------
	sa = node->my_sa;
#ifndef NO_READ
	std::atomic<uint8_t>* seg_lock;
#endif
	kvp_p = hash_index->insert(key,&seg_lock,read_lock);
	kvp_p->padding = sa.value;
	hash_index->unlock_entry2(seg_lock,read_lock);
	//-------------------------------
	return node;
#endif
}
#endif


#if 0
SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next,volatile uint8_t &read_lock, KVP &kvp) // what if max
{
#ifdef ADDR_CACHE

#else
	return find_node(key,prev,next);
#endif
	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
	SkipAddr sa,next_sa;
	if (kvp.value != 0 && kvp.padding != INV0)
	{
		sa.value = kvp.padding;
		node = sa_to_node(sa);
		next_sa = node->next[0];
		next_node = sa_to_node(next_sa);
		if (node->ver == sa.ver && node->key <= key && next_node->ver == next_sa.ver && key < next_node->key)
		{
#ifdef STAT
			addr2_hit++;
#endif
			return node;
		}
#ifdef STAT
		else
			addr2_miss++;
#endif
	}
#ifdef STAT
	else
		addr2_no++;
#endif
	//addr2
	node = find_node(key,prev,next);

	return node;
}
#endif
SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next, NodeAddr &warm_cache) // what if max // unsafe without lock
{
#ifdef WARM_CACHE

#else
	return find_node(key,prev,next);
#endif
	SkiplistNode* node;// = start_node;
	if (warm_cache != emptyNodeAddr)
	{
		node = &skiplist->node_pool_list[warm_cache.pool_num][warm_cache.node_offset];
#ifdef WARM_STAT
//		if (node->key <= key && key < next_node->key && next_node->ver == next_sa.ver) 
		if (node->ver == 0 || node->key > key || find_next_node(node)->key <= key)
		{
			my_thread->warm_miss_cnt++;
		}
		else
		{
			my_thread->warm_hit_cnt++;
			return node;
		}
#else
		return node;	
#endif
	}
#ifdef WARM_STAT
	else
		//		addr2_no++;
		my_thread->warm_no_cnt++;
#endif
	//addr2
	node = find_node(key,prev,next);

	return node;
}


// will be removed
/*
void SkiplistNode::update_cold_node(ListNode* cold_node) // will not use
{
	const uint64_t key = cold_node->key;
	int i;
	for (i=0;i<cold_cnt;i++)
	{
		if (key == cold_keys[i])
		{
			cold_nodes[i] = cold_node;
			break;
		}
	}
}

void SkiplistNode::insert_cold_node(ListNode* cold_node)
{
	const uint64_t key = cold_node->key;
	int i;

	if (cold_cnt >= cold_nodes.size()) // expand cold array
	{
		cold_nodes.push_back(NULL);
		cold_keys.push_back(0);
	}

	for (i=cold_cnt;i>0;i--)
	{
		if (key > cold_keys[i-1])
		{
			cold_keys[i] = key;
			cold_nodes[i] = cold_node;
			break;
		}
		cold_keys[i] = cold_keys[i-1];
		cold_nodes[i] = cold_nodes[i-1];
	}
	if (i == 0)
	{
		cold_keys[i] = key;
		cold_nodes[i] = cold_node;
	}

	cold_cnt++;
}
*/
//----------------------------------------------------------

void Skiplist::setLimit(size_t size)
{
	SKIPLIST_NODE_POOL_LIMIT = size / (NODE_POOL_SIZE * NODE_SIZE) +1;
}

void Skiplist::delete_node(SkiplistNode* node)//,SkipAddr** prev,SkipAddr** next)
{
	node->ver = 0;
	node->key = INV64; // what does it means??? // IT PREVENT WARM CACHE BUG AND DEADLOCK

	//delete data node now
	// lazy delete will consume capacity until all scan

	int i;
	for (i=0;i<node->data_node_cnt;i++)
		nodeAllocator->free_node(nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr[i])); // it should be clear that no one access here

#if 0
	//	size_t key = node->key;
	//	at_lock2(node->delete_lock);
	while(1)
	{
		//		node = find_node(key,prev,next);
		//		if (at_try_lock2(node->delete_lock))
		{
			if (delete_node_with_fail(node))//,prev,next))
				return;
			//			at_unlock2(node->delete_lock);
		}
		//		else
		//			printf("impossible use lock\n");

		//		if (node->key != key)
		//			return;
	}
#endif
}
#if 0
bool Skiplist::delete_node_with_fail(SkiplistNode* node)//, SkipAddr** prev_sa_list,SkipAddr** next_sa_list) // always success?? // need lock
{
	//	if (try_at_lock2(node->lock) == 0)
	//		return false;

	//	SkiplistNode* pn;
	//	SkiplistNode* nn;
	SkipAddr old_sa,new_sa;
	int i;
	uint64_t v;
	i = node->level;
	//	for (i=node->level;i>=0;i--)
	while(i>=0)
	{
		//		if (prev[i]->delete_lock) // prev first...
		//			return false;
		//		while(node->next[i]->delete_lock);
		//		if (at_try_lock2(prev[i]->lock) == 0)
		//			continue;
		//		pn = prev[i]->next[i];
		//		if (pn != node)
		//			return false;
		//		if (prev[i]->next[i].compare_exchange_strong(pn,nn) == false)
		//			return false;
		old_sa = node->next[i];
		if (old_sa.ver > 0)
		{
			new_sa = old_sa;
			new_sa.ver = 0;
			//			node->next[i].value = new_sa.value;
			v = old_sa.value;
			if (node->next[i].value.compare_exchange_strong(v,new_sa.value) == false)
				continue;

		}
		i--;
		//		node->level--;
	}
	return true;

}
#endif
bool Skiplist::insert_node_with_fail(SkiplistNode* node, SkipAddr* prev_sa_list, SkipAddr* next_sa_list)// SkiplistNode** prev,SkiplistNode** next)
{
	// level already
	int i;

	SkiplistNode *pnn;
	SkipAddr old_sa,new_sa,next_sa;
	uint64_t pnn_next;

	for (i=node->built;i<=node->level;i++)
	{
		//		if (prev[i]->delete_lock) // not gonna happend
		//			return false;

		if (prev_sa_list[i].ver == 0 || next_sa_list[i].ver == 0) // some node is deleted
			return false;

		//		pn = prev[i]->next[i];
		//		old_sa.value = prev_sa_list[i].value.load();
		pnn = sa_to_node(prev_sa_list[i]);
		pnn_next = pnn->next[i].value;
		if (pnn_next != next_sa_list[i].value) // prev_sa->next == next_sa
			return false;
		node->next[i].value = next_sa_list[i].value.load();

		new_sa.value = node->my_sa.value.load();

		if (pnn->next[i].value.compare_exchange_strong(pnn_next,new_sa.value) == false)
			return false;
		node->built++;
	}
	return true;
}

void Skiplist::insert_node(SkiplistNode* node, SkipAddr* prev,SkipAddr* next)
{
	while(1)
	{
		find_node(node->key,prev,next);
		if (insert_node_with_fail(node,prev,next))
			return;
	}
}

void Skiplist::traverse_test()
{
	//--------------traverse test

	bool sr = true;
	SkiplistNode* skiplistNode;
	SkiplistNode* ps;
	NodeMeta* nodeMeta;
#if 0
	int cnt0=0;
	{
		DataNode* dn;
		NodeAddr dna;
		dna = skiplist->empty_node->data_node_addr[0];
		dn = nodeAllocator->nodeAddr_to_node(dna);
		while (dna != end_node->data_node_addr[0])
		{
			dna = dn->next_offset;
			dn = nodeAllocator->nodeAddr_to_node(dna);
			cnt0++;
		}
	}
#endif

	uint64_t size_sum=0;
	int entry_cnt=0;
	int node_cnt=0;
	int list_cnt=0;
	int i,j,k,size;

	if (sr)
	{
		skiplistNode = empty_node;
		while(skiplistNode != end_node)
		{
			list_cnt++;
			node_cnt+=skiplistNode->data_node_cnt;
			//			if (skiplistNode->my_listNode == NULL)
			//				debug_error("no linked listNode\n");
			for (i=0;i<skiplistNode->data_node_cnt;i++)
			{
				nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(skiplistNode->data_node_addr[i]);
				for (j=0;j<WARM_BATCH_CNT;j++)
				{
					size = nodeMeta->batch_info[j].el.size();
					for (k=0;k<size-1;k++)
					{
						if (nodeMeta->batch_info[j].el[k].valid)
							entry_cnt++;
					}
					size_sum+=nodeMeta->batch_info[j].size_sum;
				}
			}


			ps = skiplistNode;
			skiplistNode = find_next_node(skiplistNode);
		}
	}
	printf("entry cnt %d size_sum %lu\n",entry_cnt,size_sum);
	printf("list cnt %d node cnt %d\n",list_cnt,node_cnt);

}

void Skiplist::recover()
{
//	printf("not now\n");
//return;
#if 0 // modify skipaddr to uint64 // not now
	//	NodeMeta* nodeMeta;
	SkiplistNode* skiplistNode;
	SkiplistNode* prev_skiplistNode;
	DataNode* dataNode;
	NodeAddr dataAddr;
	NodeAddr i_dataAddr;

	dataNode = nodeAllocator->nodeAddr_to_node(empty_node->data_node_addr[0]); // TODO empty 0 1

	start_node->data_node_addr[0] = dataNode->next_offset;

	skiplistNode = start_node;

	int i;
	EntryAddr list_addr;

	//	ListNode* listNode = list->start_node;

	SkipAddr **sa_array;
	sa_array = new SkipAddr*[MAX_LEVEL+1];
	for (i=0;i<=MAX_LEVEL;i++)
		sa_array[i] = &skiplistNode->next[i];

	//start node
	dataAddr = skiplistNode->data_node_addr[0];
	nodeAllocator->expand(dataAddr);

	list_addr = nodeAddr_to_listAddr(WARM_LIST,skiplistNode->myAddr);
	skiplistNode->key = recover_node(dataAddr,WARM_LIST,i,list_addr,skiplistNode); // don care
	skiplistNode->key = 0; // start node
			       //	skiplist_node->my_listNode = listNode; // not now
			       //	dataNode = nodeAllocator->nodeAddr_to_node(skiplistNode->data_node_addr[0]);

			       // next node
	dataNode = nodeAllocator->nodeAddr_to_node(dataNode->next_offset); // now start node
	dataAddr = dataNode->next_offset;
	dataNode = nodeAllocator->nodeAddr_to_node(dataNode->next_offset);

	int test_cnt=0;
	while(dataAddr != end_node->data_node_addr[0])
	{
		test_cnt++;

		nodeAllocator->expand(dataAddr);
		prev_skiplistNode = skiplistNode;
		skiplistNode = alloc_sl_node();
		skiplistNode->data_node_addr[0] = dataAddr;

		//		nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(dataAddr);
		list_addr = nodeAddr_to_listAddr(WARM_LIST,skiplistNode->myAddr);

		skiplistNode->key = recover_node(dataAddr,WARM_LIST,i,list_addr,skiplistNode); // don care
											       //		if (skiplistNode->key == KEY_MAX) // no entry // delete later
		if (prev_skiplistNode->key != KEY_MAX && prev_skiplistNode->key >= skiplistNode->key)
			debug_error("skip recover key error\n");

		for (i=0;i<=skiplistNode->level;i++)
		{
			*sa_array[i] = skiplistNode->my_sa;
			sa_array[i] = &skiplistNode->next[i];
		}
		skiplistNode->built = skiplistNode->level;

		skiplistNode->prev = prev_skiplistNode;
		//		i = 0;
		//		i_dataAddr = dataAddr;

		//		nodeAllocator->recover_node(i_dataAddr,WARM_LIST,i,skiplistNode);
		/*	
			while(i_dataAddr != emptyNodeAddr)
			{
			skiplistNode->data_node_addr[i++] = i_dataAddr;
			dataNode = nodeAllocator->nodeAddr_to_node(i_dataAddr);

			nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(i_dataAddr);
			nodeMeta->list_addr = skiplistNode->myAddr;

			i_dataAddr = dataNode->next_offset_in_group;
			}
		 */	

		//------------- next
		dataAddr = dataNode->next_offset;
		dataNode = nodeAllocator->nodeAddr_to_node(dataNode->next_offset);
	}

	//end node
	end_node->prev = skiplistNode;

	for (i=0;i<=MAX_LEVEL;i++)
		*sa_array[i] = end_node->my_sa;
	delete sa_array;

	//	skiplistNode = end_node;	

	// need my_node and warm cache
	//debug_error("stop here\n");


	//link warm and cold
	//remove empty warm

	ListNode* listNode;
	//	SkiplistNode* skiplistNode;

	listNode = list->start_node;
	skiplistNode = skiplist->start_node;

	SkiplistNode* next_skiplistNode;
	SkiplistNode* next_next_skiplistNode;

	NodeMeta* prev_meta;
	NodeMeta* nodeMeta;

	next_skiplistNode = find_next_node(skiplistNode);

	uint64_t next_key;
	next_key = next_skiplistNode->key;

	uint64_t list_key,list_next_key;
	list_key = listNode->key;

	while(listNode != list->end_node)
	{
		list_next_key = listNode->next->key;
		//		if (list_key <= next_key && next_key < list_next_key)
		if (next_key < list_next_key)
		{
			skiplistNode = next_skiplistNode;
			next_skiplistNode = find_next_node(skiplistNode);
			while(next_skiplistNode->key == KEY_MAX && next_skiplistNode != skiplist->end_node) // empty warm node
			{
				prev_meta = nodeAllocator->nodeAddr_to_nodeMeta(skiplistNode->data_node_addr[0]);
				next_next_skiplistNode = find_next_node(skiplistNode);
				nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(next_next_skiplistNode->data_node_addr[0]);

				nodeAllocator->linkNext(prev_meta,nodeMeta); // linked or not persist..

				next_next_skiplistNode->prev = skiplistNode;
				delete_node(next_skiplistNode);
				next_skiplistNode = next_next_skiplistNode;
			}
			next_key = next_skiplistNode->key;

			//			if (listNode == NULL)
			//				debug_error("list null\n");
			//			skiplistNode->my_listNode = listNode;
			skiplistNode->key = list_key;
			listNode->hold = 1;
		}

		{
			listNode->warm_cache = skiplistNode->myAddr; // cold_bl..
								     //			skiplistNode->cold_block_sum+=listNode->block_cnt;
			skiplistNode->insert_cold_node(listNode);
		}

		list_key = list_next_key;
		listNode = listNode->next;
	}

	//	traverse_test();

	//	debug_error("end of skip recov\n");
#endif

}
}
