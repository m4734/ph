
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
	PH_List* list;

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
		for (j=key_list_size-1;j>=0;j--)
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

		key_list_size--;

	}

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

	void SkiplistNode::update_wc() // do we have lock?
	{
		SkiplistNode* node = this;
		uint64_t current_key,next_key;
		//		ListNode* listNode;
		ListNode* listNode;

		current_key = node->key;
//		next_key = (skiplist->sa_to_node(node->next[0]))->key;
		next_key = skiplist->find_next_node(node)->key;
		listNode = node->my_listNode;
		while (next_key > listNode->key)
		{
			listNode->warm_cache = node->myAddr;
			listNode = listNode->next;
		}
	}

	void SkiplistNode::setLevel(size_t l)
	{
		level = l;
		//	delete next;
		//	next.clear();
		//	next = new std::atomic<SkiplistNode*>[l+1];
		//	next.resize(l+1);
		if (next_size < l+1)
		{
			delete next;
			next = new SkipAddr[l+1];
			next_size = l+1;
		}
		built = 0;
	}

	void SkiplistNode::setLevel()
	{
		level = getRandomLevel();
		//	delete next;
		//	next = new std::atomic<SkiplistNode*>[level+1];
		//	next.clear();
		//	next.resize(level+1);
		if (next_size < level+1)
		{
			delete next;
			next = new SkipAddr[level+1];
			next_size = level+1;
		}

		built = 0;
	}
	/*
	   void SkiplistNode::free()
	   {
	   delete next;
	   }
	 */
	extern size_t ENTRY_SIZE;

	SkiplistNode* Skiplist::allocate_node()
	{
		SkiplistNode* node;
		NodeMeta* nodeMeta;
		node = alloc_sl_node();
		nodeMeta = NULL;
		int i;
		for (i=0;i<WARM_MAX_NODE_GROUP;i++)
		{
			nodeMeta = append_group(nodeMeta,WARM_LIST);
			node->data_node_addr[i] = nodeMeta->my_offset;
			nodeMeta->list_addr = nodeAddr_to_listAddr(WARM_LIST,node->myAddr);
//			nodeMeta->list_addr = node->myAddr;
		}

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
		empty_node->my_listNode = list->empty_node;
		nodeAddr = {3,0};
		empty_node->data_node_addr[0] = nodeAddr;


//		start_node = allocate_node();
		start_node = alloc_sl_node();
		start_node->setLevel(MAX_LEVEL);
		start_node->key = KEY_MIN;
		start_node->my_listNode = list->start_node;
		nodeAddr = {1,1};
		start_node->data_node_addr[0] = nodeAddr;

//		end_node = allocate_node();
		end_node = alloc_sl_node();
		end_node->setLevel(MAX_LEVEL);
		end_node->key = KEY_MAX;
		end_node->my_listNode = list->end_node;
		nodeAddr = {3,1};
		end_node->data_node_addr[0] = nodeAddr;


		for (i=0;i<=MAX_LEVEL;i++)
		{
			empty_node->next[i] = start_node->my_sa;
			start_node->next[i] = end_node->my_sa;
		}
		start_node->built = MAX_LEVEL;
		//	start_node->dataNodeHeader = start_node->data_node_addr[0];
		empty_node->built = MAX_LEVEL;
		//	empty_node->dataNodeHeader = empty_node->data_node_addr[0];

		empty_node->prev = NULL;
//		start_node->prev = empty_node;// 
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
		const_init;

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

		empty_node = allocate_node();
		empty_node->setLevel(MAX_LEVEL);
		empty_node->key = KEY_MIN;
		empty_node->my_listNode = list->empty_node;

		start_node = allocate_node();
		start_node->setLevel(MAX_LEVEL);
		start_node->key = KEY_MIN;
		start_node->my_listNode = list->start_node;

		end_node = allocate_node();
		end_node->setLevel(MAX_LEVEL);
		end_node->key = KEY_MAX;
		end_node->my_listNode = list->end_node;


		for (i=0;i<=MAX_LEVEL;i++)
		{
			empty_node->next[i] = start_node->my_sa;
			start_node->next[i] = end_node->my_sa;
		}

		empty_node->prev = NULL;
		start_node->prev = empty_node;
		end_node->prev = start_node;

		start_node->built = MAX_LEVEL;
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
		   SkiplistNode* node;
		   NodeAddr dataNode_addr;
		   NodeAddr next_dataNode_addr;
		   node = empty_node;

		dataNode_addr = node->data_node_addr[0];
		next_dataNode_addr = nodeAllocator->nodeAddr_to_node(dataNode_addr)->next_offset;

			k1 = 0;

		   while(node != end_node)
		   {
//		   node = skiplist->sa_to_node(node->next[0]);
			node = find_next_node(node);
		   cnt0++;

			dataNode_addr = node->data_node_addr[0];
			if (dataNode_addr != next_dataNode_addr)
				debug_error("link brok\n");
#if 0
k2 = find_skip_min(node);
if (k1 >= k2)
{
	debug_error("key revser\n");
k2 = find_skip_min(node);
}
if (k2 == KEY_MAX)
{
	k2 = 0;
}
	k1 = k2;
#endif

//---------------
			next_dataNode_addr = nodeAllocator->nodeAddr_to_node(dataNode_addr)->next_offset;

		   }
		   printf("warm node cnt %d = %lfGB?\n",cnt0,(double)cnt0*WARM_MAX_NODE_GROUP*NODE_SIZE/1024/1024/1024);
#endif
		size_t cnt = node_pool_list_cnt*NODE_POOL_SIZE+node_pool_cnt;
		printf("warm list cnt %ld max size %lfGB\n",cnt,double(cnt)*WARM_MAX_NODE_GROUP*NODE_SIZE/1024/1024/1024);

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
			return NULL;

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
			if (node_pool_cnt >= NODE_POOL_SIZE)
			{

				if (node_pool_list_cnt >= SKIPLIST_NODE_POOL_LIMIT)//NODE_POOL_LIST_SIZE)
				{
					printf("no space for node1!\n");
					at_unlock2(node_alloc_lock);
					return NULL;
				}

				++node_pool_list_cnt;
				node_pool_list[node_pool_list_cnt] = new SkiplistNode[NODE_POOL_SIZE];//(SkiplistNode*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
												      //			node_pool_list.push_back(new SkiplistNode[NODE_POOL_SIZE]);
				node_pool_cnt = 0;
			}

			node = &node_pool_list[node_pool_list_cnt][node_pool_cnt]; // first alloc
			node->myAddr.pool_num = node_pool_list_cnt;
			node->myAddr.node_offset = node_pool_cnt;
			//		node->next = NULL;
			//		node->next_size = 0;
			node->my_sa.pool_num = node_pool_list_cnt;
			node->my_sa.offset = node_pool_cnt;

//			node->key_list.resize(WARM_MAX_NODE_GROUP*WARM_NODE_ENTRY_CNT);
			node->key_list.resize(NODE_SLOT_MAX);
			node->entry_list.resize(NODE_SLOT_MAX);

			node_pool_cnt++;

			at_unlock2(node_alloc_lock);
		}
		//	node->next = NULL;
		node->setLevel();
		node->dst_cnt = node->level+1;
		node->recent_entry_cnt = 0;

		node->list_head = node->list_tail = 0;
		node->list_size_sum = 0;
		node->data_head = node->data_tail = 0;
		//	node->remain_cnt = WARM_BATCH_ENTRY_CNT; //8
		//	node->data_node_addr = nodeAllocator->alloc_node();

		node->key_list_size = 0;

		node->ver = node_counter.fetch_add(1);
		node->my_sa.ver = node->ver;

		node->cold_block_sum = 0;
		node->half_listNode = NULL;

		_mm_sfence();

		node->lock = 0;
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
			//			next_node = sa_to_node(sa);
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
							for (j=0;j<WARM_MAX_NODE_GROUP;j++)
							{
//								if (nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j])->list_addr.value != nodeAddr_to_listAddr(WARM_LIST,next_node->myAddr).value)
//									debug_error("free888\n");
								nodeAllocator->free_node(nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j]));
							}
							free_sl_node(next_node);
						}
					}
				}
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
	SkiplistNode temp;
	node = start_node;
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
				if (next_node->ver == 0)
				{
					v = sa.value;
					if (node->next[i].value.compare_exchange_strong(v,next_node->next[i].value.load()))
					{
//						next_node->dst_cnt--; // passed cas
						int rv = next_node->dst_cnt.fetch_sub(1);
						if (rv == 1)
						{
							for (j=0;j<WARM_MAX_NODE_GROUP;j++)
							{
//								if (nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j])->list_addr.value != nodeAddr_to_listAddr(WARM_LIST,next_node->myAddr).value)
//									debug_error("free333\n");
								nodeAllocator->free_node(nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr[j]));
							}
							free_sl_node(next_node);
						}
					}
				}
				continue;
			}
			if (next_node->key <= key) // == for split
				node = next_node;
			else
				break;
		}
		prev[i] = node->my_sa;
		next[i] = node->next[i];
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
SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next, NodeAddr &warm_cache) // what if max
{
#ifdef WARM_CACHE

#else
	return find_node(key,prev,next);
#endif
	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
	SkipAddr next_sa;
	if (warm_cache != emptyNodeAddr)
	{
		node = &skiplist->node_pool_list[warm_cache.pool_num][warm_cache.node_offset];	
		//	node = sa_to_node(sa);
		next_sa = node->next[0];
		next_node = sa_to_node(next_sa);
		if (node->key <= key && key < next_node->key && next_node->ver == next_sa.ver) 
		{
#ifdef WARM_STAT
//			addr2_hit++;
			my_thread->warm_hit_cnt++;
#endif
			return node;
		}
#ifdef WARM_STAT
		else
//			addr2_miss++;
			my_thread->warm_miss_cnt++;
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


void Skiplist::setLimit(size_t size)
{
	SKIPLIST_NODE_POOL_LIMIT = size / (NODE_POOL_SIZE * NODE_SIZE) +1;
}

void Skiplist::delete_node(SkiplistNode* node)//,SkipAddr** prev,SkipAddr** next)
{
	node->ver = 0;
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

	if (sr)
	{
		skiplistNode = start_node;
		while(skiplistNode != end_node)
		{
			if (skiplistNode->my_listNode == NULL)
				debug_error("no linked listNode\n");
			ps = skiplistNode;
			skiplistNode = find_next_node(skiplistNode);
		}
	}

}

void Skiplist::recover()
{

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

//			if (skiplistNode->ver == 417)
//				debug_error("here2\n");
//			if (listNode == NULL)
//				debug_error("list null\n");
			skiplistNode->my_listNode = listNode;
			skiplistNode->key = list_key;
			listNode->hold = 1;
		}

		{
			listNode->warm_cache = skiplistNode->myAddr; // cold_bl..
			skiplistNode->cold_block_sum+=listNode->block_cnt;
		}

		list_key = list_next_key;
		listNode = listNode->next;
	}

//	traverse_test();

//	debug_error("end of skip recov\n");

}
//---------------------------------------------- list

void PH_List::recover()
{
//	NodeMeta* nodeMeta;
	ListNode* listNode;
	DataNode* dataNode;
	NodeAddr dataAddr;
	EntryAddr list_addr;

	dataNode = nodeAllocator->nodeAddr_to_node(empty_node->data_node_addr);

	start_node->data_node_addr = dataNode->next_offset;

	listNode = start_node;

	ListNode* prev=empty_node;

//--
	dataAddr = dataNode->next_offset; // empty to start
	nodeAllocator->expand(dataAddr);

	listNode->prev = prev;
//	if (prev)
		prev->next = listNode;
	prev = listNode;

	listNode->data_node_addr = dataAddr;

//	nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(dataAddr);
	list_addr = nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr);
//	nodeMeta->list_addr = listNode->myAddr;
	listNode->key = recover_node(dataAddr,COLD_LIST,listNode->block_cnt,list_addr);
	listNode->key = 0; // start node

//------------- next
	dataNode = nodeAllocator->nodeAddr_to_node(dataAddr);
	dataAddr = dataNode->next_offset;

//---
#if 1
	while(dataAddr != end_node->data_node_addr)
//	while(dataAddr != emptyNodeAddr)
	{
//		cnt++;
		nodeAllocator->expand(dataAddr);

		listNode = alloc_list_node(); // myAddr lock
		
		listNode->prev = prev;
//		if (prev)
			prev->next = listNode;
		prev = listNode;

		listNode->data_node_addr = dataAddr;

//		nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(dataAddr);
		list_addr = nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr);
//		nodeMeta->list_addr = listNode->myAddr;
		listNode->key = recover_node(dataAddr,COLD_LIST,listNode->block_cnt,list_addr);

//------------- next
		dataNode = nodeAllocator->nodeAddr_to_node(dataAddr);
		dataAddr = dataNode->next_offset;
//		dataNode = nodeAllocator->nodeAddr_to_node(dataNode->next_offset);
	}

	//last node
//	nodeAllocator->expand(dataAddr);

	listNode = end_node;
	listNode->prev = prev;
//		if (prev)
	prev->next = listNode;
	listNode->next = NULL;

//	listNode->data_node_addr = dataAddr;

//	nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(dataAddr);
//	nodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr);
//	nodeMeta->list_addr = listNode->myAddr;

//	listNode->key = recover_node(dataAddr,COLD_LIST,listNode->block_cnt);

//	debug_error("xxx\n");
#else
	int cnt = 0;
	while(dataAddr != end_node->data_node_addr)
	{
		cnt++;
		nodeAllocator->expand(dataAddr);

		dataNode = nodeAllocator->nodeAddr_to_node(dataAddr);
		dataAddr = dataNode->next_offset;
	}
#endif

}
void PH_List::recover_init()
{


	//	node_pool_list = (Tree_Node**)malloc(sizeof(SkiplistNode*) * NODE_POOL_LIST_SIZE);
	node_pool_list = new ListNode*[NODE_POOL_LIST_SIZE];

	//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
	node_pool_list[0] = new ListNode[NODE_POOL_SIZE];
	//	node_pool_list.push_back(new ListNode[NODE_POOL_SIZE]);
	node_pool_cnt=0;
	node_pool_list_cnt = 0;
	node_free_head = NULL;

	node_alloc_lock = 0;

	NodeMeta* dataNodeMeta;
	NodeAddr tempAddr;

	empty_node = alloc_list_node();
	empty_node->key = KEY_MIN;
	tempAddr = {0,0};
	empty_node->data_node_addr = tempAddr;//nodeAllocator->expand_node();
	empty_node->block_cnt=1;
	empty_node->hold = 1;
	dataNodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	dataNodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,empty_node->myAddr);
//	dataNodeMeta->list_addr = empty_node->myAddr;


	start_node = alloc_list_node();
	start_node->key = KEY_MIN;
	tempAddr = {1,0};
	start_node->data_node_addr = tempAddr;//nodeAllocator->expand_node();
	start_node->block_cnt=1;
	start_node->hold = 1;
	dataNodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	dataNodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,start_node->myAddr);
//	dataNodeMeta->list_addr = start_node->myAddr;

	end_node = alloc_list_node();
	end_node->key = KEY_MAX;
	tempAddr = {2,0};
	end_node->data_node_addr = tempAddr;//nodeAllocator->expand_node();
	end_node->block_cnt=1;
	end_node->hold = 1;
	dataNodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);
	dataNodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,end_node->myAddr);
//	dataNodeMeta->list_addr = end_node->myAddr;

	//	List_Node* node = alloc_list_node();
	/*
	empty_node->next = start_node;
	start_node->next = end_node;
	start_node->prev = empty_node;
	end_node->prev = start_node;

	NodeMeta* nm_empty = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	NodeMeta* nm_start = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	NodeMeta* nm_end = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);
*/
//	nodeAllocator->linkNext(nm_empty,nm_start);
//	nodeAllocator->linkNext(nm_start,nm_end);

}

void PH_List::init()
{
	//	node_pool_list = (Tree_Node**)malloc(sizeof(SkiplistNode*) * NODE_POOL_LIST_SIZE);
	node_pool_list = new ListNode*[NODE_POOL_LIST_SIZE];

	//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
	node_pool_list[0] = new ListNode[NODE_POOL_SIZE];
	//	node_pool_list.push_back(new ListNode[NODE_POOL_SIZE]);
	node_pool_cnt=0;
	node_pool_list_cnt = 0;
	node_free_head = NULL;

	node_alloc_lock = 0;

	NodeMeta* dataNodeMeta;

	empty_node = alloc_list_node();
	empty_node->key = KEY_MIN;
	empty_node->data_node_addr = nodeAllocator->alloc_node(COLD_LIST);
	empty_node->block_cnt=1;
	empty_node->hold = 1;
	dataNodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	dataNodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,empty_node->myAddr);
//	dataNodeMeta->list_addr = empty_node->myAddr;

	start_node = alloc_list_node();
	start_node->key = KEY_MIN;
	start_node->data_node_addr = nodeAllocator->alloc_node(COLD_LIST);
	start_node->block_cnt=1;
	start_node->hold = 1;
	dataNodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	dataNodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,start_node->myAddr);
//	dataNodeMeta->list_addr = start_node->myAddr;

	end_node = alloc_list_node();
	end_node->key = KEY_MAX;
	end_node->data_node_addr = nodeAllocator->alloc_node(COLD_LIST);
	end_node->block_cnt=1;
	end_node->hold = 1;
	dataNodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);
	dataNodeMeta->list_addr = nodeAddr_to_listAddr(COLD_LIST,end_node->myAddr);
//	dataNodeMeta->list_addr = end_node->myAddr;


	//	List_Node* node = alloc_list_node();
	empty_node->next = start_node;
	start_node->next = end_node;
	start_node->prev = empty_node;
	end_node->prev = start_node;

	NodeMeta* nm_empty = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	NodeMeta* nm_start = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	NodeMeta* nm_end = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);

	nodeAllocator->linkNext(nm_empty,nm_start);
	nodeAllocator->linkNext(nm_start,nm_end);
}

void PH_List::clean()
{
#ifdef LIST_TRAVERSE_TEST
	size_t max,use,bc;
	ListNode* node;
	NodeMeta* nodeMeta;
	node = start_node;
	max = use = bc = 0;
	while(node != end_node)
	{
		nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(node->data_node_addr);
		while (nodeMeta)
		{
//			if (nodeMeta->list_addr != nodeAddr_to_listAddr(COLD_LIST,node->myAddr))
//				debug_error("node-list error\n");
#ifdef PER_TEST
DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
if (nodeMeta->next_addr != dataNode->next_offset || nodeMeta->next_addr_in_group != dataNode->next_offset_in_group)
	debug_error("failed\n");
#endif
			bc++;
			max+=NODE_SLOT_MAX;
//			use+=nodeMeta->valid_cnt;
			use+=nodeMeta->size_sum;
			nodeMeta = nodeMeta->next_node_in_group;
		}
		node = node->next;
	}
	printf("cold use %ld max %ld = %lf\n",use,max,double(use)/max);
	printf("cold node cnt %ld size %lfGB\n",bc,double(bc)*NODE_SIZE/1024/1024/1024);
#endif
	int cnt = node_pool_list_cnt * NODE_POOL_SIZE + node_pool_cnt;
	printf("cold list cnt %d max %lfGB\n",cnt,double(cnt)*MAX_NODE_GROUP*NODE_SIZE/1024/1024/1024);

	//	printf("list pool cnt %ld\n",node_pool_list_cnt);

	int i;
	for (i=0;i<=node_pool_list_cnt;i++)
	{
		//		free(node_pool_list[i]);
		delete[] node_pool_list[i];
	}
	//	free(node_pool_list);
	delete[] node_pool_list;
}
/*
void PH_List::expand(NodeAddr nodeAddr)
{
	if (node_pool_list_cnt < nodeAddr.pool_num)
		node_pool_cnt = 0;

	while (node_pool_list_cnt < nodeAddr.pool_num)
	{
		node_pool_list_cnt++;
		node_pool_list[node_pool_list_cnt] = new ListNode[NODE_POOL_SIZE];
	}
	if (node_pool_cnt < nodeAddr.node_offset)
		node_pool_cnt = nodeAddr.node_offset;
}
*/
ListNode* PH_List::alloc_list_node()
{
	//just use lock
	while(node_alloc_lock);
	at_lock2(node_alloc_lock);

	ListNode* node;

	if (node_free_head)
	{
		node = node_free_head;
		node_free_head = node_free_head->next;
/*
		rv->next = NULL;
		rv->prev = NULL;
		rv->block_cnt = 0;
		_mm_sfence();
		rv->lock = 0;
*/
		at_unlock2(node_alloc_lock);
//		return rv;
	}
	else // alloc new
	{
		if (node_pool_cnt >= NODE_POOL_SIZE)
		{
			if (node_pool_list_cnt >= NODE_POOL_LIST_SIZE)
				printf("no space for node2!\n");
			++node_pool_list_cnt;
			node_pool_list[node_pool_list_cnt] = new ListNode[NODE_POOL_SIZE];//(SkiplistNode*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
											  //		node_pool_list.push_back(new ListNode[NODE_POOL_SIZE]);
			node_pool_cnt = 0;
		}
		node = &node_pool_list[node_pool_list_cnt][node_pool_cnt];
		node->myAddr.pool_num = node_pool_list_cnt;
		node->myAddr.node_offset = node_pool_cnt;

		node_pool_cnt++;
		at_unlock2(node_alloc_lock);
	}

	node->block_cnt = 0;
//	node->valid_cnt = 0;
	node->size_sum = 0;
	node->next = NULL;
	node->prev = NULL;
	node->hold = 0;
	node->warm_cache = emptyNodeAddr;
	//	node->data_node_addr = nodeAllocator->alloc_node();

	_mm_sfence();
	node->lock = 0;
	//	if (node_pool_cnt < NODE_POOL_SIZE)
	{
		return node;
	}
}

void PH_List::free_list_node(ListNode* node)
{
	at_lock2(node_alloc_lock);
	node->next = node_free_head;
	node_free_head = node;
	at_unlock2(node_alloc_lock);
}

ListNode* PH_List::find_node(size_t key,ListNode* node) 
{

	while(node->next && node->next->key < key)
	{
		node = node->next;
	}
	return node;

}

void PH_List::delete_node(ListNode* node) // never delete head or tail
{

	ListNode* prev = node->prev;
	ListNode* next = node->next;

	at_lock2(prev->lock);
	at_lock2(node->lock);
	at_lock2(next->lock); // always not null

	prev->next = next;
	next->prev = prev;

	at_unlock2(next->lock);
	at_unlock2(node->lock);
	at_unlock2(prev->lock);

}

void PH_List::insert_node(ListNode* prev, ListNode* node)
{
	ListNode* next = prev->next;

	//	at_lock2(prev->lock);
	at_lock2(next->lock); // always not null

	prev->next = node;
	node->next = next;
	node->prev = prev;
	next->prev = node;

	at_unlock2(next->lock);
	//	at_unlock2(prev->lock);
}
#if 0
	bool need_reduce(ListNode* listNode)
	{
		return (listNode->valid_cnt + NODE_SLOT_MAX*2 < listNode->block_cnt * NODE_SLOT_MAX) // try shorten group
	}
#endif
	bool try_reduce_group(ListNode* listNode) // remove last block
	{
		return false;
#if 0
		bool rv = false;
		if (try_at_lock2(listNode->lock) == false)
			return rv;
		if (need_reduce(listNode)) // try shorten group
		{
			rv = true;
			my_thread->reduce_group_cnt++;
//			printf("reduce----------------------------------------\n");

			NodeAddr nodeAddr;
			nodeAddr = listNode->data_node_addr;
			NodeMeta* nodeMeta_list[MAX_NODE_GROUP+1];
			NodeMeta* src_nodeMeta;
			int dst_i,dst_j;
			nodeMeta_list[0] = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
			int i;
			for (i=0;i<listNode->block_cnt;i++)
			{
				at_lock2(nodeMeta_list[i]->rw_lock);
				nodeMeta_list[i+1] = nodeMeta_list[i]->next_node_in_group;
			}

			src_nodeMeta = nodeMeta_list[listNode->block_cnt-1];

//			int entry_num = nodeMeta->valid_cnt;

			DataNode* dataNode;
			DataNode* dst_dataNode;
			unsigned char* src_addr;
			unsigned char* dst_addr;
			uint64_t key;
			std::atomic<uint8_t> *seg_lock;
			KVP* kvp_p;
			EntryAddr src_ea,dst_ea;
			unsigned char* split_buffer = (unsigned char*)my_thread->split_buffer;

			dataNode = nodeAllocator->nodeAddr_to_node(src_nodeMeta->my_offset);

			reverse_memcpy(split_buffer,(unsigned char*)dataNode,NODE_SIZE);
			src_addr = split_buffer+NODE_HEADER_SIZE;

			dst_i = 0;
			dst_j = 0;
			dst_dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta_list[dst_i]->my_offset);

			src_ea.loc = COLD_LIST;
			src_ea.file_num = src_nodeMeta->my_offset.pool_num;
			src_ea.offset = src_nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

			dst_ea.loc = COLD_LIST;
			dst_ea.file_num = nodeMeta_list[0]->my_offset.pool_num;
			dst_ea.offset = nodeMeta_list[0]->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

			for (i=0;i<NODE_SLOT_MAX;i++)
			{
				if (src_nodeMeta->valid[i])
				{
					key = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE);
					kvp_p = hash_index->insert(key,&seg_lock,my_thread->read_lock);
					if (kvp_p->value == src_ea.value)
					{
//						while(dst_i<listNode->block_cnt-1)
						while(nodeMeta_list[dst_i]->valid[dst_j])
						{
							++dst_j;
							dst_ea.offset+=ENTRY_SIZE;
							if (dst_j >= NODE_SLOT_MAX)
							{
								dst_j = 0;
								dst_i++;

								if (dst_i >= listNode->block_cnt-1)
									debug_error("block group overflow\n");

								dst_dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta_list[dst_i]->my_offset);
								dst_ea.file_num = nodeMeta_list[dst_i]->my_offset.pool_num;
								dst_ea.offset = nodeMeta_list[dst_i]->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;
							}
						}


						dst_addr = (unsigned char*)dst_dataNode+NODE_HEADER_SIZE+ENTRY_SIZE*dst_j;
						// copy - valid - index???

						memcpy(dst_addr,src_addr,ENTRY_SIZE);// copy src to dst // mem to pmem
						_mm_sfence();
						nodeMeta_list[dst_i]->valid[dst_j] = true;
						nodeMeta_list[dst_i]->valid_cnt++;

						kvp_p->value = dst_ea.value;
					}
					_mm_sfence();
					hash_index->unlock_entry2(seg_lock,my_thread->read_lock);

				}
				src_ea.offset+=ENTRY_SIZE;
				src_addr+=ENTRY_SIZE;
			}

			//flush
			for (i=0;i<listNode->block_cnt-1;i++)
			{
				dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta_list[i]->my_offset);
				pmem_persist(dataNode,NODE_SIZE);
			}
			_mm_sfence();

			//link
			dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta_list[listNode->block_cnt-1-1]->my_offset);
#if 1
			dataNode->next_offset_in_group = emptyNodeAddr;
			pmem_persist(&dataNode->next_offset_in_group,sizeof(NodeAddr));
#else
			pmem_next_in_group_write(dataNode,emptyNodeAddr);
#endif
			_mm_sfence();

			nodeMeta_list[listNode->block_cnt-1-1]->next_addr_in_group = emptyNodeAddr;
			nodeMeta_list[listNode->block_cnt-1-1]->next_node_in_group = NULL;

			_mm_sfence(); // need?

			for (i=0;i<listNode->block_cnt-1;i++)
				at_unlock2(nodeMeta_list[i]->rw_lock);

			//free
			listNode->block_cnt--;
			nodeAllocator->free_node(src_nodeMeta);

		}
		at_unlock2(listNode->lock);
		return rv;
#endif
	}

	bool try_merge_listNode(ListNode* left_listNode,ListNode* right_listNode)
	{
		return false;
#if 0
		bool rv = false;
		if (try_at_lock2(left_listNode->lock) == false)
			return rv;
		if (try_at_lock2(right_listNode->lock) == false)
		{
			at_unlock2(left_listNode->lock);
			return rv;
		}
		if (right_listNode->hold || left_listNode->valid_cnt + right_listNode->valid_cnt > NODE_SLOT_MAX)
		{
			at_unlock2(left_listNode->lock);
			at_unlock2(right_listNode->lock);
			return rv;
		}

		// OK
		rv = true;
		my_thread->list_merge_cnt++;
		printf("mer?-------------------------------------------\n");

		NodeMeta* left_nodeMeta_list[MAX_NODE_GROUP+1];
		NodeMeta* right_nodeMeta_list[MAX_NODE_GROUP+1];

		int i;

		left_nodeMeta_list[0] = nodeAllocator->nodeAddr_to_nodeMeta(left_listNode->data_node_addr);
		for (i=0;i<left_listNode->block_cnt;i++)
		{
			at_lock2(left_nodeMeta_list[i]->rw_lock);
			left_nodeMeta_list[i+1] = left_nodeMeta_list[i]->next_node_in_group;
		}

		right_nodeMeta_list[0] = nodeAllocator->nodeAddr_to_nodeMeta(right_listNode->data_node_addr);
		for (i=0;i<right_listNode->block_cnt;i++)
		{
			at_lock2(right_nodeMeta_list[i]->rw_lock);
			right_nodeMeta_list[i+1] = right_nodeMeta_list[i]->next_node_in_group;
		}

		EntryAddr src_ea,dst_ea;
		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		uint64_t key;
		DataNode* dataNode;
		int j;
		unsigned char* src_addr;
		unsigned char* dst_addr;
		NodeMeta* dst_nodeMeta = left_nodeMeta_list[0];
		int dst_i = 0;
//		dst_addr = (unsigned char*)my_thread->sorted_buffer[0]+NODE_HEADER_SIZE;
		dst_addr = (unsigned char*)nodeAllocator->nodeAddr_to_node(dst_nodeMeta->my_offset) + NODE_HEADER_SIZE;

		src_ea.loc = COLD_LIST;

		dst_ea.loc = COLD_LIST;
		dst_ea.file_num = dst_nodeMeta->my_offset.pool_num;
		dst_ea.offset = dst_nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

		//use direct copy without dram ...

		NodeMeta* nodeMeta;

		//left first
		for (i=1;i<left_listNode->block_cnt;i++)
		{
			nodeMeta = left_nodeMeta_list[i];
			dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
//			my_thread->split_buffer[i] = *dataNode;
//			src_addr = (unsigned char*)(my_thread->split_buffer[i])+NODE_HEADER_SIZE;
			src_addr = (unsigned char*)dataNode+NODE_HEADER_SIZE;

			src_ea.file_num = nodeMeta->my_offset.pool_num;
			src_ea.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

			for (j=0;j<NODE_SLOT_MAX;j++)
			{
				if (nodeMeta->valid[j])
				{
					key = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE);
					kvp_p = hash_index->insert(key,&seg_lock,my_thread->read_lock);
					if (kvp_p->value == src_ea.value)
					{
						while (dst_nodeMeta->valid[dst_i]) // check dst
						{
							dst_i++;
							dst_addr+=ENTRY_SIZE;
							dst_ea.offset+=ENTRY_SIZE;
						}

						// copy valid index
						memcpy(dst_addr,src_addr,ENTRY_SIZE); //pmem to pmem

						dst_nodeMeta->valid[dst_i] = true;
						dst_nodeMeta->valid_cnt++; // hold varaible block movement between warm range
						kvp_p->value = dst_ea.value;
					}
					hash_index->unlock_entry2(seg_lock,my_thread->read_lock);
				}
				src_addr+=ENTRY_SIZE;
				src_ea.offset+=ENTRY_SIZE;
			}
		}

		//right second // duplicateddd
		for (i=0;i<right_listNode->block_cnt;i++)
		{
			nodeMeta = right_nodeMeta_list[i];
			dataNode = nodeAllocator->nodeAddr_to_node(nodeMeta->my_offset);
//			my_thread->split_buffer[i] = *dataNode;
//			src_addr = (unsigned char*)(my_thread->split_buffer[i])+NODE_HEADER_SIZE;
			src_addr = (unsigned char*)dataNode + NODE_HEADER_SIZE;

			src_ea.file_num = nodeMeta->my_offset.pool_num;
			src_ea.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + NODE_HEADER_SIZE;

			for (j=0;j<NODE_SLOT_MAX;j++)
			{
				if (nodeMeta->valid[j])
				{
					key = *(uint64_t*)(src_addr+ENTRY_HEADER_SIZE);
					kvp_p = hash_index->insert(key,&seg_lock,my_thread->read_lock);
					if (kvp_p->value == src_ea.value)
					{
						while (dst_nodeMeta->valid[dst_i]) // check dst
						{
							dst_i++;
							dst_addr+=ENTRY_SIZE;
							dst_ea.offset+=ENTRY_SIZE;
						}

						// copy valid index
						memcpy(dst_addr,src_addr,ENTRY_SIZE); //pmem to pmem

						dst_nodeMeta->valid[dst_i] = true;
						dst_nodeMeta->valid_cnt++; // hold varaible block movement between warm range
						kvp_p->value = dst_ea.value;
					}
					hash_index->unlock_entry2(seg_lock,my_thread->read_lock);
				}
				src_addr+=ENTRY_SIZE;
				src_ea.offset+=ENTRY_SIZE;
			}
		}

		dataNode = nodeAllocator->nodeAddr_to_node(dst_nodeMeta->my_offset);
		pmem_persist(dataNode,NODE_SIZE);
		_mm_sfence();

		dataNode->next_offset = right_nodeMeta_list[0]->next_addr;
		dataNode->next_offset_in_group = emptyNodeAddr;
		pmem_persist(dataNode,NODE_HEADER_SIZE);
		_mm_sfence();

		dst_nodeMeta->next_addr = right_nodeMeta_list[0]->next_addr;
		dst_nodeMeta->next_addr_in_group = emptyNodeAddr;
		dst_nodeMeta->next_p = right_nodeMeta_list[0]->next_p;
		dst_nodeMeta->next_node_in_group = NULL;

		for (i=1;i<left_listNode->block_cnt;i++)
			nodeAllocator->free_node(left_nodeMeta_list[i]);
		for (i=0;i<right_listNode->block_cnt;i++)
			nodeAllocator->free_node(right_nodeMeta_list[i]);

// cold split // cold merge ...
		left_listNode->next = right_listNode->next;
		left_listNode->next->prev = left_listNode;
		list->free_list_node(right_listNode);

		at_unlock2(left_listNode->lock);

		return rv;
#endif
	}
#if 0
	void test_before_free(ListNode* listNode)
	{
		NodeAddr nodeAddr;
		NodeMeta* nodeMeta;
		nodeAddr.value = listNode->data_node_addr;

		while(nodeAddr.value)
		{
			nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
			if (nodeMeta->list_addr.value != nodeAddr_to_listAddr(COLD_LIST,listNode->myAddr).value)
			{
				debug_error("missmatch2\n");
				break;
			}
			nodeAddr = nodeMeta->next_addr_in_group;
		}
	}
#endif
}
