
#include <atomic>
#include <stdlib.h> // rand
#include <stdio.h>

#include "skiplist.h"
#include "lock.h"
#include "data2.h"

namespace PH
{

extern size_t HARD_EVICT_SPACE;
extern size_t SOFT_EVICT_SPACE;

//const size_t DATA_SIZE = 100*1000*1000 * 100;//100M * 100B = 10G

//const size_t NODE_POOL_LIST_SIZE = 1024;
const size_t NODE_POOL_LIST_SIZE = 1024*1024;
const size_t NODE_POOL_SIZE = 1024; //4MB?
const size_t SKIPLIST_NODE_POOL_LIMIT = 1024 * 4*3;//DATA_SIZE/10/(NODE_SIZE*NODE_POOL_SIZE);
//4MB * 1024 * 4 * 3 = 4GB * 12GB

const size_t KEY_MIN = 0x0000000000000000;
const size_t KEY_MAX = 0xffffffffffffffff;

//const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?

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

void SkiplistNode::setLevel(size_t l)
{
	level = l;
	delete next;
	next = new std::atomic<SkiplistNode*>[l+1];
	built = 0;
}

void SkiplistNode::setLevel()
{
	level = getRandomLevel();
	delete next;
	next = new std::atomic<SkiplistNode*>[level+1];
	built = 0;
}

void Skiplist::init()
{
//	node_pool_list = (Tree_Node**)malloc(sizeof(SkiplistNode*) * NODE_POOL_LIST_SIZE);
	node_pool_list = new SkiplistNode*[NODE_POOL_LIST_SIZE];

//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
	node_pool_list[0] = new SkiplistNode[NODE_POOL_SIZE];
	node_pool_cnt=0;
	node_pool_list_cnt = 0;
	node_free_head = NULL;

	node_alloc_lock = 0;

	empty_node = alloc_sl_node();
	empty_node->key = KEY_MIN;
	empty_node->my_listNode = list->empty_node; // may not need

	start_node = alloc_sl_node();
	start_node->setLevel(MAX_LEVEL);
	start_node->key = KEY_MIN;
	start_node->my_listNode = list->start_node;

	end_node = alloc_sl_node();
//	end_node->built = MAX_LEVEL;
	end_node->setLevel(MAX_LEVEL);
	end_node->key = KEY_MAX;
	end_node->my_listNode = list->end_node;

	int i;
	for (i=0;i<=MAX_LEVEL;i++)
		start_node->next[i] = end_node;
	start_node->built = MAX_LEVEL;

	NodeMeta* nm_empty = nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	NodeMeta* nm_start = nodeAddr_to_nodeMeta(start_node->data_node_addr);
	NodeMeta* nm_end = nodeAddr_to_nodeMeta(end_node->data_node_addr);

	nodeAllocator->linkNext(nm_empty,nm_start);
	nodeAllocator->linkNext(nm_start,nm_end);

//	nodeAllocator->linkNext(empty_node->data_node_addr);
//	nodeAllocator->linkNext(start_node->data_node_addr);

}

void Skiplist::clean()
{
	printf("sc cnt %ld pool0 %p  pool %p \n",node_pool_list_cnt,node_pool_list[0],node_pool_list);
	int i;
	for (i=0;i<=node_pool_list_cnt;i++)
	{
//		free(node_pool_list[i]);
		delete[] node_pool_list[i];
	}
//	free(node_pool_list);
	delete[] node_pool_list;
}


SkiplistNode* Skiplist::alloc_sl_node()
{
	//just use lock
	while(node_alloc_lock);
	at_lock2(node_alloc_lock);

	if (node_free_head) // no pmem alloc...
	{
		SkiplistNode* rv = node_free_head;
		node_free_head = node_free_head->next[0];
		at_unlock2(node_alloc_lock);
		return rv;
	}
	

	if (node_pool_cnt >= NODE_POOL_SIZE)
	{
		if (node_pool_list_cnt >= SKIPLIST_NODE_POOL_LIMIT)//NODE_POOL_LIST_SIZE)
		{
			printf("no space for node!\n");
			return NULL;
		}
		++node_pool_list_cnt;
		node_pool_list[node_pool_list_cnt] = new SkiplistNode[NODE_POOL_SIZE];//(SkiplistNode*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
		node_pool_cnt = 0;
	}

	SkiplistNode* node = &node_pool_list[node_pool_list_cnt][node_pool_cnt];
	node->lock = 0;
	node->delete_lock = 0;
	node->next = NULL;
	node->setLevel();
	node->data_node_addr = nodeAllocator->alloc_node();

//	if (node_pool_cnt < NODE_POOL_SIZE)
	{
		node_pool_cnt++;
		at_unlock2(node_alloc_lock);
		return node;
	}
}

void Skiplist::free_sl_node(SkiplistNode* node)
{
	at_lock2(node_alloc_lock);
	node->next[0] = node_free_head;
	node_free_head = node;
	at_unlock2(node_alloc_lock);
}

SkiplistNode* Skiplist::find_node(size_t key,SkiplistNode** prev,SkiplistNode** next) // what if max
{
	SkiplistNode* node = start_node;
	int i;
	for (i=MAX_LEVEL;i>=0;i--)
	{
		while(true)
		{
			while(node->next[i].load()->delete_lock);
			if (node->next[i].load()->key <= key)
				node = node->next[i];
			else
				break;
		}
		prev[i] = node;
		next[i] = node->next[i];

	}
	return node;
}

void Skiplist::delete_node(SkiplistNode* node,SkiplistNode** prev,SkiplistNode** next)
{
	size_t key = node->key;
	at_lock2(node->delete_lock);
	while(1)
	{
//		if (at_try_lock2(node->delete_lock))
		{
			if (delete_node_with_fail(node,prev,next))
				return;
//			at_unlock2(node->delete_lock);
		}
//		else
//			printf("impossible use lock\n");

		node = find_node(key,prev,next);
//		if (node->key != key)
//			return;
	}
}

bool Skiplist::delete_node_with_fail(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next)
{
	if (try_at_lock2(node->lock) == 0)
		return false;
	
	SkiplistNode* pn;
	SkiplistNode* nn;
	int i;

	for (i=node->level;i>=0;i--)
	{
		if (prev[i]->delete_lock) // prev first...
			return false;
//		while(node->next[i]->delete_lock);
//		if (at_try_lock2(prev[i]->lock) == 0)
//			continue;
		pn = prev[i]->next[i];
		nn = node->next[i];
		if (pn != node)
			return false;
		if (prev[i]->next[i].compare_exchange_strong(pn,nn) == false)
			return false;
		node->level--;
	}
	return true;

}

bool Skiplist::insert_node_with_fail(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next)
{
	// level already
	int i;

	SkiplistNode *pn;

	for (i=node->built;i<=node->level;i++)
	{
		if (prev[i]->delete_lock) // not gonna happend
			return false;
		pn = prev[i]->next[i];
		if (pn != next[i])
			return false;
		node->next[i] = next[i];
		if (prev[i]->next[i].compare_exchange_strong(pn,node) == false)
			return false;
		node->built++;
	}
	return true;
}

void Skiplist::insert_node(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next)
{
	while(1)
	{
		find_node(node->key,prev,next);
		if (insert_node_with_fail(node,prev,next))
			return;
	}
}

//---------------------------------------------- list

void PH_List::init()
{
//	node_pool_list = (Tree_Node**)malloc(sizeof(SkiplistNode*) * NODE_POOL_LIST_SIZE);
	node_pool_list = new ListNode*[NODE_POOL_LIST_SIZE];

//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
	node_pool_list[0] = new ListNode[NODE_POOL_SIZE];
	node_pool_cnt=0;
	node_pool_list_cnt = 0;
	node_free_head = NULL;

	node_alloc_lock = 0;


	empty_node = alloc_list_node();
	empty_node->key = KEY_MIN;

	start_node = alloc_list_node();
	start_node->key = KEY_MIN;

	end_node = alloc_list_node();
	end_node->key = KEY_MAX;

//	List_Node* node = alloc_list_node();
	empty_node->next = start_node;
	start_node->next = end_node;
	start_node->prev = empty_node;
	end_node->prev = start_node;

	NodeMeta* nm_empty = nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	NodeMeta* nm_start = nodeAddr_to_nodeMeta(start_node->data_node_addr);
	NodeMeta* nm_end = nodeAddr_to_nodeMeta(end_node->data_node_addr);

	nodeAllocator->linkNext(nm_empty,nm_start);
	nodeAllocator->linkNext(nm_start,nm_end);
}

void PH_List::clean()
{
	printf("lc cnt %ld pool0 %p  pool %p \n",node_pool_list_cnt,node_pool_list[0],node_pool_list);

	int i;
	for (i=0;i<=node_pool_list_cnt;i++)
	{
//		free(node_pool_list[i]);
		delete[] node_pool_list[i];
	}
//	free(node_pool_list);
	delete[] node_pool_list;
}


ListNode* PH_List::alloc_list_node()
{
	//just use lock
	while(node_alloc_lock);
	at_lock2(node_alloc_lock);

	if (node_free_head)
	{
		ListNode* rv = node_free_head;
		node_free_head = node_free_head->next;

		rv->lock = 0;
		rv->next = NULL;
		rv->prev = NULL;

		at_unlock2(node_alloc_lock);
		return rv;
	}
	

	if (node_pool_cnt >= NODE_POOL_SIZE)
	{
		if (node_pool_list_cnt >= NODE_POOL_LIST_SIZE)
			printf("no space for node!\n");
		++node_pool_list_cnt;
		node_pool_list[node_pool_list_cnt] = new ListNode[NODE_POOL_SIZE];//(SkiplistNode*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
		node_pool_cnt = 0;
	}

	ListNode* node = &node_pool_list[node_pool_list_cnt][node_pool_cnt];
	node->lock = 0;
	node->next = NULL;
	node->prev = NULL;
	node->data_node_addr = nodeAllocator->alloc_node();

//	if (node_pool_cnt < NODE_POOL_SIZE)
	{
		node_pool_cnt++;
		at_unlock2(node_alloc_lock);
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

}
