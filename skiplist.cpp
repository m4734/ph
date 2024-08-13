
#include <atomic>
#include <stdlib.h> // rand
#include <stdio.h>

#include "skiplist.h"
#include "lock.h"
#include "data2.h"
#include "cceh.h"

namespace PH
{

extern CCEH* hash_index;

extern size_t HARD_EVICT_SPACE;
extern size_t SOFT_EVICT_SPACE;

//const size_t DATA_SIZE = 100*1000*1000 * 100;//100M * 100B = 10G

//const size_t NODE_POOL_LIST_SIZE = 1024;

//NODE_POOL[NODE_POOL_LIST_SIZE][NODE_POOL_SIZE]
// NODE_POOL size = NODE_POOL_LIST_SIZE * NODE_POOL_SIZE * NODE_SIZE = 1024*1024*1024*4096 4TB?

//const size_t NODE_POOL_LIST_SIZE = 1024*1024; // 4GB?
//const size_t NODE_POOL_SIZE = 1024; //4MB?

const size_t NODE_POOL_LIST_SIZE = 1024*32;
const size_t NODE_POOL_SIZE = 1024*32;

//size_t SKIPLIST_NODE_POOL_LIMIT = 1024 * 4*3;//DATA_SIZE/10/(NODE_SIZE*NODE_POOL_SIZE);
//4MB * 1024 * 4 * 3 = 4GB * 12GB

const size_t KEY_MIN = 0x0000000000000000;
const size_t KEY_MAX = 0xffffffffffffffff;

//const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?

//const size_t WARM_BATCH_MAX_SIZE = 1024; // 1KB
size_t WARM_BATCH_MAX_SIZE = 1024;
//#define WARM_BATCH_MAX_SIZE 1024
size_t WARM_BATCH_ENTRY_CNT; // 8-9
size_t WARM_BATCH_CNT; // 4096/1024
//size_t WARM_BATCH_SIZE; // 120 * 8-9
size_t WARM_NODE_ENTRY_CNT; // 8-9 * 4
size_t WARM_LOG_MAX; // 8*2~3
size_t WARM_LOG_MIN; // 8
size_t WARM_LOG_THRESHOLD; // 16


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

inline unsigned char* SkiplistNode::get_entry(int index)
{
	int group_num = index/WARM_NODE_ENTRY_CNT;
	int batch_num = (index%WARM_NODE_ENTRY_CNT)/WARM_BATCH_ENTRY_CNT;
	int offset = index%WARM_BATCH_ENTRY_CNT;
	return group_node_p[group_num] + batch_num*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE + offset*ENTRY_SIZE;
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

void Skiplist::init(size_t size)
{
	WARM_BATCH_CNT = NODE_SIZE/(WARM_BATCH_MAX_SIZE-NODE_HEADER_SIZE);
	WARM_BATCH_ENTRY_CNT = (WARM_BATCH_MAX_SIZE-NODE_HEADER_SIZE)/ENTRY_SIZE;
//	WARM_BATCH_SIZE = WARM_BATCH_ENTRY_CNT*ENTRY_SIZE;
	WARM_NODE_ENTRY_CNT = WARM_BATCH_ENTRY_CNT*(WARM_BATCH_CNT);//(NODE_SIZE/(WARM_BATCH_SIZE+NODE_HEADER_SIZE)); //8-9 * 4
	WARM_LOG_MAX = WARM_BATCH_ENTRY_CNT * 3;
	WARM_LOG_MIN = WARM_BATCH_ENTRY_CNT;
	WARM_LOG_THRESHOLD = WARM_BATCH_ENTRY_CNT * 2;

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

	start_node = alloc_sl_node();
	start_node->setLevel(MAX_LEVEL);
	start_node->key = KEY_MIN;
	start_node->my_listNode = list->start_node;
	start_node->data_node_addr = nodeAllocator->alloc_node();
	nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	for (i=1;i<MAX_NODE_GROUP;i++)
	{
		nodeMeta = append_group(nodeMeta);
	}

	end_node = alloc_sl_node();
	end_node->setLevel(MAX_LEVEL);
	end_node->key = KEY_MAX;
	end_node->my_listNode = list->end_node;
	end_node->data_node_addr = nodeAllocator->alloc_node();
	nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);
	for (i=1;i<MAX_NODE_GROUP;i++)
		nodeMeta = append_group(nodeMeta);

	empty_node = alloc_sl_node();
	empty_node->setLevel(MAX_LEVEL);
	empty_node->key = KEY_MIN;
	empty_node->my_listNode = list->empty_node;
	empty_node->data_node_addr = nodeAllocator->alloc_node();
	nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	for (i=1;i<MAX_NODE_GROUP;i++)
		nodeMeta = append_group(nodeMeta);

	for (i=0;i<=MAX_LEVEL;i++)
	{
		start_node->next[i] = empty_node->my_sa;
		empty_node->next[i] = end_node->my_sa;
	}
	start_node->built = MAX_LEVEL;
	start_node->dataNodeHeader = empty_node->data_node_addr;
	empty_node->built = MAX_LEVEL;
	empty_node->dataNodeHeader = end_node->data_node_addr;

	NodeMeta* nm_empty = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	NodeMeta* nm_start = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	NodeMeta* nm_end = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);

	nodeAllocator->linkNext(nm_empty,nm_start);
	nodeAllocator->linkNext(nm_start,nm_end);

//	nodeAllocator->linkNext(empty_node->data_node_addr);
//	nodeAllocator->linkNext(start_node->data_node_addr);

	//check
	addr2_hit = 0;
	addr2_miss = 0;
	addr2_no = 0;


}

void Skiplist::clean()
{
//	printf("skiplist cnt %ld\n",node_pool_list_cnt);

	int cnt=0;
	SkiplistNode* node;
	node = start_node;
	while(node != end_node)
	{
		node = skiplist->sa_to_node(node->next[0]);
		cnt++;
	}
	printf("warm node cnt %d\n",cnt);

	printf("warm node %ld size %lfGB\n",node_pool_list_cnt*NODE_POOL_SIZE+node_pool_cnt,double(node_pool_list_cnt)*NODE_POOL_SIZE*NODE_SIZE/1024/1024/1024);
	printf("addr2 hit %ld miss %ld no %ld\n",addr2_hit.load(),addr2_miss.load(),addr2_no.load());

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
		node_free_head = sa_to_node(node_free_head->next[0]);
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
		node_pool_cnt++;
	}
	node->lock = 0;
	node->rw_lock = 0;
//	node->next = NULL;
	node->setLevel();
	node->dst_cnt = 0;
	node->recent_entry_cnt = 0;

	node->list_head = node->list_tail = 0;
	node->data_head = node->data_tail = 0;
//	node->remain_cnt = WARM_BATCH_ENTRY_CNT; //8
	node->entry_list.resize(NODE_SLOT_MAX);
//	node->data_node_addr = nodeAllocator->alloc_node();

	node->ver = node_counter.fetch_add(1);
	node->my_sa.ver = node->ver;
	node->my_sa.pool_num = node_pool_list_cnt;
	node->my_sa.offset = node_pool_cnt-1;

	at_unlock2(node_alloc_lock);
	return node;
}

void Skiplist::free_sl_node(SkiplistNode* node)
{
	at_lock2(node_alloc_lock);
	node->next[0] = node_free_head->my_sa;
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
SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next) // what if max
{
	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
	SkiplistNode temp;
	node = start_node;
	int i;
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
						next_node->dst_cnt--; // passed cas
						if (next_node->dst_cnt == 0)
						{
							nodeAllocator->free_node(nodeAllocator->nodeAddr_to_nodeMeta(next_node->data_node_addr));
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

SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next,volatile uint8_t &read_lock) // what if max
{
	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
// addr2
	KVP kvp;
	KVP* kvp_p;
	int split_cnt;
	int ex;
	volatile int* split_cnt_p;
	SkipAddr sa,next_sa;
	ex = hash_index->read(key,&kvp,&kvp_p,&split_cnt,&split_cnt_p);
	if (kvp.padding != INV0)
	{
		sa.value = kvp.padding;
		node = sa_to_node(sa);
		next_sa = node->next[0];
		next_node = sa_to_node(next_sa);
		if (node->ver == sa.ver && node->key <= key && next_node->ver == next_sa.ver && key < next_node->key)
		{
			addr2_hit++;
			return node;
		}
		else
			addr2_miss++;
	}
	else
		addr2_no++;
//addr2
	node = find_node(key,prev,next);

	//-------------------------------
	sa = node->my_sa;
	std::atomic<uint8_t>* seg_lock;
	kvp_p = hash_index->insert(key,&seg_lock,read_lock);
	kvp_p->padding = sa.value;
	hash_index->unlock_entry2(seg_lock,read_lock);
	//-------------------------------
	return node;
}

SkiplistNode* Skiplist::find_node(size_t key,SkipAddr* prev,SkipAddr* next,volatile uint8_t &read_lock, KVP &kvp) // what if max
{

	SkiplistNode* node;// = start_node;
	SkiplistNode* next_node;
	SkipAddr sa,next_sa;
	if (kvp.padding != INV0)
	{
		sa.value = kvp.padding;
		node = sa_to_node(sa);
		next_sa = node->next[0];
		next_node = sa_to_node(next_sa);
		if (node->ver == sa.ver && node->key <= key && next_node->ver == next_sa.ver && key < next_node->key)
		{
			addr2_hit++;
			return node;
		}
		else
			addr2_miss++;
	}
	else
		addr2_no++;
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
//		printf("not now\n");
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

		if (new_sa.pool_num >= 65535) // test
			printf("insert error\n");

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

//---------------------------------------------- list

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

	start_node = alloc_list_node();
	start_node->key = KEY_MIN;
	start_node->data_node_addr = nodeAllocator->alloc_node();

	end_node = alloc_list_node();
	end_node->key = KEY_MAX;
	end_node->data_node_addr = nodeAllocator->alloc_node();

	empty_node = alloc_list_node();
	empty_node->key = KEY_MIN;
	empty_node->data_node_addr = nodeAllocator->alloc_node();

//	List_Node* node = alloc_list_node();
	start_node->next = empty_node;
	empty_node->next = end_node;
	empty_node->prev = start_node;
	end_node->prev = empty_node;

	NodeMeta* nm_empty = nodeAllocator->nodeAddr_to_nodeMeta(empty_node->data_node_addr);
	NodeMeta* nm_start = nodeAllocator->nodeAddr_to_nodeMeta(start_node->data_node_addr);
	NodeMeta* nm_end = nodeAllocator->nodeAddr_to_nodeMeta(end_node->data_node_addr);

	nodeAllocator->linkNext(nm_start,nm_empty);
	nodeAllocator->linkNext(nm_empty,nm_end);
}

void PH_List::clean()
{

	int cnt=0;
	ListNode* node;
	node = start_node;
	while(node != end_node)
	{
		node = node->next;
		cnt++;
	}
	printf("cold node cnt %d\n",cnt);


//	printf("list pool cnt %ld\n",node_pool_list_cnt);
	printf("cold node %ld size %lfGB\n",node_pool_list_cnt*NODE_POOL_SIZE*MAX_NODE_GROUP+node_pool_cnt,double(node_pool_list_cnt+1)*NODE_POOL_SIZE*NODE_SIZE*MAX_NODE_GROUP/1024/1024/1024);

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
			printf("no space for node2!\n");
		++node_pool_list_cnt;
		node_pool_list[node_pool_list_cnt] = new ListNode[NODE_POOL_SIZE];//(SkiplistNode*)malloc(sizeof(SkiplistNode) * NODE_POOL_SIZE);
//		node_pool_list.push_back(new ListNode[NODE_POOL_SIZE]);
		node_pool_cnt = 0;
	}

	ListNode* node = &node_pool_list[node_pool_list_cnt][node_pool_cnt];
	node->lock = 0;
	node->next = NULL;
	node->prev = NULL;
//	node->data_node_addr = nodeAllocator->alloc_node();

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
