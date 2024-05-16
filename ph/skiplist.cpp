
#include <atomic>
#include <stdlib.h> // rand
#include <stdio.h>

#include "skiplist.h"
#include "lock.h"

namespace PH
{

const size_t NODE_POOL_LIST_SIZE = 1024;
const size_t NODE_POOL_SIZE = 1024*1024;

const size_t KEY_MIN = 0x0000000000000000;
const size_t KEY_MAX = 0xffffffffffffffff;

const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?


size_t getRandomLevel()
{
	return rand() % MAX_LEVEL;
}

void Skiplist::init()
{
//	node_pool_list = (Tree_Node**)malloc(sizeof(Skiplist_Node*) * NODE_POOL_LIST_SIZE);
	node_pool_list = new Skiplist_Node*[NODE_POOL_LIST_SIZE];

//	node_pool_list[0] = (Tree_Node*)malloc(sizeof(Skiplist_Node) * NODE_POOL_SIZE);
	node_pool_list[0] = new Skiplist_Node[NODE_POOL_SIZE];
	node_pool_cnt=0;
	node_pool_list_cnt = 0;
//	node_free_head = NULL;

//	node_alloc_lock = 0;

	int i;

	start_node = alloc_sl_node();
	start_node->level = MAX_LEVEL;
	start_node->key = KEY_MIN;

	end_node = alloc_sl_node();
	end_node->level = MAX_LEVEL;
	end_node->key = KEY_MAX;

	for (i=0;i<MAX_LEVEL;i++)
		start_node->next[i] = end_node;


}

void Skiplist::clean()
{
	int i;
	for (i=0;i<=node_pool_list_cnt;i++)
	{
//		free(node_pool_list[i]);
		delete node_pool_list[i];
	}
//	free(node_pool_list);
	delete node_pool_list;
}


Skiplist_Node* Skiplist::alloc_sl_node()
{
	//just use lock
	while(node_alloc_lock);
	at_lock2(node_alloc_lock);

	if (node_free_head)
	{
		Skiplist_Node* rv = node_free_head;
		node_free_head = node_free_head->next[0];
		at_unlock2(node_alloc_lock);
		return rv;
	}
	

	if (node_pool_cnt >= NODE_POOL_SIZE)
	{
		if (node_pool_list_cnt >= NODE_POOL_LIST_SIZE)
			printf("no space for node!\n");
		++node_pool_list_cnt;
		node_pool_list[node_pool_list_cnt] = new Skiplist_Node[NODE_POOL_SIZE];//(Skiplist_Node*)malloc(sizeof(Skiplist_Node) * NODE_POOL_SIZE);
	}

//	if (node_pool_cnt < NODE_POOL_SIZE)
	{
		at_unlock2(node_alloc_lock);
		return &node_pool_list[node_pool_list_cnt][node_pool_cnt++];
	}
}

void Skiplist::free_sl_node(Skiplist_Node* node)
{
	at_lock2(node_alloc_lock);
	node->next[0] = node_free_head;
	node_free_head = node;
	at_unlock2(node_alloc_lock);
}

}
