#include "tree.h"

#include <atomic>

const size_t NODE_POOL_LIST_SIZE = 1024;
const size_t NODE_POOL_SIZE = 1024*1024;

const size_t KEY_MIN 0x0000000000000000
const size_t KEY_MAX 0xffffffffffffffff

Tree_Node* root;

Tree_Node** node_pool_list;
size_t node_pool_cnt;
size_t node_pool_list_cnt;

Tree_node* node_free_head;

std::atomic<uint8_t> node_alloc_lock;

void tree_init()
{
	node_pool_list = (Tree_Node**)malloc(sizeof(Tree_Node*) * NODE_POOL_LIST_SIZE);

	node_pool_list[0] = (Tree_Node*)malloc(sizeof(Tree_Node) * NODE_POOL_SIZE);
	node_pool_cnt=0;
	node_pool_list_cnt = 0;
	node_free_head = NULL;

	node_alloc_lock = 0;

	root = alloc_tree_node();
	root->len = 0;
	root->node_p[0] = 

}

void tree_clean()
{
	int i;
	for (i=0;i<=node_pool_list_cnt;i++)
	{
		free(node_pool_list[i]);
	}
	free(node_pool_list);
}


Tree_Node* alloc_tree_node()
{
	//just use lock
	while(node_alloc_lock);
	at_lock2(node_alloc_lock);

	if (node_free_head)
	{
		Tree_Node* rv = node_free_head;
		node_free_head = node_free_head->next;
		at_unlock2(node_alloc_lock);
		return rv;
	}
	

	if (node_pool_cnt >= NODE_POOL_SIZE)
	{
		if (node_pool_list_cnt >= NODE_POOL_LIST_SIZE)
			printf("no space for node!\n");
		++node_pool_list_cnt;
		node_pool_list[node_pool_list_cnt] = (Tree_Node*)malloc(sizeof(Tree_Node) * NODE_POOL_SIZE);
	}

//	if (node_pool_cnt < NODE_POOL_SIZE)
	{
		at_unlock2(node_alloc_lock);
		return node_pool_list[node_pool_list_cnt][node_pool_cnt++];
	}
}

void free_tree_node(Tree_Node* node)
{
	at_lock2(node_alloc_lock);
	node->next = node_free_head;
	node_free_head = node;
	at_unlock2(node_alloc_lock);
}
