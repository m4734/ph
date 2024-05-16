#include<vector>

namespace PH
{


//struct Skiplist_Node
class Skiplist_Node
{
	public:
	size_t key;
//	Skiplist_Node* node_p; // tree node or leaf
	std::vector<Skiplist_Node*> next;
	size_t level;
//	Tree_Node* next;
};

class Skiplist
{
	private:
	Skiplist_Node* start_node;
	Skiplist_Node* end_node;

	Skiplist_Node** node_pool_list;
	size_t node_pool_cnt;
	size_t node_pool_list_cnt;

	std::atomic<uint8_t> node_alloc_lock; // lock?
	Skiplist_Node* node_free_head;

	public:
	void init();
	void clean();
	
	Skiplist_Node* alloc_sl_node();
	void free_sl_node(Skiplist_Node* node);

};


}
