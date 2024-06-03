#include<vector>
#include<atomic>

#include "shared.h"

namespace PH
{

const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?

//class NodeMeta;
//struct NodeAddr;

#if 0
class SkiplistNode;
class AtomicPointer
{
	public:
	AtomicPointer() {};
//	AtomicPointer() = default;
//	AtomicPointer(AtomicPointer* ap) {snp = ap->snp;}
	AtomicPointer(AtomicPointer &ap) {snp = ap.snp.load();}
//	template<typename T> AtomicPointer(T const&) {}
//	AtomicPointer& AtomicPointer::operator=(const AtomicPointer &rhs) {snp = rhs.snp; return *this;} 

	std::atomic<SkiplistNode*> snp=NULL;
};
#endif

struct LogLoc
{
	int log_num;
	size_t offset;
/*
	//test
	uint64_t test_key1;
	uint64_t test_key2;
	void* test_ptr;
*/	
};


class ListNode
{
	public:
	ListNode() : key(0), next(NULL), prev(NULL), lock(0) {};
	size_t key;
	ListNode* next;
	ListNode* prev;

//	NodeMeta* my_node;
	NodeAddr data_node_addr;

	std::atomic<uint8_t> lock;
};

class PH_List
{
	public:

	ListNode* empty_node;
	ListNode* start_node;
	ListNode* end_node;

	ListNode** node_pool_list;
	size_t node_pool_cnt;
	size_t node_pool_list_cnt;

	std::atomic<uint8_t> node_alloc_lock;
	ListNode* node_free_head;

	void init();
	void clean();

	ListNode* alloc_list_node();
	void free_list_node(ListNode* node);

	ListNode* find_node(size_t key,ListNode* node);
	void insert_node(ListNode* prev,ListNode* node);
	void delete_node(ListNode* node);

};


//struct SkiplistNode
class SkiplistNode
{
	public:
	~SkiplistNode() { delete next; }
	size_t key;
//	SkiplistNode* node_p; // tree node or leaf
//	std::vector<std::atomic<SkiplistNode*>> next;
//	std::vector<AtomicPointer> next;
	std::atomic<SkiplistNode*> *next = NULL;

	std::vector<LogLoc> entry_list;
	LogLoc torn_entry;
	size_t torn_left=0;
	size_t torn_right=0;
	size_t entry_size_sum=0;

	size_t level;
	size_t built;
	ListNode* my_listNode;
//	NodeMeta* my_node;
	NodeAddr data_node_addr;

//	Tree_Node* next;
	std::atomic<uint8_t> lock;
	std::atomic<uint8_t> delete_lock;

	void setLevel();
	void setLevel(size_t l);
};

class Skiplist
{
	private:
	SkiplistNode* empty_node;
	SkiplistNode* start_node;
	SkiplistNode* end_node;

	SkiplistNode** node_pool_list;
	size_t node_pool_cnt;
	size_t node_pool_list_cnt;

	std::atomic<uint8_t> node_alloc_lock; // lock?
	SkiplistNode* node_free_head;

	public:
	void init();
	void clean();
	
	SkiplistNode* alloc_sl_node();
	void free_sl_node(SkiplistNode* node);

	SkiplistNode* find_node(size_t key,SkiplistNode** prev,SkiplistNode** next);
	bool delete_node_with_fail(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next);
	void delete_node(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next);
	bool insert_node_with_fail(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next);
	void insert_node(SkiplistNode* node, SkiplistNode** prev,SkiplistNode** next);


//	void split(
};

}
