#include<vector>
#include<atomic>

#include "addr.h"

namespace PH
{

const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?

//class NodeMeta;
//struct NodeAddr;

#if 0
class Skiplist_Node;
class AtomicPointer
{
	public:
	AtomicPointer() {};
//	AtomicPointer() = default;
//	AtomicPointer(AtomicPointer* ap) {snp = ap->snp;}
	AtomicPointer(AtomicPointer &ap) {snp = ap.snp.load();}
//	template<typename T> AtomicPointer(T const&) {}
//	AtomicPointer& AtomicPointer::operator=(const AtomicPointer &rhs) {snp = rhs.snp; return *this;} 

	std::atomic<Skiplist_Node*> snp=NULL;
};
#endif

struct LogLoc
{
	int log_num;
	size_t offset;
};


class List_Node
{
	public:
	List_Node() : key(0), next(NULL), prev(NULL), lock(0) {};
	size_t key;
	List_Node* next;
	List_Node* prev;

//	NodeMeta* my_node;
	NodeAddr data_node_addr;

	std::atomic<uint8_t> lock;
};

class PH_List
{
	public:

	List_Node* empty_node;
	List_Node* start_node;
	List_Node* end_node;

	List_Node** node_pool_list;
	size_t node_pool_cnt;
	size_t node_pool_list_cnt;

	std::atomic<uint8_t> node_alloc_lock;
	List_Node* node_free_head;

	void init();
	void clean();

	List_Node* alloc_list_node();
	void free_list_node(List_Node* node);

	List_Node* find_node(size_t key,List_Node* node);
	void insert_node(List_Node* prev,List_Node* node);
	void delete_node(List_Node* node);

};


//struct Skiplist_Node
class Skiplist_Node
{
	public:
	~Skiplist_Node() { delete next; }
	size_t key;
//	Skiplist_Node* node_p; // tree node or leaf
//	std::vector<std::atomic<Skiplist_Node*>> next;
//	std::vector<AtomicPointer> next;
	std::atomic<Skiplist_Node*> *next = NULL;

	std::vector<LogLoc> entry_list;
	LogLoc torn_entry;
	size_t torn_left=0;
	size_t torn_right=0;

	size_t level;
	size_t built;
	List_Node* list_node;
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
	Skiplist_Node* empty_node;
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

	Skiplist_Node* find_node(size_t key,Skiplist_Node** prev,Skiplist_Node** next);
	bool delete_node_with_fail(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);
	void delete_node(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);
	bool insert_node_with_fail(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);
	void insert_node(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);


//	void split(
};

}
