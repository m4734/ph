#include<vector>
#include<atomic>

namespace PH
{

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

	size_t level;
	size_t built;
//	Tree_Node* next;
	std::atomic<uint8_t> lock;
	std::atomic<uint8_t> delete_lock;

	void setLevel();
	void setLevel(size_t l);
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

	Skiplist_Node* find_node(size_t key,Skiplist_Node** prev,Skiplist_Node** next);
	bool delete_node_with_fail(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);
	void delete_node(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);
	bool insert_node_with_fail(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);
	void insert_node(Skiplist_Node* node, Skiplist_Node** prev,Skiplist_Node** next);

//	void split(
};


}
