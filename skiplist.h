#pragma once

#include<vector>
#include<atomic>
//#include<queue>

#include "shared.h"

namespace PH
{

//extern size_t NODE_SLOT_MAX;

struct KVP;
//const size_t MAX_LEVEL = 30; // 2^30 = 1G entry?

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

const uint16_t INV_LOG = 65535;

struct LogLoc
{
	uint16_t log_num;
	uint16_t size;
	size_t offset;
};

//struct SkiplistNode

class SkiplistNode
{
	public:
	SkiplistNode() :next(NULL),next_size(0) {}
	~SkiplistNode() 
	{
		 delete next;
	}

	void remove_key_from_list(uint64_t key);
//	void find_half_listNode();
	void update_wc();

	uint64_t key;
//	volatile uint64_t key;
//	SkiplistNode* node_p; // tree node or leaf
//	std::vector<std::atomic<SkiplistNode*>> next;
//	std::vector<AtomicPointer> next;
//	std::atomic<SkiplistNode*> *next = NULL;
//	std::vector<SkipAddr> next; // don't want MAX LEVEL space
	SkipAddr *next; // may need atomic
//	volatile uint64_t *next;
//	std::atomic<uint64_t> *next;

	int next_size;

	volatile uint32_t ver;
	std::atomic<int> dst_cnt;

	int level;
	int built;
//	std::atomic<ListNode*> my_listNode;
//	ListNode* my_listNode;

	NodeAddr myAddr; // nodeMeta addr // skiplist addr?
//	NodeAddr prev;
	SkiplistNode* volatile prev;
	int data_node_cnt;
	NodeAddr data_node_addr[WARM_MAX_NODE_GROUP];

	//here it protectd by skiplist lock...
	/*
	std::atomic<int> dtc_batch_num;
	std::atomic<int> dtc_batch_size;
	*/
//	volatile int dtc_batch_num;
//	volatile int dtc_batch_size;
	int dtc_batch_num;
	int dtc_batch_size;

//	/*volatile*/ uint64_t data_node_addr[WARM_MAX_NODE_GROUP];

//	std::atomic<uint8_t> key_list_lock; // hot key list
	std::atomic<uint8_t> insert_lock; // HL TO WL and evict list
//	std::atomic<uint8_t> evict_lock; // WL TO CL
	std::atomic<uint8_t> split_lock; // WN split
//	std::atomic<uint8_t> thread_counter; // split prevent

	void setLevel();
	void setLevel(size_t l);
//	void free();

	//--------------------------entry in log

	std::vector<uint64_t> key_list;
//	/*volatile*/ int key_list_size; 

	std::vector<LogLoc> entry_list;
//	std::queue<LogLoc> entry_list;
	/*
	LogLoc torn_entry;
	size_t torn_left=0;
	size_t torn_right=0;
	*/
//	size_t entry_size_sum=0;

	int list_head,list_tail;
	int list_size_sum;
//	int current_batch_size;
//	int current_batch_index;

//	int data_head,data_tail; // was linar log
//	int remain_cnt;
	int recent_entry_cnt;

//----------------------------------------------------

//	NodeAddr dataNodeHeader;
	SkipAddr my_sa;

//	unsigned char* group_node_p[WARM_MAX_NODE_GROUP];
//	NodeMeta* nodeMeta_p[WARM_MAX_NODE_GROUP];

//	inline unsigned char* get_entry(int index);
	int target_batch;
	int empty_batch;
};

class Skiplist
{
	private:
	public: // test
	SkiplistNode** node_pool_list;
	private:
//	std::vector<SkiplistNode*> node_pool_list;

	std::atomic<uint8_t> node_alloc_lock; // lock?
	SkiplistNode* node_free_head;

	std::atomic<uint64_t> node_counter;

	size_t SKIPLIST_NODE_POOL_LIMIT;


	public:
	void init(size_t size);
	void clean();

	void recover();
	void recover_init(size_t size);

	void setLimit(size_t size);
	
	SkiplistNode* alloc_sl_node();
	void free_sl_node(SkiplistNode* node);

	SkiplistNode* find_node(size_t key,SkipAddr* prev,SkipAddr* next);
//	SkiplistNode* find_node(size_t key,SkipAddr* prev,SkipAddr* next,volatile uint8_t &read_lock);
//	SkiplistNode* find_node(size_t key,SkipAddr* prev,SkipAddr* next,volatile uint8_t &read_lock,KVP &kvp);
	SkiplistNode* find_node(size_t key,SkipAddr* prev,SkipAddr* next,NodeAddr &warm_cache);
	SkiplistNode* find_next_node(SkiplistNode* start_node); // what if max
//	SkiplistNode* find_node_and_lock(size_t key, SkiplAddr* prev, SkipAddr* next);
//	SkiplistNode* find_node_and_lock(size_t key, SkiplAddr* prev, SkipAddr* next,NodeAddr &warm_cache);


//	bool delete_node_with_fail(SkiplistNode* node);//, SkipAddr** prev,SkipAddr** next);
	void delete_node(SkiplistNode* node);//, SkipAddr** prev,SkipAddr** next);
	bool insert_node_with_fail(SkiplistNode* node, SkipAddr* prev,SkipAddr* next);
	void insert_node(SkiplistNode* node, SkipAddr* prev,SkipAddr* next);

	inline SkiplistNode* sa_to_node(SkipAddr *sa)
	{
		return &node_pool_list[sa->pool_num][sa->offset];
	}
	inline SkiplistNode* sa_to_node(SkipAddr &sa)
	{
		return &node_pool_list[sa.pool_num][sa.offset];
	}

	SkiplistNode* allocate_node();

	size_t node_pool_cnt;
	size_t node_pool_list_cnt;

	SkiplistNode* empty_node;
	SkiplistNode* start_node;
	SkiplistNode* end_node;

	void traverse_test();
};

}
