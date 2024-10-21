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

class ListNode
{
	public:
	ListNode() : key(0), next(NULL), prev(NULL), lock(0),block_cnt(0) {};

	size_t key;
	ListNode* volatile next; // pointer should be voaltile
	ListNode* volatile prev;

	NodeAddr myAddr;

//	NodeMeta* my_node;
	/*volatile*/ NodeAddr warm_cache;

	NodeAddr data_node_addr; // changed by split // in lock
//	std::atomic<NodeAddr> data_node_addr;
//	/*volatile*/ uint64_t data_node_addr;

	int block_cnt;
	int hold;

//	std::atomic<uint8_t> valid_cnt; // 256 ...
	std::atomic<uint64_t> size_sum;
	std::atomic<uint8_t> lock;
};

class PH_List
{
	public:

	ListNode* empty_node;
	ListNode* start_node;
	ListNode* end_node;

	ListNode** node_pool_list;
//	std::vector<ListNode*> node_pool_list;
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

	void recover();
	void recover_init();
/*
	inline ListNode* addr_to_listNode(NodeAddr &list_addr) // have to be loc 3
	{
		return (ListNode*)&node_pool_list[list_addr.pool_num][list_addr.node_offset];
	}
	*/


	inline ListNode* addr_to_listNode(EntryAddr &list_addr) // have to be loc 3
	{
		return (ListNode*)&node_pool_list[list_addr.file_num][list_addr.offset];
	}
	

};

bool try_reduce_group(ListNode* listNode);
bool try_merge_listNode(ListNode* left_listNode,ListNode* right_listNode);

//struct SkiplistNode


class SkiplistNode
{
	public:
	SkiplistNode() :next(NULL),next_size(0) {}
	~SkiplistNode() { delete[] next; }

	void remove_key_from_list(uint64_t key);
	void find_half_listNode();
	void update_wc();

	size_t key;
//	SkiplistNode* node_p; // tree node or leaf
//	std::vector<std::atomic<SkiplistNode*>> next;
//	std::vector<AtomicPointer> next;
//	std::atomic<SkiplistNode*> *next = NULL;
//	std::vector<SkipAddr> next; // don't want MAX LEVEL space
	SkipAddr *next;
	int next_size;

	std::vector<uint64_t> key_list;
	/*volatile*/ int key_list_size; 

	std::vector<LogLoc> entry_list;
//	std::queue<LogLoc> entry_list;
	/*
	LogLoc torn_entry;
	size_t torn_left=0;
	size_t torn_right=0;
	*/
//	size_t entry_size_sum=0;

	volatile uint32_t ver;
	std::atomic<int> dst_cnt;

	int level;
	int built;
	std::atomic<ListNode*> my_listNode;
//	NodeMeta* my_node;
	NodeAddr myAddr; // nodeMeta addr // skiplist addr?
//	NodeAddr prev;
	SkiplistNode* volatile prev;
	NodeAddr data_node_addr[WARM_MAX_NODE_GROUP];
//	/*volatile*/ uint64_t data_node_addr[WARM_MAX_NODE_GROUP];

	std::atomic<uint8_t> lock;
//	std::atomic<uint8_t> rw_lock; // ???

	void setLevel();
	void setLevel(size_t l);
//	void free();

	int list_head,list_tail;
	int list_size_sum;
	int current_batch_size;
	int current_batch_index;

	int data_head,data_tail;
//	int remain_cnt;
	int recent_entry_cnt;

//	NodeAddr dataNodeHeader;
	SkipAddr my_sa;

	int cold_block_sum;
	ListNode* half_listNode;

//	unsigned char* group_node_p[WARM_MAX_NODE_GROUP];
//	NodeMeta* nodeMeta_p[WARM_MAX_NODE_GROUP];

//	inline unsigned char* get_entry(int index);
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

//	bool need_reduce(ListNode* listNode);
#if 1
	inline bool need_reduce(ListNode* listNode)
	{
//		return (listNode->valid_cnt + NODE_SLOT_MAX*2 < listNode->block_cnt * NODE_SLOT_MAX); // try shorten group
//		return (((listNode->valid_cnt-1) / NODE_SLOT_MAX + 1)+1 < listNode->block_cnt); // try shorten group
		return false;

	}
#endif


//void test_before_free(ListNode* listNode);

}
