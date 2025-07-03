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
//	std::atomic<uint64_t> size_sum;
//	int size_sum;
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

const uint32_t FBB_SIZE = 4096;
const uint32_t BB_SIZE = 256;
const uint32_t BB_PER_FBB = FBB_SIZE/BB_SIZE;
const uint32_t FBB_MAX = 1024*1024; // 4GB / 4096 = 1M


// we do not have exit implementaiton we need free?

class FBB
{
	FBB() : FBBA(NULL),FBBAi(0) {}
	~FBB()
	{
		int i;
		for (i=0;i<used_count;i+=BB_PER_FBB)
			free(FBBA[i]);
		free(FBBA);
	}

	unsigned char** FBBA; //flexible batch buffer // need memalign
	thread_local int th_free_head;

	thread_local int th_used_count;
	thread_local int th_FBBAi;

	std::atomic<int> FBBAi;

	public:

	// BB = 256
	// FBB = 1MB...
	// FBBA < 1TB
	// FBBA / FBB = 1M

	void init()
	{
		FBBA = (unsigned char*)malloc(sizeof(unsigned char*) * FBB_MAX);
		FBBAi = 0;
	}

	void local_init()
	{
		th_free_head = -1;
		th_used_count = BB_PER_FBB;
		th_FBBAi = 0;
	}

	int alloc_buffer()
	{
		if (th_free_head >= 0)
		{
			int rv;
			unsigned char* bb;
			rv = th_free_head;
			bb = get_buffer(rv);
			th_free_head = *(int*)bb;
			return rv;
		}
		if (used_count >= BB_PER_FBB)
		{
			th_FBBAi = FBBAi.fetch_add(1);
			if (th_FBBAi >= FBB_MAX) // 256GB / BB_SIZE(256B) == 1G?
			{
				printf("fbb alloc buffer overflow\n");
				return -1;
			}
			posix_memalign(FBBA[th_FBBAi],FBB_SIZE,FBB_SIZE); // 4096 aligb
			th_used_count = 0;
		}
		return th_used_count++;
	}
	
	void free_buffer(int index)
	{
		unsigned char* bb = get_FBB(index);
		*(int*)bb = th_free_head;
		th_free_head = index;
	}
	
	unsigned char* get_FBB(int index) // actually its get BB
	{
		return &FBBA[index/BB_PER_FBB][(index%BB_PER_FBB)*BB_SIZE];
	}
};

FBB global_fbb;

bool try_reduce_group(ListNode* listNode);
bool try_merge_listNode(ListNode* left_listNode,ListNode* right_listNode);

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
	void find_half_listNode();
	void update_wc();

//	void update_cold_node(ListNode* cold_node);
//	void insert_cold_node(ListNode* cold_node);

	uint64_t key;
//	volatile uint64_t key;
//	SkiplistNode* node_p; // tree node or leaf
//	std::vector<std::atomic<SkiplistNode*>> next;
//	std::vector<AtomicPointer> next;
//	std::atomic<SkiplistNode*> *next = NULL;
//	std::vector<SkipAddr> next; // don't want MAX LEVEL space
	SkipAddr *next;
//	volatile uint64_t *next;
//	std::atomic<uint64_t> *next;

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
//	std::atomic<ListNode*> my_listNode;
//	ListNode* my_listNode;

	NodeAddr myAddr; // nodeMeta addr // skiplist addr?
//	NodeAddr prev;
	SkiplistNode* volatile prev;
	NodeAddr data_node_addr[WARM_MAX_NODE_GROUP];
//	/*volatile*/ uint64_t data_node_addr[WARM_MAX_NODE_GROUP];

//	std::atomic<uint8_t> key_list_lock; // hot key list
	std::atomic<uint8_t> insert_lock; // HL TO WL and evict list
	std::atomic<uint8_t> evict_lock; // WL TO CL
	std::atomic<uint8_t> split_lock; // WN split
//	std::atomic<uint8_t> thread_counter; // split prevent

	void setLevel();
	void setLevel(size_t l);
//	void free();

	int list_head,list_tail;
	int list_size_sum;
//	int current_batch_size;
//	int current_batch_index;

	int data_head,data_tail;
//	int remain_cnt;
	int recent_entry_cnt;

//	NodeAddr dataNodeHeader;
	SkipAddr my_sa;

	struct FBB_INFO
	{
		int fbb_start; // start fbb index
		int remain; // remiain size in last fbb
		int fbb_cnt; // lenght of the list
		int fbb_end; // last fbb index

		int fbb_next_list[FBB_SIZE/BB_SIZE]; // index to next fbb
		int fbb_filled_cnt[FBB_SIZE/BB_SIZE]; // size of data in fbb
	}

	FBB_INFO fbb_info;

	int alloc_new_fbb()
	{
		int new_buffer_index = global_fbb.alloc_buffer();
		if (new_buffer_index >= 0)
		{
		fbb_info.remain = FBB_SIZE;
		fbb_info.fbb_end = new_buffer_index;
		fbb_info.fbb_cnt++;
		if (fbb_info.fbb_cnt == 1)
			fbb_info.fbb_start = new_buffer_index;
		}
		return new_buffer_index;
	}

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
