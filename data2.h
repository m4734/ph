#include <cstdint>
#include <atomic>
#include <vector>

#include "shared.h"
//#include "global2.h"

namespace PH
{

class SkiplistNode;

//extern size_t NODE_SLOT_MAX;

//const size_t ENTRY_HEADER_SIZE = 8;
//const size_t KEY_SIZE = 8;

/*
struct NodeOffset
{
	uint32_t file_num;
	uint32_t offset;
};
*/

/*
const size_t POOL_MAX = 1024;
const size_t POOL_SIZE = 1024*1024*1024;//1GB
*/
const size_t POOL_MAX = 64*1024;
const size_t POOL_SIZE = 64*1024*4096;
const size_t POOL_NODE_MAX = POOL_SIZE/NODE_SIZE; // 1GB / 4KB = 256K
//const size_t POOL_NODE_MAX = 64*1024;

//POOL[POOL_MAX][POOL_NODE_MAX]*4096

// may 2^16 * 2^16 = 64*1024 * 64*1024 = 2^32 == 4GB * 4KB = 16TB

//const size_t NODE_SLOT_MAX = NODE_BUFFER_SIZE/ble_len;

struct EntryLoc
{
	//volatile uint16_t valid : 1; // always rw_lock
	uint16_t valid : 1;
	uint16_t offset : 15; // 32k
};

struct NodeMeta
{
	void init_warm_el();
	void init_cold_el();
	int invalidate(EntryAddr ea);// size is regutned...
	int el_clean(int entry_size,int &bfv);
//	NodeMeta() : valid(NULL) {}
//	~NodeMeta() { delete valid; }
//	volatile uint64_t next_offset;
//	volatile NodeMeta* next_p;
	NodeAddr next_addr;
	NodeAddr next_addr_in_group;

//	/*volatile*/ NodeAddr list_addr;
	/*volatile*/ EntryAddr list_addr;

	NodeMeta* next_p;
	NodeMeta* next_node_in_group;
	int group_cnt;

//	int alloc_cnt_for_test;

//	size_t written_size;
//	size_t pool_num;
//	uint64_t 
//	NodeOffset my_offset;
//	uint64_t my_offset;
	NodeAddr my_offset;
//	NodeAddr next_offset_in_group;

//	Node* node;
//	volatile bool valid[NODE_SLOT_MAX];
//	volatile bool *valid; // must have rw lock...
//	std::vector<bool> valid;
	// vecotr uint64_t key .... but we have to read pmem to split so it will be waste of dram cap

//	int slot_cnt; // filled slot for warm
//	std::atomic<uint8_t> valid_cnt;
//	int valid_cnt;

	EntryLoc* entryLoc;
	int el_cnt;
	std::atomic<int> size_sum;
	int max_empty;
	bool need_clean;

	std::atomic<uint8_t> rw_lock;

	//test
//	int test=0;
};


uint64_t find_half_in_node(NodeMeta* nm,DataNode* node);
NodeMeta* append_group(NodeMeta* list_nodeMeta,int loc);

//void linkNext(NodeMeta* nm);
//void linkNext(NodeAddr nodeAddr);

class NodeAllocator
{
	public:
	void init();
	void clean();

//	Node* get_node(NodeMeta* nm);
	//NodeMeta* alloc_node();
	NodeAddr alloc_node(int loc);
	void free_node(NodeMeta* nm);
//	void free_node(NodeMeta* nm,SkiplistNode* sln = NULL);

	void expand(NodeAddr nodeAddr);
	uint64_t recover_node(NodeAddr nodeAddr,int loc,int &group_cnt,SkiplistNode* skiplistNode);

//	private:
	public:

	inline DataNode* nodeAddr_to_node(NodeAddr &nodeAddr)
	{
		return (DataNode*)(nodePoolList[nodeAddr.pool_num] + nodeAddr.node_offset*sizeof(DataNode));
	}
	/*
	inline DataNode* nodeAddr_to_node(uint64_t value)
	{
		NodeAddr nodeAddr;
		nodeAddr.value = value;
		return (DataNode*)(nodePoolList[nodeAddr.pool_num] + nodeAddr.node_offset*sizeof(DataNode));
	}
	*/

	inline NodeMeta* nodeAddr_to_nodeMeta(NodeAddr &nodeAddr)
	{
		return (NodeMeta*)(nodeMetaPoolList[nodeAddr.pool_num] + nodeAddr.node_offset*sizeof(NodeMeta));
	}
	/*
	inline NodeMeta* nodeAddr_to_nodeMeta(uint64_t value)
	{
		NodeAddr nodeAddr;
		nodeAddr.value = value;
		return (NodeMeta*)(nodeMetaPoolList[nodeAddr.pool_num] + nodeAddr.node_offset*sizeof(NodeMeta));
	}
	*/


	void linkNext(NodeMeta* nm1,NodeMeta* nm2);
//	void linkNext(NodeAddr nodeAddr);

	void alloc_pool(bool fill = false);
	void collect_free_node(); // only for recovdery

	unsigned char** nodeMetaPoolList;
	unsigned char** nodePoolList;
//	NodeMeta** nodeMetaPoolList;
//	Node** nodePoolList;


	//volatile?? lock
	int pool_cnt;
	int* node_cnt;

//	NodeMeta* start_node;
//	NodeMeta* end_node;

//	size_t free_head;
//	size_t free_tail;
//	volatile NodeMeta* free_head_p=NULL;
	NodeMeta* /*volatile*/ free_head_p;

	
	std::atomic<uint8_t> lock=0;

//	size_t alloc_cnt;
	std::atomic<uint64_t> alloc_cnt;
};

//extern NodeAllocator* nodeAllocator;
//extern NodeAllocator* 


}
