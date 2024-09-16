#include <cstdint>
#include <atomic>
#include <vector>

#include "shared.h"
//#include "global2.h"

namespace PH
{

extern size_t NODE_SLOT_MAX;

/*
#define VERSION_SIZE 8
#define KEY_SIZE 8
#define VALUE_SIZE 100

#define VER_DELETE (size_t(1)<<62)
#define VER_CL_LOC1 (size_t(1)<<61)
#define VER_CL_LOC2 (size_t(1)<<60)
#define VER_PL_LOC1 (size_t(1)<<59)
#define VER_PL_LOC2 (size_t(1)<<58)
*/

const size_t ENTRY_HEADER_SIZE = 8;
const size_t KEY_SIZE = 8;
//const size_t VALUE_SIZE = 100;
//const size_t ENTRY_SIZE = HEADER_SIZE + KEY_SIZE + VALUE_SIZE;

union EntryHeader
{
	struct
	{
//		size_t prev_loc : 2;
		size_t valid : 1;
		size_t version : 63;
	};
	uint64_t value;
};

const size_t VER_DRAM_VALID = (size_t(1)<<63);
const size_t VER_DELETE = (size_t(1)<<62);
const size_t VER_CL_LOC1 = (size_t(1)<<61);
const size_t VER_CL_LOC2 = (size_t(1)<<60);
const size_t VER_PL_LOC1 = (size_t(1)<<59);
const size_t VER_PL_LOC2 = (size_t(1)<<58);
const size_t VER_CHECKED = (size_t(1)<<57);

const size_t VER_LOC_MASK = (VER_CL_LOC1+VER_CL_LOC2+VER_PL_LOC1+VER_PL_LOC2);
const size_t VER_LOC_INV_MASK = ~VER_LOC_MASK;
const size_t VER_CL_MASK = (VER_CL_LOC1+VER_CL_LOC2);

const size_t VER_NUM_MASK = (size_t(1)<<57)-1;

/*
inline unsigned char* value_to_node_addr(uint64_t value)
{
	return nodePoolList[(value & VALUE_SECOND_MASK) >> VALUE_SECOND_SHIFT] + value & VALUE_THIRD_MASK;
}


inline size_t get_ver_num(uint64_t version)
{
	return version & VER_NUM_MASK;
}
*/




// 0 0
// 0 1 hot
// 1 0 warm
// 1 1 cold
//const size_t VER_CL_HOT = VER_CL_LOC2;
#if 0
bool inline is_checked(uint64_t version)
{
	return version & VER_CHECKED;
}
void inline set_checked(uint64_t* version)
{
	*version |= VER_CHECKED;
}

bool inline is_loc_hot(uint64_t version)
{
	return (((version & VER_CL_LOC1) == 0) && (version & VER_CL_LOC2));
}
#if 0
void inline remove_loc_mask(uint64_t &version)
{
	version &= VER_LOC_INV_MASK;
}
#endif
uint64_t inline set_loc_hot(uint64_t version)
{
	version &= VER_LOC_INV_MASK;
	version |= VER_CL_LOC2;
	return version;
}
uint64_t inline set_loc_cold(uint64_t version)
{
	version &= VER_LOC_INV_MASK;
	version |= VER_CL_LOC1 | VER_CL_LOC2;
	return version;
}
uint64_t inline set_loc_warm(uint64_t version)
{
	version &= VER_LOC_INV_MASK;
	version |= VER_CL_LOC1;
	return version;
}



void inline set_prev_loc(uint64_t &dst_version,uint64_t &src_version)
{
	dst_version |= ((src_version&VER_CL_MASK) >> 2);
}
void inline set_valid(uint64_t &version)
{
	version |= VER_DRAM_VALID;
}
void inline set_invalid(uint64_t &version)
{
	version &= (~VER_DRAM_VALID);
}
void inline set_invalid(uint64_t *version)
{
	*version &= (~VER_DRAM_VALID);
}
bool inline is_valid(uint64_t &version)
{
	return version & VER_DRAM_VALID;
}
bool inline is_valid(uint64_t *version)
{
	return *version & VER_DRAM_VALID;
}
#endif

void inline set_invalid(EntryHeader &h)
{
	h.valid = false;
}
void inline set_invalid(EntryHeader *h)
{
	h->valid = false;
}

void inline set_valid(EntryHeader &h)
{
	h.valid = true;
}
void inline set_valid(EntryHeader *h)
{
	h->valid = true;
}

bool inline is_valid(EntryHeader &h)
{
	return h.valid;
}
bool inline is_valid(EntryHeader *h)
{
	return h->valid;
}



#if 0
struct BaseLogEntry
{
	uint64_t header; // delete + version
	uint64_t key; // key
	unsigned char value[VALUE_SIZE];
	unsigned char pad[4];
}; // 8 + 8 + 100 = 116 + 4 = 120
#endif
// need 8bytes align

//const size_t ble_len = sizeof(BaseLogEntry);
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
//const size_t POOL_NODE_MAX = POOL_SIZE/NODE_SIZE; // 1GB / 4KB = 256K
const size_t POOL_NODE_MAX = 64*1024;

//POOL[POOL_MAX][POOL_NODE_MAX]*4096

// may 2^16 * 2^16 = 64*1024 * 64*1024 = 2^32 == 4GB * 4KB = 16TB

//const size_t NODE_SLOT_MAX = NODE_BUFFER_SIZE/ble_len;

struct NodeMeta
{
//	NodeMeta() : valid(NULL) {}
//	~NodeMeta() { delete valid; }
//	volatile uint64_t next_offset;
//	volatile NodeMeta* next_p;

	NodeAddr list_addr;

	NodeAddr next_addr;
	NodeAddr next_addr_in_group;

	NodeMeta* next_p;
	NodeMeta* next_node_in_group;
	int group_cnt;

//	size_t written_size;
//	size_t pool_num;
//	uint64_t 
//	NodeOffset my_offset;
//	uint64_t my_offset;
	NodeAddr my_offset;
//	NodeAddr next_offset_in_group;

//	Node* node;
//	volatile bool valid[NODE_SLOT_MAX];
	volatile bool *valid; // must have rw lock...
//	std::vector<bool> valid;
	// vecotr uint64_t key .... but we have to read pmem to split so it will be waste of dram cap

//	int slot_cnt; // filled slot for warm
	std::atomic<uint8_t> valid_cnt;
//	int valid_cnt;

	std::atomic<uint8_t> rw_lock;

	//test
//	int test=0;
};


uint64_t find_half_in_node(NodeMeta* nm,DataNode* node);
NodeMeta* append_group(NodeMeta* list_nodeMeta);

//void linkNext(NodeMeta* nm);
//void linkNext(NodeAddr nodeAddr);

class NodeAllocator
{
	public:
	void init();
	void clean();

//	Node* get_node(NodeMeta* nm);
	//NodeMeta* alloc_node();
	NodeAddr alloc_node();
	void free_node(NodeMeta* nm);

	void check_expand(NodeAddr nodeAddr);

//	private:
	public:

	inline DataNode* nodeAddr_to_node(NodeAddr nodeAddr)
	{
		return (DataNode*)(nodePoolList[nodeAddr.pool_num] + nodeAddr.node_offset*sizeof(DataNode));
	}
	inline NodeMeta* nodeAddr_to_nodeMeta(NodeAddr nodeAddr)
	{
		return (NodeMeta*)(nodeMetaPoolList[nodeAddr.pool_num] + nodeAddr.node_offset*sizeof(NodeMeta));
	}

	void linkNext(NodeMeta* nm1,NodeMeta* nm2);
//	void linkNext(NodeAddr nodeAddr);

	void alloc_pool();

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
	NodeMeta* free_head_p;

	
	std::atomic<uint8_t> lock=0;

//	size_t alloc_cnt;
	std::atomic<uint64_t> alloc_cnt;
};

//extern NodeAllocator* nodeAllocator;
//extern NodeAllocator* 


}
