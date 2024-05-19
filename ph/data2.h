#include <cstdint>
#include <atomic>

namespace PH
{

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

const size_t VERSION_SIZE = 8;
const size_t KEY_SIZE = 8;
const size_t VALUE_SIZE = 100;
const size_t ENTRY_SIZE = VERSION_SIZE + KEY_SIZE + VALUE_SIZE;

const size_t VER_DRAM_VALID = (size_t(1)<<63);
const size_t VER_DELETE = (size_t(1)<<62);
const size_t VER_CL_LOC1 = (size_t(1)<<61);
const size_t VER_CL_LOC2 = (size_t(1)<<60);
const size_t VER_PL_LOC1 = (size_t(1)<<59);
const size_t VER_PL_LOC2 = (size_t(1)<<58);

const size_t VER_LOC_MASK = (VER_CL_LOC1+VER_CL_LOC2+VER_PL_LOC1+VER_PL_LOC2);
const size_t VER_LOC_INV_MASK = ~VER_LOC_MASK;
const size_t VER_CL_MASK = (VER_CL_LOC1+VER_CL_LOC2);

// 0 0
// 0 1 hot
// 1 0 warm
// 1 1 cold
//const size_t VER_CL_HOT = VER_CL_LOC2;

bool inline is_loc_hot(uint64_t version)
{
	return (((version & VER_CL_LOC1) == 0) && (version & VER_CL_LOC2));
}
void inline remove_loc_mask(uint64_t &version)
{
	version &= VER_LOC_INV_MASK;
}
void inline set_loc_hot(uint64_t &version)
{
	version |= VER_CL_LOC2;
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


struct BaseLogEntry
{
	uint64_t header; // delete + version
	uint64_t key; // key
	unsigned char value[VALUE_SIZE];
	unsigned char pad[4];
}; // 8 + 8 + 100 = 116 + 4 = 120
// need 8bytes align

struct NodeOffset
{
	uint32_t file_num;
	uint32_t offset;
};

const size_t NODE_SIZE = 4096; // 4KB
const size_t NODE_BUFFER = NODE_SIZE-8; // unstable
const size_t POOL_MAX = 1024;
const size_t POOL_SIZE = 1024*1024*1024;//1GB
const size_t POOL_NODE_MAX = POOL_SIZE/NODE_SIZE; // 1GB / 4KB = 256K

struct Node
{
	NodeOffset next_offset; // 8 byte
//	uint64_t next_offset;
	unsigned char buffer[NODE_BUFFER];
};

struct NodeMeta
{
//	volatile uint64_t next_offset;
	volatile NodeMeta* next_p;
//	size_t pool_num;
//	uint64_t 
	NodeOffset my_offset;
	Node* node;

};

class NodeAllocator
{
	public:
	void init();
	void clean();

//	Node* get_node(NodeMeta* nm);
	void alloc_pool();

	private:
	unsigned char** nodeMetaPoolList;
	unsigned char** nodePoolList;

	//volatile?? lock
	int pool_cnt;
	int* node_cnt;

	NodeMeta* start_node;
	NodeMeta* end_node;

	volatile NodeMeta* free_head_p=NULL;

	
	std::atomic<uint8_t> lock=0;

	NodeMeta* alloc_node();
	void free_node(NodeMeta* nm);
	size_t alloc_cnt;

};

}
