#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>

//--------------------------------

#define HOT_KEY_LIST
#define WARM_CACHE
#define SCAN_SORT
#define USE_DTC

//-----------------------------

#define WARM_STAT
//#define SCAN_TIME

//------------------------------

//#define CC // cor check

#ifdef CC

#define KEY_CHECK
#define HTW_KEY_CHECK
#define SKIPLIST_TRAVERSE_TEST
#define LIST_TRAVERSE_TEST
#define VALID_CHECK
#define UNLOCK_TEST
#define INV_TEST

#endif


//-----------------------------------

namespace PH
{

	void debug_error(const char* msg);

	const uint32_t ENTRY_HEADER_SIZE = 8;
	const uint32_t KEY_SIZE = 8;
	const uint32_t SIZE_SIZE = 8;
//value size ??
	const uint32_t WARM_CACHE_SIZE = 8;
	const uint32_t JUMP_SIZE = 8;

	const uint32_t ENTRY_SIZE_WITHOUT_VALUE = ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE;
	const uint32_t LOG_ENTRY_SIZE_WITHOUT_VALUE = ENTRY_HEADER_SIZE + KEY_SIZE + SIZE_SIZE + 0 + WARM_CACHE_SIZE;

//	const uint32_t ENTRY_SIZE = ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE;

	const size_t KEY_MIN = 0x0000000000000000;
	const size_t KEY_MAX = 0xffffffffffffffff;

#if 1 // big
	const uint32_t NODE_SIZE = 4096; // 4KB // 2KB // 1KB by value size...

	const uint32_t WARM_MAX_NODE_GROUP = 4;
	const uint32_t MAX_NODE_GROUP = 4;  // 4KB * 4 = 16KB
#else // small
	const size_t NODE_SIZE = 1024; // 4KB // 2KB // 1KB by value size...

	const size_t WARM_MAX_NODE_GROUP = 2;
	const size_t MAX_NODE_GROUP = 4;  // 4KB * 4 = 16KB
#endif

	const uint32_t WARM_BATCH_MAX_SIZE = 1024; // 1KB

	const uint32_t NODE_HEADER_SIZE = 16; //8 + 8
	const uint32_t NODE_BUFFER_SIZE = NODE_SIZE-NODE_HEADER_SIZE; // unstable

	const uint32_t WARM_BATCH_ENTRY_CNT = 20;
	const uint32_t WARM_BATCH_CNT = 4;

	const uint32_t 	WARM_NODE_ENTRY_CNT = WARM_BATCH_ENTRY_CNT*(WARM_BATCH_CNT);//(NODE_SIZE/(WARM_BATCH_SIZE+NODE_HEADER_SIZE)); //8-9 * 4
	const uint32_t WARM_GROUP_BATCH_CNT = WARM_BATCH_CNT * WARM_MAX_NODE_GROUP; // 4*4 = 16


#if 1
	struct NodeAddr
	{
		//	size_t loc : 2;
		//	size_t pool_num : 10;
		//	size_t offset : 52;
		uint32_t pool_num;
		uint32_t node_offset;
//		uint16_t pool_num;
//		uint16_t node_offset; // need * NODE_SIZE
		bool operator==(const NodeAddr &na)
		{
			return (pool_num == na.pool_num && node_offset == na.node_offset);
		}
		bool operator!=(const NodeAddr &na)
		{
			return (pool_num != na.pool_num || node_offset != na.node_offset);
		}
		/*
		NodeAddr operator=(const NodeAddr &na)
		{
			pool_num = na.pool_num;
			node_offset = na.node_offset;
			return *this;
		}
		*/
	}; // may 16
#else
	union NodeAddr
	{
		struct
		{
			uint32_t pool_num;
			uint32_t node_offset;
		};
		uint64_t value;

		bool operator==(const NodeAddr &na)
		{
			return value == na.value;
		}
		bool operator!=(const NodeAddr &na)
		{
			return value != na.value;
		}
	};
#endif

union EntryHeader
{
	struct
	{
		size_t valid_bit : 1;
		size_t delete_bit : 1;
		size_t version : 62;
	};
	uint64_t value;
};

	const NodeAddr emptyNodeAddr = (NodeAddr) {0,0};

	enum Loc
	{
		NONE, // historical

		HOT_LOG,
		WARM_LIST,
		COLD_LIST
	};

	union EntryAddr
	{
		struct
		{
			size_t loc : 2; // 1 hot / 2 warm / 3 cold	
			size_t file_num : 14;
			size_t offset : 48; 
		};
		uint64_t value;
		bool operator!=(const EntryAddr &ea)
		{
			return value != ea.value;
		}
	/*	
		EntryAddr operator=(const EntryAddr &ea)
		{
			value = ea.value;
			return *this;
		}
		*/
	};

	const EntryAddr emptyEntryAddr = (EntryAddr) {.value = 0};

	const size_t MAX_LEVEL = 30;

	union SkipAddr
	{
		SkipAddr() : value(0) {}
		SkipAddr(uint64_t v) : value(v) {}

		struct
		{
			uint32_t ver;
			uint16_t pool_num; 
			uint16_t offset;
		};
//		uint64_t value;
		std::atomic<uint64_t> value;

		SkipAddr operator=(const SkipAddr &sa)
		{
			value = sa.value.load();
			return (SkipAddr)value;
		}
	};

	struct DataNode
	{
		NodeAddr next_offset;
		NodeAddr next_offset_in_group;
		unsigned char buffer[NODE_BUFFER_SIZE];
	};

	void invalidate_entry(EntryAddr &ea,bool try_merge = true);

//	   void pmem_node_nt_write(DataNode* dst_node,DataNode* src_node, size_t offset, size_t len);
	void pmem_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len);
	void pmem_reverse_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len); //need len align
	void reverse_memcpy(unsigned char* dst_addr,unsigned char* src_addr, size_t len); //need len align
	void pmem_entry_write(unsigned char* dst, unsigned char* src, size_t len);
	void pmem_next_write(DataNode* dst_node,NodeAddr nodeAddr);

	unsigned char* get_entry(EntryAddr &ea);

	inline EntryAddr nodeAddr_to_listAddr(Loc loc, NodeAddr &nodeAddr)
	{
		EntryAddr ea;
		ea.loc = loc;
		ea.file_num = nodeAddr.pool_num;
		ea.offset = nodeAddr.node_offset;
		return ea;
	}

	void EA_test(uint64_t key, EntryAddr ea);

	inline uint64_t get_v8(uint64_t &value_size)
	{
		return (value_size+8-1)/8*8;
//		return value_size + (8-value_size%8);
	}
}
