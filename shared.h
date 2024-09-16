#pragma once

//#define WARM_STAT
//#define KVP_VER
#define HOT_KEY_LIST
#define WARM_CACHE
#define SCAN_SORT

namespace PH
{
	const size_t NODE_HEADER_SIZE = 16; //8 + 8
	const size_t NODE_SIZE = 4096; // 4KB // 2KB // 1KB by value size...
	const size_t NODE_BUFFER_SIZE = NODE_SIZE-NODE_HEADER_SIZE; // unstable
	const size_t MAX_NODE_GROUP = 4;  // 4KB * 4 = 16KB

	const size_t WARM_MAX_NODE_GROUP = 2;
	const size_t WARM_BATCH_MAX_SIZE = 1024; // 1KB

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

	}; // may 16

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

}
