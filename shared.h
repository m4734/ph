#pragma once

namespace PH
{

	const size_t NODE_HEADER_SIZE = 16; //8 + 8
	const size_t NODE_SIZE = 4096; // 4KB // 2KB // 1KB by value size...
	const size_t NODE_BUFFER_SIZE = NODE_SIZE-NODE_HEADER_SIZE; // unstable
	const size_t MAX_NODE_GROUP = 4;  // 4KB * 4 = 16KB


	struct NodeAddr
	{
		//	size_t loc : 2;
		//	size_t pool_num : 10;
		//	size_t offset : 52;
		uint32_t pool_num;
		uint32_t node_offset; // need * NODE_SIZE
	};

	const NodeAddr emptyNodeAddr = (NodeAddr) {0,0};

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




	struct DataNode
	{
		NodeAddr next_offset;
		NodeAddr next_offset_in_group;
		unsigned char buffer[NODE_BUFFER_SIZE];

	};

}
