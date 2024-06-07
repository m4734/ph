#pragma once

namespace PH
{


struct NodeAddr
{
//	size_t loc : 2;
//	size_t pool_num : 10;
//	size_t offset : 52;
	uint32_t pool_num;
	uint32_t node_offset; // need * NODE_SIZE
};

const size_t NODE_SIZE = 4096; // 4KB
const size_t NODE_BUFFER_SIZE = NODE_SIZE-8; // unstable

struct DataNode
{
	NodeAddr next_offset;
	unsigned char buffer[NODE_BUFFER_SIZE];

};

}
