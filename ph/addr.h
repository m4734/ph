#pragma once

namespace PH
{

struct NodeAddr
{
//	size_t loc : 2;
//	size_t pool_num : 10;
//	size_t offset : 52;
	uint32_t pool_num;
	uint32_t offset;
};

}
