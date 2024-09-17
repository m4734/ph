//#pragma once

#include "shared.h"
#include "skiplist.h"

namespace PH
{
	unsigned char* get_entry(EntryAddr &ea);
	uint64_t recover_cold_kv(NodeAddr &nodeAddr);
	uint64_t recover_warm_kv(NodeAddr &nodeAddr);
	uint64_t recover_node(NodeAddr nodeAddr,int loc,int &group_idx, SkiplistNode* skiplistNode = NULL);

}
