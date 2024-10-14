//#pragma once

#include "shared.h"
#include "skiplist.h"
#include "thread2.h"

namespace PH
{
	uint64_t recover_node(NodeAddr nodeAddr,int loc,int &group_idx,EntryAddr list_addr, SkiplistNode* skiplistNode = NULL);
/*
	class PH_Recovery_Thread : public PH_Thread
	{
		public:
		void init();
		void clean();
	};
	*/
}
