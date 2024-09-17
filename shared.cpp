#include "shared.h"
#include "log.h"
#include "data2.h"

namespace PH
{
#if 1
	extern DoubleLog* doubleLogList;
	extern NodeAllocator* nodeAllocator;
	extern size_t WARM_BATCH_ENTRY_CNT;
	extern size_t ENTRY_SIZE;

	void invalidate_entry(EntryAddr &ea) // need kv lock
	{
		unsigned char* addr;
		if (ea.loc == HOT_LOG)// || ea.loc == WARM_LOG) // hot log
		{
			addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
			set_invalid((EntryHeader*)addr); // invalidate
//			hot_to_hot_cnt++; // log to hot
		}
		else // warm or cold
		{
			size_t offset_in_node;
			NodeMeta* nm;
			int cnt;
			int node_cnt;

			offset_in_node = ea.offset % NODE_SIZE;
			node_cnt = ea.offset/NODE_SIZE;
			nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
			//			at_lock2(nm->rw_lock);
			// it doesn't change value just invalidate with meta
#if 1 // we don't need batch... // no i need the batch
			if (ea.loc == 2)
			{
				int batch_num,offset_in_batch;
				batch_num = offset_in_node/WARM_BATCH_MAX_SIZE;
				offset_in_batch = offset_in_node%WARM_BATCH_MAX_SIZE;
				cnt = batch_num*WARM_BATCH_ENTRY_CNT + (offset_in_batch-NODE_HEADER_SIZE)/ENTRY_SIZE;
			}
			else
				cnt = (offset_in_node-NODE_HEADER_SIZE)/ENTRY_SIZE;
#else
			cnt = (offset_in_node-NODE_HEADER_SIZE)/ENTRY_SIZE;
#endif
#if 0
			if (nm->valid[cnt])
			{
				nm->valid[cnt] = false; // invalidate
				--nm->valid_cnt;
			}
			else
				debug_error("impossible\n");
#else
			nm->valid[cnt] = false; // invalidate
			--nm->valid_cnt;
#endif
			//			at_unlock2(nm->rw_lock);
		}

		// no nm rw lock but need hash entry lock...
	}
#endif

}
