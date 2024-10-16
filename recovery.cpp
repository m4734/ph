#include "recovery.h"
#include "log.h"
#include "data2.h"
#include "cceh.h"
#include "global2.h"

namespace PH
{

	extern thread_local PH_Thread* my_thread;

	extern CCEH* hash_index;
	extern NodeAllocator* nodeAllocator;
	extern DoubleLog* doubleLogList;
	extern std::atomic<uint64_t> global_seq_num[COUNTER_MAX];

	extern size_t WARM_BATCH_CNT;
	extern size_t WARM_BATCH_ENTRY_CNT;
	extern size_t WARM_GROUP_ENTRY_CNT;

	extern PH_List* list;

	uint64_t recover_block(int loc, NodeAddr &nodeAddr)
	{
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
		DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
		DataNode dram_dataNode = *dataNode; // pmem to dram

		// have to be first access
		if (nodeMeta->valid != NULL)
			debug_error("double alloc\n");
		nodeMeta->valid = (volatile bool*)malloc(sizeof(volatile bool) * NODE_SLOT_MAX); // TODO CHECK DUP OF start empty end
		nodeMeta->valid_cnt = 0;

		int offset = 0;
		int i,j;
		unsigned char* addr;
		EntryHeader* header;
		EntryHeader* old_header;
		EntryAddr ea,old_ea;
		uint64_t key,version,old_version;

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		bool update;

		for (i=0;i<NODE_SLOT_MAX;i++)
			nodeMeta->valid[i] = false;

		ea.loc = loc;
		ea.file_num = nodeAddr.pool_num;

		uint64_t rv = KEY_MAX;

		if (loc == COLD_LIST) // DUPLICATED CODE...
		{
			ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr);
			addr = dram_dataNode.buffer;

			for (i=0;i<NODE_SLOT_MAX;i++)
			{
				header = (EntryHeader*)addr;
				if (header->version > 0)
				{
					update = true;
					key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
					kvp_p = hash_index->insert(key,&seg_lock,my_thread->read_lock);
					version = header->version;
					if (kvp_p->key == key)
					{
						old_ea.value = kvp_p->value;
						old_header = (EntryHeader*)get_entry(old_ea);
						old_version = old_header->version;
						if (version < old_version)
						{
//							nodeMeta->valid[i] = false;
							update = false;
						}
						else
							invalidate_entry(old_ea,false);
					}

					if (update)
					{
						recover_counter(key,version);
						kvp_p->key = key;
						ea.offset = nodeAddr.node_offset*NODE_SIZE+NODE_HEADER_SIZE+i*ENTRY_SIZE;
						kvp_p->value = ea.value;
						nodeMeta->valid[i] = true;
						nodeMeta->valid_cnt++;

						listNode->valid_cnt++;

						if (rv > key)
							rv = key;
					}		
					hash_index->unlock_entry2(seg_lock,my_thread->read_lock);
				}
				addr+=ENTRY_SIZE;
			}
		}
		else // if WARM_LIST
		{
			int cnt = 0;
			for (i=0;i<WARM_BATCH_CNT;i++)
			{
				addr = (unsigned char*)&dram_dataNode + i*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE;
				for (j=0;j<WARM_BATCH_ENTRY_CNT;j++)
				{
					header = (EntryHeader*)addr;
					if (header->version > 0)
					{
						update = true;
						key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);

						kvp_p = hash_index->insert(key,&seg_lock,my_thread->read_lock);
						version = header->version;
						if (kvp_p->key == key)
						{
							old_ea.value = kvp_p->value;
							old_header = (EntryHeader*)get_entry(old_ea);
							old_version = old_header->version;
							if (version < old_version)
							{
								update = false;
//								nodeMeta->valid[cnt] = false;
							}
							else
								invalidate_entry(old_ea,false);
						}

						if (update)
						{
							recover_counter(key,version);
							kvp_p->key = key;
							ea.offset = nodeAddr.node_offset*NODE_SIZE + i*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE+ j*ENTRY_SIZE;
							kvp_p->value = ea.value;
							nodeMeta->valid[cnt] = true;
							nodeMeta->valid_cnt++;

							if (rv > key)
								rv = key;
						}
						hash_index->unlock_entry2(seg_lock,my_thread->read_lock);
					}
					cnt++;
					addr+=ENTRY_SIZE;
				}
			}

		}
		return rv;
	}

	uint64_t recover_node(NodeAddr nodeAddr,int loc,int &group_idx, EntryAddr list_addr,SkiplistNode* skiplistNode)
	{
		group_idx = 0;
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
		DataNode* dataNode;// = nodeAllocator->nodeAddr_to_node(nodeAddr);
		NodeAddr first_nodeAddr = nodeAddr;

		uint64_t rv,min;
		min = KEY_MAX;

		if (skiplistNode)
		{
			skiplistNode->data_tail = 0;
			skiplistNode->data_head = WARM_GROUP_ENTRY_CNT; 
		}

		while(nodeAddr != emptyNodeAddr)
		{
			if (skiplistNode)
				skiplistNode->data_node_addr[group_idx] = nodeAddr;

			nodeMeta->next_addr = emptyNodeAddr;
			nodeMeta->next_p = NULL;

			dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
			nodeMeta->next_addr_in_group = dataNode->next_offset_in_group;
			nodeAllocator->expand(dataNode->next_offset_in_group);
			if (nodeMeta->next_addr_in_group == emptyNodeAddr)
				nodeMeta->next_node_in_group = NULL;
			else
				nodeMeta->next_node_in_group = nodeAllocator->nodeAddr_to_nodeMeta(nodeMeta->next_addr_in_group);

			nodeMeta->list_addr = list_addr;

			nodeMeta->group_cnt = ++group_idx;
			nodeMeta->my_offset = nodeAddr;
			nodeMeta->rw_lock = 0;

			rv = recover_block(loc,nodeAddr);

			if (rv < min)
				min = rv;

			nodeAddr = nodeMeta->next_addr_in_group;
			nodeMeta = nodeMeta->next_node_in_group;

		}

		// first node
		dataNode = nodeAllocator->nodeAddr_to_node(first_nodeAddr);
		nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(first_nodeAddr);
		nodeMeta->next_addr = dataNode->next_offset;
		nodeAllocator->expand(dataNode->next_offset);
		nodeMeta->next_p = nodeAllocator->nodeAddr_to_nodeMeta(nodeMeta->next_addr);

		return min;
	}
/*
	void PH_Recovery_Thread::init()
	{
		read_lock = 0;
		temp_seg = hash_index->ret_seg();
		buffer_init();
	}
	void PH_Recovery_Thread::clean()
	{
		read_lock = 0;
		hash_index->remove_ts(temp_seg);
		buffer_clean();
	}
	*/
}
