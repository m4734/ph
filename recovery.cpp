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

//	extern uint32_t WARM_BATCH_CNT;
//	extern uint32_t WARM_BATCH_ENTRY_CNT;
//	extern uint32_t WARM_GROUP_ENTRY_CNT;

#if 1
	uint64_t recover_block(int loc, NodeAddr &nodeAddr)
	{
		return 0;
	}
#else
	uint64_t recover_block(int loc, NodeAddr &nodeAddr)
	{
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
		DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
		DataNode dram_dataNode = *dataNode; // pmem to dram

		// have to be first access
		if (nodeMeta->entryLoc != NULL)
			debug_error("double alloc\n");

		nodeMeta->entryLoc = (EntryLoc*)malloc(sizeof(EntryLoc) * NODE_SLOT_MAX);
		nodeMeta->el_cnt[0]=0;

		int offset = 0;
		int i,j;
		unsigned char* addr;
		EntryHeader* header;
		EntryHeader* old_header;
		EntryAddr ea,old_ea;
		uint64_t key,version,old_version;
		int value_size8;
		size_t base_node_offset;

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		bool update;
/*
		if (loc == WARM_LIST)
			nodeMeta->init_warm_el();
		else if (loc == COLD_LIST)
			nodeMeta->init_cold_el();
*/

//		if (nodeAddr.pool_num == 10 && nodeAddr.node_offset == 15043)
//			debug_error("debug\n");

		ea.loc = loc;
		ea.file_num = nodeAddr.pool_num;

		uint64_t rv = KEY_MAX;

		base_node_offset = nodeAddr.node_offset*NODE_SIZE;

		if (loc == COLD_LIST) // DUPLICATED CODE...
		{
			ListNode* listNode = list->addr_to_listNode(nodeMeta->list_addr);
//			addr = dram_dataNode.buffer;
			addr = (unsigned char*)&dram_dataNode;

			offset = NODE_HEADER_SIZE;
			i = 0;

			while (offset<NODE_SIZE)
			{
				nodeMeta->entryLoc[i].offset = offset;
				nodeMeta->entryLoc[i].valid = 0;
				nodeMeta->el_cnt[0]++; // invalid during recover

				header = (EntryHeader*)(addr+offset);
				if (header->value == 0)
				{
//					i++; // no it does not need
					break;
				}
				else if (header->valid_bit == 0 && header->delete_bit == 0) //jump
				{
					offset = header->version;
//					nodeMeta->entryLoc[i].valid = 0;
					i++;
					continue;
				}

//				if (header->version > 0)
				{
					update = true;
					key = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE);
//					value_size8 = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE+KEY_SIZE);
					value_size8 = header->size;
					value_size8 = get_v8(value_size8);
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
							invalidate_entry(old_ea,old_ea.large,false);
					}

					if (update)
					{
						recover_counter(key,version);
						kvp_p->key = key;
//						ea.offset = nodeAddr.node_offset*NODE_SIZE+NODE_HEADER_SIZE+i*ENTRY_SIZE;
						ea.offset = base_node_offset + offset;
						kvp_p->value = ea.value;
//						nodeMeta->valid[i] = true;
//						nodeMeta->valid_cnt++;
						nodeMeta->entryLoc[i].valid = 1;
						i++;

//						listNode->valid_cnt++;

						if (rv > key)
							rv = key;
					}		
					hash_index->unlock_entry2(seg_lock,my_thread->read_lock);
				}
				offset+=ENTRY_SIZE_WITHOUT_VALUE + value_size8;
			}
			nodeMeta->entryLoc[i].offset = offset;
			nodeMeta->entryLoc[i].valid = 0;
			i++;
			nodeMeta->entryLoc[i].offset = NODE_SIZE;
			nodeMeta->entryLoc[i].valid = 0;
			nodeMeta->el_cnt[0] = i+1;
		}
		else // if WARM_LIST // TODO el cnt 
		{
			for (i=0;i<WARM_BATCH_CNT;i++)
			{
//				addr = (unsigned char*)&dram_dataNode + i*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE;
				addr = (unsigned char*)&dram_dataNode;

				if (i == 0)
					offset = NODE_HEADER_SIZE;
				else
					offset = i*WARM_BATCH_MAX_SIZE;
				j = i*WARM_BATCH_ENTRY_CNT; // 20

				while(offset < (i+1) * WARM_BATCH_MAX_SIZE)
				{
					nodeMeta->entryLoc[j].offset = offset;
					nodeMeta->entryLoc[j].valid = 0;

					header = (EntryHeader*)(addr+offset);
					if (header->value == 0)
					{
						break;
					}
					else if (header->valid_bit == 0 && header->delete_bit == 0) //jump
					{
						offset = header->version;
//						nodeMeta->entryLoc[i].valid = 0;
						j++;
						continue;
					}
//					if (header->version > 0)
					{
						update = true;
						key = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE);

//						value_size8 = *(uint64_t*)(addr+offset+ENTRY_HEADER_SIZE+KEY_SIZE);
						value_size8 = header->size;
						value_size8 = get_v8(value_size8);

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
								invalidate_entry(old_ea,old_ea.large,false);
						}

						if (update)
						{
							recover_counter(key,version);
							kvp_p->key = key;
//							ea.offset = nodeAddr.node_offset*NODE_SIZE + i*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE+ j*ENTRY_SIZE;
							ea.offset = base_node_offset + offset;
							kvp_p->value = ea.value;
//							nodeMeta->valid[cnt] = true;
//							nodeMeta->valid_cnt++;
							nodeMeta->entryLoc[j].valid = 1;
							j++;

							if (rv > key)
								rv = key;
						}
						hash_index->unlock_entry2(seg_lock,my_thread->read_lock);
					}
					offset+=ENTRY_SIZE_WITHOUT_VALUE + value_size8;
				}
				nodeMeta->entryLoc[j].offset = offset;
				nodeMeta->entryLoc[j].valid = 0;
				j++;
				nodeMeta->entryLoc[j].offset = WARM_BATCH_MAX_SIZE*(i+1);;
				nodeMeta->entryLoc[j].valid = 0;
			}
			// warm node el cnt...

		}
		return rv;
	}
#endif

#if 1 // not now
	uint64_t recover_node(NodeAddr nodeAddr,int loc,int &group_idx, EntryAddr list_addr,SkiplistNode* skiplistNode)
	{
		return 0;
	}

#else
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
//			skiplistNode->data_head = WARM_GROUP_ENTRY_CNT; 
			skiplistNode->data_head = WARM_BATCH_CNT * WARM_MAX_NODE_GROUP; // 4 * GROUP
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
#endif
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
