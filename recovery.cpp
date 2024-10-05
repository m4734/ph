#include "recovery.h"
#include "log.h"
#include "data2.h"
#include "cceh.h"
#include "global2.h"

namespace PH
{

	extern CCEH* hash_index;
	extern NodeAllocator* nodeAllocator;
	extern DoubleLog* doubleLogList;
	extern std::atomic<uint64_t> global_seq_num[COUNTER_MAX];

	extern size_t WARM_BATCH_CNT;
	extern size_t WARM_BATCH_ENTRY_CNT;


	unsigned char* get_entry(EntryAddr &ea)
	{
		if (ea.loc == HOT_LOG)
			return  doubleLogList[ea.file_num].dramLogAddr + ea.offset;
		else
			return (unsigned char*)nodeAllocator->nodePoolList[ea.file_num]+ea.offset;
	}

	uint64_t recover_cold_kv(NodeAddr &nodeAddr)
	{
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);

		// have to be first
		nodeMeta->valid = (volatile bool*)malloc(sizeof(volatile bool) * NODE_SLOT_MAX); // TODO CHECK DUP OF start empty end
		nodeMeta->valid_cnt = 0;

		DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
		DataNode dram_dataNode = *dataNode;
		int offset = 0;
		int i;
		unsigned char* addr;
		EntryHeader* header;
		EntryHeader* header2;
		EntryAddr ea,ea2;
		uint64_t key,v1,v2;
		addr = dram_dataNode.buffer;

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		volatile uint8_t read_lock;
		int ow;

		ea.loc = COLD_LIST;
		ea.file_num = nodeAddr.pool_num;

		uint64_t rv = KEY_MAX;

		for (i=0;i<NODE_SLOT_MAX;i++)
		{
			header = (EntryHeader*)addr;
			if (header->version > 0)
			{
				ow = 1;
				key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
				kvp_p = hash_index->insert(key,&seg_lock,read_lock);
				v1 = header->version;
				if (kvp_p->key == key)
				{
					ea2.value = kvp_p->value;
					header2 = (EntryHeader*)get_entry(ea2);
					v2 = header2->version;
					if (v1 < v2)
					{
						nodeMeta->valid[i] = false;
						ow = 0;
					}
					else
						invalidate_entry(ea2);
				}

				if (ow)
				{
					recover_counter(key,v1);
					kvp_p->key = key;
					ea.offset = nodeAddr.node_offset*NODE_SIZE+NODE_HEADER_SIZE+i*ENTRY_SIZE;
					kvp_p->value = ea.value;
					nodeMeta->valid[i] = true;
					nodeMeta->valid_cnt++;

					if (rv > key)
						rv = key;
				}
				//				else
				//					nodeMeta->valid[i] = false;

				hash_index->unlock_entry2(seg_lock,read_lock);

			}

			addr+=ENTRY_SIZE;
		}
		return rv;
	}

	uint64_t recover_warm_kv(NodeAddr &nodeAddr)
	{
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
		DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
		DataNode dram_dataNode = *dataNode;

		nodeMeta->valid = (volatile bool*)malloc(sizeof(volatile bool) * NODE_SLOT_MAX);
		nodeMeta->valid_cnt = 0;

		int offset = 0;
		int i,j;
		unsigned char* addr;
		EntryHeader* header;
		EntryHeader* header2;
		EntryAddr ea,ea2;
		uint64_t key,v1,v2;
		addr = dram_dataNode.buffer;

		KVP* kvp_p;
		std::atomic<uint8_t> *seg_lock;
		volatile uint8_t read_lock;

		int cnt=0;
		int ow;

		ea.loc = WARM_LIST;
		ea.file_num = nodeAddr.pool_num;

		uint64_t rv = KEY_MAX;

		for (i=0;i<WARM_BATCH_CNT;i++)
		{
			addr = (unsigned char*)&dram_dataNode + i*WARM_BATCH_MAX_SIZE + NODE_HEADER_SIZE;
			for (j=0;j<WARM_BATCH_ENTRY_CNT;j++)
			{
				header = (EntryHeader*)addr;
				if (header->version > 0)
				{
					ow = 1;
					key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					v1 = header->version;
					if (kvp_p->key == key)
					{
						ea2.value = kvp_p->value;
						header2 = (EntryHeader*)get_entry(ea2);
						v2 = header2->version;
						if (v1 < v2)
						{
							ow = 0;
							nodeMeta->valid[cnt] = false;
						}
						else
							invalidate_entry(ea2);
					}

					if (ow)
					{
						recover_counter(key,v1);
						kvp_p->key = key;
						ea.offset = nodeAddr.node_offset*NODE_SIZE+NODE_HEADER_SIZE+i*ENTRY_SIZE;
						kvp_p->value = ea.value;
						nodeMeta->valid[cnt] = true;
						nodeMeta->valid_cnt++;

						if (rv > key)
							rv = key;
					}

					hash_index->unlock_entry2(seg_lock,read_lock);
				}
				cnt++;
				addr+=ENTRY_SIZE;
			}
		}
		return rv;

	}

	uint64_t recover_node(NodeAddr nodeAddr,int loc,int &group_idx, SkiplistNode* skiplistNode)
	{
		group_idx = 0;
		NodeMeta* nodeMeta = nodeAllocator->nodeAddr_to_nodeMeta(nodeAddr);
		DataNode* dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
		NodeMeta* fn = nodeMeta;
		//		int group_idx=0;

		// first

		if (skiplistNode)
			skiplistNode->data_node_addr[group_idx] = nodeAddr;

		nodeMeta->next_addr = dataNode->next_offset;
		nodeAllocator->expand(dataNode->next_offset);
		nodeMeta->next_p = nodeAllocator->nodeAddr_to_nodeMeta(nodeMeta->next_addr);

		nodeMeta->next_addr_in_group = dataNode->next_offset_in_group;
		nodeAllocator->expand(dataNode->next_offset_in_group);
		nodeMeta->next_node_in_group = nodeAllocator->nodeAddr_to_nodeMeta(nodeMeta->next_addr_in_group);

		nodeMeta->group_cnt = ++group_idx;
		nodeMeta->my_offset = nodeAddr;
		nodeMeta->rw_lock = 0;

		uint64_t rv,min;
		min = KEY_MAX;

		if (loc == WARM_LIST)
		{
			rv = recover_warm_kv(nodeAddr);
		}
		else
		{
			rv = recover_cold_kv(nodeAddr);
		}

		if (rv < min)
			min = rv;

		nodeAddr = nodeMeta->next_addr_in_group;
		nodeMeta = nodeMeta->next_node_in_group;


		while(nodeAddr != emptyNodeAddr)
		{

			if (skiplistNode)
				skiplistNode->data_node_addr[group_idx] = nodeAddr;

			nodeMeta->next_addr = emptyNodeAddr;
			nodeMeta->next_p = NULL;

			dataNode = nodeAllocator->nodeAddr_to_node(nodeAddr);
			nodeMeta->next_addr_in_group = dataNode->next_offset_in_group;
			nodeAllocator->expand(dataNode->next_offset_in_group);
			nodeMeta->next_node_in_group = nodeAllocator->nodeAddr_to_nodeMeta(nodeMeta->next_addr_in_group);

			nodeMeta->group_cnt = ++group_idx;
			nodeMeta->my_offset = nodeAddr;
			nodeMeta->rw_lock = 0;

			if (loc == WARM_LIST)
			{
				rv = recover_warm_kv(nodeAddr);
			}
			else
			{
				rv = recover_cold_kv(nodeAddr);
			}

			if (rv < min)
				min = rv;

			nodeAddr = nodeMeta->next_addr_in_group;
			nodeMeta = nodeMeta->next_node_in_group;

		}
		return min;
	}

}
