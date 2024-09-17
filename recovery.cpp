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

	inline void recover_counter(uint64_t key,uint64_t value) // single thread
	{
		if (global_seq_num[key%COUNTER_MAX] < value)
			global_seq_num[key%COUNTER_MAX] = value;
	}

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
				if (kvp_p->key == key)
				{
					ea2.value = kvp_p->value;
					header2 = (EntryHeader*)get_entry(ea2);
					v1 = header->version;
					v2 = header2->version;
					if (v1 < v2)
					{
						nodeMeta->valid[i] = false;
						ow = 0;
					}
					else
						recover_counter(key,v1);
				}

				if (ow)
				{
					kvp_p->key = key;
					ea.offset = nodeAddr.node_offset*NODE_SIZE+NODE_HEADER_SIZE+i*ENTRY_SIZE;
					kvp_p->value = ea.value;
					nodeMeta->valid[i] = true;
					nodeMeta->valid_cnt++;

					if (rv < key)
						rv = key;
				}


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
					if (kvp_p->key == key)
					{
						ea2.value = kvp_p->value;
						header2 = (EntryHeader*)get_entry(ea2);
						v1 = header->version;
						v2 = header2->version;
						if (v1 < v2)
						{
							ow = 0;
							nodeMeta->valid[cnt] = false;
						}
						else
							recover_counter(key,v1);

					}
					
					if (ow)
					{
						kvp_p->key = key;
						ea.offset = nodeAddr.node_offset*NODE_SIZE+NODE_HEADER_SIZE+i*ENTRY_SIZE;
						kvp_p->value = ea.value;
						nodeMeta->valid[cnt] = true;
						nodeMeta->valid_cnt++;

						if (rv < key)
							rv = key;
					}
				}
				cnt++;
				addr+=ENTRY_SIZE;
			}
		}
		return rv;

	}


}
