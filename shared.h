#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <time.h>
#include <stdio.h>

//--------------------------------

#define HOT_KEY_LIST
#define WARM_CACHE
#define SCAN_SORT
#define LARGE_ALLOC
//-----------------------------

//#define SCAN_TIME

//------------------------------

//#define SYNCER // single thread order syncer for debug

//#define CC // correctness check

#ifdef CC

#define KEY_CHECK
#define HTW_KEY_CHECK
#define VALID_CHECK
#define UNLOCK_TEST
#define INV_TEST
#define DST_CHECK
#define SPLIT_KEY_TEST
#define ENTRY_LIST_CHECK

#define NO_EXIST


#endif

//--------------perf

//#define STAT
#ifdef STAT

#define WARM_STAT
#define TIME_STAT
//#define TIME_STAT2

#define SKIPLIST_TRAVERSE_TEST
#define LIST_TRAVERSE_TEST
//#define ENTRY_WRITE_TEMP // remove this

#endif
//--------------------------------------

#define INTERLEAVE
//#define NO_EXIST

//-----------------------------------

namespace PH
{

	void debug_error(const char* msg);

	const uint32_t LARGE_VALUE_THRESHOLD = 300;// must be smllaer than WARM_BATCH_MAX_SIZE 1024 - ..

	const uint32_t ENTRY_HEADER_SIZE = 8;
	const uint32_t KEY_SIZE = 8;
//	const uint32_t SIZE_SIZE = 8;
	const uint32_t SIZE_SIZE_L = 8;
//value size ??
	const uint32_t WARM_CACHE_SIZE = 8;
	const uint32_t JUMP_SIZE = 8;

	const uint32_t ENTRY_SIZE_WITHOUT_VALUE = ENTRY_HEADER_SIZE+KEY_SIZE;//+SIZE_SIZE;
	const uint32_t LOG_ENTRY_SIZE_WITHOUT_VALUE = ENTRY_HEADER_SIZE + KEY_SIZE + /*SIZE_SIZE + */0 + WARM_CACHE_SIZE;

//	const uint32_t ENTRY_SIZE = ENTRY_HEADER_SIZE+KEY_SIZE+SIZE_SIZE;

	const size_t KEY_MIN = 0x0000000000000000;
	const size_t KEY_MAX = 0xffffffffffffffff;

#if 1 // big
	const uint32_t NODE_SIZE = 4096;//*2; // 4KB // 2KB // 1KB by value size...

	const uint32_t WARM_MAX_NODE_GROUP = 4*2;//8; // 2 4 8?
	const uint32_t MAX_NODE_GROUP = WARM_MAX_NODE_GROUP;  // 4KB * 4 = 16KB
#else // small
	const size_t NODE_SIZE = 1024; // 4KB // 2KB // 1KB by value size...

	const size_t WARM_MAX_NODE_GROUP = 2;
	const size_t MAX_NODE_GROUP = 4;  // 4KB * 4 = 16KB
#endif

	const uint32_t EXPECTED_ENTRY_SIZE = 100+20;//200; // temp

	const uint32_t WARM_BATCH_MAX_SIZE = 1024; // 1KB

	const uint32_t NODE_HEADER_SIZE = 16; //8 + 8
	const uint32_t NODE_BUFFER_SIZE = NODE_SIZE-NODE_HEADER_SIZE; // unstable

//	const uint32_t WARM_BATCH_ENTRY_CNT = 20;
	const uint32_t WARM_BATCH_CNT = NODE_SIZE/WARM_BATCH_MAX_SIZE;////4;

//	const uint32_t WARM_NODE_ENTRY_CNT = WARM_BATCH_ENTRY_CNT*(WARM_BATCH_CNT);//(NODE_SIZE/(WARM_BATCH_SIZE+NODE_HEADER_SIZE)); //8-9 * 4
	const uint32_t WARM_LOG_LIST_MAX = 20*WARM_MAX_NODE_GROUP; //1024/200 = 5... // 4096 / 200 // 4096 * 4 / 200 // scan list max(size) from warm node
	const uint32_t WARM_GROUP_BATCH_CNT = WARM_BATCH_CNT * WARM_MAX_NODE_GROUP; // 4*4 = 16

//	const uint32_t NODE_SLOT_MAX = 80; // 4096/50 // may use WARM_LOG_LIST_MAX
	const uint32_t WARM_KEY_LIST_DEFAULT = WARM_MAX_NODE_GROUP * NODE_SIZE/EXPECTED_ENTRY_SIZE;
	const uint32_t WARM_KEY_LIST_MAX = WARM_KEY_LIST_DEFAULT * 10; //4096/200 = 20

//	const int WARM_COLD_MAX_RATIO = 14; // split when bigger than  // about 10%
	const int WARM_COLD_MAX_RATIO = 20; // split when bigger than  // about 10%

	const int WARM_COLD_MIN_RATIO = 10; // merge when smaller than (after merge smaller than )
//	const int WARM_COLD_MAX_RATIO_TEMP = 20; // for cold nodes

	const int HARD_EVICT_RATIO = 5;
	const int SOFT_EVICT_RATIO = 50;
	
	const int VALID_RATIO = 50;
	const uint32_t WARM_EVICT_THRESHOLD = WARM_BATCH_MAX_SIZE*VALID_RATIO/*80*//100; // NODE_HEADER_SIZE

#if 1
	struct NodeAddr
	{
		//	size_t loc : 2;
		//	size_t pool_num : 10;
		//	size_t offset : 52;
		uint32_t pool_num;
		uint32_t node_offset; // 2^31-1
//		uint16_t pool_num;
//		uint16_t node_offset; // need * NODE_SIZE
		bool operator==(const NodeAddr &na)
		{
			return (pool_num == na.pool_num && node_offset == na.node_offset);
		}
		bool operator!=(const NodeAddr &na)
		{
			return (pool_num != na.pool_num || node_offset != na.node_offset);
		}
		/*
		NodeAddr operator=(const NodeAddr &na)
		{
			pool_num = na.pool_num;
			node_offset = na.node_offset;
			return *this;
		}
		*/
	}; // may 16
#else
	union NodeAddr
	{
		struct
		{
			uint32_t pool_num;
			uint32_t node_offset;
		};
		uint64_t value;

		bool operator==(const NodeAddr &na)
		{
			return value == na.value;
		}
		bool operator!=(const NodeAddr &na)
		{
			return value != na.value;
		}
	};
#endif

const uint8_t LARGE_PTR_SIZE = 8;

// empty bit requires init

union EntryHeader
{
	struct
	{
		size_t valid_bit : 1;
		size_t delete_bit : 1;
		size_t large_bit : 1;
//		size_t empty_bit : 1;
		size_t size : 13;
		size_t version : 48; // 
	};
	uint64_t value;
};

	const NodeAddr emptyNodeAddr = (NodeAddr) {0,0};

	enum Loc
	{
		NONE, // historical

		HOT_LOG,
		WARM_LIST,
		COLD_LIST
	};

	union EntryAddr
	{
		struct
		{
			size_t loc : 2; // 1 hot / 2 warm / 3 cold	
			size_t large : 1;
//			size_t empty : 1;
			size_t size : 11; // 10bit 1KB
			size_t file_num : 10; // 1 k files
			size_t offset : 40;  // .. 2^16 * 4 G // 1TB
			// TODO offset need recycle
			// log ( 136 * 1B )
		};
		uint64_t value;
		bool operator!=(const EntryAddr &ea)
		{
			return value != ea.value;
		}
	/*	
		EntryAddr operator=(const EntryAddr &ea)
		{
			value = ea.value;
			return *this;
		}
		*/
	};

	const EntryAddr emptyEntryAddr = (EntryAddr) {.value = 0};

	extern size_t log_size;

/*
	inline size_t get_log_offset(size_t &offset)
	{
		return offset % log_size;
	}
	*/
	inline size_t get_log_offset(EntryAddr &ea)
	{
		return ea.offset % log_size;
	}

	const size_t MAX_LEVEL = 30;

	union SkipAddr
	{
		SkipAddr() : value(0) {}
		SkipAddr(uint64_t v) : value(v) {}

		struct
		{
			uint32_t ver;
			uint16_t pool_num; 
			uint16_t offset;
		};
//		uint64_t value;
		std::atomic<uint64_t> value;

		SkipAddr operator=(const SkipAddr &sa)
		{
			value = sa.value.load();
			return (SkipAddr)value;
		}
	};

	struct DataNode
	{
		NodeAddr next_offset;
		NodeAddr next_offset_in_group;
		unsigned char buffer[NODE_BUFFER_SIZE];
	};

	void invalidate_entry(EntryAddr &ea,bool inv_large,bool try_merge = true);

//	   void pmem_node_nt_write(DataNode* dst_node,DataNode* src_node, size_t offset, size_t len);
	void pmem_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len);
	void pmem_reverse_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len); //need len align
	void reverse_memcpy(unsigned char* dst_addr,unsigned char* src_addr, size_t len); //need len align
	void pmem_entry_write(unsigned char* dst, unsigned char* src, size_t len, unsigned char* temp_header);
	void pmem_next_write(DataNode* dst_node,NodeAddr nodeAddr);

	/*inline */unsigned char* get_entry(EntryAddr &ea);

	inline EntryAddr nodeAddr_to_listAddr(Loc loc, NodeAddr &nodeAddr)
	{
		EntryAddr ea;
		ea.loc = loc;
		ea.file_num = nodeAddr.pool_num;
		ea.offset = nodeAddr.node_offset;
		return ea;
	}

	void EA_test(uint64_t key, EntryAddr ea);

	inline int get_v8(int &value_size)
	{
		return (value_size+8-1)/8*8;
//		return value_size + (8-value_size%8);
	}


	const uint16_t INV16 = 0xffff;//2^16-1
	const uint64_t INV64 = 0xffffffffffffffff;
	struct LargeAddr
	{
		uint16_t unit;
		uint16_t pool;
		uint32_t cnt;
	};

/*inline */unsigned char* get_large_from_addr(unsigned char* addr);
/*inline */void invalidate_large_from_ea(EntryAddr &ea);
/*inline */void invalidate_large_from_addr(unsigned char* addr);

//--------------------------------------- statistic
	struct TimeEntry
	{
		timespec start,end;
		uint64_t sum;
		uint64_t cnt;
//		uint64_t avg;
	};

	//add time list // add time name // insert tes and tee
	enum TimeList
	{
		INSERT_ENTRY_TO_SLOT,
		DIRECT_TO_COLD,
		INSERT_TO_COLD_OF_DTC,
		INSERT_TO_COLD_FROM_WARM,
		INSERT_TO_COLD,
		APPEND_OF_ITC,
		SPLIT_OF_ITC,
		TEMP1,
		TEMP2,
		TEMP3,
		TEMP4,
		DRAM,
		PMEM,
		VERSION,
		INSERT,
		READ,
		SKIP_LOCK,
		INSERT_LOG,
		INSERT_DTC,
		HTW,
		WTC,
		TIME_LIST_END
	};
	static const char *time_name[] = {"INSERT_ENTRY_TO_SLOT","DIRECT_TO_COLD","INSERT_TO_COLD_OF_DTC","INSERT_TO_COLD_FORM_WARM","INSERT_TO_COLD","APPEND_OF_ITC","SPLIT_OF_ITC","TEMP1","TEMP2","TEMP3","TEMP4","DRAM","PMEM","VERSION","INSERT","READ","SKIP_LOCK","INESRT_LOG","INSERT_DTC","HTW","WTC","TIME_LIST_END"};

	extern std::atomic<uint64_t> time_sum[TIME_LIST_END];
	extern std::atomic<uint64_t> time_cnt[TIME_LIST_END];
//	extern thread_local TimeEntry timeEntry[TIME_LIST_END];

	inline void timeInit()
	{
		int i;
		for (i=0;i<TIME_LIST_END;i++)
		{
			time_sum[i] = 0;
			time_cnt[i] = 0;
		}
	}
	inline void timePrint()
	{
		printf("timeList\n");
		printf("---------------------------\n");
		int i;
		for (i=0;i<TIME_LIST_END;i++)
		{
			if (time_cnt[i].load() == 0)
				printf("div zero\n");
			else
				printf("%s %lu %lu\n",time_name[i],time_cnt[i].load(),time_sum[i].load()/time_cnt[i].load());
		}
		printf("---------------------------\n");
	}
}
