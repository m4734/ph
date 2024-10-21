#include <vector>

#include "shared.h"

namespace PH
{
const size_t LARGE_POOL_MAX = (1024*1024 * 10); // 1MB
const size_t POOL_LIST_MAX = 1024;
/*
class Pool
{
	public:
	Pool(int u,int index);
	~Pool();

	LargeAddr insert(uint64_t value_size,unsigned char* value);
	void invalidate(int cnt);

//	private:
	public:
	int unit,index;
	unsigned char* addr;
	int max;
	std::vector<int> free_list;

	std::atomic<uint8_t> lock=0;
};
*/
class UnitPool
{
	public:
	UnitPool();
	~UnitPool();

	void invalidate(LargeAddr largeAddr);
	void expand();

	int unit;
//	std::vector<Pool*> pool_list;
//	Pool* pool_list[1000];
	unsigned char* pool_list[POOL_LIST_MAX];
//	unsigned char** pool_list;
	int current_size=LARGE_POOL_MAX;
	int current_pool_cnt=0;
	int cnt=0;

	LargeAddr insert(uint64_t value_size,unsigned char* value);
	std::atomic<uint8_t> lock=0;
	std::vector<LargeAddr> free_list;
};

class LargeAlloc
{


	public:
	LargeAlloc();
	~LargeAlloc();

	LargeAddr insert(uint64_t value_size,unsigned char* value);
	unsigned char* read(LargeAddr largeAddr);
	void invalidate(LargeAddr largeAddr);

	private:

	void expand_unit();

//	int unit_cnt=1;
//	std::vector<UnitPool*> unit_list;
	UnitPool* unit_list[100];
//	UnitPool* unit_list;
	int unit_list_max;

	std::atomic<uint8_t> lock=0;

};

}
