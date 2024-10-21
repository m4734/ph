#include <vector>

#include "shared.h"

namespace PH
{
const size_t LARGE_POOL_MAX = (1024*1024); // 1MB

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

class UnitPool
{
	public:
	UnitPool(int u);
	~UnitPool();

	int unit;
	std::vector<Pool*> pool_list;

	LargeAddr insert(uint64_t value_size,unsigned char* value);
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
	std::vector<UnitPool*> unit_list;


};

}
