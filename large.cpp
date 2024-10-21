#include <stdio.h>
#include <libpmem.h>
#include <string.h>
#include <x86intrin.h>

#include "shared.h"
#include "large.h"
#include "lock.h"

namespace PH {

LargeAlloc* largeAlloc;
extern int num_pmem;

UnitPool::UnitPool()
{
}
UnitPool::~UnitPool()
{
	int i;
	for (i=0;i<current_pool_cnt;i++)
	{
		pmem_unmap(pool_list[i],LARGE_POOL_MAX);
	}
}

void insert2(unsigned char *dst, uint64_t value_size, unsigned char* value)
{
	memcpy(dst,&value_size,SIZE_SIZE);
	memcpy(dst+SIZE_SIZE,value,value_size);
	pmem_persist(dst,SIZE_SIZE+value_size);
	_mm_sfence();
}

void UnitPool::invalidate(LargeAddr largeAddr)
{
	at_lock2(lock);

	free_list.push_back(largeAddr);

	at_unlock2(lock);
}

void UnitPool::expand()
{
	if (current_pool_cnt >= POOL_LIST_MAX)
	{
		printf("pool list max\n");
		return;
	}
	char path[100];
	size_t rs;
	int is_pmem;
	unsigned char* addr;

	sprintf(path,"/mnt/pmem%d/lv%d_%d",current_pool_cnt%num_pmem+1,unit,current_pool_cnt);
	addr = (unsigned char*)pmem_map_file(path,LARGE_POOL_MAX,PMEM_FILE_CREATE,0777,&rs,&is_pmem);

	if (rs != LARGE_POOL_MAX)
		printf("rs is not lpm\n");
	if (is_pmem == 0)
		printf("is not pmem\n");

	pool_list[current_pool_cnt] = addr;
	current_pool_cnt++;
}

LargeAddr UnitPool::insert(uint64_t value_size,unsigned char* value)
{
	at_lock2(lock);
	if (free_list.size())
	{
		LargeAddr la = free_list.back();
		free_list.pop_back();

		insert2(pool_list[la.pool]+la.cnt*unit,value_size,value);

		at_unlock2(lock);
		return la;
	}

	int required_size = value_size + SIZE_SIZE;

	if (current_size + required_size > LARGE_POOL_MAX)
	{
		expand();
//		pool_list[current_pool_cnt] = 
		cnt = 0;
		current_size = 0;
	}

	LargeAddr rv;

	rv.pool = current_pool_cnt-1;
	rv.cnt = cnt;

	insert2(pool_list[rv.pool]+rv.cnt*unit,value_size,value);

	cnt++;
	current_size+=unit;
	at_unlock2(lock);
	return rv;
}

LargeAlloc::LargeAlloc()
{
	unit_list_max = 1;
	unit_list[0] = new UnitPool();
	unit_list[0]->unit=256;
}
LargeAlloc::~LargeAlloc()
{
	int i;
	size_t sum=0;
	for (i=0;i<unit_list_max;i++)
	{
		sum+=unit_list[i]->current_pool_cnt;
		delete unit_list[i];
	}
	sum*=LARGE_POOL_MAX;
	printf("large alloc %lfGB\n",double(sum)/1024/1024/1024);
}

LargeAddr LargeAlloc::insert(uint64_t value_size,unsigned char* value)
{
	int target_size;
	target_size = value_size + SIZE_SIZE;

	while(unit_list[unit_list_max-1]->unit < target_size)
	{
		at_lock2(lock);
		if (unit_list[unit_list_max-1]->unit < target_size)
		{
			unit_list[unit_list_max] = new UnitPool();
			unit_list[unit_list_max]->unit = unit_list[unit_list_max-1]->unit*2;
			unit_list_max++;
		}
		at_unlock2(lock);
	}


	int i;
	for (i=0;i<unit_list_max;i++)
	{
		if (target_size <= unit_list[i]->unit)
			break;
	}

	LargeAddr rv;
	rv = unit_list[i]->insert(value_size,value);
	rv.unit = i;
	return rv;
}

unsigned char* LargeAlloc::read(LargeAddr largeAddr)
{
	// no check...

	return unit_list[largeAddr.unit]->pool_list[largeAddr.pool]+largeAddr.cnt*unit_list[largeAddr.unit]->unit;
//	return unit_list[largeAddr.unit].read(largeAddr.pool,largeAddr.offset);
}

void LargeAlloc::invalidate(LargeAddr largeAddr)
{
	unit_list[largeAddr.unit]->invalidate(largeAddr);
}

}
