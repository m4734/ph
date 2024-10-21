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

Pool::Pool(int u,int i) : unit(u),index(i)
{
	char path[100];
	size_t rs;
	int is_pmem;

	sprintf(path,"/mnt/pmem%d/lv%d_%d",index%num_pmem+1,u,index);
	addr = (unsigned char*)pmem_map_file(path,LARGE_POOL_MAX,PMEM_FILE_CREATE,0777,&rs,&is_pmem);

	if (rs != LARGE_POOL_MAX)
		printf("rs is not lpm\n");
	if (is_pmem == 0)
		printf("is not pmem\n");
};
Pool::~Pool()
{
	pmem_unmap(addr,LARGE_POOL_MAX);
};


LargeAddr Pool::insert(uint64_t value_size,unsigned char* value)
{
	unsigned char* dst;
	LargeAddr rv;
	int cnt;

	at_lock2(lock);

	if (free_list.size())
	{
		cnt = free_list.back();
		free_list.pop_back();
	}
	else if (unit*max < LARGE_POOL_MAX)
	{
		cnt = max;
		max++;
	}
	else
	{
		rv.unit = INV16;
		at_unlock2(lock);
		return rv;
	}

	dst = addr + cnt * unit;
	memcpy(dst,&value_size,SIZE_SIZE);
	memcpy(dst+SIZE_SIZE,value,value_size);
	pmem_persist(dst,SIZE_SIZE+value_size);
	_mm_sfence();

	rv.unit = 0;
	rv.cnt = cnt;

	at_unlock2(lock);

	return rv;
}
void Pool::invalidate(int cnt)
{
	free_list.push_back(cnt);
}

UnitPool::UnitPool(int u) : unit(u)
{
}
UnitPool::~UnitPool()
{
	int i;
	for (i=0;i<pool_list.size();i++)
	{
		delete pool_list[i];
	}
}

LargeAddr UnitPool::insert(uint64_t value_size,unsigned char* value)
{
	LargeAddr rv;
	int i;
	for (i=0;i<pool_list.size();i++)
	{
		rv = pool_list[i]->insert(value_size,value);
		if (rv.unit != INV16)
		{
			rv.pool = i;
			return rv;
		}
	}
	Pool *pool = new Pool(unit,i);
	pool_list.push_back(pool);
	rv = pool_list[i]->insert(value_size,value);
	rv.pool = i;
	return rv;
}

LargeAlloc::LargeAlloc()
{
	UnitPool* lp = new UnitPool(512);
	unit_list.push_back(lp);
}
LargeAlloc::~LargeAlloc()
{
	int i;
	for (i=0;i<unit_list.size();i++)
	{
		delete unit_list[i];
	}
}

LargeAddr LargeAlloc::insert(uint64_t value_size,unsigned char* value)
{
	int i;
	for (i=0;i<unit_list.size();i++)
	{
		if (value_size+SIZE_SIZE <= unit_list[i]->unit)
			break;
	}
	if (unit_list.size() <= i)
	{
		int unit_size = unit_list[i]->unit;
		while (value_size + SIZE_SIZE > unit_size)
		{
			unit_size*=2;
			UnitPool* up = new UnitPool(unit_size);
			unit_list.push_back(up);
			i++;
		}
		i--;
	}

	LargeAddr rv;
	rv = unit_list[i]->insert(value_size,value);
	rv.unit = i;
	return rv;
}

unsigned char* LargeAlloc::read(LargeAddr largeAddr)
{
	// no check...

	return unit_list[largeAddr.unit]->pool_list[largeAddr.pool]->addr+largeAddr.cnt*largeAddr.unit;
//	return unit_list[largeAddr.unit].read(largeAddr.pool,largeAddr.offset);
}

void LargeAlloc::invalidate(LargeAddr largeAddr)
{
	unit_list[largeAddr.unit]->pool_list[largeAddr.pool]->invalidate(largeAddr.cnt);
}

}
