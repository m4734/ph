#include <atomic>
#include <stdio.h>
#include <x86intrin.h>

#include "lock.h"

namespace PH
{
#define USE_CAS
void at_lock2(std::atomic<uint8_t> &lock)
{
#ifdef USE_CAS
	uint8_t z;
	while(true)
	{
		z = 0;
		if (lock.compare_exchange_strong(z,1))
			return;
	}
#else
	while(true)
	{
		lock++;
		if (lock == 1)
			return;
		lock--;
	}
#endif
	_mm_mfence();
}

//#define UNLOCK_TEST

void at_unlock2(std::atomic<uint8_t> &lock)
{
	_mm_mfence();
#ifdef UNLOCK_TEST
	if (lock == 0)
		printf("unlock unlock!!------------------------------------\n");
#endif
	lock = 0;
//	lock.store(0,std::memory_order_release);
}

int try_at_lock2(std::atomic<uint8_t> &lock)
{
#ifdef USE_CAS
	uint8_t z=0;
	return lock.compare_exchange_strong(z,1);
#else
	lock++;
	if (lock == 1)
		return 1;
	lock--;
	return 0;
#endif
}

}
