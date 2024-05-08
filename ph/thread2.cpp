#include <stdio.h>
#include <cstring>

#include "thread2.h"
#include "log.h"

namespace PH
{

extern int log_max;
extern DoubleLog* doubleLogList;

void PH_Query_Thread::init()
{

	int i;
	uint8_t z;

	my_log = 0;

	for (i=0;i<log_max;i++)
	{
		while (doubleLogList[i].use == 0)
		{
			z = 0;
			if (doubleLogList[i].use.compare_exchange_strong(z,1))
				my_log = &doubleLogList[i];
		}
		if (my_log)
			break;
	}

	if (my_log == 0)
		printf("new query no log!!!\n");

}

void PH_Query_Thread::clean()
{
	my_log->use = 0;
	my_log = NULL;
}

int PH_Query_Thread::insert_op(uint64_t key,unsigned char* value)
{
	BaseLogEntry ble;
	ble.key = key;
	memcpy(ble.value,value,VALUE_SIZE);

	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
	my_log->insert_log(&ble);

	// 4 add dram list

	// 5 add to key list

	// 2 lock index ------------------------------------- lock from here

	// 3 get and write new version <- persist
	uint64_t version;

	//--------------------------------------------------- make version

	my_log->write_version(version);

	// 6 update index

	// 7 unlock index -------------------------------------- lock to here

	// 8 remove old dram list

	// 9 check GC

	return 0;
}
int PH_Query_Thread::read_op(uint64_t key,unsigned char* buf)
{
	return 0;
}
int PH_Query_Thread::delete_op(uint64_t key)
{
	return 0;
}
int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t end_key)
{
	return 0;
}
int PH_Query_Thread::next_op(unsigned char* buf)
{
	return 0;
}


}
