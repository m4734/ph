#include <string.h>
//#include <stdlib.h>
#include <sys/mman.h>
#include <libpmem.h>
#include <atomic>

#include "log.h"
#include "global.h"

namespace PH
{

	std::atomic<unsigned int> GT; // 4B

void LOG::set_num(int i)
{
	num = i;
}

int LOG::init_log()
{
	size_t len;
	int is,file_len;
	char file[100];
	if (num == 0)
		return -1;

	log_start = 0;
	log_end = 0;
	log_max = log_size;

	file_len = strlen(log_file);
	strcpy(file,log_file);
	file[file_len] = num/10;
	file[file_len+1] = num%10;
	file[file_len+2] = 0;
	pmem_log_addr = (unsigned char*)pmem_map_file(file,log_size,PMEM_FILE_CREATE,0777,&len,&is);
	if (pmem_log_addr == NULL)
		return -1;
	dram_log_addr = (unsigned char*)mmap(NULL,log_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);
	if (dram_log_addr == NULL)
		return -1;
	return 0;
}

void LOG::clean_log()
{
	munmap(dram_log_addr,log_size);
	pmem_unmap(pmem_log_addr,log_size);
}

unsigned char* write_log(unsigned char* key_p,unsigned char* value_p,int value_len)
{
	unsigned int timestamp;
	uint16_t vl16 = value_len;
	unsigned char* rv;
	timestamp = GT.fetch_add(1);
	memcpy(dram_log_addr+log_end+len_size,key_p,key_size);
	memcpy(dram_log_addr+log_end+len_size+key_size,value_p,value_len);
	memcpy(dram_log_addr+log_end+len_size+key_size+value_len,&timestamp,sizeof(unsigned int));
	memcpy(dram_log_addr,&vl16,sizeof(uint16_t));

	memcpy(pmem_log_addr+log_end+len_size,dram_log_addr+log_end+len_size,key_size+value_len+sizeof(unsigned int));
	pmem_persist(pmem_log_addr+log_end+len_size,key_size+value_len+sizeof(unsigned int));
	_mm_sfence();

	memcpy(pmem_log_addr+log_end,dram_log_addr+log_end,len_size);
	pmem_persist(pmem_log_addr+log_end,len_size);

	rv = dram_log_addr+log_end;
	log_end+=len_size+key_size+value_len+sizeof(unsigned int);
	return rv;
}

}
