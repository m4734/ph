#include <libpmem.h>
#include <string.h>

#include "log.h"
#include "data.h"

using namespace PH;

extern unsigned char** pmem_addr;

void init_log()
{
	global_ts = 0;
	log_file_cnt = 0;
}

void LOG::init()
{
	file_max = 0;
	file_index = 0;
	kv_in_offset = {INIT_OFFSET,0,0};
	kv_out_offset = {INIT_OFFSET,0,0};

	new_log_file();
	new_log_file();
}

void LOG::clean()
{
	int i;
	for (i=0;i<file_max;i++)
	{
		munmap(dram_array[i],FILE_SIZE);
		pmem_unmap(pmem_addr[i],FILE_SIZE);
	}
}

// point hash - lock
// (range hash)
// insert log
// flush log - point rehash
// split?
void LOG::insert_log(unsigned char* &key_p, unsigned char* &value_p)
{
	ValueEntry_u ve_u;
	volatile uint64_t* v64_p;
}

void LOG::new_log_file()
{
	int cnt;
	cnt = log_file_cnt.fetch_add(1)+1;

	char file_name[100];
	char buffer[10];
	int len,num,i;
	strcpy(file_name,"log");
	len = strlen(file_name);
	num = cnt+1;
	i = 0;
	while (num > 0)
	{
		buffer[i] = num%10+'0';
		i++;
		num/=10;
	}
	for (i=i-1;i>=0;i--)
		file_name[len++] = buffer[i];
	file_name[len] = 0;


	int is_pmem;
	size_t pmem_len;
	dram_array[file_max] = (unsigned char*)mmap(NULL,FILE_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
	pmem_addr[MAX_FILE_NUM-cnt] = dram_array[file_max];
	pmem_array[file_max] = (unsigned char*)pmem_map_file(file_name,FILE_SIZE,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);

	file_max++;
}
