#include <libpmem.h>
#include <string.h>
#include <x86intrin.h>

#include "log.h"
#include "data.h"
#include "hash.h"

//using namespace PH;
#ifdef DOUBLE_LOG
namespace PH
{

// len key value file offset index
#define LF_SIZE 2+2+2

extern unsigned char** pmem_addr;

std::atomic<uint8_t> log_file_cnt;

void init_log()
{
//	global_ts = 0;
	log_file_cnt = 0;
}

void LOG::init()
{
	file_max = 0;
	file_index = 0;
//	kv_in_offset = {INIT_OFFSET,0,0};
//	kv_out_offset = {INIT_OFFSET,0,0};

	new_log_file();
	new_log_file();

	kv_in = {0,0};
	kv_out = {0,0};
}

void LOG::clean()
{
	int i;
	for (i=0;i<file_max;i++)
	{
		munmap(pmem_addr[dram_num[i]],FILE_SIZE);
		pmem_unmap(pmem_array[i],FILE_SIZE);
	}
}

// point hash - lock
// (range hash)
// insert log
// flush log - point rehash
// split?
#if 0
void LOG::insert_log(unsigned char* &key_p, unsigned char* &value_p,int value_len)
{
	ValueEntry_u ve_u,old_ve_u;
	volatile uint64_t* v64_p;
	void* unlock;
	Node_offset start_offset,locked_offset;

	v64_p = find_or_insert_point_entry(key_p,&unlock);
	old_ve_u.ve_64 = *v64_p;
	while(old_ve_u.ve.node_offset != INIT_OFFEST)
	{
		start_offset = get_start_offset(old_ve_u.ve.node_offset);
		if (inc_ref(start_offset))
		{
			ve_u.ve.node_offset = start_offset;
			break;
		}
		unlock_entry(unlock);
		v64_p = find_or_insert_point_entry(key_p,&unlock);
		old_ve_u.ve_64 = *v64_p;
	}

	int continue_len = 0;
	if (ve_u.ve.node_offset == INIT_OFFSET)
	{
		while(1)
		{
			if ((ve_u.ve.node_offset = find_range_entry2(key_p,&continue_len)) == INIT_OFFSET)
				continue;
			if (inc_ref(ve_u.ve.node_offset))
			{
				old_ve_u.ve.node_offset = INIT_OFFSET;
				break;
			}
		}
	}

	locked_offset = ve_u.ve.node_offset;

}
#endif
ValueEntry LOG::insert_log(Node_offset start_node_offset,unsigned char* &key_p, unsigned char* &value_p,int value_len)
{
	ValueEntry rv;
	int entry_size = len_size + key_size + value_len;
	LogOffset kv_in_next = kv_in;

	if (entry_size%2)
		++entry_size;

	kv_in_next.offset+=entry_size+LF_SIZE;

	if (kv_in_next.offset+len_size >= FILE_SIZE)
	{
		kv_in_next.offset = entry_size+LF_SIZE;
		kv_in_next.file++;
		if (kv_in_next.file >= file_max)
		{
			if (kv_in_next.file * LOG_RATIO * num_of_thread  <= file_num)
				new_log_file();
			else
				kv_in_next.file = 0;
		}
		kv_in.file = kv_in_next.file;
		kv_in.offset = 0;
	}

	Node_offset end_offset = offset_to_node(start_node_offset)->end_offset;
	Node_meta* end_meta = offset_to_node(end_offset);

	if (end_meta->size + end_meta->flush_size + entry_size + len_size >= NODE_BUFFER)
	{
		// split compact or append
		if (end_meta->part == PART_MAX-1)
		{
//			Node_offset start_offset = meta->start_offset;
			/*
			if (split_or_compact(start_node_offset))
				split(start_node_offset);
			else
				compact(start_node_offset);
				*/
			rv.len = 0;
			return rv;
		}
		else
		{
			end_offset = append_node(start_node_offset);
			end_meta = offset_to_node(end_offset);
			// continue insert
		}
	}

	uint16_t kv_offset = sizeof(Node_offset)*3 + end_meta->size + end_meta->flush_size; // meta + [buffer]...
	uint16_t z = 0;
	uint16_t vl16 = value_len;
	unsigned char* const dest = pmem_addr[dram_num[kv_in.file]]+kv_in.offset;


	{
		memcpy(dest,&vl16,len_size);
		memcpy(dest+len_size,key_p,key_size);
		memcpy(dest+len_size+key_size,value_p,value_len);
		memcpy(dest+entry_size,&end_offset,sizeof(Node_offset));
		memcpy(dest+entry_size+sizeof(Node_offset),&kv_offset,sizeof(uint16_t));
		memcpy(dest+entry_size+sizeof(Node_offset)+sizeof(uint16_t),&z,sizeof(uint16_t));

		pmem_memcpy(pmem_array[kv_in.file]+kv_in.offset+len_size,dest+len_size,entry_size+LF_SIZE+len_size,PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
		pmem_memcpy(pmem_array[kv_in.file]+kv_in.offset,dest,len_size,PMEM_F_MEM_NONTEMPORAL);
		_mm_sfence();
	}


	rv.node_offset.file = dram_num[kv_in.file];
	rv.node_offset.offset = kv_in.offset / sizeof(Node);
	rv.kv_offset = kv_in.offset % sizeof(Node);
	rv.len = value_len;

	end_meta->flush_size+=entry_size;
	Node_meta* start_meta = offset_to_node(end_meta->start_offset);
	start_meta->group_size+=entry_size;
	
	if (end_meta->flush_cnt == end_meta->flush_max)
	{
		end_meta->flush_max*=2;
		end_meta->flush_kv = (unsigned char**)realloc(end_meta->flush_kv,sizeof(unsigned char*)*end_meta->flush_max);
	}
	end_meta->flush_kv[end_meta->flush_cnt++] = dest;

	kv_in = kv_in_next;

	return rv;
}
ValueEntry LOG::insert_log(Node_offset node_offset,unsigned char* &key_p, unsigned char* &value_p)
{
	return insert_log(node_offset,key_p,value_p,value_size);
}
void LOG::ready(int value_len)
{
	int size = len_size + key_size + value_len + LF_SIZE;

//	Node_offset kv_in2;
	LogOffset kv_in2;
	int t_file,t_offset;
	t_file = kv_in.file;
	t_offset = kv_in.offset;
	t_offset+=size;

	if (t_offset % 2)
		++t_offset;

	if (t_offset >= FILE_SIZE)
	{
		t_offset = size;
		if (t_offset % 2)
			++t_offset;
		t_file++;
		if (t_file >= file_max)
			t_file = 0;
	}
	kv_in2.file = t_file;
	kv_in2.offset = t_offset;

	int rv;
	unsigned char* kvp;
	uint16_t vl16;
	Node_offset node_offset;
	Node_offset start_offset;
	const int default_size = len_size + key_size + LF_SIZE;
	while (
			(kv_in2.file == kv_out.file && kv_in2.offset < kv_out.offset) ||
			(((kv_in2.file+1)%file_max) == kv_out.file && kv_in2.offset > kv_out.offset)
	      )
	{
		kvp = (unsigned char*)pmem_addr[dram_num[kv_out.file]] + kv_out.offset;
		vl16 = *((uint16_t*)kvp);
		if (vl16 & INV_BIT)
		{
			vl16-=INV_BIT;
			kv_out.offset+=default_size+vl16;
			if (kv_out.offset % 2)
				++kv_out.offset;
		}
		else if(vl16 == 0)
		{
			kv_out.file = (kv_out.file+1)%file_max;
			kv_out.offset = 0;
		}
		else
		{
			node_offset = *((Node_offset*)(kvp+len_size+key_size+vl16));
			start_offset = get_start_offset(node_offset);

			while(1)
			{
				if (*((uint16_t*)kvp) & INV_BIT)
					break;
//				node_offset.file = *((uint16_t*)(kvp + len_size + key_size + vl16));
//				node_offset.offset = *((uint16_t*)(kvp + len_size + key_size + vl16 + sizeof(uint16_t)));
				if (inc_ref(start_offset))
				{
					rv = flush(node_offset);
					if (rv == 1) // flush success
					{
						dec_ref(start_offset);
						break;
					}
					else if (rv == 0) // split success
						break;
					else // split fail and retry
						dec_ref(start_offset);
				}
			}
			kv_out.offset+=default_size+vl16;
			if (kv_out.offset % 2)
				++kv_out.offset;
		}
	}
}
void LOG::new_log_file()
{
	int cnt;
	cnt = log_file_cnt.fetch_add(1)+1;

	char file_name[100];
	char buffer[10];
	int len,num,i;
	strcpy(file_name,pmem_file);
	strcat(file_name,"log");
	len = strlen(file_name);
	num = cnt;
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
//	dram_array[file_max] = (unsigned char*)mmap(NULL,FILE_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
//	pmem_addr[MAX_FILE_NUM-cnt] = dram_array[file_max];
	pmem_addr[MAX_FILE_NUM-cnt] = (unsigned char*)mmap(NULL,FILE_SIZE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
	dram_num[file_max] = MAX_FILE_NUM-cnt;
	pmem_array[file_max] = (unsigned char*)pmem_map_file(file_name,FILE_SIZE,PMEM_FILE_CREATE,0777,&pmem_len,&is_pmem);

	file_max++;
}
}

#endif
