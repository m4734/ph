#include <libpmem.h>
#include <string.h>
#include <x86intrin.h>
#include <stdio.h> // test
#include <sys/mman.h> // mmap
#include <unistd.h> //usleep

#include "log.h"
#include "data2.h"
#include "thread2.h"
#include "global2.h"
#include "cceh.h"
#include "skiplist.h"
#include "recovery.h"

//#include "hash.h"

//using namespace PH;
namespace PH
{

//extern void recover_counter(uint64_t key,uint64_t version);

extern CCEH* hash_index;
extern Skiplist* skiplist;

//const size_t ble_len = sizeof(BaseLogEntry);

size_t log_size;
int log_max;
DoubleLog* doubleLogList; // should be private

//DoubleLog* WDLL;

size_t HARD_EVICT_SPACE;
size_t SOFT_EVICT_SPACE;

extern thread_local PH_Thread* my_thread;

//#define INTERLEAVE

void recover_log()
{
	int i;
	for (i=0;i<log_max;i++)
	{
		doubleLogList[i].recover();
	}
}

void init_log(int num_pmem, int num_log)
{

	printf("init log\n");

	log_max = num_pmem*num_log;
	doubleLogList = new DoubleLog[log_max];

//	log_size = LOG_SIZE_PER_PMEM/size_t(num_log);
	log_size = TOTAL_DATA_SIZE/10/(num_pmem*num_log); // TOTAL DATA SIZE / HOT RATIO / LOG NUM
#if 0
	if (log_size < 1024*1024*1024) // minimum
		log_size = 1024*1024*1024;
#endif

#if 1
	HARD_EVICT_SPACE = log_size/20; // 5%
	SOFT_EVICT_SPACE = log_size/2; // 10%
#else
	HARD_EVICT_SPACE = (ENTRY_SIZE * 1000 * 50) * 2; // 5MB
	SOFT_EVICT_SPACE = (ENTRY_SIZE * 1000 * 100) * 2; // (10MB) * x
#endif
	printf("HARD EVICT SPACE %lu\n",HARD_EVICT_SPACE);
	printf("SOFT EVICT SPACE %lu\n",SOFT_EVICT_SPACE);

	if (SOFT_EVICT_SPACE > log_size)
		printf("SOFT EVICT TOO BIG\n");

#if 0
	if (SOFT_EVICT_SPACE < 1024*1024*1024)
	{
		SOFT_EVICT_SPACE = 1024*1024*1024;
		if (SOFT_EVICT_SPACE > log_size)
			SOFT_EVICT_SPACE = log_size;

	}
#endif
	printf("LOG NUM %d LOG SIZE %lfGB SUM %lfGB\n",num_log,double(log_size)/1024/1024/1024,double(log_size)*num_log*num_pmem/1024/1024/1024);

	int i,j,cnt=0;
	for (i=0;i<num_log;i++)
	{
		for (j=0;j<num_pmem;j++) // inner loop pmem
		{
			char path[100];
			int len;
#ifdef INTERLEAVE
			sprintf(path,"/mnt/pmem0/log%d",cnt);
#else
			sprintf(path,"/mnt/pmem%d/log%d",j+1,i+1); // 1~
#endif
			len = strlen(path);
			path[len] = 0;
			doubleLogList[cnt].log_num = cnt;
			doubleLogList[cnt++].init(path,log_size,HARD_EVICT_SPACE,SOFT_EVICT_SPACE);
		}
	}
}

void clean_log()
{

	printf("clean log\n");

	int i;
	for (i=0;i<log_max;i++)
	{
		doubleLogList[i].clean();
	}

	delete[] doubleLogList; // array

	printf("clean now\n");
}

#define SKIP_MEMSET
#if 0
void DoubleLog::alloc_new_dram_pool()
{
//	printf("aaaaaa %lu\n",sizeof(Dram_List)*DRAM_LIST_UNIT);
	dram_list_pool[dram_list_pool_cnt] = (Dram_List*)malloc(sizeof(Dram_List)*DRAM_LIST_UNIT);
	++dram_list_pool_cnt;
	if (dram_list_pool_cnt >= dram_list_pool_max)
		printf("impl realloc\n"); // not now
	dram_list_pool_alloced = 0;
}

Dram_List* DoubleLog::alloc_new_dram_list()
{
	if (free_dram_list_head)
	{
		Dram_List* rv = free_dram_list_head;
		free_dram_list_head = free_dram_list_head->next;
		return rv;
	}

	if (dram_list_pool_alloced >= DRAM_LIST_UNIT)
	{
//		printf("new?\n");
		alloc_new_dram_pool();
	}
	return &dram_list_pool[dram_list_pool_cnt-1][dram_list_pool_alloced++];
}

Dram_List* DoubleLog::append_new_dram_list(uint64_t version,uint64_t key,unsigned char* value)
{
	Dram_List* dl = alloc_new_dram_list();
	dl->ble.header = version;
	dl->ble.key = key;
	memcpy(dl->ble.value,value,VALUE_SIZE);

	if (dram_list_head)
		dram_list_head->next = dl;
	dl->prev = dram_list_head;
	dl->next = NULL;
	dram_list_head = dl;

	return dl;
}

void DoubleLog::remove_dram_list(Dram_List* dl)
{
	//remove from
	if (dl->next && dl->prev)
	{
		dl->next->prev = dl->prev;
		dl->prev->next = dl->next;
	}
	else if (dl->next) // tail
	{
		dl->next->prev = NULL;
		dram_list_tail = dl->next;
	}
	else if (dl->prev) // head
	{
		dl->prev->next = NULL;
		dram_list_head = dl->prev;
	}
	else
	{
		dram_list_head = dram_list_tail = NULL;
	}

	//add to
	//prev?
	dl->next = free_dram_list_head;
	free_dram_list_head = dl;
}
#endif

void DoubleLog::init(char* filePath, size_t req_size,size_t hes,size_t ses)
{
//	tail_offset = LOG_BLOCK_SIZE;
//	head_offset = LOG_BLOCK_SIZE;

//	printf("size %lu\n",req_size);

	int is_pmem;
	pmemLogAddr = (unsigned char*)pmem_map_file(filePath,req_size,PMEM_FILE_CREATE,0777,&my_size,&is_pmem);
	if (my_size != req_size)
		printf("my_size %lu is not req_size %lu\n",my_size,req_size);
	if (is_pmem == 0)
		printf("is not pmem\n");
#if 0
#ifdef SKIP_MEMSET
	printf("----------------skip memset----------------------\n");
#else
	memset(pmemLogAddr,0,my_size);
#endif
#endif

#if 0
	head_p = pmemLogAddr;
	tail_p = pmemLogAddr;
	end_p = pmemLogAddr + my_size;

	dram_list_pool_max = LIST_POOL_UNIT;
	dram_list_pool = (Dram_List**)malloc(sizeof(Dram_List*)*dram_list_pool_max);
	dram_list_pool_cnt = 0;
	alloc_new_dram_pool();
//	dram_list_pool_alloced = 0;

	free_dram_list_head = NULL;
	dram_list_head = dram_list_tail = NULL;
#endif

	head_sum = tail_sum = 0;

#ifdef USE_DRAM_CACHE
	dramLogAddr = (unsigned char*)mmap(NULL,req_size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,-1,0);
	if (!dramLogAddr)
		printf("dram mmap error\n");
#endif


	use = 0;
	evict_alloc = 0;
	min_tail_sum = 0;

	soft_adv_offset = 0;

	hard_evict_space = hes;
	soft_evict_space = ses;

	block_cnt = 0;
}

void DoubleLog::recover() // should be last...
{
	memcpy(dramLogAddr,pmemLogAddr,my_size);

	size_t offset;
	uint64_t key,v1,v2;
	EntryHeader* header1;
	EntryHeader* header2;
	offset = 0;
	unsigned char* addr = dramLogAddr;

	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;
	volatile uint8_t read_lock;
	EntryAddr ea1,ea2;

	ea1.loc = HOT_LOG;
	ea1.file_num = log_num;

	int ow;

	SkiplistNode* skiplistNode;
	SkipAddr prev_sa_list[MAX_LEVEL+1];
	SkipAddr next_sa_list[MAX_LEVEL+1];

	while(offset < my_size)
	{
//do 
		header1 = (EntryHeader*)addr;

		if (header1->version > 0)
		{
			ow = 1;

			key = *(uint64_t*)(addr+ENTRY_HEADER_SIZE);
			kvp_p = hash_index->insert(key,&seg_lock,read_lock);

			ea2.value = kvp_p->value;

			v1 = header1->version;

			if (kvp_p->key == key)
			{
				header2 = (EntryHeader*)get_entry(ea2);
				v2 = header2->version;
				if (v1 < v2)
				{
					ow = 0;
					header1->valid = false;
				}
				else
					invalidate_entry(ea2);
			}

			if (ow)
			{
				recover_counter(key,v1);
				kvp_p->key = key;
				ea1.offset = offset;
				kvp_p->value = ea1.value;
				if (ea2.loc != HOT_LOG)
				{
					skiplistNode = skiplist->find_node(key,prev_sa_list,next_sa_list);
					skiplistNode->key_list[skiplistNode->key_list_size++] = key;
				}

			}

			hash_index->unlock_entry2(seg_lock,read_lock);

		}


		addr += LOG_ENTRY_SIZE;
		offset += LOG_ENTRY_SIZE;
	}

	head_sum = my_size-(my_size%LOG_ENTRY_SIZE);
	tail_sum = 0;
}

void DoubleLog::clean()
{
//	printf("%lu %lu\n",my_size,log_size);
#if 0
	int i;
	for (i=0;i<dram_list_pool_cnt;i++)
		free(dram_list_pool[i]);
	free(dram_list_pool);
#endif
//printf(" my size %lu\n",my_size);
#ifdef USE_DRAM_CACHE
	munmap(dramLogAddr,my_size);
#endif
	pmem_unmap(pmemLogAddr,my_size);
}
/*
void DoubleLog::check_turn(size_t &sum, size_t len)
{
	size_t offset = sum % my_size;
	if (offset+len > my_size) // check the space
		sum+=(my_size-offset);
}
*/
void DoubleLog::ready_log()//(size_t len)
{
//	check_turn(head_sum,ble_len);
	size_t offset = head_sum % my_size;
	if (offset+LOG_ENTRY_SIZE >= my_size) // check the space
		head_sum+=(my_size-offset);
#if 0
	if (min_tail_sum + my_size < head_sum + ENTRY_SIZE)
		min_tail_sum = get_min_tail(log_num);
	while(min_tail_sum + my_size < head_sum + ENTRY_SIZE)
	{
//		printf("log %d full\n",log_num);
//		printf("haed %lu\ntail %lu\nmint %lu\n",head_sum,tail_sum,min_tail_sum);
		usleep(1000*100);// sleep
//		my_thread->update_tail_sum();
		my_thread->sync_thread();
		min_tail_sum = get_min_tail(log_num);
	}
#endif
//	if (tail_sum + my_size < head_sum + ENTRY_SIZE)
//		block_cnt++;
#if 0
	while(tail_sum + my_size < head_sum + ENTRY_SIZE);
#else
	while(tail_sum + my_size < head_sum + LOG_ENTRY_SIZE)
	{
//		printf("log %d full\n",log_num);
//		printf("haed %lu\ntail %lu\nmint %lu\n",head_sum,tail_sum,min_tail_sum);
		usleep(1);// sleep
//		my_thread->update_tail_sum();
		my_thread->sync_thread();
	}
#endif


}
#if 0
void DoubleLog::insert_log(struct BaseLogEntry *baseLogEntry_p)
{
	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
	unsigned char* src = (unsigned char*)baseLogEntry_p;
	memcpy(head_p+header_size ,src+header_size ,ble_len-header_size);
	pmem_persist(head_p+header_size,ble_len-header_size);
	_mm_sfence();

}
#endif
void DoubleLog::insert_pmem_log(uint64_t key,unsigned char *value)
{
	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
	//memcpy(head_p+header_size ,src+header_size ,ble_len-header_size);
	unsigned char* head_p = pmemLogAddr + head_sum%my_size;
	memcpy(head_p+ENTRY_HEADER_SIZE, &key, sizeof(uint64_t));
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE, value, VALUE_SIZE0);
//	pmem_persist(head_p+header_size,ble_len-header_size);
	pmem_persist(head_p+ENTRY_HEADER_SIZE,KEY_SIZE+VALUE_SIZE0);
	_mm_sfence();
}
#if 0
const size_t CACHE_MASK = 0xffffffffffffffc0; // 64 // 1111...11000000
void clwb(unsigned char* addr,size_t len)
{
	int i;
	unsigned char* start = (addr & CACHE_MASK);
	if (len % 64 > 0)
		len+=64;
	for (i=0;i<len;i+=64)
		_mm_clwb((void*)(start+i));
}
#endif
void DoubleLog::insert_dram_log(uint64_t version, uint64_t key,unsigned char *value)
{
	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
	//memcpy(head_p+header_size ,src+header_size ,ble_len-header_size);
	unsigned char* head_p = dramLogAddr + head_sum%my_size;
	memcpy(head_p,&version,ENTRY_HEADER_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE, &key, KEY_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE, value, VALUE_SIZE0);

//	_mm_clwb();
//	clwb(head_p,ENTRY_SIZE);
	_mm_sfence();
}

void DoubleLog::insert_dram_log(uint64_t version, uint64_t key,unsigned char *value,NodeAddr* warm_cache)
{
	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
	//memcpy(head_p+header_size ,src+header_size ,ble_len-header_size);
	unsigned char* head_p = dramLogAddr + head_sum%my_size;
	memcpy(head_p,&version,ENTRY_HEADER_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE, &key, KEY_SIZE);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE, value, VALUE_SIZE0);
	memcpy(head_p+ENTRY_HEADER_SIZE+KEY_SIZE+VALUE_SIZE0,warm_cache,sizeof(NodeAddr));

//	_mm_clwb();
//	clwb(head_p,ENTRY_SIZE);
	_mm_sfence();
}


#define VERSION

void DoubleLog::write_version(uint64_t version)
{
#ifdef VERSION
	memcpy(pmemLogAddr+head_sum%my_size ,&version,header_size);
	pmem_persist(pmemLogAddr + head_sum%my_size ,header_size);
	_mm_sfence();
#endif
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

#if 0
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
#endif
}

