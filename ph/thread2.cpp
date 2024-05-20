#include <stdio.h>
#include <cstring>
#include <x86intrin.h> //fence
#include <unistd.h> //usleep
#include <libpmem.h> 

#include "thread2.h"
#include "log.h"
#include "lock.h"
#include "cceh.h"
#include "skiplist.h"
#include "data2.h"

namespace PH
{

//extern int num_thread;
extern int num_query_thread;
extern int num_evict_thread;
extern int log_max;
extern DoubleLog* doubleLogList;
extern volatile unsigned int seg_free_cnt;
extern CCEH* hash_index;
extern const size_t ble_len;


// need to be private...

//PH_Thread thred_list[QUERY_THREAD_MAX+EVICT_THREAD_MAX];
PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];
PH_Evict_Thread evict_thread_list[EVICT_THREAD_MAX];

extern Skiplist* skiplist;
extern PH_List* list;
//extern const size_t MAX_LEVEL;

//---------------------------------------------- seg

unsigned int min_seg_free_cnt()
{
	int i;
	unsigned int min=999999999;
	for (i=0;i<num_query_thread;i++)
	{
		if (query_thread_list[i].run && min > query_thread_list[i].local_seg_free_cnt)
			min = query_thread_list[i].local_seg_free_cnt;
	}
	for (i=0;i<num_evict_thread;i++)
	{
		if (evict_thread_list[i].run && min > evict_thread_list[i].local_seg_free_cnt)
			min = evict_thread_list[i].local_seg_free_cnt;
	}
	if (min == 999999999)
		return seg_free_cnt;
	return min;
}

void PH_Thread::update_free_cnt()
{
		op_cnt++;
		if (op_cnt % 128 == 0)
		{
			local_seg_free_cnt = seg_free_cnt;
#ifdef wait_for_slow
			int min = min_seg_free_cnt();
			if (min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
			{
				printf("in2\n");
				while(min + FREE_SEG_LEN/2 < my_thread->local_seg_free_cnt)
				{
					update_idle();
					min = min_seg_free_cnt();
				}
					printf("out2\n");
			}
#endif
			
		}
#if 0
	int i;
	pthread_t pt;

	pt = pthread_self();
	for (i=0;i<num_of_thread;i++)
	{
		if (pthread_equal(thread_list[i].tid,pt))
		{
			thread_list[i].free_cnt = free_cnt;
			thread_list[i].seg_free_cnt = seg_free_cnt;
			return;
		}
	}
	new_thread();
#endif
}

//-------------------------------------------------


void PH_Query_Thread::init()
{

	int i;

	my_log = 0;

	for (i=0;i<log_max;i++)
	{
		while (doubleLogList[i].use == 0)
		{
//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
			if (try_at_lock2(doubleLogList[i].use))
				my_log = &doubleLogList[i];
		}
		if (my_log)
			break;
	}

	if (my_log == 0)
		printf("new query thread no log!!!\n");
	else
		printf("log allocated\n");

//	local_seg_free_cnt = min_seg_free_cnt();
//	local_seg_free_cnt = INV9;
	local_seg_free_cnt = seg_free_cnt;

	read_lock = 0;

	run = 1;
}

void PH_Query_Thread::clean()
{
	my_log->use = 0;
	my_log = NULL;

	run = 0;
}

#define INDEX

int PH_Query_Thread::insert_op(uint64_t key,unsigned char* value)
{
	update_free_cnt();

	unsigned char* new_addr;
	unsigned char* old_addr;
	unsigned char* pmem_head_p = my_log->get_pmem_head_p();
	unsigned char* dram_head_p = my_log->get_dram_head_p();

#if 0 
	unsigned char* checksum_i = (unsigned char*)&ble;
	int i,cnt=0;
	for (i=0;i<8+8+VALUE_SIZE;i++)
	{
		if (*checksum_i == 0)
			cnt++;
	}
	if (cnt == -1)
		printf("xxx\n");
#endif
	//fixed size;

	// use checksum or write twice

	// 1 write kv
	//baseLogEntry->dver = 0;
//	my_log->insert_log(&ble);

	my_log->ready_log();
	my_log->insert_pmem_log(key,value);

//	my_log->ready_log();
//	head_p = my_log->get_head_p();
/*
	BaseLogEntry *ble = (BaseLogEntry*)head_p;
	ble->key = key;
	memcpy(ble->value,value,VALUE_SIZE);
*/


	// 2 lock index ------------------------------------- lock from here

#ifdef INDEX
	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;
	kvp_p = hash_index->insert(key,&seg_lock,read_lock);
//	kvp_p = hash_index->insert_with_fail(key,&seg_lock,read_lock);
	old_addr = (unsigned char*)kvp_p->value;
#endif

	// 3 get and write new version <- persist

	uint64_t old_version,new_version;
#ifdef INDEX
	old_version = kvp_p->version;
	new_version = old_version+1;
	remove_loc_mask(new_version);
	set_prev_loc(new_version,old_version);
	set_loc_hot(new_version);
#endif
	// 4 add dram list
#ifdef USE_DRAM_CACHE
	new_addr = dram_head_p;
	my_log->insert_dram_log(new_version,key,value);
#else
	new_addr = pmem_head_p; // PMEM
#endif

	//--------------------------------------------------- make version
//if (kvp_p)
	my_log->write_version(new_version);

	// 6 update index
#ifdef INDEX
	
//	kvp_p->value = (uint64_t)my_log->get_head_p();
	kvp_p->value = (uint64_t)new_addr;
	kvp_p->version = new_version;
	_mm_sfence(); // value first!
	if (kvp_p->key != key) // not so good
	{
		kvp_p->key = key;
		_mm_sfence();
	}
	
#endif

	// 7 unlock index -------------------------------------- lock to here
#ifdef INDEX
//	if (kvp_p)
	hash_index->unlock_entry2(seg_lock,read_lock);
#endif
	// 5 add to key list if new key

	// 8 remove old dram list
#ifdef USE_DRAM_CACHE
	if (is_loc_hot(old_version))
	{
//		printf("old\n");
//		dl = (Dram_List*)old_addr; 
//		my_log->remove_dram_list(dl);
		set_invalid((uint64_t*)old_addr);
	}
#endif
	// 9 check GC

	return 0;
}
int PH_Query_Thread::read_op(uint64_t key,unsigned char* buf)
{
	update_free_cnt();

	uint64_t ret;
	hash_index->read(key,&ret);
	if (ret == 0)
		return -1;
	memcpy(buf,((unsigned char*)ret)+HEADER_SIZE+KEY_SIZE,VALUE_SIZE);

	return 0;
}
int PH_Query_Thread::delete_op(uint64_t key)
{
	update_free_cnt();

	return 0;
}
int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t end_key)
{
	update_free_cnt();

	return 0;
}
int PH_Query_Thread::next_op(unsigned char* buf)
{
	update_free_cnt();

	return 0;
}

//--------------------------------------------------------------------------------

void PH_Evict_Thread::init()
{

	int ln = log_max / num_evict_thread;

	log_cnt = 0;
	log_list = new DoubleLog*[ln];

	int i;
	for (i=0;i<log_max;i++)
	{
		while (doubleLogList[i].evict_alloc == 0)
		{
//			if (doubleLogList[i].use.compare_exchange_strong(z,1))
			if (try_at_lock2(doubleLogList[i].evict_alloc))
			{
				log_list[log_cnt++] = &doubleLogList[i];
				break;
			}
		}
		if (log_cnt >= ln)
			break;
	}

	for (i=log_cnt;i<ln;i++)
		log_list[i] = NULL;

	read_lock = 0;
	run = 1;
}

void PH_Evict_Thread::clean()
{
	run = 0;

	int i;
	for (i=0;i<log_cnt;i++)
	{
		if (log_list[i]) // don't need
			free(log_list[i]);
	}
}

void pmem_node_nt_write(Node* dst_node,Node* src_node, size_t offset, size_t len)
{
	pmem_memcpy((unsigned char*)dst_node+offset,(unsigned char*)src_node+offset,len,PMEM_F_MEM_NONTEMPORAL);
	_mm_sfence();
}


void pmem_entry_write(unsigned char* dst, unsigned char* src, size_t len)
{
	memcpy(dst,src,len);
	pmem_persist(dst,len);
	_mm_sfence();
}

const size_t PMEM_BUFFER_SIZE = 256;

void PH_Evict_Thread::warm_to_cold(Skiplist_Node* node)
{

	List_Node* ln;
	NodeMeta *nm = node->my_node;
	size_t offset = sizeof(NodeOffset);
	int cnt = 0;
	uint64_t key;
	unsigned char* addr = (unsigned char*)nm->node;
	int i;

	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;

	while (offset < NODE_SIZE)
	{
		if (nm->valid[cnt])
		{
			key = *(uint64_t*)(addr+offset+HEADER_SIZE);
			ln = node->list_node;
			while (key > ln->next->key)
				ln = ln->next;

			for (i=0;i<NODE_SLOT_SIZE;i++)
			{
				if (ln->my_node->valid[i] == false)
				{
					// lock here
					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					if (kvp_p->value != (uint64_t)addr) // moved
					{
						hash_index->unlock_entry2(seg_lock,read_lock);
						break;
					}


					pmem_entry_write((unsigned char*)ln->my_node->node + sizeof(NodeOffset) + ble_len*i , addr, ble_len);
					ln->my_node->valid[i] = true;

					// modify hash index here
//					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					kvp_p->value = (uint64_t)addr;
					_mm_sfence();
					hash_index->unlock_entry2(seg_lock,read_lock);
					

					nm->valid[cnt] = false;
					cnt++;
					break;
				}
			}
			if (i >= NODE_SLOT_SIZE) // need split
			{
				//split here
				continue;
			}
		}
		offset+=ble_len;
	}

}

void PH_Evict_Thread::hot_to_warm(Skiplist_Node* node,bool force)
{
	int i;
	LogLoc ll;
	unsigned char* addr;
	uint64_t* header;
	DoubleLog* dl;
//	unsigned char node_buffer[NODE_SIZE]; 
	size_t entry_list_cnt;

	size_t write_size=0;
	Node temp_node;
	Node* dst_node = node->my_node->node;

	uint64_t* old_torn_header = NULL;
/*
	if (node->my_node->entry_sum == 0) // impossible???
	{
		temp_node.next_offset = node->next_offset;
		write_size+=sizeof(NodeOffset);
	}
	*/
	if (node->torn_left)
	{
		dl = &doubleLogList[node->torn_entry.log_num];
		addr = dl->dramLogAddr + (ll.offset%dl->my_size);
		old_torn_header = (uint64_t*)addr;
		memcpy(temp_node.buffer+node->my_node->size,addr+node->torn_left,ble_len-node->torn_left);
		write_size+=ble_len-node->torn_left;
	}

	entry_list_cnt = node->entry_list.size();
	for (i-0;i<entry_list_cnt;i++)
	{
		ll = node->entry_list[i];
		dl = &doubleLogList[ll.log_num];
		addr = dl->dramLogAddr + (ll.offset%dl->my_size);
		header = (uint64_t*)addr;
		if (dl->tail_sum > ll.offset || is_valid(header) == false)
			continue;
		memcpy(temp_node.buffer,addr,ble_len);
		write_size+=ble_len;
	}

	// we may need to cut!!!
	if (force)
	{
		pmem_node_nt_write(dst_node, &temp_node,node->my_node->size ,write_size);
		node->torn_left = 0;
		node->torn_right = 0;
	}
	else if (write_size > 0)
	{
		node->torn_entry = ll;
		node->torn_right = write_size % PMEM_BUFFER_SIZE;
		node->torn_left = ble_len-node->torn_right;
		write_size-=node->torn_right;
		pmem_node_nt_write(dst_node, &temp_node,node->my_node->size,write_size);
		--entry_list_cnt;
	}
	node->my_node->size+=write_size;

	// invalidate

	if (old_torn_header)
		set_invalid(old_torn_header);

	for (i-0;i<entry_list_cnt;i++)
	{
		ll = node->entry_list[i];
		dl = &doubleLogList[ll.log_num];
		addr = dl->dramLogAddr + (ll.offset%dl->my_size);
		header = (uint64_t*)addr;
		if (dl->tail_sum > ll.offset || is_valid(header) == false)
			continue;
		set_invalid(header);
	}
	node->entry_list.clear();

}

int PH_Evict_Thread::try_push(DoubleLog* dl)
{

	unsigned char* addr;
	uint64_t header;
	int rv=0;

	//pass invalid
	while(dl->tail_sum+ble_len <= dl->head_sum)
	{
		addr = dl->dramLogAddr+(dl->tail_sum%dl->my_size);
		header = *(uint64_t*)addr;

		if (is_valid(header))
			break;
		dl->tail_sum+=ble_len;
		rv = 1;
	}
	return rv;
}

int PH_Evict_Thread::try_hard_evict(DoubleLog* dl)
{
//	size_t head_sum = dl->get_head_sum();
//	size_t tail_sum = dl->get_tail_sum();

	unsigned char* addr;
	uint64_t header;
	uint64_t key;
	int rv=0;

//	if (try_push(dl))
//		rv = 1;

	//check
	if (dl->tail_sum + HARD_EVICT_SPACE <= dl->head_sum)
		return rv;

	//need hard evict
	addr = dl->dramLogAddr + (dl->tail_sum % dl->my_size);
	key = *(uint64_t*)(addr+HEADER_SIZE);
	// evict now

	Skiplist_Node* prev[MAX_LEVEL+1];
	Skiplist_Node* next[MAX_LEVEL+1];
	Skiplist_Node* node;

	node = skiplist->find_node(key,prev,next);
	hot_to_warm(node,true);

//	dl->head_sum+=ble_len;

	// do we need flush?

	return 1;
}

int PH_Evict_Thread::try_soft_evict(DoubleLog* dl) // need return???
{
	unsigned char* addr;
	size_t adv_offset=0;
	uint64_t header,key;
	int rv = 0;
//	addr = dl->dramLogAddr + (dl->tail_sum%dl->my_size);

	Skiplist_Node* prev[MAX_LEVEL+1];
	Skiplist_Node* next[MAX_LEVEL+1];
	Skiplist_Node* node;
	LogLoc ll;
	
	while(dl->tail_sum+adv_offset + ble_len <= dl->head_sum && adv_offset <= SOFT_EVICT_SPACE )
	{
		addr = dl->dramLogAddr + ((dl->tail_sum + adv_offset) % dl->my_size);
		header = *(uint64_t*)addr;
		key = *(uint64_t*)(addr+HEADER_SIZE);

		if (is_valid(header))
		{
			rv = 1;
			// regist the log num and size_t
			node = skiplist->find_node(key,prev,next);

			if (node->my_node->size + node->torn_left + ble_len > NODE_SIZE) // NODE_BUFFER_SIZE???
			{
				hot_to_warm(node,true);
				warm_to_cold(node);
			}

			ll.log_num = dl->log_num;
			ll.offset = dl->tail_sum+adv_offset;
			node->entry_list.push_back(ll);
			// need to try flush
//			node->try_hot_to_warm();
			hot_to_warm(node,false);
		}

		adv_offset+=ble_len;
		// don't move tail sum
	}
	return rv;
}

int PH_Evict_Thread::evict_log(DoubleLog* dl)
{
	int diff=0;
	if (try_soft_evict(dl))
		diff = 1;
	if (try_push(dl))
		diff = 1;
	if (try_hard_evict(dl))
		diff = 1;
	if (try_push(dl))
		diff = 1;
	return diff;		
}

void PH_Evict_Thread::evict_loop()
{
	int i,done;
//	while(done == 0)
	while(true)
	{
		update_free_cnt();

		done = 1;
		for (i=0;i<log_cnt;i++)
		{
			if (evict_log(log_list[i]))
				done = 0;
		}
		if (done)
			usleep(1);
	}
}

}
