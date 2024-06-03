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

extern size_t HARD_EVICT_SPACE;
extern size_t SOFT_EVICT_SPACE;

//extern int num_thread;
extern int num_query_thread;
extern int num_evict_thread;
extern int log_max;
extern DoubleLog* doubleLogList;
//extern volatile unsigned int seg_free_cnt;
//extern std::atomic<uint32_t> seg_free_head;
extern CCEH* hash_index;
extern const size_t ble_len;


// need to be private...

//PH_Thread thred_list[QUERY_THREAD_MAX+EVICT_THREAD_MAX];
PH_Query_Thread query_thread_list[QUERY_THREAD_MAX];
PH_Evict_Thread evict_thread_list[EVICT_THREAD_MAX];

extern Skiplist* skiplist;
extern PH_List* list;
//extern const size_t MAX_LEVEL;

union EntryAddr
{
	struct
	{
		size_t loc : 2;
		size_t file_num : 14;
		size_t offset : 48; 
	};
	uint64_t value;
};
#if 0
//---------------------------------------------- seg

unsigned int min_seg_free_cnt()
{
	int i,mi;
	unsigned int min=999999999;
	for (i=0;i<num_query_thread;i++)
	{
		if (query_thread_list[i].run && min > query_thread_list[i].local_seg_free_head)
		{
			min = query_thread_list[i].local_seg_free_head;
			mi = i;
		}
	}
	for (i=0;i<num_evict_thread;i++)
	{
		if (evict_thread_list[i].run && min > evict_thread_list[i].local_seg_free_head)
		{
			min = evict_thread_list[i].local_seg_free_head;
			mi = i + num_query_thread;
		}
	}
	if (min == 999999999)
		return seg_free_head; // what happend?
	if (mi < num_query_thread)
		query_thread_list[mi].update_request = 1;
	else
		evict_thread_list[mi-num_query_thread].update_request = 1;
	return min;
}
#endif
size_t get_min_tail(int log_num)
{
	int i,mi;
	size_t min = 0xffffffffffffffff;
	for (i=0;i<num_query_thread;i++)
	{
		if (query_thread_list[i].run && min > query_thread_list[i].recent_log_tails[log_num])
		{
			min = query_thread_list[i].recent_log_tails[log_num];
			mi = i;
		}
	}
		query_thread_list[mi].update_request = 1;
	return min;
}

void PH_Thread::op_check()
{
	++op_cnt;
	if (op_cnt % 128 == 0 || update_request) // 128?
		sync_thread();
}

void PH_Thread::sync_thread()
{
//	update_free_cnt();
	update_tail_sum();	
	update_request = 0;
}

void PH_Thread::update_tail_sum()
{
	int i;
	for (i=0;i<log_max;i++)
		recent_log_tails[i] = doubleLogList[i].tail_sum;
}
#if 0
void PH_Thread::update_free_cnt()
{
			local_seg_free_head = seg_free_head;
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
#endif
//-------------------------------------------------


void PH_Query_Thread::init()
{

	int i;

	my_log = 0;
	exit = 0;

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

//	my_log->my_thread = this;

//	local_seg_free_cnt = min_seg_free_cnt();
//	local_seg_free_cnt = INV9;
//	local_seg_free_head = seg_free_head;
	hash_index->thread_local_init();

//	recent_log_tails = new size_t[log_num];

	read_lock = 0;

	run = 1;
}

void PH_Query_Thread::clean()
{
	my_log->use = 0;
	my_log = NULL;

	hash_index->thread_local_clean();
	run = 0;
	read_lock = 0;

//	delete recent_log_tails;
}

#define INDEX

int PH_Query_Thread::insert_op(uint64_t key,unsigned char* value)
{
//	update_free_cnt();
	op_check();

//	unsigned char* new_addr;
//	unsigned char* old_addr;
	uint64_t new_addr,old_addr;
	EntryAddr ea;
	unsigned char* pmem_head_p;// = my_log->get_pmem_head_p();
	unsigned char* dram_head_p;// = my_log->get_dram_head_p();

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

	pmem_head_p = my_log->pmemLogAddr + my_log->head_sum%my_log->my_size;
//	dram_head_p = my_log->dramLogAddr + my_log->head_sum%my_log->my_size;

	ea.loc = 1;
	ea.file_num = my_log->log_num;
	ea.offset = my_log->head_sum%my_log->my_size;

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
	old_addr = kvp_p->value;
#endif

	// 3 get and write new version <- persist

	uint64_t old_version,new_version;
#ifdef INDEX
	old_version = kvp_p->version;
	new_version = old_version+1;
	new_version = set_loc_hot(new_version);
	set_valid(new_version);
#endif
	// 4 add dram list
#ifdef USE_DRAM_CACHE
//	new_addr = dram_head_p;
	new_addr = ea.value;

	my_log->insert_dram_log(new_version,key,value);
#else
	new_addr = pmem_head_p; // PMEM
#endif

	//--------------------------------------------------- make version
//if (kvp_p)
	my_log->write_version(new_version);
	my_log->head_sum+=ble_len;

	// 6 update index
#ifdef INDEX
bool new_key;	
//	kvp_p->value = (uint64_t)my_log->get_head_p();
	kvp_p->value = new_addr;
	kvp_p->version = new_version;
	_mm_sfence(); // value first!
	if (kvp_p->key != key) // not so good
	{
		new_key = true;
		kvp_p->key = key;
		_mm_sfence();
	}
	else
		new_key = false;	
#endif

	// 7 unlock index -------------------------------------- lock to here
#ifdef INDEX
//	if (kvp_p)
	hash_index->unlock_entry2(seg_lock,read_lock);
#endif
	// 5 add to key list if new key

	// 8 remove old dram list
#ifdef USE_DRAM_CACHE
	if (new_key)
		return 0;
	EntryAddr old_ea;
	unsigned char* addr;
	old_ea.value = old_addr;
	if (old_ea.loc == 1) // hot log
	{
		addr = doubleLogList[old_ea.file_num].dramLogAddr + old_ea.offset;
		set_invalid((uint64_t*)addr); // invalidate
	}
	else // warm or cold
	{
		size_t offset_in_node;
		NodeMeta* nm;
		int cnt;
		int node_cnt;

		offset_in_node = old_ea.offset % NODE_SIZE;
		node_cnt = old_ea.offset/NODE_SIZE;
		nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[old_ea.file_num]+node_cnt*sizeof(NodeMeta));
		cnt = (offset_in_node-sizeof(NodeAddr))/ble_len;
		nm->valid[cnt] = false; // invalidate
		--nm->valid_cnt;

	}

/*
	if (is_loc_hot(old_version))
	{
//		dl = (Dram_List*)old_addr; 
//		my_log->remove_dram_list(dl);
		set_invalid((uint64_t*)old_addr);
	}
	*/
#endif
	// 9 check GC

	return 0;
}
int PH_Query_Thread::read_op(uint64_t key,unsigned char* buf)
{
//	update_free_cnt();
	op_check();

//	uint64_t ret;
//	hash_index->read(key,&ret);

	EntryAddr ea;
	hash_index->read(key,&ea.value);

	if (ea.value == 0)
		return -1;

	unsigned char* addr;
	if (ea.loc == 1) // hot
	{
		addr = doubleLogList[ea.file_num].dramLogAddr + ea.offset;
		memcpy(buf,addr+HEADER_SIZE+KEY_SIZE,VALUE_SIZE);
	}
	else // warm cold
	{
		NodeMeta* nm;
		int node_cnt;

		while(true)
		{
	
		node_cnt = ea.offset/NODE_SIZE;
		nm = (NodeMeta*)(nodeAllocator->nodeMetaPoolList[ea.file_num]+node_cnt*sizeof(NodeMeta));
		at_lock2(nm->lock);
		addr = (unsigned char*)nodeAllocator->nodePoolList[ea.file_num]+ea.offset;
		if (key == *(uint64_t*)(addr+HEADER_SIZE))
			break;
		at_unlock2(nm->lock);

	hash_index->read(key,&ea.value);//retry

		}
		memcpy(buf,addr+HEADER_SIZE+KEY_SIZE,VALUE_SIZE);
		at_unlock2(nm->lock);
	}


	return 0;
}
int PH_Query_Thread::delete_op(uint64_t key)
{
//	update_free_cnt();
	op_check();

	return 0;
}
int PH_Query_Thread::scan_op(uint64_t start_key,uint64_t end_key)
{
//	update_free_cnt();
	op_check();

	return 0;
}
int PH_Query_Thread::next_op(unsigned char* buf)
{
//	update_free_cnt();
	op_check();

	return 0;
}

//--------------------------------------------------------------------------------

void PH_Evict_Thread::init()
{
	exit = 0;

	int ln = (log_max-1) / num_evict_thread+1;

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

	hash_index->thread_local_init();
	read_lock = 0;
	run = 1;
}

void PH_Evict_Thread::clean()
{
	hash_index->thread_local_clean();
	run = 0;
	read_lock = 0;

	delete[] log_list;
}
/*
void pmem_node_nt_write(DataNode* dst_node,DataNode* src_node, size_t offset, size_t len)
{
	pmem_memcpy((unsigned char*)dst_node+offset,(unsigned char*)src_node+offset,len,PMEM_F_MEM_NONTEMPORAL);
	_mm_sfence();
}
*/
void pmem_nt_write(unsigned char* dst_addr,unsigned char* src_addr, size_t len)
{
	pmem_memcpy(dst_addr,src_addr,len,PMEM_F_MEM_NONTEMPORAL);
	_mm_sfence();
}


void pmem_entry_write(unsigned char* dst, unsigned char* src, size_t len)
{
	// need version clean - kv write - version write ...
	memcpy(dst,src,len);
	pmem_persist(dst,len);
	_mm_sfence();
}
void pmem_next_write(DataNode* dst_node,NodeAddr nodeAddr)
{
	dst_node->next_offset = nodeAddr;
	pmem_persist(dst_node,sizeof(NodeAddr));
	_mm_sfence();
}

const size_t PMEM_BUFFER_SIZE = 256;

void PH_Evict_Thread::split_listNode(ListNode *listNode,SkiplistNode *skiplistNode)
{

	DataNode *list_dataNode = nodeAddr_to_node(listNode->data_node_addr);
	NodeMeta *list_nodeMeta = nodeAddr_to_nodeMeta(listNode->data_node_addr);

	if (list_nodeMeta->valid_cnt == 0)
		return;

				DataNode temp_node = *list_dataNode; // move to dram
				DataNode temp_new_node;
				uint64_t half_key;
				uint64_t key;

				size_t offset;
				size_t offset2;
				unsigned char* addr;
				addr = (unsigned char*)&temp_node;
				offset = sizeof(NodeAddr);
				offset2 = sizeof(NodeAddr);
				
				half_key = find_half_in_node(list_nodeMeta,&temp_node);
				if (half_key == listNode->key)
					return; // fail -- all same key?

				ListNode* new_listNode = list->alloc_list_node();
				NodeMeta* new_nodeMeta = nodeAddr_to_nodeMeta(new_listNode->data_node_addr);
	DataNode* new_dataNode = nodeAddr_to_node(new_listNode->data_node_addr);

				new_listNode->key = half_key;
	int moved_idx[NODE_SLOT_MAX];
	int moved_cnt=0;
	int i;
				for (i=0;i<NODE_SLOT_MAX;i++)
				{
					if (list_nodeMeta->valid[i] == false)
					{
//						new_nodeMeta->valid[i] = false;
						offset+=ble_len;
						continue;
					}
					key = *(uint64_t*)(addr+offset+HEADER_SIZE);
					if (key >= half_key)
					{
						new_nodeMeta->valid[moved_cnt] = true;
						memcpy((unsigned char*)&temp_new_node+offset2,(unsigned char*)&temp_node+offset,ble_len);
						//need invalicdation
						moved_idx[moved_cnt] = i;
						++moved_cnt;

//						++new_nodeMeta->valid_cnt;
//						--list_nodeMeta->valid_cnt;

						offset2+=ble_len;
					}
					offset+=ble_len;
				}
	new_nodeMeta->valid_cnt = moved_cnt;

				temp_new_node.next_offset = list_nodeMeta->next_p->my_offset;
//				pmem_node_nt_write(nodeAddr_to_node(new_listNode->data_node_addr),&temp_new_node,0,NODE_SIZE);
				pmem_nt_write((unsigned char*)new_dataNode,(unsigned char*)&temp_new_node,offset2);
				new_nodeMeta->next_p = list_nodeMeta->next_p;

				//link
				pmem_next_write(list_dataNode,new_nodeMeta->my_offset);
				list_nodeMeta->next_p = new_nodeMeta;

	while(skiplistNode->my_listNode->key < half_key)// && skiplistNode->key >= half_key)
	{
		if (skiplistNode->key >= half_key)
			skiplistNode->my_listNode = new_listNode;
		skiplistNode = skiplistNode->next[0];
	}

if (listNode->key >= new_listNode->key)
	{
		printf("?------------------------\n");
	}

				list->insert_node(listNode,new_listNode);
	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;
	EntryAddr ea;
				for (i=0;i<moved_cnt;i++)
				{
					key = *(uint64_t*)(temp_new_node.buffer + ble_len*i);
					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
					ea.loc = 3; // cold
					ea.file_num = list_nodeMeta->my_offset.pool_num;
					ea.offset = list_nodeMeta->my_offset.node_offset*NODE_SIZE + sizeof(NodeAddr) + ble_len*moved_idx[i];
					if (kvp_p->value == ea.value) // doesn't moved
					{
						// ea cold
							ea.file_num = new_nodeMeta->my_offset.pool_num;
						ea.offset = new_nodeMeta->my_offset.node_offset*NODE_SIZE * sizeof(NodeAddr) + ble_len*i;
						kvp_p->value = ea.value;
						kvp_p->version = set_loc_cold(kvp_p->version);
					}
					else
						new_nodeMeta->valid[i] = false; // moved to hot
					_mm_sfence(); // need this?
					hash_index->unlock_entry2(seg_lock,read_lock);


					list_nodeMeta->valid[moved_idx[i]] = false;
				}
	list_nodeMeta->valid_cnt-=moved_cnt;


}

void PH_Evict_Thread::warm_to_cold(SkiplistNode* node)
{
//	printf("warm_to_cold\n");
	ListNode* listNode;
	NodeMeta *nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
	DataNode dataNode = *nodeAddr_to_node(node->data_node_addr); // dram copy
	size_t src_offset = sizeof(NodeAddr);
	int cnt = 0;
	uint64_t key;
	unsigned char* addr = (unsigned char*)&dataNode;
	EntryAddr old_ea,new_ea;
	int i;
	int slot_idx;

	KVP* kvp_p;
	std::atomic<uint8_t> *seg_lock;

	old_ea.loc = 2; // warm
	old_ea.file_num = node->data_node_addr.pool_num;
//	old_ea.offset = node->data_node_addr.offset*NODE_SIZE + sizeof(NodeAddr);

	new_ea.loc = 3; // cold

	//while (offset < NODE_SIZE)
	cnt = 0;
	while (cnt < nodeMeta->slot_cnt)
//	for (cnt=0;cnt<nodeMeta->slot_cnt;cnt++)
	{
		if (nodeMeta->valid[cnt] == false)
		{
			++cnt;
		src_offset+=ble_len;
			continue;
		}

			key = *(uint64_t*)(addr+src_offset+HEADER_SIZE);
			listNode = node->my_listNode;
			while (key > listNode->next->key)
				listNode = listNode->next;

			NodeMeta* list_nodeMeta = nodeAddr_to_nodeMeta(listNode->data_node_addr);
			DataNode* list_dataNode = nodeAddr_to_node(listNode->data_node_addr);
			new_ea.file_num = listNode->data_node_addr.pool_num;
//			new_ea.offset = ln->data_node_addr.offset*NODE_SIZE;

			at_lock2(list_nodeMeta->lock);//-------------------------------------lock dst cold

			for (slot_idx=0;slot_idx<NODE_SLOT_MAX;slot_idx++)
			{
				if (list_nodeMeta->valid[slot_idx] == false)
					break;
			}
			if (slot_idx < NODE_SLOT_MAX)
				{
					old_ea.offset = node->data_node_addr.node_offset*NODE_SIZE + src_offset;
					new_ea.offset = listNode->data_node_addr.node_offset*NODE_SIZE + sizeof(NodeAddr) + ble_len*slot_idx;
					// lock here
					kvp_p = hash_index->insert(key,&seg_lock,read_lock);
//					if (kvp_p->value != (uint64_t)addr) // moved
					if (kvp_p->value != old_ea.value)
					{
//						EntryAddr test_entryAddr;
//						test_entryAddr.value= kvp_p->value;

						hash_index->unlock_entry2(seg_lock,read_lock); // unlock
						at_unlock2(list_nodeMeta->lock);
						++cnt;
						src_offset+=ble_len;
						continue; 
					}


					pmem_entry_write((unsigned char*)list_dataNode + sizeof(NodeAddr) + ble_len*slot_idx , addr + src_offset, ble_len);
					list_nodeMeta->valid[slot_idx] = true; // validate
					++list_nodeMeta->valid_cnt;

					// modify hash index here
//					kvp_p = hash_index->insert(key,&seg_lock,read_lock);

//					set_loc_cold(kvp_p->version);
					kvp_p->version = set_loc_cold(kvp_p->version);
//					kvp_p->value = (uint64_t)addr;
					kvp_p->value = new_ea.value;
					_mm_sfence();
					hash_index->unlock_entry2(seg_lock,read_lock);
					
//					nodeMeta->valid[cnt] = false; //invalidate
					// not here...
					++cnt;
		src_offset+=ble_len;
				}
//			if (i >= NODE_SLOT_MAX) // need split
			else // cold split
			{
				//split cold here
				//find half

				split_listNode(listNode,node); // we have the lock
				// retry
			}
			at_unlock2(list_nodeMeta->lock);//-----------------------------------------unlock dst cold
	}

	// split or init warm
	at_lock2(nodeMeta->lock);

	if (nodeMeta->valid_cnt)
	{
	SkiplistNode* new_skipNode = NULL;

	new_skipNode = skiplist->alloc_sl_node();

	if (new_skipNode) // warm split
	{
		uint64_t half_key;
		half_key = find_half_in_node(nodeMeta,&dataNode);

		if (half_key > node->key)
		{
		new_skipNode->key = half_key;

		ListNode* list_node = node->my_listNode;
		while(list_node->next->key < half_key)
			list_node = list_node->next;
		new_skipNode->my_listNode = list_node;

		SkiplistNode* prev[MAX_LEVEL+1];
		SkiplistNode* next[MAX_LEVEL+1];
		skiplist->insert_node(new_skipNode,prev,next);
		}
		else
			skiplist->free_sl_node(new_skipNode);

	}
	}
	/*
	else // warm init
	{
		// may need memset...
		// hot->cold...
	}
	*/

	//init nodeMeta
		for (i=0;i<nodeMeta->slot_cnt;i++) // vinalifd warm
			nodeMeta->valid[i] = false;
		nodeMeta->written_size = 0;
		nodeMeta->slot_cnt = 0;
		nodeMeta->valid_cnt = 0;

	at_unlock2(nodeMeta->lock);

}

void PH_Evict_Thread::hot_to_warm(SkiplistNode* node,bool force)
{
//	printf("hot to warn\n");
	int i;
	LogLoc ll;
	unsigned char* addr;
	uint64_t* header;
	DoubleLog* dl;
//	unsigned char node_buffer[NODE_SIZE]; 
	size_t entry_list_cnt;


	NodeMeta* nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);
	at_lock2(nodeMeta->lock);//------------------------------------------------lock here

//	Node temp_node;
	DataNode* dst_node = nodeAddr_to_node(node->data_node_addr);

	uint64_t* old_torn_header = NULL;
	EntryAddr old_torn_addr;
	size_t old_torn_right = node->torn_right;
	bool moved[NODE_SLOT_MAX] = {false,};

	size_t write_size=0;
	unsigned char* buffer_write_start = temp_node.buffer+nodeMeta->written_size;

	std::atomic<uint8_t>* seg_lock;
/*
	if (node->my_node->entry_sum == 0) // impossible???
	{
		temp_node.next_offset = node->next_offset;
		write_size+=sizeof(NodeOffset);
	}
	*/
	if (node->torn_left) // prepare torn right
	{
		dl = &doubleLogList[node->torn_entry.log_num];
		addr = dl->dramLogAddr + (ll.offset%dl->my_size);
		old_torn_header = (uint64_t*)addr;
		old_torn_addr.loc = 0;
		old_torn_addr.file_num = node->torn_entry.log_num;
		old_torn_addr.offset = ll.offset%dl->my_size;
		memcpy(buffer_write_start,addr+node->torn_left,node->torn_right); // cop torn wright
		write_size+=node->torn_right;
	}

	unsigned char* old_addr[100]; //test ------------------------

	entry_list_cnt = node->entry_list.size();
	for (i=0;i<entry_list_cnt;i++)
	{
		ll = node->entry_list[i];
		dl = &doubleLogList[ll.log_num];
		addr = dl->dramLogAddr + (ll.offset%dl->my_size);

		old_addr[i] = addr; // test--------------------

		header = (uint64_t*)addr;
		if (dl->tail_sum > ll.offset || is_valid(header) == false)
		{
			node->entry_list[i].log_num = -1; // invalid
			continue;
		}
		//test
		uint64_t key;
		key = *(uint64_t*)(addr+HEADER_SIZE);
		if (key < node->key)
		{
			printf("key is smaller than node key\n");
		}

		memcpy(buffer_write_start+write_size,addr,ble_len);
		write_size+=ble_len;
	}

	// we may need to cut!!!
	if (force)
	{
		pmem_nt_write(dst_node->buffer + nodeMeta->written_size , buffer_write_start , write_size);
		node->torn_left = 0;
		node->torn_right = 0;
	}
	else if (write_size > 0)
	{
		node->torn_entry = ll;
		node->torn_right = (sizeof(NodeAddr)+nodeMeta->written_size+write_size) % PMEM_BUFFER_SIZE;
		node->torn_left = ble_len-node->torn_right;
		write_size-=node->torn_right;
//		pmem_node_nt_write(dst_node, &temp_node,nodeMeta->size,write_size);
		pmem_nt_write(dst_node->buffer + nodeMeta->written_size , buffer_write_start , write_size);
		--entry_list_cnt; // for torn
	}

	// invalidate from here

	_mm_sfence();

	KVP* kvp_p;
	size_t key;
//	unsigned char* dst_addr = (unsigned char*)dst_node + nodeMeta->size;
	EntryAddr dst_addr,src_addr;
	dst_addr.loc = 2; // warm	
	dst_addr.file_num = nodeMeta->my_offset.pool_num;
	dst_addr.offset = nodeMeta->my_offset.node_offset * NODE_SIZE + sizeof(NodeAddr) + nodeMeta->written_size;

	if (old_torn_header)
	{
		key = *(uint64_t*)((unsigned char*)old_torn_header+HEADER_SIZE);
		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
		if (kvp_p->value == old_torn_addr.value)
		{
			kvp_p->value = dst_addr.value;
			kvp_p->version = set_loc_warm(kvp_p->version);
			nodeMeta->valid[nodeMeta->slot_cnt++] = true; // validate
		}
		else
			nodeMeta->valid[nodeMeta->slot_cnt++] = false; // validate fail

		hash_index->unlock_entry2(seg_lock,read_lock);
		set_invalid(old_torn_header); // invalidate
//		dst_addr.offset+=ble_len;
		dst_addr.offset+=old_torn_right;
	}

	src_addr.loc = 1; //hot

	for (i=0;i<entry_list_cnt;i++)
	{
		ll = node->entry_list[i];
		if (ll.log_num == -1)
			continue;
		dl = &doubleLogList[ll.log_num];
		addr = dl->dramLogAddr + (ll.offset%dl->my_size);
		header = (uint64_t*)addr;
		key = *(uint64_t*)(addr+HEADER_SIZE);
//		if (dl->tail_sum > ll.offset || is_valid(header) == false)
//			continue;

		src_addr.file_num = ll.log_num;
		src_addr.offset = ll.offset%dl->my_size;

		kvp_p = hash_index->insert(key,&seg_lock,read_lock);
		if (kvp_p->value == src_addr.value)
		{
			kvp_p->value = dst_addr.value;
			kvp_p->version = set_loc_warm(kvp_p->version);
			nodeMeta->valid[nodeMeta->slot_cnt++] = true; // validate
			++nodeMeta->valid_cnt;
		}
		else
			nodeMeta->valid[nodeMeta->slot_cnt++] = false; // validate fail

		hash_index->unlock_entry2(seg_lock,read_lock);
		set_invalid(header); // invalidate

		if (is_valid((uint64_t*)addr)) // test code
			printf("valid erorr\n");

		dst_addr.offset+=ble_len;
	}

	nodeMeta->written_size+=write_size;


	//test--------------------------------------
#if 0
	for (i=0;i<node->entry_list.size();i++)
	{
		addr = old_addr[i];
		header = (uint64_t*)addr;
		key = *(uint64_t*)(addr+HEADER_SIZE);
		if (is_valid(header))
			printf("hot to warm validerror\n");

	}
#endif

	if (node->entry_list.size() * ble_len != node->entry_size_sum) // test
	{
		printf("missmatch %lu %lu\n",node->entry_list.size() * ble_len, node->entry_size_sum);
	}

	node->entry_list.clear();
	node->entry_size_sum = 0;

	at_unlock2(nodeMeta->lock);//--------------------------------------------- unlock here

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
//		dl->check_turn(dl->tail_sum,ble_len);
		if (dl->tail_sum%dl->my_size + ble_len > dl->my_size)
			dl->tail_sum+= (dl->my_size - (dl->tail_sum%dl->my_size));
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

	//check
//	if (dl->tail_sum + HARD_EVICT_SPACE > dl->head_sum)
//	if (dl->tail_sum + ble_len + dl->my_size > dl->head_sum + HARD_EVICT_SPACE)
//		return rv;

	while(dl->tail_sum + dl->my_size <= dl->head_sum + HARD_EVICT_SPACE)
	{
	//need hard evict
	addr = dl->dramLogAddr + (dl->tail_sum % dl->my_size);
	key = *(uint64_t*)(addr+HEADER_SIZE);
	// evict now

	SkiplistNode* prev[MAX_LEVEL+1];
	SkiplistNode* next[MAX_LEVEL+1];
	SkiplistNode* node;

	node = skiplist->find_node(key,prev,next);
	hot_to_warm(node,true);

//	dl->head_sum+=ble_len;

	// do we need flush?

		rv = 1;
		try_push(dl);
	}

	return rv;
}

int PH_Evict_Thread::try_soft_evict(DoubleLog* dl) // need return???
{
	unsigned char* addr;
//	size_t adv_offset=0;
	uint64_t header,key;
	int rv = 0;
//	addr = dl->dramLogAddr + (dl->tail_sum%dl->my_size);

	SkiplistNode* prev[MAX_LEVEL+1];
	SkiplistNode* next[MAX_LEVEL+1];
	SkiplistNode* node;
	NodeMeta* nodeMeta;
	LogLoc ll;

//	if (dl->tail_sum + SOFT_EVICT_SPACE > dl->head_sum)
//		return rv;
	
//	while(dl->tail_sum+adv_offset + ble_len <= dl->head_sum && adv_offset <= SOFT_EVICT_SPACE )
//	while (adv_offset <= SOFT_EVICT_SPACE)

//
	if (dl->soft_adv_offset < dl->tail_sum) // jump if passed
		dl->soft_adv_offset = dl->tail_sum;

	while(dl->soft_adv_offset + ble_len + dl->my_size <= dl->head_sum + SOFT_EVICT_SPACE)
	{
		addr = dl->dramLogAddr + ((dl->soft_adv_offset) % dl->my_size);
		header = *(uint64_t*)addr;
		key = *(uint64_t*)(addr+HEADER_SIZE);

		if (is_valid(header))
		{
			rv = 1;
			// regist the log num and size_t
			node = skiplist->find_node(key,prev,next);
			nodeMeta = nodeAddr_to_nodeMeta(node->data_node_addr);


			if (sizeof(NodeAddr) + nodeMeta->written_size + node->entry_size_sum + ble_len > NODE_SIZE) // NODE_BUFFER_SIZE???
			{
				// warm is full
				hot_to_warm(node,true);

				if (sizeof(NodeAddr) + nodeMeta->written_size + ble_len > NODE_SIZE)
					warm_to_cold(node);
				continue; // retry
			}

			//add entry
			ll.log_num = dl->log_num;
			ll.offset = dl->soft_adv_offset;
			node->entry_list.push_back(ll);
			node->entry_size_sum+=ble_len;

			if (key < node->key)
				printf("thread2 1026\n");
				/*
			ll.test_key1 = node->key;
			ll.test_key2 = key;
			ll.test_ptr = node;
*/
			// need to try flush
//			node->try_hot_to_warm();
#ifdef SOFT_FLUSH
			hot_to_warm(node,false);
#endif
		}

		dl->soft_adv_offset+=ble_len;
		if ((dl->soft_adv_offset)%dl->my_size  + ble_len > dl->my_size)
			dl->soft_adv_offset+=(dl->my_size-((dl->soft_adv_offset)%dl->my_size));
//		dl->check_turn(tail_sum,ble_len);


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
printf("evict start\n");
	while(exit == 0)
	{
//		update_free_cnt();
		op_check();

		done = 1;
		for (i=0;i<log_cnt;i++)
		{
			if (evict_log(log_list[i]))
				done = 0;
		}
		if (done)
		{
			run = 0;
			printf("evict idle\n");
			usleep(1000*1000);
			_mm_mfence();
			sync_thread();
			_mm_mfence();
			run = 1;
		}
	}
	run = 0;
	printf("evict end\n");
}

}
