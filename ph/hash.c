#include <stdlib.h>

#include <stdio.h> //test

#include <x86intrin.h> 

#include "global.h"
#include "hash.h"
#include "cceh.h"

#include "data.h"

//#include <atomic.h>

namespace PH
{

	CCEH* point_hash;
	CCEH* range_hash_array;

// OP it will be no hash until 16

#define print 0
//#define print 0
//
//

uint64_t htt1,htt2,htt3,fpc,htt4,htt5,fpc2;

uint64_t pre_bit_mask[128+1];
uint64_t pre_bit_mask2[128+1];

unsigned char* decode_entry(unsigned char* entry, int* value_len_p)
{
	
//	return entry;
	unsigned char* ptr;
//	*result_len_p = *((uint16_t)(&entry+sizeof(uint32_t)));
	if (value_len_p != NULL)	
		*value_len_p = ((uint64_t)entry >> 16) % (256*256);
	ptr = (unsigned char*)((uint64_t)offset_to_node_data((uint64_t)entry >> 32) | ((uint64_t)entry % (256*256)));
		return ptr;
		
	/*
	ValueEntry ve = (ValueEntry)entry;
	value_len_p = ve.length;
	return offset_to_node_data(ve.node_offset) | ve.kv_offset;
*/
}

unsigned char* encode_entry(unsigned char* ptr, int value_len)
{
//	return ptr;
	
	unsigned char* entry;

	entry = (unsigned char*)(((uint64_t)calc_offset_data(ptr) << 32) | (value_len << 16) | ((uint64_t)ptr % (256*256)));

	return entry;
	
	/*
	ValueEntry ve;
	ve.node_offset = calc_offset_data(ptr);
	ve.length = value_len;
	ve.kv_offset = ptr % (256*256);
	return ve;
	*/
}

void print64(uint64_t v)
{
	int i;
	uint64_t mask;
	mask = (uint64_t)1 << 63;
	for (i=0;i<64;i++)
	{
		if (v & mask)
			printf("1");
		else
			printf("0");
		mask/=2;
	}
	printf("\n");
}

static unsigned int hash_function(const unsigned char *buf/*,int len*/) // test hash from hiredis
{
	unsigned int hash = 5381;
	int len=8;//key_size;//8; // can't change need function OP
	while (len--)
		hash = ((hash << 5) + hash) + (*buf++);
	return hash;
}

ValueEntry find_point_entry(unsigned char* &key_p)
{
#ifdef htt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	fpc++;
	_mm_mfence();
#endif
//	unsigned char* entry;
//	unsigned char* ptr;
	ValueEntry entry;	
//	entry = point_hash->find(*((uint64_t*)key_p));
	entry = point_hash->find(key_p);

//	if (entry == NULL)
//		return NULL;
//	ptr = decode_entry(entry,result_len_p);
//	return ptr;
#ifdef htt
	_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt1+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
		return entry;
}

ValueEntry* find_or_insert_point_entry(unsigned char* &key_p,void* unlock)
{
#ifdef htt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	fpc2++;
	_mm_mfence();
#endif
//	unsigned char* entry;
//	unsigned char* ptr;
	ValueEntry* entry_p;
	ValueEntry ve;
	ve.kv_offset = ve.node_offset = 0;	
	while((entry_p=point_hash->insert((/**(uint64_t*)*/key_p),ve,unlock)) == NULL);

//	if (entry == NULL)
//		return NULL;
//	ptr = decode_entry(entry,result_len_p);
//	return ptr;
#ifdef htt
	_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt5+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
		return entry_p;
}

void unlock_entry(void* unlock)
{
	point_hash->unlock_entry2(unlock);
}

void insert_point_entry(unsigned char* key_p,ValueEntry ve)
{
#ifdef htt
	timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
//	unsigned char* entry = encode_entry(value,value_len);
	while(point_hash->insert((/**(uint64_t*)*/key_p),ve) == 0);
#ifdef htt
	_mm_mfence();

			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt3+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
}

void remove_point_entry(unsigned char* &key_p)
{
//	point_hash->remove(*((uint64_t*)key_p));
	point_hash->remove(key_p);	
}

#if 0
void bit_flush(unsigned char* prefix,unsigned char* key_p,int start,int end)
{
	// | 01234567 | 89012345 | 67890123 |
	int i,start_byte,end_byte,target_byte;
	int bit_count,bit_mask;
	if (key_p == NULL)
	{
		start_byte = start / 8+1;
		end_byte = end / 8;
		for (i=start_byte;i<=end_byte;i++)
			prefix[key_size-1 - i] = 0;
		target_byte = start_byte -1; //start / 8;
		bit_count = 8-start%8;

		if (bit_count == 8)
			prefix[key_size-1 - target_byte] = 0;
		else
		{
			bit_mask = ~((1 << (bit_count))-1);
			prefix[key_size-1 - target_byte] = prefix[key_size-1 - target_byte] & bit_mask;
		}

	}
	else
	{
		start_byte = start / 8;
		end_byte = end / 8 -1;
		for (i=start_byte;i<=end_byte;i++)
			prefix[key_size-1 - i] = key_p[key_size-1 - i];
		target_byte = end_byte+1; //end / 8;
		bit_count = 8 - (end % 8 + 1);

		if (bit_count == 0)
			prefix[key_size-1 - target_byte] = key_p[key_size-1 - target_byte];
		else
		{
			bit_mask = ~((1 << (bit_count))-1);
			prefix[key_size-1 - target_byte] = key_p[key_size-1 - target_byte] & bit_mask;
		}
	}

}
#endif
unsigned int find_range_entry2(unsigned char* &key_p,int* continue_len) //binary
{
#ifdef htt
	timespec ts1,ts2;
			clock_gettime(CLOCK_MONOTONIC,&ts1);
//			tt1+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
	_mm_mfence();

#endif

	int min,max,mid;
	unsigned int hash;
//	unsigned char prefix[8]={0,}; // 8?
	unsigned char prefix[16] = {0,};
	unsigned char* prefix2=0;
	unsigned char* key_p2=0;
//	uint64_t entry; // old name
/*
static int cnt=0;
cnt++;
if (cnt % 1000000 == 0)
	printf("cnt %d\n",cnt);
*/	
	if (*continue_len > 0)
	{
//		return find_range_entry(key_p,continue_len);
		hash = find_range_entry(key_p,continue_len);	
#ifdef htt
_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
return hash;
	}

	if (key_size > 8)
	{
		prefix2 = prefix+8;
		key_p2 = key_p+8;
	}

	max = key_bit;//key_size * 8; // 64
	min = 0;
	mid = (min+max)/2;
	
/*
	if (*continue_len < 0)
	{
		min = 0;
		mid = (min+max)/2;
	}
	else
	{
		min = mid = *continue_len;
	}
*/

	//	bit_flush(prefix,key_p,0,mid-1);	
ValueEntry ve;
	while(1)	
	{
		*(uint64_t*)prefix = *(uint64_t*)key_p & pre_bit_mask[mid];
		if (prefix2)
			*(uint64_t*)prefix2 = *(uint64_t*)key_p2 & pre_bit_mask2[mid];
/*
		printf("find mid %d ",mid);
		print64(*(uint64_t*)prefix);
		printf("\n");
*/
//		ve = range_hash_array[mid].find(*((uint64_t*)prefix));
		ve = range_hash_array[mid].find(prefix);

		if (ve.node_offset == 0) // NULL not found
		{
			max = mid;// - 1;	
//			bit_flush(prefix,NULL,(min+max)/2,mid-1);
// aaaaaaaaaaaaa ... 32 0000000 ... 32
// aaaaaaaa ... 15 000000000000 ... 49

//			*(uint64_t*)prefix = *(uint64_t*)key_p & pre_bit_mask[
		}
		else if (ve.node_offset == SPLIT_OFFSET) // splited
		{
			//found but splited

			if (mid+1 == max)
				min = mid+1;
			else
				min = mid;// + 1;
//			bit_flush(prefix,key_p,mid,(min+max)/2-1);
		}
	else // found
	{
#ifdef htt
_mm_mfence();

			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

		*continue_len = mid;
		return ve.node_offset;
	}

		if (mid == (min+max)/2)
		{
			/*
		printf("not found mid %d ",mid);
		print64(*(uint64_t*)prefix);
		printf("\n");
*/
			// it is chaning need retry
//			printf("fre2 error\n");
//			return find_range_entry2(key_p,continue_len);
				
			return 0; // need retry but why return?
		}
		mid = (min+max)/2;

	}

	printf("never come here\n");
//	*continue_len = mid; //??	
	return 0;

}

unsigned int find_range_entry(unsigned char* key_p,int* continue_len)//need OP binary
{
#ifdef htt
	timespec ts1,ts2;
			clock_gettime(CLOCK_MONOTONIC,&ts1);
//			tt1+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
			_mm_mfence();

#endif

	int i;
	unsigned int hash;
//	unsigned char prefix[8]={0,};
	unsigned char prefix[16] = {0,};	
//	uint64_t entry;

	unsigned char* prefix2;
	unsigned char* key_p2;
	uint64_t b;

	ValueEntry ve;

//	for (i=0;i<*continue_len;i++)
//		b=(b<<1)+1;
//
	if (*continue_len < 64)
	{
	b = ((uint64_t)1 << *continue_len) -1;
	b = b << (64-*continue_len);
	*((uint64_t*)prefix) = (b & *((uint64_t*)key_p));
	b = (uint64_t)1 << (63 -*continue_len);

	}
	else
		*((uint64_t*)prefix) = (*((uint64_t*)key_p));

//	b = (uint64_t)1 << (63 -*continue_len);
	if (print)
	printf("key %lx clen %d\n",*((uint64_t*)key_p),*continue_len);
	for (i=*continue_len;i<=64;i++)
	{
//		(*((uint64_t*)prefix)) |= (*((uint64_t*)key_p) & b);
//		b = b >> 1;
		if (print)		
		printf("prefix %lx\n",*((uint64_t*)prefix));

//		ve = range_hash_array[i].find(*((uint64_t*)prefix));
		ve = range_hash_array[i].find(prefix);
	

		if (ve.node_offset != SPLIT_OFFSET) // split continue
		{
			*continue_len = i; //need retry from here
#ifdef htt
_mm_mfence();

			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
			return ve.node_offset;//(unsigned int)entry; // 0 means not found else is found
		}

		(*((uint64_t*)prefix)) |= (*((uint64_t*)key_p) & b);
		b = b >> 1;

	}

	prefix2 = prefix+8;
	key_p2 = key_p+8;
	if (*continue_len < 128)
	{
	b = ((uint64_t)1 << *continue_len) -1;
	b = b << (64-*continue_len);
	*((uint64_t*)prefix2) = (b & *((uint64_t*)key_p2));
	}
	else
		*((uint64_t*)prefix2) = (*((uint64_t*)key_p2));
	b = (uint64_t)1 << (128-1 -*continue_len);


	for (i=64+1;i<=key_bit;i++) // 128
	{
//		ve = range_hash_array[i].find(*((uint64_t*)prefix));
		ve = range_hash_array[i].find(prefix);

		if (ve.node_offset != SPLIT_OFFSET) // split continue
		{
			*continue_len = i; //need retry from here
#ifdef htt
_mm_mfence();

			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
			return ve.node_offset;//(unsigned int)entry; // 0 means not found else is found
		}

		(*((uint64_t*)prefix2)) |= (*((uint64_t*)key_p2) & b);
		b = b >> 1;

	}

	//never here
	printf("range hash error\n");
	return 0;
}


// there will be no double because it use e lock on the split node but still need cas
void insert_range_entry(unsigned char* key_p,int len,unsigned int offset) // need to check double
{
//	if (len >= 64)
//		return;
#ifdef htt
	timespec ts1,ts2;
			clock_gettime(CLOCK_MONOTONIC,&ts1);
//			tt1+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
	_mm_mfence();	
#endif

	unsigned char prefix[16] = {0,};
//	int i;

//	uint64_t b=0;	
//	uint64_t offset64 = offset;
	/*
	for (i=0;i<len;i++)
		b=(b<<1)+1;
		*/
/*
	if (len < 64)
	{
	b = ((uint64_t)1 << len)-1;
	b = b << (64-len);
	
	*((uint64_t*)prefix) = (b & *((uint64_t*)key_p));
	}
	else
		*((uint64_t*)prefix) = (*((uint64_t*)key_p));
*/
	if (len <= 64)
		*((uint64_t*)prefix) = (pre_bit_mask[len] & *((uint64_t*)key_p));
	else
	{
		unsigned char* prefix2 = prefix+8;
		unsigned char* key_p2 = key_p+8;
		*((uint64_t*)prefix) = *((uint64_t*)key_p);
		*((uint64_t*)prefix2) = (pre_bit_mask2[len] & *((uint64_t*)key_p2));
	}



if (print)
	printf("key_p %lx range %lx len %d offset %d insert\n",*((uint64_t*)key_p),*((uint64_t*)prefix),len,offset);	
/*
	printf("insert mid %d ",len);
	print64(*(uint64_t*)prefix);
	printf("\n");
*/
ValueEntry ve;
ve.node_offset = offset;
//	while(range_hash_array[len].insert(*((uint64_t*)prefix),ve) == 0);
	while(range_hash_array[len].insert(prefix,ve) == 0);
#ifdef htt
_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts2);
			htt4+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif

}

void init_hash()
{

	int i;

	pre_bit_mask[0] = 0;
//	pre_bit_mask[1] = (uint64_t)1 << 63;
	for (i=1;i<=64;i++)
		pre_bit_mask[i] = (pre_bit_mask[i-1] >> 1) + ((uint64_t)1 << 63);
	for (i=64+1;i<=128;i++)
		pre_bit_mask[i] = pre_bit_mask[64];
	for (i=1;i<=64;i++)
		pre_bit_mask2[i] = pre_bit_mask[64];
	for (i=64+1;i<=128;i++)
		pre_bit_mask2[i] = pre_bit_mask[i-64];


	init_cceh();

	point_hash = new CCEH(20);
	range_hash_array = new CCEH[64+1];


/*
	// insert 0
	uint64_t zero=0;
	unsigned int hash = hash_function(&zero);
	range_hash_table_array[0][hash]->eky
*/

	htt4 = htt3 = htt5 = htt1 = htt2 = 0;
	fpc2 = fpc = 1;
}

void clean_hash()
{
	// clean point hash

	delete point_hash;
	delete[] range_hash_array;

	clean_cceh();
#ifdef htt
	printf("hash\n");
	printf("point read %ld %ld\n",htt1/1000000000,htt1%1000000000);
	printf("point read avg %ld %ld %ld\n",(htt1/fpc)/1000000000,(htt1/fpc)%1000000000,fpc);
	printf("point read2 %ld %ld\n",htt5/1000000000,htt5%1000000000);
	printf("point read2 avg %ld %ld %ld\n",(htt5/fpc2)/1000000000,(htt5/fpc2)%1000000000,fpc2);
	printf("range read %ld %ld\n",htt2/1000000000,htt2%1000000000);
	printf("range insert %ld %ld\n",htt4/1000000000,htt4%1000000000);
	printf("point insert %ld %ld\n",htt3/1000000000,htt3%1000000000);
#endif

}

}
