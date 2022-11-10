#include "cceh.h"
#include "thread.h"

#include <queue>
#include <string.h> //memset
#include <stdio.h> //test
#include <stdlib.h> //free
#include <x86intrin.h> // mm fence
//#include <mutex> //dir lock

#include <time.h> //test

namespace PH
{
//std::queue <SEG*> seg_free_list;
volatile unsigned int seg_free_cnt; // atomic?
volatile unsigned int seg_free_min;
volatile unsigned int seg_free_index;

#define FREE_SEG_LEN 10000
SEG* free_seg_queue[FREE_SEG_LEN];

std::atomic<uint8_t> free_seg_lock;

uint64_t r_mask[65];

unsigned char** key_array = 0;
int* key_cnt = 0;
std::atomic<int> key_array_cnt;
thread_local int key_array_index=0;
int max_index;

//test
uint64_t alloc_seg_cnt;


void at_lock2(std::atomic<uint8_t> &lock)
{
	uint8_t z;
	while(true)
	{
		z = 0;
		if (lock.compare_exchange_strong(z,1))
			return;
	}
}
void at_unlock2(std::atomic<uint8_t> &lock)
{
	lock = 0;
//	lock.store(0,std::memory_order_release);
}

int try_at_lock2(std::atomic<uint8_t> &lock)
{
	uint8_t z=0;
	return lock.compare_exchange_strong(z,1);
}

uint64_t MurmurHash64A_L8 ( const void * key )
{
	const int len = 8;
	const uint64_t seed = 5516;

//  const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
  const uint64_t m = 0xc6a4a7935bd1e995;

  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t * data = (const uint64_t *)key;
  const uint64_t * end = data + (len/8);

  while(data != end)
  {
    uint64_t k = *data++;

    k *= m; 
    k ^= k >> r; 
    k *= m; 
    
    h ^= k;
    h *= m; 
  }

  const unsigned char * data2 = (const unsigned char*)data;

  switch(len & 7)
  {
  case 7: h ^= ((uint64_t) data2[6]) << 48;
  case 6: h ^= ((uint64_t) data2[5]) << 40;
  case 5: h ^= ((uint64_t) data2[4]) << 32;
  case 4: h ^= ((uint64_t) data2[3]) << 24;
  case 3: h ^= ((uint64_t) data2[2]) << 16;
  case 2: h ^= ((uint64_t) data2[1]) << 8;
  case 1: h ^= ((uint64_t) data2[0]);
          h *= m;
  };
 
  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

inline unsigned char* CCEH::load_key(const uint64_t &key)
{
	return (unsigned char*)&key;
}

inline bool CCEH::compare_key(/*const */const volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash)
{
	return key1 == (*(uint64_t*)key2);
}
inline void CCEH::insert_key(volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash)
{
	key1 = (*((uint64_t*)key2));
}

inline uint64_t CCEH::hf(unsigned char* const &key) // len 8?
{
/*	
	unsigned int hash = 5381;
	int len=8;//key_size;//8; // can't change need function OP
	while (len--)
		hash = ((hash << 5) + hash) + (*key++);
	return hash;
*/	
//	return MurmurHash64A_L8 (key);
//	if (point)
//	return *(uint64_t*)key;
//	else	
	return std::_Hash_bytes(key,8,5516);	
//	return *(uint64_t*)key;	
}

inline bool CCEH::zero_check(unsigned char* const &key)
{
	return *(uint64_t*)key == INV0;
}

void inv_seg(SEG* seg)//,int sn)
{
	int i;
//	uint64_t inv;
//	SEG* seg;
	KVP* kvp_p;
//	seg = seg_list[sn];
	kvp_p = (KVP*)seg->cl;

	/*
	if (sn % 2 == 0)
		inv = INV1;
	else
		inv = INV0;

	for (i=0;i<CL_PER_SEG * KVP_PER_CL;i++)
		*(uint64_t*)(kvp_p[i].key) = inv;
*/
//	if (sn % 2 == 0)
		memset(seg->cl,0xffffffff,CL_PER_SEG*CL_SIZE);
//	else
//		memset(seg->cl,0,CL_PER_SEG*CL_SIZE);

//	seg->lock = 0;
//	seg->depth = depth;
}

SEG* alloc_seg() // use free list
{
	SEG* seg;

	at_lock2(free_seg_lock);
	if (seg_free_index == seg_free_min)
	{
		int temp;
		temp = PH::min_seg_free_cnt();
		if (temp > seg_free_index)
			seg_free_min = temp;
	}
	if (seg_free_index < seg_free_min)
	{
		seg = free_seg_queue[seg_free_index%FREE_SEG_LEN];
		seg_free_index++;
	}
	else
	{
//		printf("aaa\n");
		alloc_seg_cnt++;
		if (posix_memalign((void**)&seg,64,sizeof(SEG)) != 0)
			printf("posix_memalign error2\n");
	}
	at_unlock2(free_seg_lock);
//	seg->seg_lock = new std::mutex;
//	printf("alloc_seg %p\n",seg);
	return seg;
}

void free_seg(SEG* seg)
{
//	printf("free_seg %p\n",seg);
//	delete seg->seg_lock;
//	free(seg);
//	seg_free_list.push(seg);

// need fetch and add

	at_lock2(free_seg_lock);
	if (seg_free_index+FREE_SEG_LEN/2 <= seg_free_cnt)
	{
		update_idle();
	if (seg_free_index+FREE_SEG_LEN <= seg_free_cnt)	
	{
		printf("free seg full %d %d %d\n",seg_free_min,seg_free_index,seg_free_cnt);
//		print_thread_info();
		while(seg_free_index+FREE_SEG_LEN <= seg_free_cnt);
	}
	}
	free_seg_queue[seg_free_cnt%FREE_SEG_LEN] = seg;	
	seg_free_cnt++;
	at_unlock2(free_seg_lock);
}
CCEH::CCEH()
{
	point = 0;
	init(1);
}
CCEH::CCEH(int in_depth)
{
	point = 1;
	init(in_depth);
}
CCEH::~CCEH()
{
	clean();
}

void CCEH::init(int in_depth)
{

	int i;
	depth = in_depth;
	dm = r_mask[depth];
	seg_cnt = 1 << depth;

//	if (posix_memalign((void**)&seg_list,64,sizeof(SEG*) * seg_cnt) != 0)
//		printf("posix memalign error0\n");

	seg_list = (struct SEG**)malloc(sizeof(SEG*) * seg_cnt);
//	seg_list = (struct SEG**)posix_memalign(
//	if (posix_memalign((void**)&seg_list,64,sizeof(SEG*) * seg_cnt) != 0)
//		printf("posix memalign error0\n");
//	seg_lock = new std::atomic<bool>[seg_cnt];

	for (i=0;i<seg_cnt;i++)
	{
//		seg[i] = (struct SEG*)malloc(sizeof(SEG));
//		if (posix_memalign(&seg_list[i],64,sizeof(SEG)) != 0)
//			printf("posix memalign error\n");
		seg_list[i] = alloc_seg();		
//	if (!point)
//	printf("seg %p\n",seg_list[i]);	
		inv_seg(seg_list[i]);//,i);
		seg_list[i]->lock = 0;
//		at_unlock2(seg_list[i]->lock);
		seg_list[i]->depth = depth;
	}

	/*failed = */inv0_value = 0;

	dir_lock = 0;
	free_seg_lock = 0;

	//test
#if ctt
	bc = sc = ctt1 = ctt3 = ctt2 = 0;
	find_cnt = pic = 1;
#endif
}

void CCEH::clean()
{
	int i;
	for (i=seg_cnt-1;i>=0;i--) // can't find depth
	{
		if (i + 1 <= ((uint64_t)1 << seg_list[i]->depth))
		{
			/*
			int j;
			for (j=0;j<i;j++)
			{
				if (seg_list[i] == seg_list[j])
				{
					printf("seg free error\n");
				}
			}
			*/
//			free_seg(seg_list[i]); //free?
			free(seg_list[i]);			
		}
	}
	free(seg_list);

#ifdef ctt
	printf("depth %d split_cnt %d insert %ld insert_avg %ld split %ld ctt3 %ld insert_cnt %d bc %d find %ld find_cnt %d find_avg %ld \n",depth,sc,ctt1,ctt1/pic,ctt2,ctt3,pic,bc,ctt4,find_cnt,ctt4/find_cnt);
#endif
}

ValueEntry CCEH::find(unsigned char* const &key)
{
	/*
	if (point)
	{
		inv0_value.node_offset = 0;
		return inv0_value;
//		return *insert_t(key,inv0_value,NULL); 
	}
	*/
#ifdef ctt
	find_cnt++;
	struct timespec ts1,ts2;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	_mm_mfence();
#endif
	int sn,cn,i;
	KVP* kvp_p;
	uint64_t hk;
	uint32_t hk2;
	const int cl_shift = 64-CL_BIT;
	int l;

	ValueEntry_u ve_u;
//	if (key == (void*)INV0 && key_size == 8) // wrong!
	if (zero_check(key))	
	{
//		return &inv0_value;
		ve_u.ve_64 = inv0_value;
return ve_u.ve;		
	}
//if (point)
//	return NULL;
//	const uint64_t
	hk = hf(key);
	hk2 = *(uint32_t*)(&hk);
//	sn = *(uint64_t*)key >> (64-depth);
//	cn = *(uint64_t*)key % CL_PER_SEG;
//	sn = *(uint64_t*)key % ((uint64_t)1 << depth);
//	cn = *(uint64_t*)key >> (64-2);
	sn = hk & dm;//((uint64_t)1 << depth);
//	cn = hk >> (64-2);
//	cn = hk >> (64-CL_BIT);	
	cn = hk >> cl_shift;	

//	kvp_p = (KVP*)((unsigned char*)seg_list[sn]->cl + cn*CL_SIZE);
	kvp_p = (KVP*)seg_list[sn]->cl;	
	l = cn*KVP_PER_CL;
//	if (point)
//return NULL;
//	const int ll = (hk >> (64-CL_BIT)) * KVP_PER_CL;
//	const int ll = cn*KVP_PER_CL;
//
	ve_u.ve.node_offset = INIT_OFFSET;


	for (i=0;i<KVP_PER_CL * LINEAR_MULTI;i++)
	{
//		l = (ll+i) % (CL_PER_SEG*KVP_PER_CL);
		l%=CL_PER_SEG*KVP_PER_CL;
//		if ((uint64_t)kvp_p[l].key == *(uint64_t*)key)
//		if (kvp_p[l].key == key)		
		if (compare_key(kvp_p[l].key,key,hk2))		
		{
#ifdef ctt
			_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts2);
			ctt4+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
			ve_u.ve_64 = kvp_p[l].value;
			break;
//			return ValueEntry(kvp_p[l].value);
		}
		l++;
	}
	return ve_u.ve;
}

void CCEH::remove(unsigned char* const &key)
{
	int sn,cn,i;
//	uint64_t inv;
	KVP* kvp_p;
	SEG* seg;
	unsigned char* rv;
	int z = 0;
//	uint64_t hk;
	int l;

//	if (*(uint64_t*)key == INV0)
//	if (key == (void*)INV0 && key_size == 8)	 // wrong!
	if (zero_check(key))	
	{
		inv0_value = 0;
		return;
	}

	const uint64_t hk = hf(key);
	const uint32_t hk2 = *(uint32_t*)(&hk);

retry:
//	sn = *(uint64_t*)key >> (64-depth);
//	cn = *(uint64_t*)key % CL_PER_SEG;
//	sn = *(uint64_t*)key % ((uint64_t)1 << depth);	
//	cn = *(uint64_t*)key >> (64-2);
	sn = hk % ((uint64_t)1 << depth);	
//	cn = hk >> (64-2);
	cn = hk >> (64-CL_BIT);	

	seg = seg_list[sn];

//	while(seg->lock.compare_exchange_weak(z,1) == 0);
//	seg->seg_lock->lock();	
	at_lock2(seg->lock);	
	
//	if (sn != *(uint64_t*)key >> (64-depth)) // didn't fixed?
	if (sn != hk % ((uint64_t)1 << depth))
	{
//		seg->lock = 0;
		at_unlock2(seg->lock);		
//		seg->seg_lock->unlock();
		goto retry;
	}

//	kvp_p = (KVP*)((unsigned char*)seg_list[sn]->cl + cn*CL_SIZE);
	kvp_p = (KVP*)seg_list[sn]->cl;
	l = cn*KVP_PER_CL;	

	for (i=0;i<KVP_PER_CL * LINEAR_MULTI;i++)
	{
		l%=KVP_PER_CL*CL_PER_SEG;
//		if ((uint64_t)kvp_p[l].key == *(uint64_t*)key)
//		if (kvp_p[l].key == key)		
		if (compare_key(kvp_p[l].key,key,hk2))
		{
			kvp_p[l].value = 0;
//			(uint64_t)(kvp_p[i].key) = inv;
//			seg->seg_lock->unlock();
//			seg->lock = 0;			
			at_unlock2(seg->lock);			
			return;
		}
		l++;
	}

//	seg->lock = 0;
	at_unlock2(seg->lock);	

//	seg->seg_lock->unlock();

	// not found
	return;
}

void seg_gc()
{
	at_lock2(free_seg_lock);
	SEG* seg;
	seg_free_min = PH::min_seg_free_cnt();
	for (;seg_free_index < seg_free_min;seg_free_index++)
	{
		/*
		seg = seg_free_list.front();
//		printf("fl %p\n",seg->seg_lock);
//		delete seg->seg_lock;
		free(seg);
		seg_free_list.pop();
		*/
		seg = free_seg_queue[seg_free_index%FREE_SEG_LEN];
		free(seg);
	}
	at_unlock2(free_seg_lock);

}

void CCEH::dir_double()
{
		SEG** new_list;
	        SEG** old_list;
		old_list = seg_list;
	        new_list = (struct SEG**)malloc(sizeof(SEG*) * seg_cnt*2);
//		memcpy(new_list,seg_list,sizeof(SEG*) * seg_cnt);
//		memcpy((unsigned char*)new_list+sizeof(SEG*)*seg_cnt,seg_list,sizeof(SEG*) * seg_cnt);
int i;
for (i=0;i<seg_cnt;i++)
	new_list[i] = new_list[seg_cnt+i] = old_list[i];

//		memset(new_list+sizeof(SEG*)*seg_cnt,0,sizeof(SEG*)*seg_cnt);

		seg_list = new_list;
		seg_cnt*=2;
		++depth;
		dm = r_mask[depth];
		_mm_sfence(); // for seg list
		free(old_list);

//		seg_gc();
}
void print_seg(SEG* seg,int sn)
{
	/*
	int i,j,k;
	uint64_t mask;
	printf("seg %d depth %d\n",sn,seg->depth);
	for (i=0;i<CL_PER_SEG;i++)
	{
		printf("-------------------------------\n");
		for (j=0;j<KVP_PER_CL;j++)
		{
			mask = (uint64_t)1 << 63;
			for (k=0;k<64;k++)
			{
				if ((uint64_t)seg->cl[i].kvp[j].key & mask)
					printf("1");
				else
					printf("0");
				mask /=2;
			}
			printf("\n");
		}
	}
	printf("--------------------------------------\n");
	scanf("%d",&k);
	*/

}
void CCEH::split(int sn) // seg locked
{
	SEG* seg;
	int i;
//	uint64_t inv,mask;
	uint64_t mask;	
	KVP* kvp_p;
	KVP* new_kvp_p1;
	KVP* new_kvp_p2;

	seg = seg_list[sn];
//	dir_lock.lock();
//	
	if (seg->depth == depth)
	{
		uint8_t lock;
		/*
		while(1)
		{
			lock = dir_lock;
			if (lock & SPLIT_MASK)
			{
				at_unlock2(seg->lock);
				return; //split fail by other dir double
			}
			if (dir_lock.compare_exchange_strong(lock,lock | SPLIT_MASK))
				break;
		}
		*/

// dir double and double can be so check depth

		while (1)
		{
		lock = dir_lock;
			if (lock & SPLIT_MASK)
			{
				at_unlock2(seg->lock);
				return;
			}
			if (dir_lock.compare_exchange_strong(lock,lock | SPLIT_MASK))
				break;
		}

		// have lock

		if (seg->depth != depth) // impossible because dir lock
		{
			at_unlock2(seg->lock);
			dir_lock-=SPLIT_MASK;
			return;
		}

		while(dir_lock != SPLIT_MASK+1); // without me
		//use no op

//		while(dir_lock.compare_exchange_weak(0,1) == 0);
//		static std::mutex dir_lock;
//		dir_lock.lock();		
//		if (seg->depth == depth)
		{
			dir_double();

		}

		dir_lock-=SPLIT_MASK;
//		dir_lock.unlock();

//		dir_lock = 0;
	}
	//print_seg(seg,sn);
/*
	if (sn%2 == 0)
		inv = INV1;
	else
		inv = INV0;
*/
	kvp_p = (KVP*)seg->cl;

	SEG* new_seg1;
	SEG* new_seg2;

	new_seg1 = alloc_seg();
	new_seg2 = alloc_seg();
//printf("alloc_seg1 %p\n",new_seg1);
//printf("alloc_seg2 %p\n",new_seg2);

	inv_seg(new_seg1);//,sn);
	inv_seg(new_seg2);//,sn);

	new_seg1->lock = 0;
//	at_unlock2(new_seg1->lock);			
	new_seg1->depth = seg->depth+1;

	new_seg2->lock = 0;
//	at_unlock2(new_seg2->lock);			
	new_seg2->depth = seg->depth+1;

	new_kvp_p1 = (KVP*)new_seg1->cl;
	new_kvp_p2 = (KVP*)new_seg2->cl;

//	mask = (uint64_t)1 << (64-seg->depth-1);
	mask = (uint64_t)1 << seg->depth;	

	int j,l,nl1,nl2;
	int nll1[CL_PER_SEG],nll2[CL_PER_SEG],kc;
	uint64_t hk;
//	l = 0;
	for (i=0;i<CL_PER_SEG;i++)
		nll1[i] = nll2[i] = KVP_PER_CL*i;	
	for (i=0;i<CL_PER_SEG;i++)
	{
		nl1 = i*KVP_PER_CL;
		nl2 = i*KVP_PER_CL;
		l = i*KVP_PER_CL;

		for (j=0;j<KVP_PER_CL;j++)
		{
			if (kvp_p[l].key == INV0 || kvp_p[l].value == 0)
			{
//				if (kvp_p[l].value == 0)
//					break;
//					//if we don't push old seg, there can be inv0 in the middle
				l++;
				continue;
//				break;
			}
			/*
			if (kvp_p[l].value == 0)
			{
				l++;
				continue;
			}
			*/
//			hk = hf((unsigned char*)(&kvp_p[l].key));
			hk = hf(load_key((uint64_t)kvp_p[l].key));// volatile?
			kc = hk >> (64-CL_BIT);
			if (hk & mask)
			{
				while(new_kvp_p2[nll2[kc]].key != INV0)
				{
					nll2[kc]++;
					nll2[kc]%=KVP_PER_CL*CL_PER_SEG;
				}
				new_kvp_p2[nll2[kc]] = kvp_p[l];
				nll2[kc]++;
				nll2[kc]%=KVP_PER_CL*CL_PER_SEG;
//				kvp_p[l].key = INV0;
			}
			else
			{
				while(new_kvp_p1[nll1[kc]].key != INV0)
				{
					nll1[kc]++;
					nll1[kc]%=KVP_PER_CL*CL_PER_SEG;
				}
				new_kvp_p1[nll1[kc]] = kvp_p[l];
				nll1[kc]++;
				nll1[kc]%=KVP_PER_CL*CL_PER_SEG;
//				kvp_p[l].key = INV0;

			}
			/*
			else
			{
				new_kvp_p1[nl1] = kvp_p[l];
				nl1++;
			}
			*/
			l++;
		}
	}

	_mm_sfence();

//	dir_lock.lock();
//	seg_list[sn] = new_seg1;

//	print_seg(seg,sn);

	l = 1 << (seg->depth+1);
//	print_seg(new_seg1,sn);
	
	for (i=sn;i<seg_cnt;i+=l)
		seg_list[i] = new_seg1;
//		seg_list[i] = seg;		
		
//	seg_list[sn + (1 << seg->depth)] = new_seg2;
//	print_seg(new_seg2,sn + (1 << seg->depth));
	for (i=sn + (1 << seg->depth);i<seg_cnt;i+=l)
		seg_list[i] = new_seg2;
//	seg_cnt*=2;
//	seg->depth++;
//	dir_lock.unlock();	

/*
	l = 0;

	for (i=0;i<CL_PER_SEG;i++)
	{

		for (j=0;j<KVP_PER_CL;j++)
		{
			if ((uint64_t)kvp_p[l].key == INV0)
			{
				l++;
				continue;
			}
			if (kvp_p[l].value == NULL)
			{
				l++;
				continue;
			}
			if (find((unsigned char*)&kvp_p[l].key) == NULL)
			{
				printf("split error\n");
				find((unsigned char*)&kvp_p[l].key);
			}
			l++;
		}
	}
*/
	//why do we unlock
//	at_unlock2(seg->lock);
	free_seg(seg);
}

volatile uint64_t* CCEH::insert(unsigned char* const &key,ValueEntry &ve,void* unlock)
{
//	int sn;
//	uint64_t inv;
//	KVP* kvp_p;
//	SEG* seg;
//	/*const*/ uint64_t hk;// = hf(&key);
//	/*const*/ int cn;// = hk >>(64-CL_BIT);
//	int l,d;

#ifdef ctt
	pic++;
	struct timespec ts1,ts2,ts3,ts4;
	clock_gettime(CLOCK_MONOTONIC,&ts3);
	_mm_mfence();
#endif
	int sn,cn,i;
	uint64_t hk;
	uint32_t hk2;
	const int cl_shift = 64-CL_BIT;
	int l;
	SEG* seg;
	KVP* kvp_p;

	ValueEntry_u ve_u;
	ve_u.ve = ve;

//	if (key == (void*)INV0 && key_size == 8)	 // wrong!
	if (zero_check(key))	
	{
		inv0_value = ve_u.ve_64;
		if (unlock)
			*(void**)unlock = NULL;
		return /*(ValueEntry_u*)&*/&inv0_value;
	}

//	int lock = dir_lock;
	// just use cas
	if (dir_lock & SPLIT_MASK)
		return 0;
//	if (dir_lock.compare_exchange_strong(lock,lock+1) == 0)
//		return 0;
	dir_lock++;		
	if (dir_lock & SPLIT_MASK)
	{
		dir_lock--;
		return 0;
	}

	
	hk = hf(key);
	hk2 = *(uint32_t*)(&hk);
//retry:
//while(1)
//{
	
//	sn = *(uint64_t*)key >> (64-depth);
//	cn = *(uint64_t*)key % CL_PER_SEG;
//	sn = *(uint64_t*)key % ((uint64_t)1 << depth);	
//	cn = *(uint64_t*)key >> (64-2);

//	sn = hk % ((uint64_t)1 << depth);	
//	sn = hk & (((uint64_t)1 << depth)-1);
//	sn = hk & r_mask[depth];
	sn = hk & dm;

//	cn = hk >> (64-2);
//	cn = hk >> (64-CL_BIT);

	cn = hk >> cl_shift;
/*		
#ifdef ctt
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;

#endif
*/
	seg = seg_list[sn];
/*	
#ifdef ctt // 6 598249128
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;
#endif
*/
//	if (seg->lock.compare_exchange_weak(z,1) == 0)
//		while(seg->lock.compare_exchange_weak(z,1) == 0);
//	seg->seg_lock->lock();
	if (try_at_lock2(seg->lock) == 0)
	{
//		printf("seg lock fail\n");
		dir_lock--;
		return 0;
	}
//	if (!point)
//	printf("%d ",(int)dir_lock);
//printf("at_lock2 seg %p\n",seg);	
//	if (sn != *(uint64_t*)key >> (64-depth))
//	if (sn != *(uint64_t*)key % ((uint64_t)1 << depth))
#if 0
	if (seg != seg_list[sn])//sn != hk % ((uint64_t)1 << depth))

	{
//		printf("seg lock retry\n");
//		seg->lock = 0;
		at_unlock2(seg->lock);
//		seg->seg_lock->unlock();
//		goto retry;
//		continue;	
		dir_lock--;			
		return 0;		
	}
#endif
#if 0	
	if (insert2(key,value,sn,cn))
	{
//		seg->lock = 0;
		at_unlock2(seg->lock);			

#ifdef ctt
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif
		return 1;
	}
#endif
#if 1
//	kvp_p = (KVP*)((unsigned char*)(seg->cl) + cn*CL_SIZE);
/*	KVP* const */kvp_p = (KVP*)seg->cl;
	l = cn*KVP_PER_CL;	
//	sd = seg->depth;
/*
	if (sn % 2 == 0)
		inv = INV1;
	else
		inv = INV0;
*/
/*	
#ifdef ctt //6 697820434
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;
#endif
*/
#ifdef ctt
//	clock_gettime(CLOCK_MONOTONIC,&ts1);
//	_mm_mfence();
#endif
#if 0
	if (point)
	{
		inv0_value.node_offset = 0;
		return &inv0_value;
	}
#endif
	for (i=0;i<KVP_PER_CL * LINEAR_MULTI;i++)
	{
#ifdef ctt
//		bc++;
#endif
		l%=KVP_PER_CL*CL_PER_SEG;
//		if (kvp_p[l].key == INV0 || kvp_p[l].key == key)
//			break;
#if 1
		if (kvp_p[l].key == INV0) // insert
		{
			kvp_p[l].value = ve_u.ve_64;
			_mm_sfence();
//			(uint64_t)kvp_p[l].key = *(uint64_t*)key; // need encode
			insert_key(kvp_p[l].key,key,hk2);			

//			seg->lock = 0;
//			seg->seg_lock->unlock();			
			if (unlock)
				*(void**)unlock = seg;
			else
			{
				at_unlock2(seg->lock);
				dir_lock--;
			}
#ifdef ctt
			_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
//			ctt3+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;
#endif
			return /*(ValueEntry_u*)&*/&kvp_p[l].value;
		}
//		else if (kvp_p[l].key == key) // update
		else if (compare_key(kvp_p[l].key,key,hk2))		
		{
			if (unlock)
				*(void**)unlock = seg;
			else
			{
				kvp_p[l].value = ve_u.ve_64;
				_mm_sfence();
				at_unlock2(seg->lock);			
				dir_lock--;
			}

//			seg->lock = 0;
//			seg->seg_lock->unlock();			
#ifdef ctt
			_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
//			ctt3+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;
//			ctt3+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;
#endif
			return /*(ValueEntry_u*)&*/&kvp_p[l].value;
		}
#endif
		l++;
	}
#endif
#if 0
	if (i < KVP_PER_CL*LINEAR_MULTI)
	{
		if (kvp_p[l].key == INV0) // insert
		{
			kvp_p[l].value = value;
			_mm_sfence();
			kvp_p[l].key = key;

//			seg->lock = 0;
//			seg->seg_lock->unlock();			
			if (unlock)
				*(void**)unlock = seg;
			else
				at_unlock2(seg->lock);
#ifdef ctt
			_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif

			return &kvp_p[l].value;
		}
		if (kvp_p[l].key == key) // update
		{
			if (unlock)
				*(void**)unlock = seg;
			else
			{
				kvp_p[l].value = value;
				_mm_sfence();
				at_unlock2(seg->lock);			
			}

//			seg->lock = 0;
//			seg->seg_lock->unlock();			
#ifdef ctt
			_mm_mfence();
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
//			ctt3+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;
#endif
			return &kvp_p[l].value;
		}
	}
#endif

	//need split
#ifdef ctt
	_mm_mfence();
	sc++;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
#endif
	split(sn % ((uint64_t)1 << seg->depth));	// unlock & free seg
#ifdef ctt
	_mm_mfence();
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	ctt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
//	seg->lock = 0;
//	at_unlock2(seg->lock);			

//	seg->seg_lock->unlock();
//	_mm_
//	printf("free_seg %p\n",seg);		
//	free_seg(seg); // moved to split
	// what if split fail??? should not free seg
//}

//	goto retry;
	dir_lock--;	
	return 0;//failed;	
//never
//	return NULL;
}

void CCEH::unlock_entry2(void* unlock)
{
//	SEG* seg = (SEG*)unlock;
//	printf("unlock_entry2 vep %p seg %p\n",vep,seg);
//	if (seg != NULL)
	/*	
	if (seg->lock == 0)
	{
		printf("unlock error\n");
		int t;
		scanf("%d",&t);
	}
	*/
	if (!point)
		printf("ue errror?\n");
	if (unlock)		
	{
		at_unlock2(((SEG*)unlock)->lock);
		dir_lock--;
	}
}

void init_cceh()
{
	seg_free_cnt = seg_free_min = seg_free_index = 0;
	printf("sizeof KVP %ld\n",sizeof(KVP));
	printf("sizeof CL %ld\n",sizeof(CL));
	int i;
	r_mask[0] = 0;
	for (i=1;i<=64;i++)
		r_mask[i] = r_mask[i-1]*2+1;

//	if (key_size != 8) // ke yarray init
	if (PH_KEY_SIZE != 8)
	{
		key_array = (unsigned char**)malloc(sizeof(unsigned char*) * KEY_ARRAY_MAX);
		key_cnt = (int*)malloc(sizeof(int*) * KEY_ARRAY_MAX);
		key_array_cnt = 0;
//		max_index = key_size*KEY_ARRAY_MAX;
		max_index = PH_KEY_SIZE*KEY_ARRAY_MAX;
		key_cnt[0] = max_index;
		key_array[0] = NULL;
//		for (i=0;i<KEY_ARRAY_MAX;i++)
//			key_cnt[i] = 0;
	}

}

void clean_cceh()
{
	seg_gc();

	printf("SEG size %d hash %lfGB\n",sizeof(SEG),double(alloc_seg_cnt*sizeof(SEG))/1024/1024/1024);

	if (key_array)
	{
		int i;
		for (i=0+1;i<key_array_cnt;i++)
			free(key_array[i]);
		free(key_array);
		free(key_cnt);
	}


}

inline unsigned char* CCEH_vk::load_key(const uint64_t &key)
{
//	return &key_array[(*((KeyEntry&)key)).key_array][(*((KeyEntry&)key)).key_offset];
//	return &key_array[static_cast<KeyEntry>(key).key_array][static_cast<KeyEntry>(key).key_offset];
	KeyEntry_u ku;
ku.ke_64 = key;	
	return &key_array[ku.ke.hp.key_array][ku.ke.hp.key_offset];

//	return (unsigned char*)&key;
}

inline unsigned char* CCEH_vk::load_key2(const KeyEntry &key)
{
//	return &key_array[(*((KeyEntry&)key)).key_array][(*((KeyEntry&)key)).key_offset];
//	return &key_array[static_cast<KeyEntry>(key).key_array][static_cast<KeyEntry>(key).key_offset];
	return &key_array[key.hp.key_array][key.hp.key_offset];

//	return (unsigned char*)&key;
}

inline bool CCEH_vk::compare_key(const volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash)
{
	KeyEntry_u key;
	key.ke_64 = key1;
	if (hash != key.ke.hp.hash)
		return false;
	unsigned char* kl = load_key2(key.ke);
	return *((uint64_t*)kl) == *((uint64_t*)key2) && *((uint64_t*)kl+1) == *((uint64_t*)key2+1);
}
inline void CCEH_vk::insert_key(volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash) // it has to be new key
{

	if (key_cnt[key_array_index] + PH_KEY_SIZE/*key_size*/ >= max_index)
	{
		key_array_index = key_array_cnt.fetch_add(1);
		key_cnt[key_array_index] = 0;
		key_array[key_array_index] = (unsigned char*)malloc(max_index);
	}

	memcpy(&key_array[key_array_index][key_cnt[key_array_index]],key2,PH_KEY_SIZE);//key_size);

	KeyEntry_u key;
	key.ke.hp.key_array = key_array_index;
	key.ke.hp.key_offset = key_cnt[key_array_index];
	key.ke.hp.hash = hash;

	key_cnt[key_array_index]+=PH_KEY_SIZE;//key_size;

	key1 = key.ke_64;

//	key1 = (unsigned char*)(*((uint64_t*)key2));
}

inline uint64_t CCEH_vk::hf(unsigned char* const &key) // len 8?
{
/*	
	unsigned int hash = 5381;
	int len=8;//key_size;//8; // can't change need function OP
	while (len--)
		hash = ((hash << 5) + hash) + (*key++);
	return hash;
*/	
//	return MurmurHash64A_L8 (key);
//	if (point)
//	return *(uint64_t*)key;
//	else	
	return std::_Hash_bytes(key,16,5516);	 //??? key size
//	return *(uint64_t*)key;	
}

inline bool CCEH_vk::zero_check(unsigned char* const &key)
{
	return false;
}

}
