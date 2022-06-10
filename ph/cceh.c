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
std::queue <SEG*> seg_free_list;
volatile unsigned int seg_free_cnt;
unsigned int seg_free_min;
unsigned int seg_free_index;

uint64_t r_mask[65];

void at_lock2(std::atomic<uint8_t> &lock)
{
	uint8_t z = 0;
	while(lock.compare_exchange_strong(z,1) == 0);
}

void at_unlock2(std::atomic<uint8_t> &lock)
{
	lock.store(0,std::memory_order_release);
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

uint64_t hf(const void *key) // len 8?
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

	if (posix_memalign((void**)&seg,64,sizeof(SEG)) != 0)
		printf("posix_memalign error2\n");

//	seg->seg_lock = new std::mutex;
//	printf("al %p\n",seg->seg_lock);
	return seg;
}

void free_seg(SEG* seg)
{
//	delete seg->seg_lock;
//	free(seg);
	seg_free_list.push(seg);
	seg_free_cnt++;
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

//	seg_list = (struct SEG**)malloc(sizeof(SEG*) * seg_cnt);
//	seg_list = (struct SEG**)posix_memalign(
	if (posix_memalign((void**)&seg_list,64,sizeof(SEG*) * seg_cnt) != 0)
		printf("posix memalign error0\n");
//	seg_lock = new std::atomic<bool>[seg_cnt];

	for (i=0;i<seg_cnt;i++)
	{
//		seg[i] = (struct SEG*)malloc(sizeof(SEG));
//		if (posix_memalign(&seg_list[i],64,sizeof(SEG)) != 0)
//			printf("posix memalign error\n");
		seg_list[i] = alloc_seg();			
		inv_seg(seg_list[i]);//,i);
//		seg_list[i]->lock = 0;
		at_unlock2(seg_list[i]->lock);
		seg_list[i]->depth = depth;
	}

	inv0_value = NULL;

	//test
	pic = bc = sc = ctt1 = ctt3 = ctt2 = 0;
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
			free_seg(seg_list[i]); //free?
		}
	}
	free(seg_list);

#ifdef ctt
	printf("d %d sc %d insert %ld split %ld hf %ld pic %d bc %d \n",depth,sc,ctt1,ctt2,ctt3,pic,bc);
#endif
}

unsigned char* CCEH::find(const uint64_t &key)
{
	int sn,cn,i;
	KVP* kvp_p;
	uint64_t hk;
	const int cl_shift = 64-CL_BIT;
	int l;

	if (key == INV0)
		return (unsigned char*)inv0_value;
//if (point)
//	return NULL;
	hk = hf(&key);
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
	for (i=0;i<KVP_PER_CL * LINEAR_MULTI;i++)
	{
//		l = (ll+i) % (CL_PER_SEG*KVP_PER_CL);
		l%=CL_PER_SEG*KVP_PER_CL;
//		if ((uint64_t)kvp_p[l].key == *(uint64_t*)key)
		if (kvp_p[l].key == key)		
			return (unsigned char*)kvp_p[l].value;
		l++;
	}
	return NULL;
}

void CCEH::remove(const uint64_t &key)
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
	if (key == INV0)	
	{
		inv0_value = NULL;
		return;
	}

	const uint64_t hk = hf(&key);

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
		if (kvp_p[l].key == key)		
		{
			kvp_p[l].value = NULL;
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
	SEG* seg;
	seg_free_min = PH::min_seg_free_cnt();
	for (;seg_free_index < seg_free_min;seg_free_index++)
	{
		seg = seg_free_list.front();
//		printf("fl %p\n",seg->seg_lock);
//		delete seg->seg_lock;
		free(seg);
		seg_free_list.pop();
	}

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

		free(old_list);

		seg_gc();
}
void print_seg(SEG* seg,int sn)
{
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
	dir_lock.lock();
	if (seg->depth == depth)
	{
//		while(dir_lock.compare_exchange_weak(0,1) == 0);
//		static std::mutex dir_lock;
//		dir_lock.lock();		
//		if (seg->depth == depth)
		{
			dir_double();

		}
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

	inv_seg(new_seg1);//,sn);
	inv_seg(new_seg2);//,sn);

//	new_seg1->lock = 0;
	at_unlock2(new_seg1->lock);			
	new_seg1->depth = seg->depth+1;

//	new_seg2->lock = 0;
	at_unlock2(new_seg2->lock);			
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
			if ((uint64_t)kvp_p[l].key == INV0)
			{
//				if (kvp_p[l].value == 0)
//					break;
//					//if we don't push old seg, there can be inv0 in the middle
				l++;
				continue;
//				break;
			}
			if (kvp_p[l].value == NULL)
			{
				l++;
				continue;
			}
//			hk = hf((unsigned char*)(&kvp_p[l].key));
			hk = hf(&kvp_p[l].key);			
			kc = hk >> (64-CL_BIT);
			if (hk & mask)
			{
				while((uint64_t)new_kvp_p2[nll2[kc]].key != INV0)
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
				while((uint64_t)new_kvp_p1[nll1[kc]].key != INV0)
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
	dir_lock.unlock();	

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
//	free_seg(seg);
}

int CCEH::insert2(const uint64_t &key, unsigned char* value, int sn,int cn)
{
//	kvp_p = (KVP*)((unsigned char*)(seg->cl) + cn*CL_SIZE);
	KVP* const kvp_p = (KVP*)(seg_list[sn]->cl);
	int l = cn*KVP_PER_CL;	
//	/*const*/ uint64_t k64 = *(uint64_t*)key;
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
	int i;
	for (i=0;i<KVP_PER_CL * LINEAR_MULTI;i++)
	{
		bc++;
		l%=KVP_PER_CL*CL_PER_SEG;
		if ((uint64_t)(kvp_p[l].key) == INV0) // insert
		{

			kvp_p[l].value = value;
			_mm_sfence();
//			kvp_p[l].key = (unsigned char*)k64;
			kvp_p[l].key = key;			

			return 1;
		}

		if ((uint64_t)(kvp_p[l].key) == key) // update
		{

			kvp_p[l].value = value;
			_mm_sfence();

			return 1;
		}
		/*
		else if ((uint64_t)(kvp_p[l].key) == INV0) // insert
		{

			kvp_p[l].value = value;
			_mm_sfence();
			kvp_p[l].key = (unsigned char*)(*(uint64_t*)key);

//			seg->lock = 0;
//			seg->seg_lock->unlock();			
#ifdef ctt
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif

			return;
		}
		*/
		l++;
	}
	return 0; // need split
}

int CCEH::insert(const uint64_t &key,unsigned char* value)
{
	int sn;
//	uint64_t inv;
//	KVP* kvp_p;
	SEG* seg;
	int z = 0;
	/*const*/ uint64_t hk = hf(&key);
	/*const*/ int cn = hk >>(64-CL_BIT);
//	int l,d;

#ifdef ctt
	pic++;
	struct timespec ts1,ts2,ts3,ts4;
//	clock_gettime(CLOCK_MONOTONIC,&ts3);
#endif
	
//	if (k64 == INV0)
//	if (*(uint64_t*)key == INV0)	
	if (key == INV0)	
	{
		inv0_value = value;
		return 1;
	}
	
	/*
#ifdef ctt // 1 338407654
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;
#endif
*/
/*
#ifdef ctt
	clock_gettime(CLOCK_MONOTONIC,&ts1);
#endif
//	hk = hf(key);
#ifdef ctt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	ctt3+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
*/
/*	
#ifdef ctt
//	clock_gettime(CLOCK_MONOTONIC,&ts3);
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;

#endif
*/
//retry:
//while(1)
//{
	
#ifdef ctt
	clock_gettime(CLOCK_MONOTONIC,&ts3);
//	clock_gettime(CLOCK_MONOTONIC,&ts1);
//	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;

#endif


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
/*		
#ifdef ctt
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;

#endif
*/
	seg = seg_list[sn];
	
#ifdef ctt // 6 598249128
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;
#endif
//	if (seg->lock.compare_exchange_weak(z,1) == 0)
//		while(seg->lock.compare_exchange_weak(z,1) == 0);
//	seg->seg_lock->lock();	
	at_lock2(seg->lock);	
	
//	if (sn != *(uint64_t*)key >> (64-depth))
//	if (sn != *(uint64_t*)key % ((uint64_t)1 << depth))
	
	if (seg != seg_list[sn])//sn != hk % ((uint64_t)1 << depth))

	{
		printf("lock retry\n");
//		seg->lock = 0;
		at_unlock2(seg->lock);
//		seg->seg_lock->unlock();
//		goto retry;
//		continue;		
		return 0;		
	}
	
	/*
#ifdef ctt // 6 747648931
	clock_gettime(CLOCK_MONOTONIC,&ts1);
	ctt3+=(ts1.tv_sec-ts3.tv_sec)*1000000000+ts1.tv_nsec-ts3.tv_nsec;
#endif
*/

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
#if 0
//	kvp_p = (KVP*)((unsigned char*)(seg->cl) + cn*CL_SIZE);
	KVP* const kvp_p = (KVP*)seg->cl;
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
	for (i=0;i<KVP_PER_CL * LINEAR_MULTI;i++)
	{
		bc++;
		l%=KVP_PER_CL*CL_PER_SEG;
		if ((uint64_t)(kvp_p[l].key) == INV0) // insert
		{

			kvp_p[l].value = value;
			_mm_sfence();
			kvp_p[l].key = (unsigned char*)k64;

			seg->lock = 0;
//			seg->seg_lock->unlock();			
#ifdef ctt
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif

			return 1;
		}

		if ((uint64_t)(kvp_p[l].key) == k64) // update
		{

			kvp_p[l].value = value;
			_mm_sfence();

			seg->lock = 0;
//			seg->seg_lock->unlock();			
#ifdef ctt
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
//			ctt3+=(ts4.tv_sec-ts1.tv_sec)*1000000000+ts4.tv_nsec-ts1.tv_nsec;
#endif
			return 1;
		}
		/*
		else if ((uint64_t)(kvp_p[l].key) == INV0) // insert
		{

			kvp_p[l].value = value;
			_mm_sfence();
			kvp_p[l].key = (unsigned char*)(*(uint64_t*)key);

//			seg->lock = 0;
//			seg->seg_lock->unlock();			
#ifdef ctt
			clock_gettime(CLOCK_MONOTONIC,&ts4);
			ctt1+=(ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
#endif

			return;
		}
		*/
		l++;
	}
#endif

	//need split
#ifdef ctt
	sc++;
	clock_gettime(CLOCK_MONOTONIC,&ts1);
#endif
	split(sn % ((uint64_t)1 << seg->depth));	
#ifdef ctt
	clock_gettime(CLOCK_MONOTONIC,&ts2);
	ctt2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;
#endif
//	seg->lock = 0;
	at_unlock2(seg->lock);			

//	seg->seg_lock->unlock();
//	_mm_	
	free_seg(seg);
//}

//	goto retry;
	return 0;	
//never
//	return NULL;
}

void init_cceh()
{
	seg_free_cnt = seg_free_min = seg_free_index = 0;
	printf("sizeof KVP %d\n",sizeof(KVP));
	printf("sizeof CL %d\n",sizeof(CL));
	int i;
	r_mask[0] = 0;
	for (i=1;i<=64;i++)
		r_mask[i] = r_mask[i-1]*2+1;
}

void clean_cceh()
{
	seg_gc();
}
}
