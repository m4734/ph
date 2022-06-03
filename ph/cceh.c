#include "cceh.h"
#include "thread.h"

#include <queue>
#include <string.h> //memset
#include <stdio.h> //test
#include <stdlib.h> //free
#include <x86intrin.h> // mm fence
//#include <mutex> //dir lock
namespace PH
{
std::queue <SEG*> seg_free_list;
volatile unsigned int seg_free_cnt;
unsigned int seg_free_min;
unsigned int seg_free_index;

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

uint64_t hf(unsigned char* key) // len 8?
{
/*	
	unsigned int hash = 5381;
	int len=8;//key_size;//8; // can't change need function OP
	while (len--)
		hash = ((hash << 5) + hash) + (*key++);
	return hash;
*/	
	return MurmurHash64A_L8 (key);
//	return std::_Hash_bytes(key,8,5516);	
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
//		memset(seg->cl,0xffffffff,CL_PER_SEG*CL_SIZE);
//	else
		memset(seg->cl,0,CL_PER_SEG*CL_SIZE);

//	seg->lock = 0;
//	seg->depth = depth;
}

SEG* alloc_seg() // use free list
{
	SEG* seg;

	if (posix_memalign((void**)&seg,64,sizeof(SEG)) != 0)
		printf("posix_memalign error2\n");

	seg->seg_lock = new std::mutex;
//	printf("al %p\n",seg->seg_lock);
	return seg;
}

void free_seg(SEG* seg)
{
	delete seg->seg_lock;
	free(seg);
//	seg_free_list.push(seg);
//	seg_free_cnt++;
}

CCEH::CCEH()
{
	init();
}
CCEH::~CCEH()
{
	clean();
}

void CCEH::init()
{

	int i;

	depth = 1;
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
		seg_list[i]->depth = depth;
	}

	inv0_value = NULL;

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
}

unsigned char* CCEH::find(unsigned char* key)
{
	int sn,cn,i;
	KVP* kvp_p;
	uint64_t hk;
	const int cl_shift = 64-CL_BIT;

	if (*(uint64_t*)key == INV0)
		return (unsigned char*)inv0_value;

	hk = hf(key);
//	sn = *(uint64_t*)key >> (64-depth);
//	cn = *(uint64_t*)key % CL_PER_SEG;
//	sn = *(uint64_t*)key % ((uint64_t)1 << depth);
//	cn = *(uint64_t*)key >> (64-2);
	sn = hk % ((uint64_t)1 << depth);
//	cn = hk >> (64-2);
//	cn = hk >> (64-CL_BIT);	
	cn = hk >> cl_shift;	

	kvp_p = (KVP*)((unsigned char*)seg_list[sn]->cl + cn*CL_SIZE);

	for (i=0;i<KVP_PER_CL;i++)
	{
		if ((uint64_t)kvp_p[i].key == *(uint64_t*)key)
			return (unsigned char*)kvp_p[i].value;
	}
	return NULL;
}

void CCEH::remove(unsigned char* key)
{
	int sn,cn,i;
//	uint64_t inv;
	KVP* kvp_p;
	SEG* seg;
	unsigned char* rv;
	int z = 0;
	uint64_t hk;

	if (*(uint64_t*)key == INV0)
	{
		inv0_value = NULL;
		return;
	}

	hk = hf(key);

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
	seg->seg_lock->lock();	
	
//	if (sn != *(uint64_t*)key >> (64-depth)) // didn't fixed?
	if (sn != hk >> (64-CL_BIT))
	{
//		seg->lock = 0;
		seg->seg_lock->unlock();
		goto retry;
	}

	kvp_p = (KVP*)((unsigned char*)seg_list[sn]->cl + cn*CL_SIZE);


	for (i=0;i<KVP_PER_CL;i++)
	{
		if ((uint64_t)kvp_p[i].key == *(uint64_t*)key)
		{
			kvp_p[i].value = NULL;
//			(uint64_t)(kvp_p[i].key) = inv;
			seg->seg_lock->unlock();
//			seg->lock = 0;			
			return;
		}
	}

//	seg->lock = 0;
	seg->seg_lock->unlock();

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
		delete seg->seg_lock;
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
		memcpy(new_list,seg_list,sizeof(SEG*) * seg_cnt);
		memcpy((unsigned char*)new_list+sizeof(SEG*)*seg_cnt,seg_list,sizeof(SEG*) * seg_cnt);

//		memset(new_list+sizeof(SEG*)*seg_cnt,0,sizeof(SEG*)*seg_cnt);

		seg_list = new_list;
		seg_cnt*=2;
		++depth;

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
	new_seg1->depth = seg->depth+1;

//	new_seg2->lock = 0;
	new_seg2->depth = seg->depth+1;

	new_kvp_p1 = (KVP*)new_seg1->cl;
	new_kvp_p2 = (KVP*)new_seg2->cl;

//	mask = (uint64_t)1 << (64-seg->depth-1);
	mask = (uint64_t)1 << seg->depth;	

	int j,l,nl1,nl2;
	uint64_t hk;

	for (i=0;i<CL_PER_SEG;i++)
	{
		nl1 = i*KVP_PER_CL;
		nl2 = i*KVP_PER_CL;
		l = i*KVP_PER_CL;

		for (j=0;j<KVP_PER_CL;j++)
		{
			if ((uint64_t)kvp_p[l].key == INV0)
			{
				break;
			}
			if (kvp_p[l].value == NULL)
			{
				l++;
				continue;
			}
			hk = hf((unsigned char*)(&kvp_p[l].key));
			if (hk & mask)
			{
				new_kvp_p2[nl2] = kvp_p[l];
				nl2++;
			}
			else
			{
				new_kvp_p1[nl1] = kvp_p[l];
				nl1++;
			}
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
//	seg_list[sn + (1 << seg->depth)] = new_seg2;
//	print_seg(new_seg2,sn + (1 << seg->depth));
	for (i=sn + (1 << seg->depth);i<seg_cnt;i+=l)
		seg_list[i] = new_seg2;
//	seg_cnt*=2;
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

void CCEH::insert(unsigned char* key,unsigned char* value)
{
	int sn,cn,i;
//	uint64_t inv;
	KVP* kvp_p;
	SEG* seg;
	int z = 0;
	uint64_t hk;

	if (*(uint64_t*)key == INV0)
	{
		inv0_value = value;
		return;
	}


	hk = hf(key);

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
	seg->seg_lock->lock();	
	
//	if (sn != *(uint64_t*)key >> (64-depth))
//	if (sn != *(uint64_t*)key % ((uint64_t)1 << depth))
	if (sn != hk % ((uint64_t)1 << depth))

	{
//		seg->lock = 0;
		seg->seg_lock->unlock();
		goto retry;
	}


	kvp_p = (KVP*)((unsigned char*)(seg->cl) + cn*CL_SIZE);
//	sd = seg->depth;
/*
	if (sn % 2 == 0)
		inv = INV1;
	else
		inv = INV0;
*/
	for (i=0;i<KVP_PER_CL;i++)
	{
		if ((uint64_t)(kvp_p[i].key) == *(uint64_t*)key) // update
		{

			kvp_p[i].value = value;
			_mm_sfence();

//			seg->lock = 0;
			seg->seg_lock->unlock();			
			return;
		}

		if ((uint64_t)(kvp_p[i].key) == INV0) // insert
		{

			kvp_p[i].value = value;
			_mm_sfence();
			kvp_p[i].key = (unsigned char*)(*(uint64_t*)key);

//			seg->lock = 0;
			seg->seg_lock->unlock();			
			return;
		}
	}


	//need split
	split(sn % ((uint64_t)1 << seg->depth));	
//	seg->lock = 0;
	seg->seg_lock->unlock();
//	_mm_	
	free_seg(seg);

	goto retry;
//never
//	return NULL;
}

void init_cceh()
{
	seg_free_cnt = seg_free_min = seg_free_index = 0;
}

void clean_cceh()
{
	seg_gc();
}
}
