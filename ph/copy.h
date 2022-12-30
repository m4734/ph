//#include "data.h"

namespace PH
{

//	extern struct Node;
	struct Node;

//#define DRAM_BUF
inline void pf64x4(unsigned char *ss)
{
	__builtin_prefetch(ss,0,1);
	__builtin_prefetch(ss+1,0,1);
	__builtin_prefetch(ss+2,0,1);
	__builtin_prefetch(ss+3,0,1);
}

inline void Rmm32x8(unsigned char* d,unsigned char *s)
{

	__m256i *ss = (__m256i*)s;
	__m256i *dd = (__m256i*)d;

	*(dd+7) = _mm256_stream_load_si256(ss+7);
	*(dd+6) = _mm256_stream_load_si256(ss+6);
	*(dd+5) = _mm256_stream_load_si256(ss+5);
	*(dd+4) = _mm256_stream_load_si256(ss+4);
	*(dd+3) = _mm256_stream_load_si256(ss+3);
	*(dd+2) = _mm256_stream_load_si256(ss+2);
	*(dd+1) = _mm256_stream_load_si256(ss+1);
	*(dd+0) = _mm256_stream_load_si256(ss+0);

}


inline void mm32x8(unsigned char* d,unsigned char *s)
{

	__m256i *ss = (__m256i*)s;
	__m256i *dd = (__m256i*)d;

	*dd = _mm256_stream_load_si256(ss);
	*(dd+1) = _mm256_stream_load_si256(ss+1);
	*(dd+2) = _mm256_stream_load_si256(ss+2);
	*(dd+3) = _mm256_stream_load_si256(ss+3);
	*(dd+4) = _mm256_stream_load_si256(ss+4);
	*(dd+5) = _mm256_stream_load_si256(ss+5);
	*(dd+6) = _mm256_stream_load_si256(ss+6);
	*(dd+7) = _mm256_stream_load_si256(ss+7);

}

inline void mm64x4h1(unsigned char* d,unsigned char *s)
{

	__m512i *ss = (__m512i*)s;
	__m512i *dd = (__m512i*)d;

	*dd = _mm512_stream_load_si512(ss);
	*(dd+2) = _mm512_stream_load_si512(ss+2);

	/*
	_mm512_store_si512(dd, _mm512_load_si512(ss));
	_mm512_store_si512(dd+1, _mm512_load_si512(ss+1));
	_mm512_store_si512(dd+2, _mm512_load_si512(ss+2));
	_mm512_store_si512(dd+3, _mm512_load_si512(ss+3));
*/

}

inline void mm64x4h2(unsigned char* d,unsigned char *s)
{

	__m512i *ss = (__m512i*)s;
	__m512i *dd = (__m512i*)d;

	*(dd+1) = _mm512_stream_load_si512(ss+1);
	*(dd+3) = _mm512_stream_load_si512(ss+3);

	/*
	_mm512_store_si512(dd, _mm512_load_si512(ss));
	_mm512_store_si512(dd+1, _mm512_load_si512(ss+1));
	_mm512_store_si512(dd+2, _mm512_load_si512(ss+2));
	_mm512_store_si512(dd+3, _mm512_load_si512(ss+3));
*/
}

inline void mm64x4(unsigned char* d,unsigned char *s)
{

	__m512i *ss = (__m512i*)s;
	__m512i *dd = (__m512i*)d;

	*dd = _mm512_stream_load_si512(ss);
	*(dd+1) = _mm512_stream_load_si512(ss+1);
	*(dd+2) = _mm512_stream_load_si512(ss+2);
	*(dd+3) = _mm512_stream_load_si512(ss+3);

	/*
	_mm512_store_si512(dd, _mm512_load_si512(ss));
	_mm512_store_si512(dd+1, _mm512_load_si512(ss+1));
	_mm512_store_si512(dd+2, _mm512_load_si512(ss+2));
	_mm512_store_si512(dd+3, _mm512_load_si512(ss+3));
*/

}


inline void cl64x4(unsigned char* d,unsigned char *s)
{

	unsigned char buffer[64];

	__m512i *ss = (__m512i*)s;
	__m512i *dd = (__m512i*)d;
/*
	int i;
	for (i=0;i<4;i++)
	{
		_mm512_stream_load_si512(ss);
	}
*/
	*dd = _mm512_stream_load_si512(ss);
	*(dd+1) = _mm512_stream_load_si512(ss+1);
	*(dd+2) = _mm512_stream_load_si512(ss+2);
	*(dd+3) = _mm512_stream_load_si512(ss+3);


}

inline void pf256(unsigned char* a,size_t s)
{
//	pmem_memcpy(a,b,s,PMEM_F_NONTEPMPORAL);

	
	int i=0;
	while(i < s)
	{
//		memcpy(a,b,256);
		pf64x4(a);
		i+=256;
		a+=256;
	}
	
}
inline void cp256h1(unsigned char* a,unsigned char* b,size_t s)
{
	int i=0;
	while(i < s)
	{
		mm64x4h1(a,b);
		i+=256;
		a+=256;
		b+=256;
	}
	
}
inline void cp256h2(unsigned char* a,unsigned char* b,size_t s)
{
	int i=0;
	while(i < s)
	{
		mm64x4h2(a,b);
		i+=256;
		a+=256;
		b+=256;
	}
	
}
//#define AVX
inline void cp256(unsigned char* a,unsigned char* b,size_t s)
{
//	pmem_memcpy(a,b,s,PMEM_F_NONTEPMPORAL);

//	pf256(a,s);
	
	int i=0;
	while(i < s)
	{
#ifndef AVX
		memcpy(a,b,256); // debug
#else
//		mm64x4(a,b);
#endif
		i+=256;
		a+=256;
		b+=256;
	}
	
}

inline void nt256(unsigned char* a,unsigned char* b,size_t s)
{
//	pmem_memcpy(a,b,s,PMEM_F_NONTEPMPORAL);
	int i=0;
	while(i < s)
	{
		pmem_memcpy(a,b,256,PMEM_F_MEM_NONTEMPORAL);
		i+=256;
		a+=256;
		b+=256;
	}
}

inline void cp256(unsigned char* a,unsigned char* b)
{
//	pmem_memcpy(a,b,s,PMEM_F_NONTEPMPORAL);

	
	int i=0;
	while(i < sizeof(Node))
	{
		memcpy(a,b,256);
		i+=256;
		a+=256;
		b+=256;
	}
	
}

inline void nt256(unsigned char* a,unsigned char* b)
{
//	pmem_memcpy(a,b,s,PMEM_F_NONTEPMPORAL);
	int i=0;
	while(i < sizeof(Node))
	{
		pmem_memcpy(a,b,256,PMEM_F_MEM_NONTEMPORAL);
		i+=256;
		a+=256;
		b+=256;
	}
}


}
