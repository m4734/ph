
// volatile and out of place

#include<atomic>
#include <mutex>

#define INV0 0
#define INV1 0xffffffffffffffff

//fixed
#define KVP_PER_CL 4 // 16 * 4 = 64
#define CL_SIZE 64


#define CL_BIT 2
#define CL_PER_SEG (2 << CL_BIT)
#define LINEAR_MULTI 2

#define ctt

namespace PH
{
struct KVP
{
//	volatile unsigned char* key; // or key ptr // key value in 8B key
//	volatile unsigned char* value;
	unsigned char* key;
	unsigned char* value;	
}; // 8 + 8 = 16

struct CL
{
	struct KVP kvp[KVP_PER_CL];
}; // 16 * 4 = 64

struct SEG
{
	struct CL cl[CL_PER_SEG];

	std::atomic<int> lock; // 4
//	std::mutex* seg_lock;	//struct c++
//	volatile int depth; // 4
	int depth;	

}; // 64 * ???

class CCEH
{
	public:
	CCEH();
	CCEH(int in_depth);
	~CCEH();
	volatile int depth;
	volatile int seg_cnt;
	
	struct SEG** seg_list;
//	std::atomic<bool>* seg_lock;
//	std::atomic<int> dir_lock;
	std::mutex dir_lock;	

	volatile unsigned char* inv0_value;

	void dir_double();
	void split(int sn);
//	void init_seg(int sn);
	void init(int in_depth);
	void clean();
	uint64_t hf(const unsigned char* key);

	public:
	void insert(const unsigned char* key,unsigned char* value);
	unsigned char* find(unsigned char* key);
	void remove(unsigned char* key); // find with lock

	int point;
	//test
	int sc,pic;
	uint64_t ctt1,ctt2,ctt3;

};

void init_cceh();
void clean_cceh();
}
