
// volatile and out of place

#include<atomic>
//#include <mutex>

#include "global.h" //value entry

//#define INV0 0
#define INV0 0xffffffffffffffff

//fixed
#define KVP_PER_CL 4 // 16 * 4 = 64
#define CL_SIZE 64


#define CL_BIT 4
#define CL_PER_SEG (2 << CL_BIT)
#define LINEAR_MULTI 4

#define SPLIT_MASK (1 << 30)

//#define ctt

namespace PH
{

const uint64_t seg_mask = ~((uint64_t)CL_SIZE*CL_PER_SEG-1);

struct KVP
{
//	volatile unsigned char* key; // or key ptr // key value in 8B key
//	volatile unsigned char* value;
//	unsigned char* key;
	uint64_t key;	
//	unsigned char* value;	
	ValueEntry value;	
}; // 8 + 8 = 16

struct CL
{
	struct KVP kvp[KVP_PER_CL];
}; // 16 * 4 = 64

struct SEG
{
	struct CL cl[CL_PER_SEG];

	std::atomic<uint8_t> lock; // 1 ?
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
	
//	volatile struct SEG** seg_list;
	std::atomic<SEG**> seg_list;	
//	std::atomic<bool>* seg_lock;
	std::atomic<int> dir_lock;
//	std::mutex dir_lock;	

	/*volatile */ValueEntry inv0_value;

	void dir_double();
	void split(int sn);
//	void init_seg(int sn);
	void init(int in_depth);
	void clean();
//	uint64_t hf(const unsigned char* key);

	public:
//	int insert(const uint64_t &key,ValueEntry ve);
//	int insert2(const uint64_t &key,ValueEntry ve, int sn, int cn);
	ValueEntry* insert(const uint64_t &key,ValueEntry &ve,void* unlock = 0);

	ValueEntry find(const uint64_t &key);
	void remove(const uint64_t &key); // find with lock

	void unlock_entry2(void* unlock);

	uint64_t dm;
	int point;
	//test
	int sc,pic,bc,find_cnt;
	uint64_t ctt1,ctt2,ctt3,ctt4;

};

void init_cceh();
void clean_cceh();
}
