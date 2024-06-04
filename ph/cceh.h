
// volatile and out of place

#include<atomic>
//#include <mutex>

//#include "global.h" //value entry

//#define INV0 0
#define INV0 0xffffffffffffffff

//fixed
//#define KVP_PER_CL 4 // 16 * 4 = 64
#define KVP_PER_CL 2 // 32 * 2 = 64
#define CL_SIZE 64


#define CL_BIT 4
#define CL_PER_SEG (1 << CL_BIT)
#define LINEAR_MULTI 4

#define SPLIT_MASK (1 << 6) // I mean split bit

//#define ctt

namespace PH
{

//const uint64_t seg_mask = ~((uint64_t)CL_SIZE*CL_PER_SEG-1);

#define KEY_ARRAY_MAX (1 << 16)
//unsigned char** key_array = 0;
//int* key_cnt = 0;
//int key_array_cnt=0;
struct HandP // hash and pointer??
{
	uint32_t hash;
	uint16_t key_array;
	uint16_t key_offset;
};

union KeyEntry
{
	uint64_t key_value;
	HandP hp;
};

union KeyEntry_u
{
	KeyEntry ke;
	uint64_t ke_64;
};
#if 0
struct KVP
{
//	volatile unsigned char* key; // or key ptr // key value in 8B key
//	volatile unsigned char* value;
//	unsigned char* key;
//	uint64_t key;	
//	unsigned char* value;	
	/*
	union Key_u
	{
	KeyEntry key;	
	volatile uint64_t key_v;
	}
	union Value_u
	{
	ValueEntry value;	
	volatile uint64_t value_v;
	}
	*/
	volatile uint64_t key;
	volatile uint64_t value;
}; // 8 + 8 = 16
#endif

struct KVP // key value version pad pair
{
	volatile uint64_t key;
	volatile uint64_t value; //addr
//	volatile unsigned char* addr;
	volatile uint64_t version; // Delete / Version
//	std::atomic<uint64_t> version;
	volatile uint64_t padding;
}; // 32Bytes
/*
struct KVP_vk
{
	//need fix
	KeyEntry key;
	ValueEntry value;
};
*/
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
	volatile int depth; // retry if split

}; // 64 * ???

class CCEH
{
	public:
	CCEH();
	CCEH(int in_depth);
	~CCEH();

	void thread_local_init();
	void thread_local_clean();

	private:

//	inline	bool compare_key(const volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash);
//	inline	void insert_key(volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash);
	inline	uint64_t hf(unsigned char* const &key); // hash function
	inline uint64_t hf(uint64_t key);
//	inline	unsigned char* load_key(const uint64_t &key);
//	inline bool zero_check(unsigned char* const &key);


	volatile int depth;
	volatile int seg_cnt;
	
	struct SEG* volatile * volatile seg_list;
//	std::atomic<SEG**> seg_list;	

//	volatile void* seg_list;


//	std::atomic<bool>* seg_lock;
//	std::atomic<uint8_t> dir_lock;
//	std::mutex dir_lock;	

//	volatile uint64_t inv0_value;//,failed;
//	volatile unsigned char* inv0_value;
//	volatile ValueEntry_u inv0_value	

	void dir_double();
	void split(int sn);
//	void init_seg(int sn);
	void init(int in_depth);
	void clean();
//	uint64_t hf(const unsigned char* key);

	public:
//	int insert(const uint64_t &key,ValueEntry ve);
//	int insert2(const uint64_t &key,ValueEntry ve, int sn, int cn);
//	volatile uint64_t* insert(unsigned char* const &key,ValueEntry &ve,void* unlock = 0);
	
	KVP* insert(uint64_t &key,std::atomic<uint8_t> **unlock_p,volatile uint8_t &read_lock); // lock
	KVP* insert_with_fail(uint64_t &key,std::atomic<uint8_t> **unlock_p,volatile uint8_t &read_lock); // lock

	void lock(KVP* kvp);	
	void unlock(KVP* kvp);

	bool read(uint64_t &key,volatile uint64_t **ret,volatile int **seg_depth);
	bool read_with_fail(uint64_t &key,volatile uint64_t **ret,volatile int **seg_depth,bool &sf);
	void remove(uint64_t &key);

//	void unlock_entry2(void* unlock);
	void unlock_entry2(std::atomic<uint8_t> *lock_p,volatile uint8_t &read_lock);

	private:

	uint64_t dm;
	int point;
	//test
	int sc,pic,bc,find_cnt;
	uint64_t ctt1,ctt2,ctt3,ctt4;

//	std::atomic<uint8_t> dir_lock;
	std::atomic<uint8_t> write_lock;

	KVP zero_entry;
	std::atomic<uint8_t> zero_lock;
};
#if 0
class CCEH_vk : public CCEH
{
inline	bool compare_key(const volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash);
inline	void insert_key(volatile uint64_t &key1,unsigned char* const &key2,const uint32_t &hash);
inline	uint64_t hf(unsigned char* const &key);
inline	unsigned char* load_key(const uint64_t &key); // keyentry
inline	unsigned char* load_key2(const KeyEntry &key); // keyentry
inline bool zero_check(unsigned char* const &key);

};
#endif
void init_cceh();
void clean_cceh();



}
