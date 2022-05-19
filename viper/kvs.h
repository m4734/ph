#include "../test/test.h"

#include "viper.hpp"

struct Byte_8
{
	unsigned char data[8];
};
struct Byte_100
{
	unsigned char data[100];
};
struct Byte_200
{
	unsigned char data[200];
};

template <typename K, typename V>
class KVS_viper : public KVS
{
	public:

//	viper::Viper<uint64_t, uint64_t>* viper_db;
//	viper::Viper<uint64_t, uint64_t>::Client v_client;

//	auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem0/viper",1024*1024*1024);
//	auto v_client = viper_db->get_client();

//	template <typename K, typename V>
	std::unique_ptr<viper::Viper<K, V>> viper_db;
//	auto viper::Viper<K, V>::Client v_client;

//	std::unique_ptr<viper::Viper<uint64_t,uint64_t>> viper_db;
//	viper::Viper<uint64_t,uint64_t>::Client v_client;

	
//	template <typename K, typename V>
	virtual void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		uint64_t initial_size = 1024*1024*1024;
		initial_size*=10;
		viper_db = viper::Viper<K, V>::create("/mnt/pmem0/viper", initial_size);
//		v_client = viper_db->get_client();


		printf("kvs_viper init\n");
	}

/*
	virtual void KVS_viper<K,V>::init(int num,int key,int value,int record)
	{
	}
	*/

	virtual void insert_op(unsigned char* key,unsigned char* value)
	{
//		v_client.put(*((uint64_t*)key),*((uint64_t*)value));
	}
	virtual void read_op(unsigned char* key,unsigned char* result)
	{
//		v_client.get(*((uint64_t*)key),(uint64_t*)result);
	}
	virtual void update_op(unsigned char* key,unsigned char* value)
	{
//		v_client.put(*((uint64_t*)key),*((uint64_t*)value));
	}
	virtual void delete_op(unsigned char* key)
	{
	}
	virtual void scan_op(unsigned char* key,int cnt,unsigned char** scan_result)
	{
	}

	virtual void clean()
	{
	}

//	template <typename K, typename V>
	virtual void run(TestQuery* tqa, int ops)
	{
		
		auto v_client = viper_db->get_client();
		V result;
		int i;
		for (i=0;i<ops;i++)
		{
			/*
			if (tqa[i].op == 1) // insert
				v_client.put(*((uint64_t*)tqa[i].key),*((uint64_t*)tqa[i].value));
			else if (tqa[i].op == 2) // read
				v_client.get(*((uint64_t*)tqa[i].key),&result);
			else if (tqa[i].op == 3) // update
				v_client.put(*((uint64_t*)tqa[i].key),*((uint64_t*)tqa[i].value));
				*/

			if (tqa[i].op == 1) // insert
				v_client.put(*((K*)tqa[i].key),*((V*)tqa[i].value));
			else if (tqa[i].op == 2) // read
				v_client.get(*((K*)tqa[i].key),&result);
			else if (tqa[i].op == 3) // update
				v_client.put(*((K*)tqa[i].key),*((V*)tqa[i].value));

			/*
			if (tqa[i].op == 1) // insert
				v_client.put(*(static_cast<K*>(tqa[i].key)),*(static_cast<V*>(tqa[i].value)));
			else if (tqa[i].op == 2) // read
				v_client.get(*(static_cast<K*>(tqa[i].key)),&V);
			else if (tqa[i].op == 3) // update
				v_client.put(*(static_cast<K*>(tqa[i].key)),*(static_cast<V*>(tqa[i].value)));
				*/

		}
		
	}

};
