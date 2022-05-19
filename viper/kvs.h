#include "../test/test.h"

#include "viper.hpp"

class KVS_viper : public KVS
{
	public:

//	viper::Viper<uint64_t, uint64_t>* viper_db;
//	viper::Viper<uint64_t, uint64_t>::Client v_client;

//	auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem0/viper",1024*1024*1024);
//	auto v_client = viper_db->get_client();

//	template <typename K, typename V>
//	std::unique_ptr<viper::Viper<K, V>> viper_db;
//	viper::Viper<uint64_t,uint64_t>::Client v_client;

	std::unique_ptr<viper::Viper<uint64_t,uint64_t>> viper_db;
//	viper::Viper<uint64_t,uint64_t>::Client v_client;

	virtual void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		const size_t initial_size = 1024*1024*1024;
		viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem0/viper", initial_size);
//		v_client = viper_db->get_client();


		printf("kvs_viper init\n");
	}

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

	virtual void run(TestQuery* tqa, int ops)
	{
		auto v_client = viper_db->get_client();
		uint64_t result;
		int i;
		for (i=0;i<ops;i++)
		{
			if (tqa[i].op == 1) // insert
				v_client.put(*((uint64_t*)tqa[i].key),*((uint64_t*)tqa[i].value));
			else if (tqa[i].op == 2) // read
				v_client.get(*((uint64_t*)tqa[i].key),&result);
			else if (tqa[i].op == 3) // update
				v_client.put(*((uint64_t*)tqa[i].key),*((uint64_t*)tqa[i].value));

		}
	}

};
