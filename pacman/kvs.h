#include "../test/test.h"
#include <stdio.h>

#include "include/config.h"
#include "include/db.h"

class KVS_pacman : public KVS
{
	public:

	virtual void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		uint64_t initial_size = 1024*1024*1024;
//		initial_size*=20; // 40G

		DB *db = new DB("/mnt/pmem0",1024*1024*1024,num,num);

		printf("kvs_pacman init\n");
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
//    printf("get query %ld\n",viper::btt3);
//    printf("index %ld\n",viper::btt1);
//    printf("data %ld\n",viper::btt2);
		free(db);	
	}

//	template <typename K, typename V>
	virtual void run(TestQuery* tqa, int ops)
	{
		
	//	auto v_client = viper_db->get_client();
	//	V result;
		int i;
//		unsigned char* result;
//		result = (unsigned char*)malloc(value_size);
//		rocksdb::Slice slice;
		std::string result;	
//		std::string result2;
		std::unique_ptr<DB::Worker> worker = db->GetWorker();
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
				worker->Put(Slice((const char*)tqa[i].key,key_size),Slice((const char*)tqa[i].value,value_size));
//				db->Put(rocksdb::WriteOptions(),(const char*)tqa[i].key,(const char*)tqa[i].value);
//				return;
			else if (tqa[i].op == 2) // read
				worker->Get(Slice((const char*)tqa[i].key,key_size),&result);
//				return;
			else if (tqa[i].op == 3) // update
				worker->Put(Slice((const char*)tqa[i].key,key_size),Slice((const char*)tqa[i].value,value_size));
//				db->Put(rocksdb::WriteOptions(),(const char*)tqa[i].key,(const char*)tqa[i].value);
#if 0
			else if (tqa[i].op == 5)
			{
//				char *result_key;
//				char *result_value;

//				result_key = (char*)malloc(key_size);
//				result_value = (char*)malloc(value_size);

				int j;
				rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
				it->Seek(rocksdb::Slice((const char*)tqa[i].key,key_size));
				for (j=0;j<tqa[i].cnt;j++)
				{
					if (!it->Valid())
						break;
					result = it->value().ToString();
					it->Next();
				}
				delete it;
//				free(result_key);
//				free(result_value);
			}
//				return;
#endif
			

			/*
			if (tqa[i].op == 1) // insert
				v_client.put(*(static_cast<K*>(tqa[i].key)),*(static_cast<V*>(tqa[i].value)));
			else if (tqa[i].op == 2) // read
				v_client.get(*(static_cast<K*>(tqa[i].key)),&V);
			else if (tqa[i].op == 3) // update
				v_client.put(*(static_cast<K*>(tqa[i].key)),*(static_cast<V*>(tqa[i].value)));
				*/

		}
//		free(result);
		
	}

};
