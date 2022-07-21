#include "../test/test.h"

#include <libpmemkv.h>

class KVS_pmemkv : public KVS
{
	public:


		pmemkv_db *db = NULL;

	virtual void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		uint64_t initial_size = 1024*1024*1024;
		initial_size*=20; // 40G

		pmemkv_config *cfg = pmemkv_config_new();
		pmemkv_config_put_path(cfg,"/mnt/pmem0/pmemkv");
		pmemkv_config_put_size(cfg,initial_size);
		pmemkv_config_put_create_if_missing(cfg,true);

		pmemkv_open("cmap",cfg,&db);


		printf("kvs_pmemkv init\n");
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
	}

//	template <typename K, typename V>
	virtual void run(TestQuery* tqa, int ops)
	{
		
	//	auto v_client = viper_db->get_client();
	//	V result;
		int i;
		unsigned char* result;
		result = (unsigned char*)malloc(value_size);
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
				pmemkv_put(db,(const char*)tqa[i].key,key_size,(const char*)tqa[i].value,value_size);
//				return;
			else if (tqa[i].op == 2) // read
				pmemkv_get_copy(db,(const char*)tqa[i].key,key_size,(char*)result,value_size,NULL);
//				return;
			else if (tqa[i].op == 3) // update
				pmemkv_put(db,(const char*)tqa[i].key,key_size,(const char*)tqa[i].value,value_size);
			else if (tqa[i].op == 5)
			{
				pmemkv_iterator *it;
				pmemkv_iterator_new(db,&it);
//				pmemkv_iterator_seek_higher_eq(it,(const char*)tqa[i].key,key_size);
				pmemkv_iterator_seek_to_first(it);
				int j;
				size_t len;
				const char *str;
				for (j=0;j<tqa[i].cnt;j++)
				{
					pmemkv_iterator_key(it,&str,&len);
//					memcpy(result,
					if (pmemkv_iterator_next(it) == PMEMKV_STATUS_NOT_FOUND)
						break;
				}
				pmemkv_iterator_delete(it);
			}
//				return;
			

			/*
			if (tqa[i].op == 1) // insert
				v_client.put(*(static_cast<K*>(tqa[i].key)),*(static_cast<V*>(tqa[i].value)));
			else if (tqa[i].op == 2) // read
				v_client.get(*(static_cast<K*>(tqa[i].key)),&V);
			else if (tqa[i].op == 3) // update
				v_client.put(*(static_cast<K*>(tqa[i].key)),*(static_cast<V*>(tqa[i].value)));
				*/

		}
		free(result);
		
	}

};
