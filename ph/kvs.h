#include "../test/test.h"

#include "query.h"
#include "global.h"
#include "data.h"
#include "hash.h"

class KVS_ph : public KVS
{
	public:

	void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		temp_static_conf();
		init_data();
		init_hash();
	}

	void insert_op(unsigned char* key,unsigned char* value)
	{
		insert_query(key,value);
	}
	void read_op(unsigned char* key,unsigned char* result)
	{
		int len;
		lookup_query(key,result,&len);
	}
	void update_op(unsigned char* key,unsigned char* value)
	{
		insert_query(key,value);
	}
	void delete_op(unsigned char* key)
	{
		delete_query(key);
	}
	void scan_op(unsigned char* key,int cnt,unsigned char** scan_result)
	{
		Query query;
		query.key_p = key;
		query.op = 6;
		scan_query(&query);

		int i,len;
		query.op = 7;
		for (i=0;i<cnt;i++)
		{
			next_query(&query,scan_result[i],&len);
		}
	}

	void clean()
	{
		clean_data();
		clean_hash();
	}

};
