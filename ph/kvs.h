#include "../test/test.h"

#include "query.h"
#include "global.h"
#include "data.h"
#include "hash.h"

class KVS_ph : public KVS
{
	public:

	virtual void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		temp_static_conf();
		init_hash();
		init_data();
		printf("kvs_ph init\n");
	}

	virtual void insert_op(unsigned char* key,unsigned char* value)
	{
		insert_query(key,value);
	}
	virtual void read_op(unsigned char* key,unsigned char* result)
	{
		int len;
		lookup_query(key,result,&len);
	}
	virtual void update_op(unsigned char* key,unsigned char* value)
	{
		insert_query(key,value);
	}
	virtual void delete_op(unsigned char* key)
	{
		delete_query(key);
	}
	virtual void scan_op(unsigned char* key,int cnt,unsigned char** scan_result)
	{
		Query query;
		init_query(&query);
		query.key_p = key;
		query.op = 6;
		scan_query(&query);

		int i,len;
		query.op = 7;
		for (i=0;i<cnt;i++)
		{
			next_query(&query,scan_result[i],&len);
		}
		free_query(&query);
	}

	virtual void clean()
	{
		clean_data();
		clean_hash();
	}

	virtual void run(TestQuery* tqa, int ops)
	{
		int i,len,j;
		char result[1000];
		for (i=0;i<ops;i++)
		{
			if (tqa[i].op == 1) // insert
				insert_query(tqa[i].key,tqa[i].value);
			else if (tqa[i].op == 2) // read
				lookup_query(tqa[i].key,result,&len);
			else if (tqa[i].op == 3) // update
				insert_query(tqa[i].key,tqa[i].value);
			else if (tqa[i].op == 4) //delete
				delete_query(tqa[i].key);
			else if (tqa[i].op == 5) //scan
			{
		Query query;
		init_query(&query);
		query.key_p = key;
		query.op = 6;
		scan_query(&query);

		int len;
		query.op = 7;
		for (j=0;j<cnt;j++)
		{
			next_query(&query,result,&len);
		}
		free_query(&query);

			}
		}
	}

};
