//#include "../test/test.h"

#include "query.h"
#include "global.h"
#include "data.h"
#include "hash.h"
#include "thread.h"

//using namespace PH;

class KVS_ph : public KVS
{
	public:
//		unsigned char* result=NULL;

		~KVS_ph()
		{
//			if (result)
//				free(result);
		}

	virtual void init(int num,int key,int value,int record)
	{
		KVS::init(num,key,value,record); // will need this
		PH::temp_static_conf(num,key,value);
//		init_hash();
//		init_thread();
//		init_data();
//		result = (unsigned char*)malloc(value+key+PH::len_size);
		printf("kvs_ph init\n");
	}

	virtual void insert_op(unsigned char* key,unsigned char* value)
	{
		PH::insert_query(key,value);
	}
	virtual void read_op(unsigned char* key,unsigned char* result)
	{
		int len;
		PH::lookup_query(key,result,&len);
	}

	virtual int read_op2(unsigned char* key,unsigned char* result)
	{
		int len;
		return PH::lookup_query(key,result,&len);
//		result[0] = 'o';
//		result[1] = 'k';
//		return 0;
	}
	virtual int read_op2(unsigned char* key,std::string *value)
	{
		return PH::lookup_query(key,value);
//		return 0;
	}
	virtual void update_op(unsigned char* key,unsigned char* value)
	{
		PH::insert_query(key,value);
	}
	virtual void delete_op(unsigned char* key)
	{
		PH::delete_query(key);
	}
	virtual void scan_op(unsigned char* key,int cnt,unsigned char** scan_result)
	{
		PH::Query query;
		PH::init_query(&query);
		query.key_p = key;
		query.op = 6;
		PH::scan_query(&query);

		int i,len;
		query.op = 7;
		for (i=0;i<cnt;i++)
		{
			PH::next_query(&query,scan_result[i],&len);
		}
		PH::free_query(&query);
	}

	virtual void clean()
	{
		printf("ph clean1\n");
//		PH::clean_data();
//		PH::clean_hash();
//		PH::clean_thread();
		PH::clean();
		printf("ph clean2\n");
	}

	virtual void run(TestQuery* tqa, int ops)
	{
		int i,len,j;
		unsigned char* result;
		result = (unsigned char*)malloc(value_size+key_size+PH::len_size);
		for (i=0;i<ops;i++)
		{
			if (tqa[i].op == 1) // insert
//				PH::insert_query_l(tqa[i].key,tqa[i].value);
				PH::insert_query(tqa[i].key,tqa[i].value);
			else if (tqa[i].op == 2) // read
			{
				PH::lookup_query(tqa[i].key,result,&len);
				if (len == 0 && result[0] == 0)
					printf("0\n");
			}
			else if (tqa[i].op == 3) // update
//				PH::insert_query_l(tqa[i].key,tqa[i].value);
				PH::insert_query(tqa[i].key,tqa[i].value);
			else if (tqa[i].op == 4) //delete
				PH::delete_query(tqa[i].key);
			else if (tqa[i].op == 5) //scan
			{
				PH::Query query;
				PH::init_query(&query);
				query.key_p = tqa[i].key;
				query.op = 6;
				PH::scan_query(&query);

				int len;
				query.op = 7;
				for (j=0;j<tqa[i].cnt;j++)
				{
					PH::next_query(&query,result,&len);
				}
				PH::free_query(&query);
			}
		}
		free(result);
	}
	
	virtual void reset()
	{
		PH::reset_thread();
	}
	virtual void exit_thread()
	{
		PH::exit_thread();
	}
};
