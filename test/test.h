
#ifndef kvs_h
#define kvs_h

class KVS
{
	int num_of_threads;
	int key_size;
	int value_size;
	int record_cnt;

	public:

	virtual void init(int num,int key,int value,int record)
	{
		// KVS::init(num,key,value,record); // will need this
		num_of_threads = num;
		key_size = key;
		value_size = value;
		record_cnt = record;
		printf("init..\n");
	}

	virtual void insert_op(unsigned char* key,unsigned char* value)
	{
	}
	virtual void read_op(unsigned char* key,unsigned char* result)
	{
	}
	virtual void update_op(unsigned char* key,unsigned char* value)
	{
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

};

#endif
