
#ifndef kvs_h
#define kvs_h

class KVS
{
	int num_of_threads;
	int key_size;
	int value_size;
	int record_cnt;

	public:

	void init(int num,int key,int value,int record)
	{
		// KVS::init(num,key,value,record); // will need this
		num_of_threads = num;
		key_size = key;
		value_size = value;
		record_cnt = record;
	}

	void insert_op(unsigned char* key,unsigned char* value)
	{
	}
	void read_op(unsigned char* key,unsigned char* result)
	{
	}
	void update_op(unsigned char* key,unsigned char* value)
	{
	}
	void delete_op(unsigned char* key)
	{
	}
	void scan_op(unsigned char* key,int cnt,unsigned char** scan_result)
	{
	}

	void clean()
	{
	}

};

#endif
