#include <list>
#include <queue>

//#include "query.h"
//#include "connection.h"
#include "writeThread.h"

#define WRITE_THREAD_MAX 10

using namespace std;
/*
struct Connection
{
//	char buffer[10001];
//	int length;
//	char* query;
	struct Query query;	
	int socket;
	unsigned char result[10000];
	unsigned int result_len;
};
*/
class Thread
{
	public:
	list <struct Connection*> connection_list; // list?
//	queue <int> next_connection_queue; // next? // need atomic operation and not now
	queue <int> new_connection_queue;

//	list <int>::iterator connection_list_iterator; //moved

	/*
	int num_of_added_connection; // by main
	int num_of_removed_connection; // by thread
	// inactive connection
	*/

	// not now and just use round-robin
	//
	pthread_t* write_pthread_array[WRITE_THREAD_MAX];
	WriteThread* write_thread_array[WRITE_THREAD_MAX]; //??
	int write_thread_num;
};

void* thread_function(void* thread_parameter);
