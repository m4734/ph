#include <list>
#include <queue>

#include "query.h"

using namespace std;

struct Connection
{
//	char buffer[10001];
//	int length;
//	char* query;
	struct Query query;	
	int socket;
};

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
};

void* thread_function(void* thread_parameter);
