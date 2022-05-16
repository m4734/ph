

#include "connection.h"

#define WRITE_THREAD_CONNECTION_QUEUE_SIZE 5

struct WriteThread
{
	struct Connection* connection_queue[WRITE_THREAD_CONNECTION_QUEUE_SIZE];
	volatile int start,end;
};

void init_write_thread(WriteThread* wt);
int add_connection(WriteThread* wt,Connection* connection);
void* process_connection(void* writeThread);
int write_thread_connection_size(WriteThread* wt);
