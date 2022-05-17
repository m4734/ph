#include "writeThread.h"
#include<unistd.h>
#include<stdio.h>

void init_write_thread(WriteThread* wt)
{
	wt->start = 0;
	wt->end = 0;
}

int add_connection(WriteThread* wt, Connection* connection)
{
	if (wt->end+1 == wt->start || (wt->start == 0 && wt->end == WRITE_THREAD_CONNECTION_QUEUE_SIZE-1))
		return -1; // FULL
	wt->connection_queue[wt->end] = connection;
	wt->end++;
	if (wt->end == WRITE_THREAD_CONNECTION_QUEUE_SIZE)
		wt->end = 0;
	return 0;
}

void* process_connection(void* writeThread)
{
	WriteThread* wt = (WriteThread*)writeThread;
	int rv;
	while(1)
	{
		if (wt->start == wt->end)
		{
			usleep(1);
			continue;
		}
		if (rv = write(wt->connection_queue[wt->start]->socket,wt->connection_queue[wt->start]->result,wt->connection_queue[wt->start]->result_len) < 0)
			printf("we4 %d\n",rv);
		wt->start++;
		if (wt->start == WRITE_THREAD_CONNECTION_QUEUE_SIZE)
			wt->start = 0;
	}
}

int write_thread_connection_size(WriteThread* wt)
{
	if (wt->end >= wt->start)
		return wt->end - wt->start;
	return wt->end + WRITE_THREAD_CONNECTION_QUEUE_SIZE - wt->start;
}
