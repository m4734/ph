#include "thread.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include<sys/socket.h>
#include <unistd.h>

void* thread_function(void* thread_parameter)
{
	struct Thread* thread = (struct Thread*)thread_parameter;

	list <Connection*>::iterator connection_list_iterator = thread->connection_list.begin(); // ??? but ok

	struct Connection* connection;
	int length;

	char query[10001];

	while(1)
	{
sleep(1); // too fast test
		printf("add connection\n");// add connection
		while (!thread->new_connection_queue.empty())
		{
			printf("new connection\n");
			connection = (struct Connection*)malloc(sizeof(struct Connection));
			connection->length = 0;
//			connection.query = connection.buffer;
			connection->socket = thread->new_connection_queue.front();

			thread->connection_list.insert(connection_list_iterator,connection);
			thread->new_connection_queue.pop();
		}

		//-------------------------------------------------------------------

		printf("select connection\n");//select connection
//		if (connection_list_lterator != connection_list.end())
//			connection_list_iterator++;
		if (connection_list_iterator == thread->connection_list.end())
		{
			printf("list loop\n");
			connection_list_iterator = thread->connection_list.begin();
			if (connection_list_iterator == thread->connection_list.end()) // empty
			{
				printf("no connection\n");
				sleep(1);
				continue;
			}
		}

		connection = *connection_list_iterator;

		//---------------------------------------------------------------------
		
		printf("get query\n");// get query
		length = read(connection->socket,connection->buffer + connection->length,10000-connection->length);
		if (length < 0)
		{
			if (errno == EAGAIN)
			{
				printf("nothing\n");
				connection_list_iterator++; //next connection
			}
			continue;
		}
		if (length == 0)
		{
			printf("disconnected\n");
			close(connection->socket);
			free(*connection_list_iterator);
			connection_list_iterator = thread->connection_list.erase(connection_list_iterator);
			continue;
		}
		connection->length+=length;
		if (connection->buffer[connection->length-1] != 0) // query continues
		{
			printf("query continue\n");
			connection_list_iterator++;
			continue;
		}
		connection->buffer[connection->length] = 0;

//-------------------------------------------------------------------------------

		printf("process query\n");// process query
		//
		// temp code
		fputs(connection->buffer,stdout);
		write(connection->socket,"nam nam\n",sizeof("nam nam\n"));

		connection->length = 0; //initialize connection buffer
		connection_list_iterator++;
	}
}
