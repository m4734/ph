#include "thread.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include<sys/socket.h>
#include <unistd.h>

//#include "query.h"

void* thread_function(void* thread_parameter)
{
	struct Thread* thread = (struct Thread*)thread_parameter;

	list <Connection*>::iterator connection_list_iterator = thread->connection_list.begin(); // ??? but ok

	struct Connection* connection;
	int length;

	char query[10001];
	unsigned char* result_v;
	unsigned char** result;
	int result_len;
	char nl;
	nl = '\n';
	int i;

	result_v = (unsigned char*)malloc(sizeof(unsigned char)*10000);
	result = &result_v;

	while(1)
	{
sleep(1); // too fast test
		printf("add connection\n");// add connection
		while (!thread->new_connection_queue.empty())
		{
			printf("new connection\n");
			connection = (struct Connection*)malloc(sizeof(struct Connection));
			connection->query.length = 0;
//			connection.query = connection.buffer;
//			connection.query = alloc_query();			
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
		length = read(connection->socket,connection->query.buffer + connection->query.length,10000-connection->query.length);
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
		connection->query.length+=length;
		if (connection->query.buffer[connection->query.length-1] != 0) // query continues
		{
			printf("query continue\n");
			if (connection->query.length >= 10000)
				printf("query length error!!!\n");
			connection_list_iterator++;
			continue;
		}
		connection->query.buffer[connection->query.length] = 0;

//-------------------------------------------------------------------------------

		printf("process query\n");// process query
		//
		// temp code

		fputs((char*)((connection->query).buffer),stdout);
//		write(connection->socket,"nam nam\n",sizeof("nam nam\n"));


		if (parse_query(&connection->query) < 0)
			printf("query parse error!!!\n");

		if (process_query(&connection->query,result,&result_len) < 0)
			printf("query process error!!!\n");

//		fputs((char*)(*result),stdout);
//		fputs("\n",stdout);
		for (i=0;i<result_len;i++)
			printf("%c",i);
		printf("\n");
		write(connection->socket,*result,result_len);
		write(connection->socket,&nl,1);

		complete_query(&connection->query);

		printf("query complete\n");

//		free_query(connection->query);
//		connection->query = alloc_query();

		connection->query.length = 0; //initialize connection buffer
		connection_list_iterator++;
	}

	free(result_v);
}
