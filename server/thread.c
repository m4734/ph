#include "thread.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include<sys/socket.h>
#include <unistd.h>
#include <cstring>

//#include "query.h"
#include "global.h"

#define print 0
//#define print 0

#define ST 1000

void* thread_function(void* thread_parameter)
{
	struct Thread* thread = (struct Thread*)thread_parameter;

	list <Connection*>::iterator connection_list_iterator = thread->connection_list.begin(); // ??? but ok

	struct Connection* connection;
	int length;

	unsigned char* result_v;
	unsigned char** result;
	int result_len;
	char nl;
	nl = '\n';
	int i;
	int idle=0;
	int idle2=0;
	int idle3=1;
	int print_limit=10000,ms=1000,sleep_limit=100000;
//	int change_connection;
	int rv;

//	ms = 0;

	unsigned char len_string[2];	

	result_v = (unsigned char*)malloc(sizeof(unsigned char)*QUERY_BUFFER);
	result = &result_v;

	while(1)
	{
//		usleep(1);
		if (idle >= sleep_limit)		
		{
			usleep(idle); // too fast test
			printf("sleep %d\n",idle);
		}
//		if (idle)
//		printf("check connection\n");// add connection
		while (!thread->new_connection_queue.empty())
		{
//			if (idle)
			printf("new connection\n");
			connection = (struct Connection*)malloc(sizeof(struct Connection));

			init_query(&connection->query);
//			connection->query.buffer_len = 0;
//			connection->query.buffer_offset = 0;
//			connection->query.cur = 0;
//			connection->query.n32=-1;
//			connection->query.buffer = connection->query.buffer_o;
//			connection.query = connection.buffer;
//			connection.query = alloc_query();			
			connection->query.node = NULL; // scan setting
			connection->query.kv_p = NULL;
			connection->query.offset = 2; // TAIL
			connection->socket = thread->new_connection_queue.front();

			thread->connection_list.insert(connection_list_iterator,connection);
			thread->new_connection_queue.pop();

//			printf("socket %d\n",connection->socket);
		}

		//-------------------------------------------------------------------
//		if (idle >= print_limit)i
//		printf("select connection\n");//select connection
//		if (connection_list_lterator != connection_list.end())
//			connection_list_iterator++;
		if (connection_list_iterator == thread->connection_list.end())
		{
//			if (idle >= print_limit)
//			printf("list loop\n");
			/*
				if (!idle2)
					usleep(ST);
				idle2=0;
				*/

			connection_list_iterator = thread->connection_list.begin();
			if (false && idle3)
			{
				printf("idle\n");
				usleep(1000*500);
			}
			idle3 = 1;
			if (connection_list_iterator == thread->connection_list.end()) // empty
			{
//				if (idle >= print_limit)
//				printf("no connection\n");
//				usleep(idle);
				if (idle < print_limit)
//					idle*=2;
					idle+=ms;					

				if (!idle2)
				{
					usleep(ST);
				}
				idle2=0;

				continue;
			}
		}

		connection = *connection_list_iterator;
//		change_connection = 0;
		//---------------------------------------------------------------------
//		if (idle >= print_limit)
//		printf("get query\n");// get query
#if 0
//		length = read(connection->socket,connection->query.buffer + connection->query.length,10000-connection->query.length);
		while(1) // read until fail or end of query
		{		
			if (connection->query.buffer_offset+connection->query.buffer_len == connection->query.cur)	// need to read
			{
//				if (print)
//					printf("read query\n");
				if (connection->query.cur == QUERY_BUFFER)
				{
					if (print)
						printf("pull buffer %d %d %d\n",connection->query.buffer_offset,connection->query.cur,connection->query.buffer_len);
					memmove(connection->query.buffer_o,connection->query.buffer_o+connection->query.buffer_offset,connection->query.buffer_len);
					if (connection->query.n32 >= 0)
						connection->query.n32-=connection->query.buffer_offset;
					connection->query.buffer_offset = 0;
					connection->query.cur = connection->query.buffer_len;
				}
				length = read(connection->socket,connection->query.buffer_o+connection->query.buffer_offset+connection->query.buffer_len,QUERY_BUFFER-connection->query.buffer_offset-connection->query.buffer_len);

				if (length < 0)
				{
					if (errno == EAGAIN)
					{
				if (idle >= print_limit)
				printf("nothing %d %d %d\n",connection->query.buffer_offset,connection->query.cur,connection->query.buffer_len);
						if (idle < print_limit)
//						idle*= 2;
							idle+=ms;			
						connection_list_iterator++; //next connection
					}
					else
						printf("errno %d\n",errno);
					change_connection = 1;
//			continue;
					break;			
				}
				if (length == 0)
				{
//			if (idle)
//					printf("disconnected %d\n",connection->socket); // unkown bug fix later
				printf("disconnected\n");
					connection_list_iterator = thread->connection_list.erase(connection_list_iterator);
					close(connection->socket);
//					free(*connection_list_iterator);
					free(connection);					
					change_connection = 1;
//			continue;
					break;
				}

				connection->query.buffer_len+=length; // don't add if -1
			}
//printf("length %d\n",length);
//int t;
//scanf("%d",&t);
			while (connection->query.cur < connection->query.buffer_offset+connection->query.buffer_len)
			{
				if (connection->query.buffer_o[connection->query.cur] == 32 && connection->query.n32 == -1)
					connection->query.n32 = connection->query.cur;
				if (connection->query.buffer_o[connection->query.cur] == 0 && connection->query.buffer_o[connection->query.cur-1] == '\n') // not in key
				{
					if (connection->query.cur - connection->query.n32 <= key_size+1) // if key size is not 8???
					{
				connection->query.cur++;

						continue;
					}
					break;
				}
				connection->query.cur++;
			}
			if (connection->query.cur < connection->query.buffer_offset+connection->query.buffer_len)
			{
			connection->query.cur++;
			if (print)
			printf("read query end\n");
			break;
			}

		}

/*
		if (connection->query.buffer[connection->query.length-1] != 0) // query continues
		{
			printf("query continue\n");
			if (connection->query.length >= 10000)
				printf("query length error!!!\n");
			connection_list_iterator++;
			continue;
		}
		connection->query.buffer[connection->query.length] = 0;
*/
//-------------------------------------------------------------------------------
if (change_connection == 0)
{
	connection->query.buffer = connection->query.buffer_o+connection->query.buffer_offset;
	connection->query.length = connection->query.cur-connection->query.buffer_offset;
	connection->query.buffer_offset = connection->query.cur;
	connection->query.buffer_len -= connection->query.length;
	connection->query.n32=-1;
if (idle >= print_limit)
		printf("process query\n");// process query
		//
		// temp code
		if (print)
		{
		printf("query length %d\n",connection->query.length);
		int i;
		for (i=0;i<connection->query.length;i++)
		{
			printf("[%d]",(unsigned int)connection->query.buffer[i]);
		}
		if (connection->query.length != 118)
		{
			int t;
			scanf("%d",&t);
		}
		}
//		fputs((char*)((connection->query).buffer),stdout);
//		write(connection->socket,"nam nam\n",sizeof("nam nam\n"));


		if (parse_query(&connection->query) < 0)
		{
			printf("query parse error!!!\n");
			printf("------------------------------------\n");
			printf("query length %d\n",connection->query.length);
			int i;
			for (i=0;i<connection->query.length;i++)
			{
				printf("[%d]",(unsigned int)connection->query.buffer[i]);
			}
	printf("------------------------------------\n");
			connection->query.length = 0; //initialize connection buffer
			connection_list_iterator++;

			len_string[0] = 0;
			len_string[1] = sizeof("error");

			if ((rv = write(connection->socket,len_string,2)) < 0)
				printf("wee1 %d\n",rv);

			if ((rv = write(connection->socket,"error",sizeof("error"))) < 0)
				printf("wee2 %d\n",rv);
//			write(connection->socket,&nl,1);
			continue;
		}
#endif
		int rv;
		length = read(connection->socket,connection->query.buffer + connection->query.length,QUERY_BUFFER-connection->query.length);
		/*
		if (connection->query.length+length >= 23 && connection->query.buffer[23] != 114)
		{
			printf("eesfesf\n");
		}
		*/
		if (length == 0) // disconnect
		{
//			if (idle)
//					printf("disconnected %d\n",connection->socket); // unkown bug fix later
			printf("disconnected\n");
			connection_list_iterator = thread->connection_list.erase(connection_list_iterator);
			close(connection->socket);
//			connection->query.offset = 0;
//			free(*connection_list_iterator);
//			free(connection->query.node);			
			free_query(&connection->query);
			free(connection);					
//			change_connection = 1;
			continue;
//			break;
		}
		if (length < 0) // nothing
		{
			if (errno == EAGAIN)
			{
//				if (idle >= print_limit)
//					printf("nothing\n");
//i				printf("nothing %d %d %d\n",connection->query.buffer_offset,connection->query.cur,connection->query.buffer_len);
				if (idle < print_limit)
//					idle*= 2;
					idle+=ms;			
				connection_list_iterator++; //next connection
			}
			else
				printf("errno %d\n",errno);
//			change_connection = 1;
			continue;
//			break;			
		}
		else
		{
			/*
		//test	
			for (i=0;i<length;i++)
				printf("[%d]",connection->query.buffer[connection->query.length+i]);
			printf("\n");
*/
			connection->query.length+=length;
		connection->query.buffer[connection->query.length] = 0;
		rv = parse_query(&connection->query);
		if (rv == -1) // error
		{
			printf("query parse error\n");
			printf("length %d\n",connection->query.length);
			for (i=0;i<connection->query.length;i++)
				printf("[%d]",connection->query.buffer[i]);
			printf("\n");
			connection_list_iterator = thread->connection_list.erase(connection_list_iterator);
			close(connection->socket);
			free(connection->query.node);
			free(connection);					
			continue;
		}
		if (rv == 0) // normal
		{
/*			
			printf("query length %d\n",connection->query.length);
			int i;
			for (i=0;i<connection->query.length;i++)
			{
				printf("[%d]",(unsigned int)connection->query.buffer[i]);
			}
			printf("\n");
*/			
		if (process_query(&connection->query,result,&result_len) < 0)
			printf("query process error!!!\n");

//		fputs((char*)(*result),stdout);
//		fputs("\n",stdout);
		if (print)
		{		
		printf("result_len %d ",result_len);
		for (i=0;i<result_len;i++)
			printf("%c",(*result)[i]);
		printf("\n");
		}
//		int t;
//		scanf("%d",&t);
		len_string[0] = result_len/256;
		len_string[1] = result_len%256;

		if ((rv = write(connection->socket,len_string,2)) < 0)
			printf("we1 %d\n",rv);

		if ((rv = write(connection->socket,*result,result_len)) < 0)
			printf("we2 %d\n",rv);
//		write(connection->socket,&nl,1);
//int t;
//scanf("%d",&t);
		complete_query(&connection->query);
//if (idle >= print_limit)
//		printf("query complete\n");
//idle = 1;
		idle/=2;

		// init query
		init_query(&connection->query);
 
		}
		//else need more rv == 1

		connection_list_iterator++;
		idle2=1;
		idle3=0;
//}
//		connection->query.length = 0; //initialize connection buffer
		}
	}

	free(result_v);
}
