#include "thread.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include<sys/socket.h>
#include <unistd.h>
#include <cstring>

#include <poll.h>

#include <time.h> // test

//#include "query.h"
#include "global.h"
//#include "writeThread.h"

#define print 0
//#define print 0

#define ST 1000
/*
int add_new_write_thread(Thread* thread)
{
	if (thread->write_thread_num >= WRITE_THREAD_MAX)
		return -1;
	thread->write_pthread_array[thread->write_thread_num] = (pthread_t*)malloc(sizeof(pthread_t));
	thread->write_thread_array[thread->write_thread_num] = (WriteThread*)malloc(sizeof(WriteThread));
	init_write_thread(thread->write_thread_array[thread->write_thread_num]);
	pthread_create((thread->write_pthread_array[thread->write_thread_num]),NULL,process_connection,(void*)thread->write_thread_array[thread->write_thread_num]);
	thread->write_thread_num++;
	return 0;
}
*/

#define MAX_CONNECTION 100

void* thread_function(void* thread_parameter)
{
	Thread* thread = (Thread*)thread_parameter;

	list <Connection*>::iterator connection_list_iterator = thread->connection_list.begin(); // ??? but ok

	struct Connection* connection;
	int length;

//	unsigned char* result_v;
	unsigned char* result;
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

	struct pollfd pollfds[MAX_CONNECTION]; // connection per thread
	for (i=0;i<MAX_CONNECTION;i++) // connection per thread
		pollfds[i].fd = -1;
	int con=0;
	int prv;

	//	ms = 0;

	struct timespec ts1,ts2,ts3,ts4;
	bool ft;
	unsigned long long int t1,t2,rt,wt,pt;
	ft = 1;
	t1 = 0;
	t2 = 0;
	rt = wt = pt = 0;

	int nc = 0;


	unsigned char len_string[2];	

//	unsigned char write_buffer[10001];

//	result_v = (unsigned char*)malloc(sizeof(unsigned char)*QUERY_BUFFER);
//	result = &result_v;

	int min,mi,cs;
	thread->write_thread_num = 0;

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
			reset_query(&connection->query);

			connection->socket = thread->new_connection_queue.front();

			thread->connection_list.insert(connection_list_iterator,connection);
			thread->new_connection_queue.pop();

			for (i=0;i<MAX_CONNECTION;i++) // connection per thread
			{
				if (pollfds[i].fd == -1)
				{
					pollfds[i].fd = connection->socket;
					pollfds[i].events = POLLIN;
					pollfds[i].revents = 0;
					break;
				}
			}
			if (i >= MAX_CONNECTION) // cpt
				printf("too much connection\n");

			//			printf("socket %d\n",connection->socket);
		}

		//-------------------------------------------------------------------
		//		if (idle >= print_limit)i
		//		printf("select connection\n");//select connection
		//		if (connection_list_lterator != connection_list.end())
		//			connection_list_iterator++;			
#if 0			
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
					if (!ft)
					{
						printf("pro %llu loop %llu\n",t2,t1);
						ft = 1;
						t2 = 0;
						t1 = 0;
					}
					usleep(ST);
				}
				idle2=0;

				continue;
			}
		}
#endif
		if (thread->connection_list.size()==0) // empty
		{
			if (t1 > 0)
			{
				printf("-------------------------\n");
				printf("pro %llu %llu\nloop %llu %llu\n",t2/1000000000,t2%1000000000,t1/1000000000,t1%1000000000);
				printf("pt %llu %llu\n",pt/1000000000,pt%10000000000);
				printf("rt %llu %llu\n",rt/1000000000,rt%10000000000);
				printf("wt %llu %llu\n",wt/1000000000,wt%10000000000);
				printf("--------------------------\n");

				ft = 1;
				t2 = 0;
				t1 = 0;
				rt = wt = pt = 0;
			}
			usleep(ST);
		}
		prv = 0;
		if (true || (con || (prv = poll(pollfds,MAX_CONNECTION,1)) > 0)) // cpt
		{
			con = 0;
			connection_list_iterator = thread->connection_list.begin();
			while(connection_list_iterator != thread->connection_list.end())
			{

				connection = *connection_list_iterator;
				if (prv > 0)
				{

				for (i=0;i<MAX_CONNECTION;i++)//cpt
				{
					if (pollfds[i].fd == connection->socket)
						break;
				}
				
				if (i >= MAX_CONNECTION || pollfds[i].revents == 0) // cpt
				{
					connection_list_iterator++;
					continue;
				}
				}
//				con = 1;
				

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

					if (ft == 0)
					{
						clock_gettime(CLOCK_MONOTONIC,&ts4);	
						t1+= (ts4.tv_sec-ts3.tv_sec)*1000000000+ts4.tv_nsec-ts3.tv_nsec;
						ft = 1;
					}
					int rv;
					clock_gettime(CLOCK_MONOTONIC,&ts1);

					length = read(connection->socket,connection->query.buffer + connection->query.length,QUERY_BUFFER-connection->query.length);

					clock_gettime(CLOCK_MONOTONIC,&ts2);
					rt+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;

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
						for (i=0;i<MAX_CONNECTION;i++) // cpt
						{
							if (pollfds[i].fd == connection->socket)
							{
								pollfds[i].fd = -1;
								break;
							}
						}
						continue;
						//			break;
					}
					if (length < 0) // nothing
					{
						if (errno == EAGAIN)
						{
							//				if (idle >= print_limit)
//							printf("nothing\n");
							//				printf("nothing %d\n",++nc);
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

						clock_gettime(CLOCK_MONOTONIC,&ts1);

						rv = parse_query(&connection->query);

						clock_gettime(CLOCK_MONOTONIC,&ts2);
						pt+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;

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
							clock_gettime(CLOCK_MONOTONIC,&ts1);
							if (process_query(&connection->query,&result,&result_len) < 0)
								printf("query process error!!!\n");
							clock_gettime(CLOCK_MONOTONIC,&ts2);
							t2+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;


							//		fputs((char*)(*result),stdout);
							//		fputs("\n",stdout);
							if (print)
							{		
								printf("result_len %d ",result_len);
								for (i=0;i<result_len;i++)
									printf("%c",(result)[i]);
								printf("\n");
							}
							//		int t;
							//		scanf("%d",&t);
							len_string[0] = result_len/256;
							len_string[1] = result_len%256;

							clock_gettime(CLOCK_MONOTONIC,&ts1);
#if 0
							if ((rv = write(connection->socket,len_string,2)) < 0)
								printf("we1 %d\n",rv);

							if ((rv = write(connection->socket,*result,result_len)) < 0)
								printf("we2 %d\n",rv);
							//		write(connection->socket,&nl,1);
#endif
							memcpy(connection->result,len_string,2);
							memcpy(connection->result+2,result,result_len);
							connection->result_len = result_len+2;
while(1)
{
							min = WRITE_THREAD_CONNECTION_QUEUE_SIZE+1;
							mi = -1;
							for (i=0;i<thread->write_thread_num;i++)
							{
								cs = write_thread_connection_size(thread->write_thread_array[i]);
								if (min > cs)
								{
										min = cs;
										mi = i;
								}
							}
							if (mi == -1 ||	add_connection(thread->write_thread_array[mi],connection) < 0)
							{
//								if (add_new_write_thread((Thread*)/*thread*/(thread_parameter)) < 0)
//									continue;
	if (thread->write_thread_num >= WRITE_THREAD_MAX)
//		return -1;
		continue;		
	thread->write_pthread_array[thread->write_thread_num] = (pthread_t*)malloc(sizeof(pthread_t));
	thread->write_thread_array[thread->write_thread_num] = (WriteThread*)malloc(sizeof(WriteThread));
	init_write_thread(thread->write_thread_array[thread->write_thread_num]);
	pthread_create((thread->write_pthread_array[thread->write_thread_num]),NULL,process_connection,(void*)thread->write_thread_array[thread->write_thread_num]);
	thread->write_thread_num++;
//	return 0;

								add_connection(thread->write_thread_array[thread->write_thread_num-1],connection);
							}
							break;
}
/*
							memcpy(write_buffer,len_string,2);
							memcpy(write_buffer+2,result,result_len);

							if ((rv = write(connection->socket,write_buffer,result_len+2)) < 0)
								printf("we3 %d\n",rv);
								*/

							clock_gettime(CLOCK_MONOTONIC,&ts2);
							wt+=(ts2.tv_sec-ts1.tv_sec)*1000000000+ts2.tv_nsec-ts1.tv_nsec;

							//int t;
							//scanf("%d",&t);
							complete_query(&connection->query);
							//if (idle >= print_limit)
							//		printf("query complete\n");
							//idle = 1;
							idle/=2;

							// init query
							reset_query(&connection->query);

						ft = 0;
						clock_gettime(CLOCK_MONOTONIC,&ts3);

						con = 1;

						}
						//else need more rv == 1

						connection_list_iterator++;
						idle2=1;
						idle3=0;

						//}
						//		connection->query.length = 0; //initialize connection buffer
				}
			}
		}
	}

	for (i=9;i<thread->write_thread_num;i++)
{
		free(thread->write_pthread_array[i]);
		free(thread->write_thread_array[i]);
}
//	free(result_v);
}
