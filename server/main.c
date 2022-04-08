#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include <errno.h>

#include <pthread.h>

#include "global.h"

#include "thread.h"
#include "data.h"

// temp global

//extern void* thread_function(void* thread_parameter);
/*
extern int num_of_thread;
extern int connection_per_thread;
extern int total_connection;
extern int port;
*/
pthread_t* pthread_list;
struct Thread* thread_list;

// config
// alloc
void conf()
{
	temp_static_conf();

}

void init_alloc()
{
	pthread_list = (pthread_t*)malloc(sizeof(pthread_t) * num_of_thread);
	thread_list = (Thread*) new Thread[num_of_thread];

	int i;
	for (i=0;i<num_of_thread;i++)
	{
		pthread_create(&pthread_list[i],NULL,thread_function,(void*)&thread_list[i]);
		//detach?
	}
}
void init()
{
	conf();
	init_alloc();
	data_init(); //???
}

// connection
void run()
{
	int server_socket, client_socket;
	int flag;
	int client_addr_size;
	int op=1;

	if ((server_socket = socket(AF_INET,SOCK_STREAM,0)) < 0)
	{
		printf("socket error\n");
		return;
	}

	if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &op, sizeof(op)) < 0)
	{
		printf("%d\n",server_socket);
		fprintf(stderr,"sockopt error %d\n",errno);
		return;
	}

	struct sockaddr_in server_addr, client_addr;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port);

	if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
	{
		printf("bind error\n");
		return;
	}

	if (listen(server_socket,1024) < 0)
	{
		printf("listen error\n");
		return;
	}

	printf("server socket ready\n");

	int thread_iterator=0;
	while(1)
	{
		printf("wait client\n");
		client_addr_size = sizeof(client_addr);
		client_socket = accept(server_socket, (struct sockaddr*)&client_addr,(socklen_t*)&client_addr_size);

		flag = fcntl(client_socket, F_GETFL, 0);
		fcntl(client_socket, F_SETFL, flag | O_NONBLOCK); // non block socket

		printf("connected\n");

		// give it to thread


		thread_list[thread_iterator].new_connection_queue.push(client_socket);
		printf("connection added\n");
		++thread_iterator;
		if (thread_iterator >= num_of_thread)
			thread_iterator = 0;

	}
}

void clean()
{
	free(pthread_list);
	delete []thread_list;

	data_clean();
}

int main()
{
	init();
	run();
//	clean(); // never end?? join thread first
	return 0;
}
