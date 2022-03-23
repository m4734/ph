#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include <errno.h>

#include "global.h"

void temp_static_conf()
{
	num_of_thread = 4;
	connection_per_thread = 1;
	port = 5516; // bus
}

void print_conf()
{
}

// config
// alloc
void conf()
{
	temp_static_conf();

	total_connection = num_of_thread * connection_per_thread;

	print_conf();
}

void init_alloc()
{
}
void init()
{
	conf();
	init_alloc();
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

	while(1)
	{
		printf("wait client\n");
		client_addr_size = sizeof(client_addr);
		client_socket = accept(server_socket, (struct sockaddr*)&client_addr,&client_addr_size);

		flag = fcntl(client_socket, F_GETFL, 0);
		fcntl(client_socket, F_SETFL, flag | O_NONBLOCK); // non block socket

		printf("connected\n");

		// give it to thread

	}
}
int main()
{
	init();
	run();
	return 0;
}
