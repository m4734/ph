#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <unistd.h>

int main()
{
	char query[10001];
	int max = 10000;

	int sock;
	sock = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in server_addr;
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr=inet_addr("127.0.0.1"); // local host
	server_addr.sin_port = htons(5516); // temp port

	if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0)
	{
		printf("connect error\n");
		return 0;
	}

	int len,rv,i;
	while(1)
	{
		printf("ready\n");
		fgets(query,max,stdin);
		if (query[0] == 'e' && query[1] == 'x' && query[2] == 'i' && query[3] == 't')
			break;
		for (i=0;query[i] != '\n';i++);
		query[i+1] = 0;
//		printf("send query %s\n",query);
		write(sock,query,i+2);
		len = 0;
		while(1)
		{
			sleep(1); //too fast
//			printf("wait\n");
			rv = read(sock,query+len,max);
			printf("receive %d\n",rv);
			if (rv < 0)
			{
				printf("read error\n");
				break;
			}
//			for (i=0;i<rv;i++)
//				printf("%d ",(int)query[len+i]);
//			for (i=0;i<rv;i++)
//				printf("%c",query[len+i]);				
			len+=rv;
			if (query[len-1] == '\n'/*0*/) // client believes it will end with zero
				break;
		}

		if (rv < 0)
			break;

			query[len] = 0;
			fputs(query,stdout);

	}

	close(sock);
	return 0;
}
