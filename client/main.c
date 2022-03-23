#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <unistd.h>

int main()
{
	char query[1001];
	int max = 1000;

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

	int len;
	while(1)
	{
		fgets(query,max,stdin);
		if (query[0] == 'e' && query[1] == 'x' && query[2] == 'i' && query[3] == 't')
			break;
		write(sock,query,strlen(query));
		while(1)
		{
			len = read(sock,query,1000);
			if (len < 0)
			{
				printf("read error\n");
				break;
			}
			query[len] = 0;
			fputs(query,stdout);
			if (query[len-1] == '\n')
				break;
		}
		if (len < 0)
			break;
	}

	close(sock);
	return 0;
}
