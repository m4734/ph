#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <unistd.h>

// insert ' ' key_8 ' ' value_len value


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

	int len,rv,i,lenlen,n32,n32_l[100];
	char key[9];
	while(1)
	{
		printf("ready\n");
		fgets(query,max,stdin);
		if (query[0] == 'e' && query[1] == 'x' && query[2] == 'i' && query[3] == 't')
			break;
		n32 = 0;
		for (i=0;query[i] != '\n';i++)
		{
			if (query[i] == ' ')
				n32_l[n32++] = i;
		}
		n32_l[n32++] = i;
		write(sock,query,n32_l[0]); // send query
		if (n32 > 1) // send key
		{
			for (i=0;i<=8;i++)
				key[i] = 0;
			key[0] = ' ';
			for (i=n32_l[1]-1;i>n32_l[0];i--)
				key[8-((n32_l[1]-1)-i)] = query[i];
			write(sock,key,9);
		}
		if (n32 > 2) // send value
		{
			len = n32_l[2]-n32_l[1]-1;
			for (i=1;i<=8;i++)
				key[i] = 0;
			key[7] = len/256;
			key[8] = len%256;
			write(sock,key,9);
			write(sock,query+n32_l[1]+1,len);

		}

		lenlen = 0;

		read(sock,key,2);
		len = key[0]*256+key[1];
		printf("len %d\n",len);
//		while(lenlen < len)
//		{
//			sleep(1); //too fast
//			printf("wait\n");
			rv = read(sock,query+lenlen,max);
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
			lenlen+=rv;
//		}

		if (rv < 0)
			break;

			query[len] = 0;
//			fputs(query,stdout);
		for (i=0;i<len;i++)
			printf("[%d]",query[i]);
		printf("\n");		

	}

	close(sock);
	return 0;
}
