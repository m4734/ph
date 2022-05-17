#include "query.h"

struct Connection
{
//	char buffer[10001];
//	int length;
//	char* query;
	struct Query query;	
	int socket;
	unsigned char result[10000];
	unsigned int result_len;
};

