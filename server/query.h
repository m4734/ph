#pragma once

struct Query
{
	unsigned char buffer[10000];
//	char* key_p;
	unsigned char key_p[8];	
	unsigned char* value_p;
	int op;
	int key_len,value_len;
	int length;
	int offset;
};

unsigned char empty[10] = {"empty"};
int empty_len = 5;

int parse_query(Query* query);

int process_query(Query* query,char** result,int* result_len);

void complete_query(Query* query);
