#include <stdio.h>
#include <stdlib.h>
#include <time.h>


int ops;
int key_size;
int value_size;

char** key_array;

char rand_char()
{
	int rv;
	rv = rand()%(26+26+10);
	if (rv < 26)
		return 'A'+rv;
	if (rv < 26+26)
		return 'a'+rv-26;
	return '0'+rv-26-26;
}

void rand_gen(char* ptr,int size)
{
	int i;
	for (i=0;i<size;i++)
		ptr[i] = rand_char();
}

int main()
{
	srand(time(NULL));

//duplication!!!!!!!!!!!


	printf("ops\n");
	scanf("%d",&ops);
	printf("key_size\n");
	scanf("%d",&key_size);
	printf("value_size\n");
	scanf("%d",&value_size);

	int i,rv;
	char* value;

	FILE* out;

	value = (char*)malloc(value_size);
	key_array = (char**)malloc(sizeof(char*)*ops);
	for (i=0;i<ops;i++)
		key_array[i] = (char*)malloc(key_size);


	printf("insert\n");
	out = fopen("insert","w");
	fprintf(out,"%d\n",ops);
	for (i=0;i<ops;i++)
	{
		rand_gen(key_array[i],key_size);
		rand_gen(value,value_size);
		fprintf(out,"INSERT %s %s\n",key_array[i],value);
	}
	fclose(out);

	printf("read\n");
	out = fopen("read","w");
	fprintf(out,"%d\n",ops);
	for (i=0;i<ops;i++)
	{
		rv = rand()%ops;
		fprintf(out,"READ %s\n",key_array[rv]);
	}
	fclose(out);

	printf("update\n");
	out = fopen("update","w");
	fprintf(out,"%d\n",ops);
	for (i=0;i<ops;i++)
	{
		rv = rand()%ops;
		rand_gen(value,value_size);
		fprintf(out,"UPDATE %s %s\n",key_array[rv],value);
	}
	fclose(out);

	printf("delete\n"); // duplicate
	out = fopen("delete","w");
	fprintf(out,"%d\n",ops);
	for (i=0;i<ops;i++)
	{
		rv = rand()%ops;
		fprintf(out,"DELETE %s\n",key_array[rv]);
	}
	fclose(out);

	printf("scan\n");
	out = fopen("scan","w");
	fprintf(out,"%d\n",ops);
	for (i=0;i<ops;i++)
	{
		rv = rand()%ops;
		rand_gen(key_array[i],key_size);
		fprintf(out,"SCAN %s 100\n",key_array[rv]);
	}
	fclose(out);

	for (i=0;i<ops;i++)
		free(key_array[i]);
	free(key_array);
	free(value);

	return 0;
}
