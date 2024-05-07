#include <stdio.h>

#include "global2.h"
#include "log.h"

namespace PH
{

void PH_Interface::global_init(int n_t,int n_p,int n_l)
{
	printf("global init\n");
	num_thread = n_t;
	num_pmem = n_p;
	num_log = n_l;

	init_log(num_pmem,num_log);

}
void PH_Interface::global_clean()
{
	printf("global clean\n");
	clean_log();
}

int PH_Interface::insert_op(uint64_t key,unsigned char* value)
{
	return 0;
}
int PH_Interface::read_op(uint64_t key,unsigned char* buf)
{
	return 0;
}
int PH_Interface::delete_op(uint64_t key)
{
	return 0;
}
int PH_Interface::scan_op(uint64_t start_key,uint64_t end_key)
{
	return 0;
}
int PH_Interface::next_op(unsigned char* buf)
{
	return 0;
}

}
