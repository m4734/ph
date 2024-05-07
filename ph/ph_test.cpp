#include <stdio.h>

#include "global2.h"

int main()
{
	printf("ph_tset start\n");

	PH::PH_Interface phi;

	phi.global_init(1,1,1);

	phi.global_clean();
	printf("ph_test end\n");
	return 0;
}
