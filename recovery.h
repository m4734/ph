#include "shared.h"

namespace PH
{
	unsigned char* get_entry(EntryAddr &ea);
	uint64_t recover_cold_kv(NodeAddr &nodeAddr);
	uint64_t recover_warm_kv(NodeAddr &nodeAddr);

}
