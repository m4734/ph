#include <cstdint>

#define VERSION_SIZE 8
#define KEY_SIZE 8
#define VALUE_SIZE 100

struct BaseLogEntry
{
	uint64_t dver; // delete + version
	uint64_t key; // key
	unsigned char value[VALUE_SIZE];
	unsigned char pad[4];
}; // 8 + 8 + 100 = 116 + 4 = 120
// need 8bytes align

