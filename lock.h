#include <atomic>
namespace PH
{

void at_lock2(std::atomic<uint8_t> &lock);
void at_unlock2(std::atomic<uint8_t> &lock);
int try_at_lock2(std::atomic<uint8_t> &lock);

void at_lock2_test(std::atomic<uint8_t> &lock);
void at_unlock2_test(std::atomic<uint8_t> &lock);

}
