#ifndef LOCK
#define LOCK

#include <omp.h>

class LockSet
{
   public:
    LockSet();
    omp_lock_t* get_node_num_lock();

    private:
    omp_lock_t node_num_lock;
};

#endif