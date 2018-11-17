//
// Created by 张艺文 on 2018/11/15.
//

#include <unistd.h>
#include <stdint.h>

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

/*
 * file_exists -- checks if file exists
 */
static inline int
file_exists(char const *file)
{
    return access(file, F_OK);
}

/*
 * find_last_set_64 -- returns last set bit position or -1 if set bit not found
 */
static inline int
find_last_set_64(uint64_t val)
{
    return 64 - __builtin_clzll(val) - 1;
}