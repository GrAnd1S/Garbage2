#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include "cut.h"

#define CHECK_NOT_EMPTY(f) do { \
    if (cut_File(f, "")) { \
        cut_Check("content of file is empty", __FILE__, __LINE__); \
    } } while(0)

#define CHECK_IS_EMPTY(f) do { \
    if (!cut_File(f, "")) { \
        cut_Check("content of file is not empty", __FILE__, __LINE__); \
    } } while(0)

#endif // TEST_UTILS_H
