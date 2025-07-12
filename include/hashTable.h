#ifndef HASHTABLE_H  
#define HASHTABLE_H 

#include <stddef.h>

struct Entry;
struct NodeHeader;

struct ListHashTable{
    NodeHeader* nodeHeaders;
    size_t size;
    size_t capacity;
};

struct StringHashTable{
    Entry* entries;
    size_t size;
    size_t capacity;

};

#endif