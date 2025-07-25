

#include <iostream>

struct Node{
    Node* after;
    Node * before;
    char* value;
};

struct NodeHeader{
    Node* first;
    Node* last;
    size_t size;
    char* key;
};