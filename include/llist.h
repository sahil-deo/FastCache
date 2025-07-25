#include <iostream>
#include <cstdint>
#include "hashTable.h"
#include <cstring>
#include <sstream>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/reader.h"

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


NodeHeader* initializeNodeHeaders(size_t capacity) {
    NodeHeader* headers = new NodeHeader[capacity];
    for(size_t i = 0; i < capacity; ++i) {
        headers[i].first = nullptr;
        headers[i].last = nullptr;
        headers[i].size = 0;
        headers[i].key = nullptr;
    }
    return headers;
}

ListHashTable ListTable = {initializeNodeHeaders(1024), 0, 1024};


std::uint64_t generateListHash(const char* key, size_t len)
{
    uint64_t hash = 14695981039346656037ull;
    for(size_t i = 0; i < len; ++i){
        hash^= static_cast<uint64_t>(key[i]);
        hash*= 1099511628211ull;
    }

    return hash;
}

size_t getListIndex(std::string key)
{
    uint64_t hash = generateListHash(key.c_str(), key.length());
    
    size_t index = hash % ListTable.capacity;
    
    
    size_t attempts = 0;
    while(ListTable.nodeHeaders[index].key != nullptr && std::strcmp(ListTable.nodeHeaders[index].key, key.c_str()) != 0){
        index = (index + 1) % ListTable.capacity;
        ++attempts;
        
        if(attempts >= ListTable.capacity){
            return ListTable.capacity;
        }
    }  

    return index;
}

void resizeListTable(size_t new_capacity)
{
    ListHashTable oldTable = ListTable;
    size_t oldCapacity = ListTable.capacity;

    ListTable.nodeHeaders = initializeNodeHeaders(new_capacity);
    ListTable.capacity = new_capacity;
    ListTable.size = 0;

    for (size_t i = 0; i < oldCapacity; ++i) {
        if (oldTable.nodeHeaders[i].key != nullptr) {

            // Rehash using old key
            uint64_t hash = generateListHash(oldTable.nodeHeaders[i].key, std::strlen(oldTable.nodeHeaders[i].key));
            size_t index = hash % new_capacity;

            size_t attempts = 0;
            while (ListTable.nodeHeaders[index].key != nullptr && 
                std::strcmp(ListTable.nodeHeaders[index].key, oldTable.nodeHeaders[i].key) != 0) {
                index = (index + 1) % new_capacity;
                ++attempts;
                if (attempts >= new_capacity) break;  
            }

            ListTable.nodeHeaders[index] = oldTable.nodeHeaders[i];

            oldTable.nodeHeaders[i].key = nullptr;
            oldTable.nodeHeaders[i].first = nullptr;
            oldTable.nodeHeaders[i].last = nullptr;
            oldTable.nodeHeaders[i].size = 0;

            ListTable.size++;
        }
    }
    
    // Clean up old table
    delete[] oldTable.nodeHeaders;
}




void pushBackList(std::string key, std::string value)
{
    if(ListTable.size >= (ListTable.capacity * 0.75)) 
        resizeListTable(ListTable.size*2);
    
    size_t index = getListIndex(key);
    if(index >= ListTable.capacity){    
        std::cout << "Out of Bounds List Push\n"; 
        return;
    } 
    
    NodeHeader* header = &ListTable.nodeHeaders[index];
    if (header->first == nullptr){
        
        // index = getListIndex(key);
        // header = &ListTable.nodeHeaders[index];
        
        ListTable.size++;

        header->first = new Node;
        header->last = header->first;
        
        header->first->after = nullptr;
        header->first->before = nullptr;
        
        header->key = new char[key.size()+1];
        std::strcpy(header->key, key.c_str());
        
        header->first->value = new char[value.size()+1];
        std::strcpy(header->first->value, value.c_str());
    }  
    else{
        Node *newNode = new Node;
        newNode->after = nullptr;
        newNode->before = header->last;
        
        header->last->after = newNode;
        header->last = newNode;
        newNode->value = new char[value.size()+1];
        std::strcpy(newNode->value, value.c_str());
    }
    header->size++;
}

std::string popBackList(std::string key)
{
    size_t index = getListIndex(key);
    if(index >= ListTable.capacity){    
        std::cout << "Out of Bounds List Push\n"; 
        return "\n";
    } 

    NodeHeader* header = &ListTable.nodeHeaders[index];
    
    if(header->key == nullptr) return "";
    
    Node* currentNode = header->last;
    
    if(currentNode == nullptr) return ""; // list is broken if current node is nullptr; add something to fix the list
    
    
    if(header->last == header->first){
        header->last = nullptr;
        header->first = nullptr;
        std::string value = currentNode->value;
        delete[] currentNode->value;
        delete currentNode;
        
        header->size--;
        return value;
    }
    
    currentNode->before->after = nullptr;
    header->last = currentNode->before;
    
    std::string value = currentNode->value;
    delete[] currentNode->value;
    delete currentNode;
    
    header->size--;
    return value;
}

void pushFrontList(std::string key, std::string value)
{
    size_t index = getListIndex(key);
    
    if(index >= ListTable.capacity){    
        std::cout << "Out of Bounds List Push\n"; 
        return;
    } 
    
    NodeHeader* header = &ListTable.nodeHeaders[index];
    if (header->first == nullptr){
        
        ListTable.size++;
        if(ListTable.size >= (ListTable.capacity * 0.75)) resizeListTable(ListTable.size*2);

        header->first = new Node;
        header->last = header->first;
        
        header->first->after = nullptr;
        header->first->before = nullptr;
        
        header->key = new char[key.size()+1];
        std::strcpy(header->key, key.c_str());
        
        header->first->value = new char[value.size()+1];
        std::strcpy(header->first->value, value.c_str());
    }  
    else{
        Node *newNode = new Node;
        newNode->before = nullptr;
        newNode->after = header->first;
        
        header->first->before = newNode;
        header->first = newNode;
        newNode->value = new char[value.size()+1];
        std::strcpy(newNode->value, value.c_str());
    }
    header->size++;
}


std::string popFrontList(std::string key)
{

    size_t index = getListIndex(key);
    if(index >= ListTable.capacity){    
        std::cout << "Out of Bounds List Push\n"; 
        return "\n";
    } 

    NodeHeader* header = &ListTable.nodeHeaders[index];
    
    if(header->key == nullptr) return "";
    
    Node* currentNode = header->first;
    
    if(currentNode == nullptr) return ""; // list is broken if current node is nullptr; add something to fix the list
    
    
    if(header->last == header->first){
        header->last = nullptr;
        header->first = nullptr;
        std::string value = currentNode->value;
        delete[] currentNode->value;
        delete currentNode;
        
        header->size--;
        return value;
    }
    
    currentNode->after->before = nullptr;
    header->first = currentNode->after;
    
    std::string value = currentNode->value;
    delete[] currentNode->value;
    delete currentNode;
    
    header->size--;
    return value;
}


std::string getList(std::string key)
{
    
    size_t index = getListIndex(key);
    
    
    if(index >= ListTable.capacity){    
        std::cout << "Out of Bounds List Get\n"; 
        return "\n";
    } 
    

    std::string result = "";
    
    NodeHeader* header = &ListTable.nodeHeaders[index];
    
    
    
    if(header->key == nullptr) return "-1\n";
    
    Node* currentNode = header->first;
    
    
    if(currentNode == nullptr) return "-1\n";
    
    while(currentNode != nullptr){
        
        if(currentNode->value){
            
            result.append(currentNode->value);
            result.append(" ");
        }
        currentNode = currentNode->after;
    }
    result.append("\n");
    
    return result;
    
}

std::string getListR(std::string key, int list_index)
{
    size_t index = getListIndex(key);

    if(ListTable.nodeHeaders[index].key == nullptr) return "Invalid Key\n";

    NodeHeader* header = &ListTable.nodeHeaders[index];

    if(header->size <= list_index) return "Index Out of Bounds\n";

    Node* currentNode; 
    if(list_index > (header->size/2)) { // If index if closer to last then search from last otherwise search from start

        currentNode = header->last;
        
        int i = header->size-1;
        
        while(i > list_index){
            currentNode = currentNode->before;
            --i;
        }
    }
    else{
                
        currentNode = header->first;
        
        int i = 0;

        while(i < list_index){
            currentNode = currentNode->after;
            ++i;
        }
    }

    std::string result = currentNode->value;
    result.append("\n");
    return result;
}

bool delList(std::string key)
{
    size_t index = getListIndex(key);

    if(index >= ListTable.capacity){    
        std::cout << "Out of Bounds List Push\n"; 
        return false;
    }

    NodeHeader* header = &ListTable.nodeHeaders[index];
    
    if(header->key == nullptr || strcmp(header->key, key.c_str()) != 0) return false;

    // Delete all nodes 
    Node* currentNode = header->first;
    while(currentNode != nullptr) {
        Node* nextNode = currentNode->after;

        if(currentNode->value != nullptr)
            delete[] currentNode->value;
        
        delete currentNode;
        currentNode = nextNode;
    }
    
    // Clean up the header
    delete[] header->key;
    header->key = nullptr;
    header->first = nullptr;
    header->last = nullptr;
    header->size = 0;
    ListTable.size--;

    return true;
}

bool delListR(std::string key, int list_index)
{
    size_t index = getListIndex(key);

    if(ListTable.nodeHeaders[index].key == nullptr) return false;

    NodeHeader* header = &ListTable.nodeHeaders[index];

    if(header->size <= list_index) 
    {
        std::cout << "Index out of bounds\n";
        return false;
    }
    
    Node* currentNode; 
    if(list_index > (header->size/2)) // If index is closer to last then search from last otherwise search from start
    {
        currentNode = header->last;
        
        int i = header->size-1;
        
        while(i > list_index){
            currentNode = currentNode->before;
            --i;
        }
    }
    else{
        currentNode = header->first;
        
        int i = 0;

        while(i < list_index){
            currentNode = currentNode->after;
            ++i;
        }
    }

    // Handle the case where we're removing the only node
    if(header->first == header->last) {
        header->first = nullptr;
        header->last = nullptr;
    }
    // Handle the case where we're removing the first node
    else if(currentNode == header->first) {
        header->first = currentNode->after;
        header->first->before = nullptr;
    }
    // Handle the case where we're removing the last node
    else if(currentNode == header->last) {
        header->last = currentNode->before;
        header->last->after = nullptr;
    }
    // Handle the case where we're removing a middle node
    else {
        currentNode->before->after = currentNode->after;
        currentNode->after->before = currentNode->before;
    }

    header->size--;

    delete[] currentNode->value;
    delete currentNode;

    return true;
}

std::string getListKeys()
{
    std::string result = "";

    if(ListTable.size == 0) return "\n";

    for(int i = 0; i < ListTable.capacity; ++i){
        if(ListTable.nodeHeaders[i].key != nullptr){
            result.append(ListTable.nodeHeaders[i].key);
            result.append(" ");
        }
    }


    result.append("\n");
    
    // std::cout << "LKEYS OBTAINED" << "\n";
    return result;
}

void getSnapList(rapidjson::Writer<rapidjson::StringBuffer>& writer)
{
    
    /*
    std::string result = "LISTS ";

    for(int i = 0; i < ListTable.capacity; i++){
        

    if(ListTable.nodeHeaders[i].key == nullptr) continue;
    NodeHeader* header = &ListTable.nodeHeaders[i];

    Node* currentNode = header->first;

    result.append(std::to_string(header->size));
    result.append(" ");
    result.append(header->key);
    result.append(" ");
    while(currentNode != nullptr)
    {
        
    result.append(currentNode->value);
    result.append(" ");
    currentNode = currentNode->after;
    }
    
    }
    return result;
    */


    for(int i = 0; i < ListTable.capacity; i++)
   {
       if(ListTable.nodeHeaders[i].key == nullptr) continue;
       
       NodeHeader* header = &ListTable.nodeHeaders[i];
       
       Node* currentNode = header->first;
       
       writer.Key(header->key);
       writer.StartArray();
       while(currentNode != nullptr)
       {
           writer.String(currentNode->value);
           currentNode = currentNode->after;
        }
        writer.EndArray();
    }   
}
