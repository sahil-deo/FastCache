#include <cstdint>
#include <iostream>
#include <cstring>
#include "entry.h"
#include "hashTable.h"



StringHashTable StringTable = {new Entry[1024], 0, 1024};



size_t getStringIndex(std::string key);
void resizeStringTable(size_t new_capacity);

std::uint64_t generateStringHash(const char* key, size_t len){
    uint64_t hash = 14695981039346656037ull;
    for(size_t i = 0; i < len; ++i){
        hash^= static_cast<uint64_t>(key[i]);
        hash*= 1099511628211ull;
    }

    return hash;
}


const char* getString(std::string key){
    uint64_t hash = generateStringHash(key.c_str(), key.length());
    size_t index = hash % StringTable.capacity;
    while(StringTable.entries[index].key != nullptr && std::strcmp(StringTable.entries[index].key, key.c_str()) != 0){ // strcmp returns 0 if both values are same
        index = (index + 1) % StringTable.capacity;
    }

    if(StringTable.entries[index].key == nullptr){
        return nullptr;
    }

    return StringTable.entries[index].value;
}

void setString(std::string key, std::string value){

    if (StringTable.size >= (StringTable.capacity * 0.75)){
        //resizeStringTable
        resizeStringTable(StringTable.capacity*2);

    }

    uint64_t hash = generateStringHash(key.c_str(), key.length());

    size_t index = hash % StringTable.capacity; // StringTable size = StringTable.capacity

    while(StringTable.entries[index].key != nullptr && std::strcmp(StringTable.entries[index].key, key.c_str()) != 0){
        index = (index + 1) % StringTable.capacity;
    }
    
    Entry *e = &StringTable.entries[index];

    //delete data if key value already exists
    
    if(e->key == nullptr) ++StringTable.size;
    
    if (e->key != nullptr) delete[] e->key;
    if (e->value != nullptr) delete[] e->value;



    e->key = new char[key.size() + 1];
    e->value = new char[value.size() + 1];


    std::strcpy(e->key, key.c_str());
    std::strcpy(e->value, value.c_str());


}

std::string getKeys(){

    std::string result;
    for(int i = 0; i < StringTable.capacity; ++i){
        if(StringTable.entries[i].key != nullptr){
            result.append(StringTable.entries[i].key);
            result.append(" ");
        }
    }
    return result;
}

bool delKey(std::string key){
    size_t index = getStringIndex(key);
    
    if(index == StringTable.capacity) return false;

    delete[] StringTable.entries[index].key;
    delete[] StringTable.entries[index].value;
    StringTable.entries[index] = Entry{};

    return true;

}

size_t getStringIndex(std::string key){
    uint64_t hash = generateStringHash(key.c_str(), key.length());

    size_t index = hash % StringTable.capacity; // StringTable size = StringTable.capacity

    size_t attempts = 0;

    while(StringTable.entries[index].key != nullptr && std::strcmp(StringTable.entries[index].key, key.c_str()) != 0){
        index = (index + 1) % StringTable.capacity;
        ++attempts;
        if(attempts == StringTable.capacity)return StringTable.capacity;
    }

    return index;    
}


void resizeStringTable(size_t new_capacity)
{
    Entry* oldTable = StringTable.entries;
    size_t oldCapacity = StringTable.capacity;

    StringTable.entries = new Entry[new_capacity];
    StringTable.capacity = new_capacity;


    for(size_t i = 0; i < oldCapacity; ++i){
    
        if(oldTable[i].key != nullptr){

            std::string key = oldTable[i].key;
            std::string value = oldTable[i].value;
            
            setString(key, value);
            
            delete[] oldTable[i].key;
            delete[] oldTable[i].value;
        }
    }
    delete[] oldTable;
}



bool checkCollision(const char* key, size_t index) {
    if (StringTable.entries[index].key == nullptr) return false;  // empty slot
    return std::strcmp(StringTable.entries[index].key, key) != 0;
}