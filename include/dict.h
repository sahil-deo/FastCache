#include <cstdint>
#include <iostream>
#include <cstring>
#include "entry.h"
#include "hashTable.h"

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"


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
    size_t index = getStringIndex(key);

    // uint64_t hash = generateStringHash(key.c_str(), key.length());

    // size_t index = hash % StringTable.capacity; // StringTable size = StringTable.capacity

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

    std::string result = "";

    if(StringTable.size == 0) return "\n";

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
        if(attempts >= StringTable.capacity)return StringTable.capacity;
    }

    return index;    
}

void resizeStringTable(size_t new_capacity)
{
    Entry* oldTable = StringTable.entries;
    size_t oldCapacity = StringTable.capacity;

    StringTable.entries = new Entry[new_capacity];
    StringTable.capacity = new_capacity;
    StringTable.size = 0;

    for(size_t i = 0; i < oldCapacity; ++i){
    
        if(oldTable[i].key != nullptr){

            char* key = oldTable[i].key;
            char* value = oldTable[i].value;
            
            uint64_t hash = generateStringHash(key, std::strlen(key));

            size_t index = hash%StringTable.capacity;

            while(StringTable.entries[i].key != nullptr)
            {
                index = (index+1) % StringTable.capacity;
            }
            
            StringTable.entries[i].key = key;
            StringTable.entries[i].value = value;
            ++StringTable.size;                       
        }
    }

    delete[] oldTable;
}

void getSnapDict(rapidjson::Writer<rapidjson::StringBuffer>& writer)
{

    for(int i = 0; i < StringTable.capacity; i++)
    {
        if (StringTable.entries[i].key == nullptr) continue;
        writer.Key(StringTable.entries[i].key);
        writer.String(StringTable.entries[i].value);
    }

}