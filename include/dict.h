#include <cstdint>
#include <iostream>
#include <cstring>
#include "entry.h"
#include "hashTable.h"

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"


StringHashTable m_stringTable = {new Entry[1024], 0, 1024};



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
    size_t index = hash % m_stringTable.capacity;
    

    while(m_stringTable.entries[index].key != nullptr && std::strcmp(m_stringTable.entries[index].key, key.c_str()) != 0){ // strcmp returns 0 if both values are same
        index = (index + 1) % m_stringTable.capacity;
    }

    if(m_stringTable.entries[index].key == nullptr){
        return nullptr;
    }

    return m_stringTable.entries[index].value;
}

void setString(std::string key, std::string value){

    if (m_stringTable.size >= (m_stringTable.capacity * 0.75)){
        //resizeStringTable
        resizeStringTable(m_stringTable.capacity*2);

    }
    size_t index = getStringIndex(key);

    // uint64_t hash = generateStringHash(key.c_str(), key.length());

    // size_t index = hash % m_stringTable.capacity; // m_stringTable size = m_stringTable.capacity

    while(m_stringTable.entries[index].key != nullptr && std::strcmp(m_stringTable.entries[index].key, key.c_str()) != 0){
        index = (index + 1) % m_stringTable.capacity;
    }
    
    Entry *e = &m_stringTable.entries[index];

    //delete data if key value already exists
    
    if(e->key == nullptr) ++m_stringTable.size;
    
    if (e->key != nullptr) delete[] e->key;
    if (e->value != nullptr) delete[] e->value;



    e->key = new char[key.size() + 1];
    e->value = new char[value.size() + 1];


    std::strcpy(e->key, key.c_str());
    std::strcpy(e->value, value.c_str());


}

std::string getKeys(){

    std::string result = "";

    if(m_stringTable.size == 0) return "\n";

    for(int i = 0; i < m_stringTable.capacity; ++i){
        if(m_stringTable.entries[i].key != nullptr){
            result.append(m_stringTable.entries[i].key);
            result.append(" ");
        }
    }
    return result;
}

bool delKey(std::string key){
    size_t index = getStringIndex(key);
    
    if(index == m_stringTable.capacity) return false;

    delete[] m_stringTable.entries[index].key;
    delete[] m_stringTable.entries[index].value;
    m_stringTable.entries[index] = Entry{};

    return true;

}

size_t getStringIndex(std::string key){
    uint64_t hash = generateStringHash(key.c_str(), key.length());

    size_t index = hash % m_stringTable.capacity; // m_stringTable size = m_stringTable.capacity

    size_t attempts = 0;

    while(m_stringTable.entries[index].key != nullptr && std::strcmp(m_stringTable.entries[index].key, key.c_str()) != 0){
        index = (index + 1) % m_stringTable.capacity;
        ++attempts;
        if(attempts >= m_stringTable.capacity)return m_stringTable.capacity;
    }

    return index;    
}

void resizeStringTable(size_t new_capacity)
{
    Entry* oldTable = m_stringTable.entries;
    size_t oldCapacity = m_stringTable.capacity;

    m_stringTable.entries = new Entry[new_capacity];
    m_stringTable.capacity = new_capacity;
    m_stringTable.size = 0;

    for(size_t i = 0; i < new_capacity; ++i) {
        m_stringTable.entries[i] = Entry{};
    }


    for(size_t i = 0; i < oldCapacity; ++i){
        if(oldTable[i].key != nullptr){

            char* key = oldTable[i].key;
            char* value = oldTable[i].value;
            
            uint64_t hash = generateStringHash(key, std::strlen(key));

            size_t index = hash%m_stringTable.capacity;

            while(m_stringTable.entries[i].key != nullptr)
            {
                index = (index+1) % m_stringTable.capacity;
            }
            
            m_stringTable.entries[index].key = key;
            m_stringTable.entries[index].value = value;
            ++m_stringTable.size;                       
        }
    }

    delete[] oldTable;
}

void getSnapDict(rapidjson::Writer<rapidjson::StringBuffer>& writer)
{

    for(int i = 0; i < m_stringTable.capacity; i++)
    {
        if (m_stringTable.entries[i].key == nullptr) continue;
        writer.Key(m_stringTable.entries[i].key);
        writer.String(m_stringTable.entries[i].value);
    }

}

void delAllStrings()
{
    for(int i = 0; i < m_stringTable.capacity; i++)
    {
        Entry& e = m_stringTable.entries[i];
        delete[] e.key;
        delete[] e.value;

        e.key = nullptr;
        e.value = nullptr;
    }
    m_stringTable.size = 0;
    // resizeStringTable(1024);
}

size_t getSizeDict()
{
    return m_stringTable.size;
}