#include "rapidjson/reader.h"
#include <unordered_map>
#include <iostream>
#include <vector>



struct JsonReader : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, JsonReader> {
    std::unordered_map<std::string, std::string> kvMap;
    std::unordered_map<std::string, std::vector<std::string>> kaMap;

    std::string m_currentKey;
    std::vector<std::string> m_currentArray;
    bool m_inArray = false;

    bool Key(const char* str, rapidjson::SizeType) {
        m_currentKey = str;
        return true;
    }

    bool Key(const char* str, rapidjson::SizeType length, bool) {
        m_currentKey = std::string(str, length);
        return true;
    }

    bool String(const char* str, rapidjson::SizeType) {
        if (m_inArray) {
            m_currentArray.push_back(str);
        } else {
            kvMap[m_currentKey] = str;
        }
        return true;
    }

    bool String(const char* str, rapidjson::SizeType length, bool) {
        std::string val(str, length);
        if (m_inArray) {
            m_currentArray.push_back(val);
        } else {
            kvMap[m_currentKey] = val;
        }
        return true;
    }

    bool StartArray() {
        m_inArray = true;
        m_currentArray.clear();
        return true;
    }

    bool EndArray(rapidjson::SizeType) {
        kaMap[m_currentKey] = m_currentArray;
        m_inArray = false;
        return true;
    }
};
