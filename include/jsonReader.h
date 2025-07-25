#include "rapidjson/reader.h"
#include <unordered_map>
#include <iostream>
#include <vector>



struct JsonReader : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, JsonReader> {
    std::unordered_map<std::string, std::string> kvMap;
    std::unordered_map<std::string, std::vector<std::string>> kaMap;

    std::string currentKey;
    std::vector<std::string> currentArray;
    bool inArray = false;

    // Existing 2-parameter version
    bool Key(const char* str, rapidjson::SizeType) {
        currentKey = str;
        return true;
    }

    // ✅ ChatGPT Fix: Required by RapidJSON's Reader
    bool Key(const char* str, rapidjson::SizeType length, bool) {
        currentKey = std::string(str, length);
        return true;
    }

    // Existing 2-parameter version
    bool String(const char* str, rapidjson::SizeType) {
        if (inArray) {
            currentArray.push_back(str);
        } else {
            kvMap[currentKey] = str;
        }
        return true;
    }

    // ✅ ChatGPT Fix: Required by RapidJSON's Reader
    bool String(const char* str, rapidjson::SizeType length, bool) {
        std::string val(str, length);
        if (inArray) {
            currentArray.push_back(val);
        } else {
            kvMap[currentKey] = val;
        }
        return true;
    }

    bool StartArray() {
        inArray = true;
        currentArray.clear();
        return true;
    }

    bool EndArray(rapidjson::SizeType) {
        kaMap[currentKey] = currentArray;
        inArray = false;
        return true;
    }
};
