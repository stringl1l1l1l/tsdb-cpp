#ifndef UTILS_HPP
#define UTILS_HPP
#include <chrono>
#include <cstddef>
#include <map>
#include <sstream>
#include <string>
#include <vector>

class Utils {
public:
    static std::string parseFormatStr(const std::string& format, const std::map<std::string, std::string>& args)
    {
        std::string result;
        std::string key;
        size_t n = format.size();
        if (format.empty())
            return std::string();

        for (size_t i = 0; i < n; i++) {
            if (format[i] == '{') {
                if (!key.empty()) {
                    result += key;
                    key.resize(0);
                }
                result += '{';
            } else if (format[i] == '}' && result[result.size() - 1] == '{' && !key.empty()) {
                result.pop_back();
                if (args.count(key))
                    result += args.at(key);
                else
                    result += '{' + key + '}';
                key.resize(0);
            } else
                key += format[i];
        }
        result += key;
        return result;
    }

    static std::string parseFormatStr(const std::string& format, const std::vector<std::string>& args)
    {
        std::string result;
        std::string key;
        size_t n = format.size();
        if (format.empty())
            return std::string();

        size_t idx = 0;
        for (size_t i = 0; i < n; i++) {
            if (format[i] == '{') {
                if (!key.empty()) {
                    result += key;
                    key.resize(0);
                }
                result += '{';
            } else if (format[i] == '}' && result[result.size() - 1] == '{' && !key.empty()) {
                result.pop_back();
                if (idx < args.size())
                    result += args[idx++];
                key.resize(0);
            } else
                key += format[i];
        }
        result += key;
        return result;
    }

    static long long getCurNanoseconds()
    {
        auto now = std::chrono::high_resolution_clock::now();
        auto duration = now.time_since_epoch();
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return nanoseconds;
    }

    static long long stringToNanoseconds(const std::string& timestampStr)
    {
        std::istringstream iss(timestampStr);

        int year, month, day, hour, minute, second;
        char delimiter;

        iss >> year >> delimiter >> month >> delimiter >> day >> hour >> delimiter >> minute >> delimiter >> second;

        struct tm timeStruct;
        timeStruct.tm_year = year - 1900; // 年份需减去 1900
        timeStruct.tm_mon = month - 1; // 月份从 0 开始，需减去 1
        timeStruct.tm_mday = day;
        timeStruct.tm_hour = hour;
        timeStruct.tm_min = minute;
        timeStruct.tm_sec = second;

        time_t time = mktime(&timeStruct);
        long long nanoseconds = static_cast<long long>(time) * 1000000000;
        return nanoseconds;
    }

    template <typename T>
    static std::vector<char> vec2Bytes(const std::vector<T>& input)
    {
        // 创建一个  std::vector<char>，其大小为输入向量大小乘以每个元素的字节大小
        std::vector<char> output(input.size() * sizeof(T));

        // 使用 memcpy 将数据从输入向量复制到输出向量中
        memcpy(output.data(), input.data(), input.size() * sizeof(T));

        return output;
    }

    template <typename T>
    static std::vector<T> bytes2Vec(const std::vector<char>& bytes)
    {
        std::vector<T> output(bytes.size() / sizeof(T));
        memcpy(output.data(), bytes.data(), bytes.size());
        return output;
    }

    template <typename T>
    static bool vec1dEqual(const std::vector<T>& vec1, const std::vector<T>& vec2)
    {
        if (vec1.size() != vec2.size())
            return false;

        // 逐个元素比较
        for (size_t i = 0; i < vec1.size(); ++i) {
            if (vec1[i] != vec2[i]) {
                return false;
            }
        }

        return true;
    }

    // template <typename T>
    // static bool vecNdEqual(const std::vector<T>& vec1, const std::vector<T>& vec2)
    // {
    //     // 检查大小是否相同
    //     if (vec1.size() != vec2.size()) {
    //         return false;
    //     }

    //     // 逐个元素比较
    //     for (size_t i = 0; i < vec1.size(); ++i) {
    //         // 如果元素是向量，递归比较
    //         if constexpr (std::is_same_v<T, std::vector<typename T::value_type>>) {
    //             if (!areVectorsEqual(vec1[i], vec2[i])) {
    //                 return false;
    //             }
    //         } else {
    //             if (vec1[i] != vec2[i]) {
    //                 return false;
    //             }
    //         }
    //     }
    //     return true;
    // }
};

#endif