#include "../tsdb.hpp"
#include "../tsdb_hf.hpp"
#include <ctime>
#include <iostream>
#include <sstream>
using namespace std;

long long stringToNanoseconds(const std::string& timestampStr)
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

void test_hf()
{
    tsdb_hf_cpp::point point("point1", 42.42, 1622547800000LL);
    tsdb_hf_cpp::tsdb_entry tsdb_entry;
    std::vector<tsdb_hf_cpp::point> points;

    tsdb_entry.insert_point(point);

    tsdb_entry.extract_points("timestamps.zst", "values.zst", points);

    for (auto point : points) {
        cout << point.name_ << "," << point.nanoseconds_ << "," << point.value_ << endl;
    }
}

int main(int argc, char const* argv[])
{
    // std::string timestampStr = "2024-02-19 12:34:56";
    // // 日期字符串转纳秒
    // long long nanoseconds = stringToNanoseconds(timestampStr);

    // // 本地或远程时序数据库的主机名和端口
    // tsdb_cpp::tsdb_entry tsdb_entry("127.0.0.1", 8191);
    // std::vector<tsdb_cpp::point> points;

    // // 数据点数
    // int point_count = 1000000;
    // for (int i = 0; i < point_count; i++) {
    //     nanoseconds += 1000000;
    //     // 构造一个点，点名为point1, 值为i，时间戳为nanoseconds
    //     tsdb_cpp::point point("point1", i, nanoseconds);
    //     points.push_back(point);
    // }

    // // 记录插入时序数据库时间，获取插入操作开始时间点
    // std::clock_t start = std::clock();
    // int ret = tsdb_entry.insert_points(points);
    // // 获取插入操作结束时间点
    // std::clock_t end = std::clock();

    // tsdb_entry.close();
    // // 计算插入操作耗时时间（以毫秒为单位）
    // double duration = static_cast<double>(end - start) / (CLOCKS_PER_SEC / 1000);
    // // 输出耗时时间
    // std::cout << "Operation took " << duration << " microseconds." << std::endl;

    // cout << ret << endl;
    test_hf();
}
