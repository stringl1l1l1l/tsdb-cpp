#include "src/tsdb_hf.hpp"
#include "test/HFUnitTest.hpp"
#include "utils/Utils.hpp"
#include <ctime>
#include <iostream>
#include <vector>
using namespace std;

void test_hf()
{
    tsdb_hf_cpp::point point("point1", 42.42, 1622547800000LL);
    tsdb_hf_cpp::tsdb_entry tsdb_entry;
    std::vector<tsdb_hf_cpp::point> points;
    std::string timestampStr = "2024-02-19 12:34:56";
    // 日期字符串转纳秒
    long long nanoseconds = Utils::stringToNanoseconds(timestampStr);

    // 数据点数
    int point_count = 100000;
    for (int i = 0; i < point_count; i++) {
        nanoseconds += 1000000;
        // 构造一个点，点名为point1, 值为i，时间戳为nanoseconds
        tsdb_hf_cpp::point point("point1", i, nanoseconds);
        points.push_back(point);
    }

    tsdb_entry.insert_points(points);
    auto extract_points = tsdb_entry.extract_points("timestamps.zst", "values.zst");

    for (auto p : extract_points) {
        cout << p.name_ << "," << p.nanoseconds_ << "," << p.value_ << endl;
    }
}

void test2()
{
}

void test()
{
    std::cout << "———————————Start Unit Tests———————————" << std::endl;
    HFUnitTest test;
    test.parseFormatStrUnitTest();
    test.streamCompressToFileUnitTest();
    std::cout << "——+—————————Unit Tests Done———————————" << std::endl;
}

int main(int argc, char const* argv[])
{
    if (argc > 1) {
        char const** p = argv;
        while (p && *p) {
            if (strcmp(*p++, "--test") == 0) {
                test();
                break;
            }
        }
    }

    using namespace tsdb_hf_cpp;
    std::string timestampStr = "2024-02-19 12:34:56";
    // 日期字符串转纳秒
    long long nanoseconds = Utils::stringToNanoseconds(timestampStr);

    // 本地或远程时序数据库的主机名和端口
    tsdb_entry tsdb_entry;
    std::vector<point> points;

    // 数据点数
    int point_count = 1000000;
    for (int i = 0; i < point_count; i++) {
        nanoseconds += 1000000;
        // 构造一个点，点名为point1, 值为i，时间戳为nanoseconds
        point point("point1", i, nanoseconds);
        points.push_back(point);
    }

    // 记录插入时序数据库时间，获取插入操作开始时间点
    std::clock_t start = std::clock();
    int ret = tsdb_entry.insert_points(points);
    // 获取插入操作结束时间点
    std::clock_t end = std::clock();

    tsdb_entry.close();
    // 计算插入操作耗时时间（以毫秒为单位）
    double duration = static_cast<double>(end - start) / (CLOCKS_PER_SEC / 1000);
    // 输出耗时时间
    std::cout << "Operation took " << duration << " microseconds." << std::endl;

    cout << ret << endl;
}
