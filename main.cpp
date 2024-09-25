#include "src/tsdb_hf.hpp"
#include "test/HFUnitTest.hpp"
#include "utils/Utils.hpp"
#include <cstddef>
#include <iostream>
#include <random>
#include <vector>
using namespace std;

vector<point> uniformDistributionPoints(int cnt, double a = 0, double b = 1)
{
    vector<point> points;
    std::default_random_engine engine(static_cast<unsigned int>(time(0)));
    std::uniform_real_distribution<double> distrib(a, b);
    for (int i = 0; i < cnt; i++) {
        point p("uniformDistributionPoints", distrib(engine), Utils::getCurNanoseconds());
        points.push_back(p);
    }
    return points;
}
vector<point> normalDistributionPoints(int cnt, double mean = 0, double stddev = 1)
{
    vector<point> points;
    std::default_random_engine engine(static_cast<unsigned int>(time(0)));
    std::normal_distribution<double> distrib(0, 1.0);
    for (int i = 0; i < cnt; i++) {
        point p("normalDistributionPoints", distrib(engine), Utils::getCurNanoseconds());
        points.push_back(p);
    }
    return points;
}

vector<point> sequentialPoints(int cnt, double begin = 0)
{
    vector<point> points;
    for (int i = 0; i < cnt; i++) {
        point p("sequentialPoints", begin + i, Utils::getCurNanoseconds());
        points.push_back(p);
    }
    return points;
}

void test()
{
    std::cout << "——————————— Start Unit Tests ———————————" << std::endl;
    HFUnitTest test;
    test.parseFormatStrUnitTest();
    test.compressBytesToFilesUnitTest();
    test.mergeRangeUnitTest();
    test.streamEmitUnitTest();
    std::cout << "——————————— Unit Tests Done ———————————" << std::endl;
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
    size_t pointsCnt = 1e6;

    using namespace tsdb_hf_cpp;
    tsdb_entry tsdb_entry;

    tsdb_entry.initialize();
    tsdb_entry.insert_points(normalDistributionPoints(pointsCnt));
    tsdb_entry.close();

    tsdb_entry.initialize();
    tsdb_entry.insert_points(sequentialPoints(pointsCnt));
    tsdb_entry.close();

    tsdb_entry.initialize();
    tsdb_entry.insert_points(uniformDistributionPoints(pointsCnt));
    tsdb_entry.close();
}
