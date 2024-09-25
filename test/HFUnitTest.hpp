#include "../src/tsdb_hf.hpp"
#include "../utils/Utils.hpp"
#include <cassert>
#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <vector>

using namespace tsdb_hf_cpp;
class HFUnitTest {
private:
    tsdb_entry entry;
    std::vector<long long> timestamps;
    std::vector<double> values;

public:
    HFUnitTest()
    {
        long long nanoseconds = Utils::getCurNanoseconds();
        int point_count = 10;
        for (int i = 0; i < point_count; i++) {
            timestamps.push_back(nanoseconds + i);
            values.push_back(i);
        }
    }

    void streamEmitUnitTest()
    {
        Stream stream;
        std::pair<size_t, size_t> range1 = { 1, 2 }, range2 = { 2, 5 }, range3 = { 7, 9 }, range4 = { 0, 0 }, range5 = { 8, 10 };
        stream.addFile("test");
        stream.addIdxRangeOfFile("test", range1);
        stream.addIdxRangeOfFile("test", range2);
        stream.addIdxRangeOfFile("test", range3);
        stream.addIdxRangeOfFile("test", range4);
        stream.addIdxRangeOfFile("test", range5);
        stream.emit("../test/data/json");
        stream.resetNumber();
    }

    void compressBytesToFilesUnitTest()
    {
        auto bytes = Utils::vec2Bytes(timestamps);
        auto [res, size] = entry.compressBytesToFiles(bytes, "../test/data", "test");
        assert(res.first != res.second);
        // assert(decoded.size() == timestamps.size());
        // assert(Utils::vec1dEqual(timestamps, decoded));
    }

    void parseFormatStrUnitTest()
    {
        auto format = "12{index}32{}{{}{prefix}}}";
        std::map<std::string, std::string> args1 = { { "prefix", "timestamps" }, { "index", "1" } };
        std::vector<std::string> args2 = { "timestamps", "1" };
        assert(Utils::parseFormatStr(format, args1) == std::string("12132{}{{}timestamps}}"));
        assert(Utils::parseFormatStr(format, args2) == std::string("12timestamps32{}{{}1}}"));
    }

    void mergeRangeUnitTest()
    {
        std::pair<size_t, size_t> p1 = { 1, 2 }, p2 = { 2, 4 };
        auto res = Utils::mergeRange(p1, p2);
        assert(p1.first == 1 && p1.second == 4);
        assert(res);
    }
};