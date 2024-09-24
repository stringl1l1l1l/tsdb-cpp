#include "../src/tsdb_hf.hpp"
#include "../utils/Utils.hpp"
#include <cassert>
#include <map>
#include <string>
#include <type_traits>
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
        int point_count = 1e6;
        for (int i = 0; i < point_count; i++) {
            timestamps.push_back(nanoseconds + i);
            values.push_back(i);
        }
    }

    void streamCompressToFileUnitTest()
    {
        auto stream = Utils::vec2Bytes(timestamps);
        auto op = entry.compressStreamToFiles(stream, "../test/data", "test");
        assert(op == tsdb_entry::CompressOp::COMPRESS_END);
        // assert(decoded.size() == timestamps.size());
        // assert(Utils::vec1dEqual(timestamps, decoded));
    }

    static void parseFormatStrUnitTest()
    {
        auto format = "12{index}32{}{{}{prefix}}}";
        std::map<std::string, std::string> args1 = { { "prefix", "timestamps" }, { "index", "1" } };
        std::vector<std::string> args2 = { "timestamps", "1" };
        assert(Utils::parseFormatStr(format, args1) == std::string("12132{}{{}timestamps}}"));
        assert(Utils::parseFormatStr(format, args2) == std::string("12timestamps32{}{{}1}}"));
    }

    template <typename T>
    static void traverse(const T& collection)
    {
        if (std::is_same<T, std::vector<T>>::val) {
            for (const auto& item : collection) {
                traverse(item);
            }
        } else {
            // 如果 T 不是 std::vector，直接输出元素
            std::cout << collection << " ";
        }
    }
};