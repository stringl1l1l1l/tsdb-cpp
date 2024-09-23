#include "../src/tsdb_hf.hpp"
#include "../utils/Utils.hpp"
#include <cassert>
#include <cstddef>
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
        ZSTD_inBuffer inBuffer = { stream.data(), stream.size(), 0 };
        std::vector<char> buffer(ArgParser::get<size_t>("bufferSize", "hf_compress"), 0);
        ZSTD_outBuffer outBuffer = { buffer.data(), buffer.size(), 0 };

        typedef tsdb_entry::CompressOp OP;

        size_t idx = 0;
        std::string targetDir = "../test/data";
        OP op = OP::COMPRESS_CONTINUE;
        std::string testFileName;
        std::map<std::string, std::string> argsMap = { { "prefix", "test" }, { "index", "0" } };
        while (op == OP::COMPRESS_CONTINUE) {
            argsMap["index"] = std::to_string(idx++);
            testFileName = Utils::parseFormatStr(ArgParser::get<std::string>("fileNameFormat", "hf"), argsMap);
            op = entry.compressStreamToFilesWithOp(targetDir, testFileName, inBuffer, outBuffer);
        }
        assert(op != OP::COMPRESS_ERROR);

        // auto decoded = Utils::bytes2Vec<long long>(
        //     entry.streamDecompressFromFile(ArgParser::get<std::string>("dataDir", "hf"), testFileName));

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