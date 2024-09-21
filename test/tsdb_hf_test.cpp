#include "../src/tsdb_hf.hpp"
#include "../utils/utils.hpp"
#include <cassert>
#include <map>
#include <string>
#include <vector>

using namespace tsdb_hf_cpp;
class HfTest {
private:
    tsdb_entry entry;
    std::vector<long long> timestamps;
    std::vector<double> values;

public:
    HfTest()
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
        auto timestampsStream = Utils::vec2Bytes(timestamps);
        ZSTD_inBuffer inputBuffer = { timestampsStream.data(), timestampsStream.size(), 0 };
        std::vector<char> buffer(entry.arguments.compress_bufferSize, 0);
        ZSTD_outBuffer compressBuffer = { buffer.data(), buffer.size(), 0 };

        auto res = entry.streamCompressToFile(entry.arguments.dataDir, "timestamps.zst", inputBuffer, compressBuffer);
        assert(res);

        auto stream = entry.streamDecompressFromFile(entry.arguments.dataDir, "timestamps.zst");
        auto decoded = Utils::bytes2Vec<long long>(stream);

        assert(decoded.size() == timestamps.size());
        assert(Utils::vec1dEqual(timestamps, decoded));
    }

    static void parseFormatStrUnitTest()
    {
        auto format = "12{index}32{}{{}{prefix}}}";
        std::map<std::string, std::string> args = { { "prefix", "timestamps" }, { "index", "1" } };
        assert(Utils::parseFormatStr(format, args) == std::string("12132{}{{}timestamps}}"));
    }
};