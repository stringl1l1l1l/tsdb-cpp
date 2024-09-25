// high frenqence data api
#ifndef TSDB_HF_CPP_HPP
#define TSDB_HF_CPP_HPP

#include "../utils/ArgParser.hpp"
#include "../utils/Utils.hpp"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <ostream>
#include <sched.h>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>
#include <zstd.h>

namespace tsdb_hf_cpp {

struct point {
    const std::string name_;
    double value_;
    long long nanoseconds_;
    point(const std::string& name, double value, long long nanoseconds)
        : name_(name)
        , value_(value)
        , nanoseconds_(nanoseconds)
    {
    }
};

struct Stream {
public:
    size_t streamInputSize;
    size_t streamOutputSize;
    double compressTimeMs;

private:
    static size_t streamNumber;
    long long timestampOffset;
    std::string streamName;
    std::string timeUnit;
    std::string datetimeStr;
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> idxRangesMap;

public:
    Stream()
    {
        streamNumber++;
        timestampOffset = 0;
        timeUnit = "ns";
        streamInputSize = 0;
        streamOutputSize = 0;
        compressTimeMs = 0;
        streamName = "";
    }

    void addFile(const std::string& file)
    {
        idxRangesMap[file] = std::vector<std::pair<size_t, size_t>>({ { 0, 0 } });
    }

    void addIdxRangeOfFile(const std::string& file, std::pair<size_t, size_t>& range)
    {
        if (range.first == range.second)
            return;
        if (idxRangesMap.count(file) == 0)
            addFile(file);
        std::vector<std::pair<size_t, size_t>>& idxRangesOfFile = idxRangesMap.at(file);
        auto res = Utils::mergeRange(idxRangesOfFile[idxRangesOfFile.size() - 1], range);
        if (!res)
            idxRangesOfFile.push_back(range);
    }

    void setTimestampOffset(long long offset)
    {
        timestampOffset = offset;
    }

    void setTimeUnit(std::string unit)
    {
        timeUnit = unit;
    }

    nlohmann::json to_json() const
    {
        nlohmann::json j;
        j["streamNumber"] = streamNumber;
        j["timestampOffset"] = timestampOffset;
        j["timeUnit"] = timeUnit;
        for (const auto& pair : idxRangesMap) {
            nlohmann::json ranges;
            for (const auto& range : pair.second) {
                ranges.push_back({ { "start", range.first }, { "end", range.second } });
            }
            j["idxRangesMap"][pair.first] = ranges;
        }
        return j;
    }

    void emit(const std::string& targetDir)
    {
        std::filesystem::create_directory(targetDir);
        std::string path = targetDir + '/' + streamName + datetimeStr + ".json";
        std::fstream file(path, std::ios::out);
        if (!file) {
            std::cerr << "Cannot open file " << path << std::endl;
            return;
        }
        auto jsonStr = this->to_json().dump(4);
        file << jsonStr;
    }

    void resetNumber()
    {
        streamNumber = 0;
    }

    void setName(std::string name)
    {
        streamName = name;
    }

    size_t getNumber() const
    {
        return streamNumber;
    }

    std::string getName() const
    {
        return streamName;
    }

    void setDatetimeStr(std::string datetime)
    {
        datetimeStr = datetime;
    }

    std::string getDatetimeStr() const
    {
        return datetimeStr;
    }

    void showPerformance()
    {
        std::cout << "stream " << streamName << "\'s performance: " << std::endl;
        std::cout << "stream input size:  \t" << streamInputSize / 1024.0 << " KB" << std::endl;
        std::cout << "stream output size: \t" << streamOutputSize / 1024.0 << " KB" << std::endl;
        std::cout << "compress time cost: \t" << compressTimeMs << " ms." << std::endl;
        std::cout << "compress ratio:     \t" << 100.0 * streamInputSize / streamOutputSize << " %." << std::endl;
        std::cout << "throughput:         \t" << (streamInputSize / compressTimeMs) * 1000 / 1024 / 1024 << " MB/s.\n"
                  << std::endl;
    }
};

size_t Stream::streamNumber = 0;

struct tsdb_entry {
    enum CompressOp {
        COMPRESS_ERROR,
        COMPRESS_CONTINUE,
        COMPRESS_END
    };

private:
    struct Arguments {
        int compress_compressionLevel;
        size_t compress_outBufferSize;
        size_t zstFileMaxSize;
        size_t indexWidth;
        std::string dataDir;
        std::string jsonDir;
        std::string fileNameFormat;
        std::string timestampsFileNamePrefix;
        std::string valuesFileNamePrefix;
    } arguments;

    Stream* stream;

public:
    tsdb_entry()
    {
        arguments.compress_outBufferSize = ArgParser::get<size_t>("outBufferSize", "hf_compress");
        arguments.compress_compressionLevel = ArgParser::get<int>("compressionLevel", "hf_compress");
        arguments.dataDir = ArgParser::get<std::string>("dataDir", "hf");
        arguments.jsonDir = ArgParser::get<std::string>("jsonDir", "hf");
        arguments.fileNameFormat = ArgParser::get<std::string>("fileNameFormat", "hf");
        arguments.timestampsFileNamePrefix = ArgParser::get<std::string>("timestampsFileNamePrefix", "hf");
        arguments.valuesFileNamePrefix = ArgParser::get<std::string>("valuesFileNamePrefix", "hf");
        arguments.indexWidth = 10;
        std::filesystem::create_directory(arguments.dataDir);
        std::filesystem::create_directory(arguments.jsonDir);
    }

    void initialize(long long timestampOffset = 0, std::string timeUnit = "ns")
    {
        stream = new Stream();
        stream->setTimestampOffset(timestampOffset);
        stream->setTimeUnit(timeUnit);
    }

    void close()
    {
        stream->showPerformance();
        stream->emit(arguments.jsonDir);
        delete stream;
        stream = nullptr;
    }

    int insert_points(const std::vector<point>& points)
    {
        if (!stream) {
            std::cerr << "You should call initialize() first." << std::endl;
            exit(0);
        }

        stream->setName(points[0].name_);
        stream->setDatetimeStr(Utils::getCurDatetimeStr());
        std::vector<long long> timestamps;
        std::vector<double> values;
        for (auto& p : points) {
            timestamps.push_back(p.nanoseconds_);
            values.push_back(p.value_);
        }

        std::string targetDir = arguments.dataDir + '/' + stream->getName() + stream->getDatetimeStr();

        auto bytes1 = Utils::vec2Bytes(timestamps);
        auto bytes2 = Utils::vec2Bytes(values);

        auto start = std::chrono::high_resolution_clock::now();
        auto [range1, outputSize1] = compressBytesToFiles(bytes1, targetDir, arguments.timestampsFileNamePrefix);
        auto [range2, outputSize2] = compressBytesToFiles(bytes2, targetDir, arguments.valuesFileNamePrefix);
        auto end = std::chrono::high_resolution_clock::now();
        auto timeCostMS = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        stream->streamInputSize += bytes1.size() + bytes2.size();
        stream->compressTimeMs += timeCostMS;
        stream->streamOutputSize += outputSize1 + outputSize2;
        stream->addIdxRangeOfFile(arguments.timestampsFileNamePrefix, range1);
        stream->addIdxRangeOfFile(arguments.valuesFileNamePrefix, range2);

        return 0;
    }

    std::vector<point> extract_points(const std::string& timestampsFilePath, const std::string& valuesFilePath)
    {
        std::vector<point> points;
        auto timestampsStream = decompressBytesFromFile(arguments.dataDir, "timestamps.zst");
        auto valuesStream = decompressBytesFromFile(arguments.dataDir, "values.zst");
        auto timestamps = Utils::bytes2Vec<long long>(timestampsStream);
        auto values = Utils::bytes2Vec<double>(valuesStream);

        if (timestamps.size() != values.size()) {
            std::cerr << "Mismatched sizes of decompressed timestamps and values" << std::endl;
            return points;
        }

        const auto name = new std::string("point");
        for (size_t i = 0; i < timestamps.size(); i++) {
            point p(*name, values[i], timestamps[i]);
            points.push_back(p);
        }
        return points;
    }

    std::pair<std::pair<size_t, size_t>, size_t> compressBytesToFiles(const std::vector<char>& bytes, const std::string targetDir, const std::string& fileNamePrefix, size_t beg = 0)
    {
        // 创建和初始化压缩上下文
        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        if (cctx == nullptr) {
            std::cerr << "Cannot create context" << std::endl;
            return { { 0, 0 }, 0 };
        }
        size_t initResult = ZSTD_initCStream(cctx, arguments.compress_compressionLevel);
        if (ZSTD_isError(initResult)) {
            ZSTD_freeCCtx(cctx);
            std::cerr << "Cannot initialize context" << std::endl;
            return { { 0, 0 }, 0 };
        }

        std::filesystem::create_directory(targetDir);

        size_t outputSize = 0;
        char* buffer = new char[arguments.compress_outBufferSize];
        ZSTD_inBuffer inBuffer = { bytes.data(), bytes.size(), 0 };
        ZSTD_outBuffer outBuffer = { buffer, arguments.compress_outBufferSize, 0 };

        size_t idx = beg;
        CompressOp op = COMPRESS_CONTINUE;
        std::string fileName;
        std::map<std::string, std::string> argsMap = { { "prefix", fileNamePrefix }, { "index", "0" } };
        while (op == COMPRESS_CONTINUE) {
            std::stringstream indexStr;
            indexStr << std::setw(arguments.indexWidth) << std::setfill('0') << idx++;
            argsMap["index"] = indexStr.str();
            fileName = targetDir + '/' + Utils::parseFormatStr(arguments.fileNameFormat, argsMap);
            op = compressToFileWithOp(cctx, fileName, inBuffer, outBuffer);
            outputSize += outBuffer.pos;
            outBuffer.pos = 0;
        }

        delete[] buffer;
        ZSTD_freeCCtx(cctx);
        return { { beg, idx }, outputSize };
    }

    CompressOp compressToFileWithOp(ZSTD_CCtx* cctx, const std::string& fileName, ZSTD_inBuffer& inBuffer, ZSTD_outBuffer& outBuffer)
    {
        std::ofstream outFile(fileName, std::ios::binary);
        if (!outFile) {
            std::cerr << "Cannot open file " << fileName << std::endl;
            return COMPRESS_ERROR;
        }

        // 如果输出缓冲区大小比输入缓冲区更大，一次压缩即可将所有输入缓冲区压缩
        // 否则，需要多次调用该函数，直到CompressOP返回COMPRESS_END
        size_t inBufferCapacity = inBuffer.size;
        inBuffer.size = std::min(inBuffer.pos + std::min(outBuffer.size, inBufferCapacity), inBufferCapacity);
        size_t remaining = 1;
        while (remaining > 0)
            remaining = ZSTD_compressStream2(cctx, &outBuffer, &inBuffer, ZSTD_e_continue);

        if (ZSTD_isError(remaining)) {
            ZSTD_freeCCtx(cctx);
            std::cerr << "Compress error" << std::endl;
            return COMPRESS_ERROR;
        }
        // 结束压缩
        size_t endResult = ZSTD_endStream(cctx, &outBuffer);
        if (ZSTD_isError(endResult)) {
            std::cerr << "Cannot end stream" << std::endl;
        }

        outFile.write((char*)outBuffer.dst, outBuffer.size);
        outFile.close();

        auto res = COMPRESS_CONTINUE;
        if (inBufferCapacity == inBuffer.pos)
            res = COMPRESS_END;

        inBuffer.size = inBufferCapacity;
        return res;
    }

    bool compressBytesToFile(const std::string& targetDir, const std::string& filename, const std::vector<char>& bytes)
    {
        std::ofstream outFile(targetDir + "/" + filename, std::ios::binary);
        if (!outFile) {
            std::cerr << "Failed to open file " << targetDir + "/" + filename << std::endl;
            return false;
        }

        // 创建压缩上下文
        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        if (!cctx) {
            std::cerr << "Failed to create ZSTD_CCtx" << std::endl;
            return false;
        }
        // 配置压缩参数
        ZSTD_initCStream(cctx, arguments.compress_compressionLevel);

        // 创建输出缓冲区
        const size_t outBuffSize = ZSTD_CStreamOutSize(); // 获取推荐的缓冲区大小
        std::vector<char> output(outBuffSize);

        ZSTD_inBuffer inBuff = { bytes.data(), bytes.size(), 0 };

        /**
         * @brief size_t ZSTD_compressStream2(ZSTD_CCtx* cctx, ZSTD_outBuffer* output, ZSTD_inBuffer* input
                                              , ZSTD_EndDirective endOp)
         * @description zstd的流式压缩API。
         * zstd在单次流式压缩时将所有数据压缩为一个frame，frame由header和多个可变大小的block组成，因此理论上可以实现无限大小的数据压缩。
         * 但由于压缩缓冲区ZSTD_outBuffer的大小有限，可能会出现缓冲区已满但没有形成一个完整大小block的情况，
         * 因此我们需要根据ZSTD_compressStream2()的返回结果,传入不同endOp，执行不同的操作。
         * @return remaining == 0 缓冲区有足够空间，endOp = ZSTD_e_continue继续压缩，编码器决定何时输出压缩结果，以优化压缩比。
         * @return remaining > 0 缓冲区需要额外的remaining大小空间，endOp = ZSTD_e_flush，立即将当前缓冲区刷入frame。
                   若压缩结束，令endOp = ZSTD_e_end，会执行与ZSTD_e_flush相同的操作，并在frame尾部写入结束符。
         * @return remaining < 0 压缩出错
         *
         */
        ZSTD_EndDirective endOp = ZSTD_e_continue;
        ZSTD_outBuffer outBuff = { output.data(), outBuffSize, 0 };
        while (inBuff.pos < inBuff.size) {
            size_t const remaining = ZSTD_compressStream2(cctx, &outBuff, &inBuff, endOp);
            if (ZSTD_isError(remaining)) {
                std::cerr << "ZSTD_compressStream error: " << ZSTD_getErrorName(remaining) << std::endl;
                ZSTD_freeCCtx(cctx);
                return false;
            }
            // 若缓冲区满，立即将当前缓冲区刷入，并重置缓冲区大小
            if (remaining > 0) {
                endOp = ZSTD_e_flush;
                outFile.write(output.data(), outBuff.size);
                outBuff.pos = 0;
            }
        }

        // 在frame尾部写入结束符，结束流压缩
        size_t const remaining = ZSTD_compressStream2(cctx, &outBuff, &inBuff, ZSTD_e_end);
        if (ZSTD_isError(remaining)) {
            std::cerr << "ZSTD_endStream error: " << ZSTD_getErrorName(remaining) << std::endl;
            ZSTD_freeCCtx(cctx);
            return false;
        }

        outFile.write(output.data(), outBuff.pos);

        // 释放上下文
        ZSTD_freeCCtx(cctx);
        return true;
    }

    std::vector<char> decompressBytesFromFile(const std::string& targetDir, const std::string& filename)
    {
        std::vector<char> res;
        std::vector<char> output;
        std::ifstream inFile(targetDir + "/" + filename, std::ios::binary);
        if (!inFile) {
            std::cerr << "Failed to open file " << targetDir + "/" + filename << std::endl;
            return output;
        }

        ZSTD_DCtx* dctx = ZSTD_createDCtx();
        if (!dctx) {
            std::cerr << "Failed to create ZSTD_DCtx" << std::endl;
            return output;
        }
        ZSTD_initDStream(dctx);

        size_t const buffInSize = ZSTD_DStreamInSize();
        std::vector<char> input(buffInSize);

        size_t const buffOutSize = ZSTD_DStreamOutSize();
        output.resize(buffOutSize);
        ZSTD_outBuffer outBuff = { output.data(), buffOutSize, 0 };

        while (inFile) {
            // 创建输入缓冲区
            inFile.read(input.data(), buffInSize);
            const size_t hasRead = inFile.gcount();
            ZSTD_inBuffer inBuff = { input.data(), hasRead, 0 };

            size_t remaining = 0;
            while ((remaining = ZSTD_decompressStream(dctx, &outBuff, &inBuff)) > 0 && inBuff.pos < inBuff.size) {
                if (outBuff.pos == outBuff.size) {
                    res.insert(res.end(), output.begin(), output.end());
                    outBuff.pos = 0;
                }

                if (ZSTD_isError(remaining)) {
                    std::cerr << "ZSTD_decompressStream error: " << ZSTD_getErrorName(remaining) << std::endl;
                    ZSTD_freeDCtx(dctx);
                    exit(-1);
                }
            }
        }
        output.resize(outBuff.pos);
        res.insert(res.end(), output.begin(), output.end());
        ZSTD_freeDCtx(dctx);
        return res;
    }

    // 将数据压缩并写入文件的辅助函数
    bool compressToFile(const std::string& filename, const void* data, size_t dataSize)
    {
        // 估算压缩后的最大缓冲区大小
        size_t const cBuffSize = ZSTD_compressBound(dataSize);
        std::vector<char> cBuff(cBuffSize);

        // 压缩数据
        size_t const cSize = ZSTD_compress(cBuff.data(), cBuffSize, data, dataSize, 1);
        if (ZSTD_isError(cSize)) {
            std::cerr << "ZSTD_compress error: " << ZSTD_getErrorName(cSize) << std::endl;
            return false;
        }

        // 将压缩后的数据写入文件
        std::ofstream outFile(filename, std::ios::trunc | std::ios::binary);
        if (!outFile) {
            std::cerr << "Failed to open file " << filename << std::endl;
            return false;
        }
        outFile.write(cBuff.data(), cSize);
        return true;
    }

    // 解压缩数据并读取到缓冲区的辅助函数
    size_t decompressFromFile(const std::string& filename, char* const decompressedData)
    {
        // 打开压缩文件
        std::ifstream inFile(filename, std::ios::binary | std::ios::ate);
        if (!inFile) {
            std::cerr << "Failed to open file " << filename << std::endl;
            return false;
        }

        // 获取文件大小
        std::streamsize compressedSize = inFile.tellg();
        inFile.seekg(0, std::ios::beg);

        // 读取压缩数据
        char* const compressedData = new char[compressedSize];
        if (!inFile.read(compressedData, compressedSize)) {
            std::cerr << "Failed to read file " << filename << std::endl;
            return false;
        }

        // 获取解压后数据的最大尺寸
        unsigned long long const decompressedBound = ZSTD_getFrameContentSize(compressedData, compressedSize);
        if (decompressedBound == ZSTD_CONTENTSIZE_ERROR) {
            std::cerr << "Not compressed by zstd" << std::endl;
            return false;
        } else if (decompressedBound == ZSTD_CONTENTSIZE_UNKNOWN) {
            std::cerr << "Original size unknown" << std::endl;
            return false;
        }

        // 解压缩数据
        size_t const dSize = ZSTD_decompress(decompressedData, 1600, compressedData, compressedSize);
        if (ZSTD_isError(dSize)) {
            std::cerr << "ZSTD_decompress error: " << ZSTD_getErrorName(dSize) << std::endl;
            return false;
        }

        delete[] compressedData;
        return dSize;
    }
};
}
#endif // TSDB_HFCPP_HPP