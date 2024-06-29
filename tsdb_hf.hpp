#ifndef TSDB_HF_CPP_HPP
#define TSDB_HF_CPP_HPP

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sched.h>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <algorithm>
#include <windows.h>
#pragma comment(lib, "ws2_32")
typedef struct iovec {
    void* iov_base;
    size_t iov_len;
} iovec;
inline __int64 writev(int sock, struct iovec* iov, int cnt)
{
    __int64 r = send(sock, (const char*)iov->iov_base, iov->iov_len, 0);
    return (r < 0 || cnt == 1) ? r : r + writev(sock, iov + 1, cnt - 1);
}
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <zstd.h>
#define closesocket close
#endif
using namespace std;

namespace tsdb_hf_cpp {

struct point {
    const string& name_;
    double value_;
    long long nanoseconds_;
    point(const string& name, double value, long long nanoseconds)
        : name_(name)
        , value_(value)
        , nanoseconds_(nanoseconds)
    {
    }
};

struct tsdb_entry {
    tsdb_entry()
        : host_(*new string(""))
    {
    }

    tsdb_entry(const string& host, int port)
        : host_(host)
        , port_(port)
    {
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if ((addr.sin_addr.s_addr = inet_addr(host.c_str())) == INADDR_NONE) {
            cout << "socket:" << -1 << endl;
        }

        if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
            cout << "socket:" << -2 << endl;
        }
    }

    // 将数据压缩并写入文件的辅助函数
    bool compressAndWriteToFile(const string& filename, const void* data, size_t dataSize)
    {
        // 估算压缩后的最大缓冲区大小
        size_t const cBuffSize = ZSTD_compressBound(dataSize);
        vector<char> cBuff(cBuffSize);

        // 压缩数据
        size_t const cSize = ZSTD_compress(cBuff.data(), cBuffSize, data, dataSize, 1);
        if (ZSTD_isError(cSize)) {
            cerr << "ZSTD_compress error: " << ZSTD_getErrorName(cSize) << endl;
            return false;
        }

        // 将压缩后的数据写入文件
        ofstream outFile(filename, ios::trunc | ios::binary);
        if (!outFile) {
            cerr << "Failed to open file " << filename << endl;
            return false;
        }
        outFile.write(cBuff.data(), cSize);
        return true;
    }

    bool streamCompressToFile(const string& filename, const vector<char>& input)
    {
        ofstream outFile(filename, ios::binary);
        if (!outFile) {
            cerr << "Failed to open file " << filename << endl;
            return false;
        }

        // 创建压缩上下文
        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        if (!cctx) {
            cerr << "Failed to create ZSTD_CCtx" << endl;
            return false;
        }
        // 配置压缩参数
        ZSTD_initCStream(cctx, ZSTD_c_compressionLevel);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 3);

        // 创建输出缓冲区
        const size_t outBuffSize = ZSTD_CStreamOutSize(); // 获取推荐的缓冲区大小
        vector<char> output(outBuffSize);

        ZSTD_inBuffer inBuff = { input.data(), input.size(), 0 };

        /**
         * @brief size_t ZSTD_compressStream2(ZSTD_CCtx* cctx, ZSTD_outBuffer* output, ZSTD_inBuffer* input
                                              , ZSTD_EndDirective endOp)
         * @description zstd的流式压缩API。
         * zstd在单次流式压缩时将所有数据压缩为一个frame，frame由header和不同的固定大小的block组成，因此理论上可以实现无限大小的数据压缩。
         * 但由于压缩缓冲区ZSTD_outBuffer的大小有限，可能会出现缓冲区已满但没有形成一个完整大小block的情况，
         * 因此我们需要根据ZSTD_compressStream2()的返回结果,传入不同endOp，执行不同的操作。
         * @return remaining == 0 缓冲区有足够空间，endOp = ZSTD_e_continue继续压缩。
         * @return remaining > 0 缓冲区需要额外的remaining大小空间，endOp = ZSTD_e_flush，立即将当前缓冲区刷入frame，
                   不足一个block的数据也形成一个block。
                   若压缩结束，令endOp = ZSTD_e_end，会执行与ZSTD_e_flush相同的操作，并在frame尾部写入结束符。
         * @return remaining < 0 压缩出错
         *
         */
        ZSTD_EndDirective endOp = ZSTD_e_continue;
        ZSTD_outBuffer outBuff = { output.data(), outBuffSize, 0 };
        while (inBuff.pos < inBuff.size) {
            size_t const remaining = ZSTD_compressStream2(cctx, &outBuff, &inBuff, endOp);
            if (ZSTD_isError(remaining)) {
                cerr << "ZSTD_compressStream error: " << ZSTD_getErrorName(remaining) << endl;
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
            cerr << "ZSTD_endStream error: " << ZSTD_getErrorName(remaining) << endl;
            ZSTD_freeCCtx(cctx);
            return false;
        }

        outFile.write(output.data(), outBuff.pos);

        // 释放上下文
        ZSTD_freeCCtx(cctx);
        return true;
    }

    vector<char> streamDecompressFromFile(const string& filename)
    {
        vector<char> output;
        ifstream inFile(filename, ios::binary);
        if (!inFile) {
            cerr << "Failed to open file " << filename << endl;
            return output;
        }

        ZSTD_DCtx* dctx = ZSTD_createDCtx();
        if (!dctx) {
            cerr << "Failed to create ZSTD_DCtx" << endl;
            return output;
        }
        ZSTD_initDStream(dctx);

        size_t const buffInSize = ZSTD_DStreamInSize();
        vector<char> input(buffInSize);

        size_t const buffOutSize = ZSTD_DStreamOutSize();
        output.resize(buffOutSize);
        ZSTD_outBuffer outBuff = { output.data(), buffOutSize, 0 };
        
        while (inFile) {
            // 创建输入缓冲区
            inFile.read(input.data(), buffInSize);
            const size_t hasRead = inFile.gcount();
            ZSTD_inBuffer inBuff = { input.data(), hasRead, 0 };
            
            // todo: 由于压缩数据过多，单次写入缓冲区不能完成解压缩的逻辑
            while (inBuff.pos < inBuff.size) {
                const size_t remaining = ZSTD_decompressStream(dctx, &outBuff, &inBuff);
                if (ZSTD_isError(remaining)) {
                    cerr << "ZSTD_decompressStream error: " << ZSTD_getErrorName(remaining) << endl;
                    ZSTD_freeDCtx(dctx);
                    return output;
                }
            }
        }
        output.resize(outBuff.pos);
        ZSTD_freeDCtx(dctx);
        return output;
    }

    // 解压缩数据并读取到缓冲区的辅助函数
    size_t decompressFromFile(const string& filename, char* const decompressedData)
    {
        // 打开压缩文件
        ifstream inFile(filename, ios::binary | ios::ate);
        if (!inFile) {
            cerr << "Failed to open file " << filename << endl;
            return false;
        }

        // 获取文件大小
        streamsize compressedSize = inFile.tellg();
        inFile.seekg(0, ios::beg);

        // 读取压缩数据
        char* const compressedData = new char[compressedSize];
        if (!inFile.read(compressedData, compressedSize)) {
            cerr << "Failed to read file " << filename << endl;
            return false;
        }

        // 获取解压后数据的最大尺寸
        unsigned long long const decompressedBound = ZSTD_getFrameContentSize(compressedData, compressedSize);
        if (decompressedBound == ZSTD_CONTENTSIZE_ERROR) {
            cerr << "Not compressed by zstd" << endl;
            return false;
        } else if (decompressedBound == ZSTD_CONTENTSIZE_UNKNOWN) {
            cerr << "Original size unknown" << endl;
            return false;
        }

        // 解压缩数据
        size_t const dSize = ZSTD_decompress(decompressedData, 1600, compressedData, compressedSize);
        if (ZSTD_isError(dSize)) {
            cerr << "ZSTD_decompress error: " << ZSTD_getErrorName(dSize) << endl;
            return false;
        }

        delete[] compressedData;
        return dSize;
    }

    inline void close()
    {
        ::close(sock);
    }

    template <typename T>
    vector<char> vec2Bytes(const vector<T>& input)
    {
        // 创建一个  vector<char>，其大小为输入向量大小乘以每个元素的字节大小
        vector<char> output(input.size() * sizeof(T));

        // 使用 memcpy 将数据从输入向量复制到输出向量中
        memcpy(output.data(), input.data(), input.size() * sizeof(T));

        return output;
    }

    template <typename T>
    vector<T> bytes2Vec(const vector<char>& bytes)
    {
        vector<T> output(bytes.size() / sizeof(T));
        memcpy(output.data(), bytes.data(), bytes.size());
        return output;
    }

    inline int insert_points(const vector<point>& points)
    {
        vector<long long> timestamps;
        vector<double> values;
        for (auto& p : points) {
            timestamps.push_back(p.nanoseconds_);
            values.push_back(p.value_);
        }

        auto timestampsInput = vec2Bytes(timestamps);
        auto valuesInput = vec2Bytes(values);
        streamCompressToFile("timestamps.zst", timestampsInput);
        streamCompressToFile("values.zst", valuesInput);

        return 0;
    }

    vector<point> extract_points(const string& timestampsFile, const string& valuesFile)
    {
        vector<point> points;
        auto timestampsStream = streamDecompressFromFile("timestamps.zst");
        auto valuesStream = streamDecompressFromFile("values.zst");
        auto timestamps = bytes2Vec<long long>(timestampsStream);
        auto values = bytes2Vec<double>(valuesStream);

        if (timestamps.size() != values.size()) {
            cerr << "Mismatched sizes of decompressed timestamps and values" << endl;
            return points;
        }

        const auto name = new string("point");
        for (size_t i = 0; i < timestamps.size(); i++) {
            point p(*name, values[i], timestamps[i]);
            points.push_back(p);
        }
        return points;
    }

protected:
    int sock;
    struct sockaddr_in addr;
    const string& host_;
    int port_;
    int points_count_per_package = 30;
};
}

#endif // TSDB_HFCPP_HPP
