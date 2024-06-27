#ifndef TSDB_HF_CPP_HPP
#define TSDB_HF_CPP_HPP

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
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
    const std::string& name_;
    double value_;
    long long nanoseconds_;
    point(const std::string& name, double value, long long nanoseconds)
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

    tsdb_entry(const std::string& host, int port)
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
    bool compressAndWriteToFile(const std::string& filename, const void* data, size_t dataSize)
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

    inline void close()
    {
        ::close(sock);
    }

    inline int insert_points(const std::vector<point>& points)
    {
    }

    inline int insert_point(const point& point)
    {
        // 压缩并写入时间戳
        if (!compressAndWriteToFile("timestamps.zst", &point.nanoseconds_, sizeof(point.nanoseconds_))) {
            return -1;
        }

        // 压缩并写入值
        if (!compressAndWriteToFile("values.zst", &point.value_, sizeof(point.value_))) {
            return -1;
        }

        return 0;
    }

    bool extract_points(const std::string& timestampsFile, const std::string& valuesFile, std::vector<point>& points)
    {

        char decompressedTimestamps[6400] = { 0 };
        char decompressedValues[6400] = { 0 };

        size_t timestampsCnt = decompressFromFile(timestampsFile, decompressedTimestamps) / sizeof(long long);
        size_t valuesCnt = decompressFromFile(valuesFile, decompressedValues) / sizeof(double);

        if (timestampsCnt != valuesCnt) {
            std::cerr << "Mismatched sizes of decompressed timestamps and values" << std::endl;
            return false;
        }

        // 组装点数据
        points.clear();
        points.reserve(valuesCnt);
        for (size_t i = 0; i < valuesCnt; ++i) {
            long long timestamp;
            double value;
            std::memcpy(&timestamp, decompressedTimestamps + i * sizeof(long long), sizeof(long long));
            std::memcpy(&value, decompressedValues + i * sizeof(double), sizeof(double));
            points.emplace_back(tsdb_hf_cpp::point("point", value, timestamp));
        }

        return true;
    }

protected:
    int sock;
    struct sockaddr_in addr;
    const std::string& host_;
    int port_;
    int points_count_per_package = 30;
};

}

#endif // TSDB_HFCPP_HPP
