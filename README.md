### 依赖项安装  

```shell
sudo apt-get install libzstd-dev libyaml-cpp-dev -y
```

### 设置配置文件路径

```shell
export TSDB_CONFIG_FILE_PATH=your/file/path/config.yml
```

### 链接静态库

```shell
g++ -o a.out main.cpp -L/location/of/lib -ltsdb_cpp -lzstd -lyaml-cpp -std=c++17
```

### 配置文件介绍

```yaml
hf:
  dataDir: data                             # 生成压缩文件的路径
  jsonDir: data/json                        # 一次流压缩完成后，生成的json文件的路径
  fileNameFormat: "{prefix}-{index}.zst"    # 生成的压缩文件的名称格式
  timestampsFileNamePrefix: timestamps      # 放入fileNameFormat中{prefix}字段的内容，时间戳和数据分开压缩，因此有不同的名称
  valuesFileNamePrefix: values
  
  compress:
    outBufferSize: 40960                    # 用于压缩的缓冲区大小。一轮次压缩生成一次outBufferSize大小的压缩文件，该数值越大，相同大小的数据被划分成的文件越少，但压缩占用的内存也更大。
    compressionLevel: 0                     # zstd的压缩等级参数，指定压缩操作的级别。该数值越小（可负），压缩速度越快，但压缩比越低。
```
