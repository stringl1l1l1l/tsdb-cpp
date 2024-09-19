/**
 * @file ArgParser.hpp
 * @author liwener (liwener200207@163.com)
 * @brief 从YAML配置文件解析参数的工具类
 * @version 0.1
 * @date 2024-09-19
 * 
 * 
 */
#ifndef ARG_PARSER_HPP
#define ARG_PARSER_HPP
#include <cxxabi.h>
#include <iostream>
#include <string>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>

#define GET_VAR_NAME(Var) #Var

static YAML::Node argsNode;
static const char* DEFAULT_CONF_PATH = "../../conf/config.yml";

class ArgParser {
public:
    /**
     * @brief 按name获取解析出的yaml配置文件参数
     * 
     * @tparam T 属性要转换为的类型
     * @param name 按delimeter分隔的属性名，如"hf.compress.blockSize"
     * @param delimiter 分隔符
     * @return T 被转换为T类型的属性
     */
    template <typename T>
    static T get(const std::string& name, std::string prefixNodeName = "", char delimiter = '_')
    {
        if (!argsNode.size())
            initialize();
        try {
            std::string fullName = name;
            if (!prefixNodeName.empty())
                fullName = prefixNodeName + delimiter + name;
            return getNode(fullName, delimiter).as<T>();
        } catch (YAML::ParserException& e) {
            std::cerr << "Parser error: " << e.what() << std::endl;
            exit(1);
        } catch (YAML::InvalidNode& e) {
            std::cerr << "Invalid argment: " << name << std::endl;
            exit(1);
        } catch (YAML::TypedBadConversion<T>& e) {
            std::cerr << "Invalid type conversion: "
                      << "("
                      << abi::__cxa_demangle(typeid(T).name(), 0, 0, 0)
                      << ")"
                      << name
                      << std::endl;
            exit(1);
        } catch (YAML::Exception& e) {
            std::cerr << e.what() << std::endl;
            exit(1);
        }
    }

private:
    static void initialize(const std::string loadPath = DEFAULT_CONF_PATH)
    {
        try {
            argsNode = YAML::LoadFile(loadPath);
        } catch (YAML::BadFile& e) {
            std::cerr << "Invalid file path: " << loadPath << std::endl;
            exit(1);
        }
    }

    static YAML::Node getNode(std::string name, char delimiter)
    {
        size_t pos = name.rfind(delimiter);
        if (pos == std::string::npos)
            return argsNode[name];

        std::string prefix = name.substr(0, pos);
        std::string suffix = name.substr(pos + 1, name.length() - pos - 1);
        return getNode(prefix, delimiter)[suffix];
    }
};
#endif // HEADER_FILE_NAME_HPP