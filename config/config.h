// config/config.h —— 【L29】命令行 8 参数配置
// 字段全 public，main.cpp 直接 c.PORT 这样用
#ifndef CONFIG_H
#define CONFIG_H

class Config 
{
public:
    Config();
    ~Config() {}
    void parse_arg(int argc, char* argv[]);

    int PORT;          // -p
    int LOGWrite;      // -l0=同步 1=异步
    int TRIGMode;      // -m  0/1/2/3 → LT-LT / ET-LT / LT-ET / ET-ET
    int OPT_LINGER;    // -o  优雅关闭
    int sql_num;       // -s
    int thread_num;    // -t
    int close_log;     // -c
    int actor_model;   // -a
};

#endif
