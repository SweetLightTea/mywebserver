// ====== 替换 config.cpp 里 parse_arg 上面那块 ======
#include "config.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <climits>
#include <unistd.h>

// 【L29】带错误检查的整型转换：输入非法 → 打印错误 → exit(1)
// （atoi 的坑：传 "abc" 返回 0 不报错，传 NULL 直接段错）
static int arg_to_int(const char* flag, const char* s) {
    if (s == nullptr || *s == '\0') {
        fprintf(stderr, "Error: -%s 需要一个数字，你什么也没传\n", flag);
        exit(1);
    }
    char* end = nullptr;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < 0 || v > INT_MAX) {
        fprintf(stderr, "Error: -%s 需要非负整数，你传了 '%s'\n", flag, s);
        exit(1);
    }
    return static_cast<int>(v);
}

Config::Config()
    : PORT(9007), LOGWrite(1), TRIGMode(3), OPT_LINGER(1),
      sql_num(8), thread_num(8), close_log(0), actor_model(1)
{}

void Config::parse_arg(int argc, char* argv[]) {
    int opt;
    const char* str = "p:l:m:o:s:t:c:a:";
    while ((opt = getopt(argc, argv, str)) != -1) 
    {
        switch (opt) 
        {
            case 'p': PORT        = arg_to_int("p", optarg); break;
            case 'l': LOGWrite    = arg_to_int("l", optarg); break;
            case 'm': TRIGMode    = arg_to_int("m", optarg); break;
            case 'o': OPT_LINGER  = arg_to_int("o", optarg); break;
            case 's': sql_num     = arg_to_int("s", optarg); break;
            case 't': thread_num  = arg_to_int("t", optarg); break;
            case 'c': close_log   = arg_to_int("c", optarg); break;
            case 'a': actor_model = arg_to_int("a", optarg); break;
            default:
                fprintf(stderr,
                    "Usage: %s [-p port] [-l log] [-m trig] [-o linger]\n"
                    "          [-s sql] [-t thread] [-c close] [-a actor]\n",
                    argv[0]);
                exit(1);
        }
    }
}
