// main.cpp —— 【L30】只剩门面：解析参数、交给 WebServer、退场
#include "web_server.h"

int main(int argc, char* argv[])
{
    Config c;
    c.parse_arg(argc, argv);      // 命令行 → 配置对象

    WebServer server;
    server.init(c);               // 配置灌进去
    server.run();                 // 开门营业，直到 Ctrl+C 优雅关停
    return 0;
}
