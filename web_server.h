// web_server.h —— 【L30】服务器门面：外界只需要知道"给它配置、让它跑"
#ifndef WEB_SERVER_H
#define WEB_SERVER_H

#include "config.h"

class WebServer 
{
public:
    void init(Config& c);   // 收参数（main.cpp 把命令行解析结果递进来）
    void run();             // 开门营业：初始化 → 事件循环 → 优雅关停
private:
    Config m_conf;          // 参数存一份，run() 里全用 m_conf.xxx
};

#endif
