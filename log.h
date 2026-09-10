// log.h —— 线程安全的简单日志系统
// 用法：LOG_INFO("格式串", 参数...)
// 输出形如：[2026-09-09 15:05:00] INFO 厨师做菜：客人 5 点 GET /time

#ifndef LOG_H
#define LOG_H

#include <cstdio>
#include <ctime>
#include <cstdarg>
#include <cstddef>
#include <cstring>
#include <mutex>

extern std::mutex g_log_lock;  // 整个工程的日志锁（main.cpp 里定义）

// ========== 内部实现：拼时间戳 + 级别 + 消息，写文件 + 终端 ==========
inline void log_write(const char* level, const char* fmt, ...) {
    char buf[4096];

    // ① 拼时间戳前缀：[2026-09-09 15:05:00] INFO 
    char timebuf[32];
    time_t now = time(nullptr);
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", localtime(&now));
    int prefix_len = snprintf(buf, sizeof(buf), "[%s] %s ", timebuf, level);

    // ② 拼用户消息（vsnprintf = printf 的安全版，限定最大长度防溢出）
    va_list ap; //ap 就是“参数指针”，可以理解为一个游标，指向当前待处理的参数。
    va_start(ap, fmt);
    vsnprintf(buf + prefix_len, sizeof(buf) - prefix_len, fmt, ap); //sizeof(buf) - prefix_len：缓冲区剩余可用空间，防止溢出。
    va_end(ap); //va_end 宏用于结束对可变参数的访问，执行必要的清理工作。

    // ③ 末尾补换行
    int len = (int)strlen(buf);
    if (len < (int)sizeof(buf) - 1) 
    {
        buf[len] = '\n';
        buf[len + 1] = '\0';
    }

    // ④ 锁门，写文件 + 终端
    std::lock_guard<std::mutex> lock(g_log_lock);
    FILE* fp = fopen("server.log", "a");  // a = append 追加模式
    if (fp) 
    {
        fputs(buf, fp);
        fclose(fp);
    }
    fputs(buf, stderr);  // 教学用：同时打到终端，方便看
}

// ========== 三个日志宏 ==========
#define LOG_INFO(...)  log_write("INFO",  __VA_ARGS__)
#define LOG_WARN(...)  log_write("WARN",  __VA_ARGS__)
#define LOG_ERROR(...) log_write("ERROR", __VA_ARGS__)

#endif
