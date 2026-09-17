// log.h —— 【L28】升级版日志：同步/异步双模式 + 阻塞队列 + 按天分文件
//
// L19 老版：每次 LOG 都是 fopen → 写 → fclose，厨师打一条日志就被磁盘卡一下。
// 本版对标原版 TinyWebServer 的 log 模块，三个升级：
//   ① 异步模式：LOG 宏只拼字符串，扔进阻塞队列（传送带）就走人，
//      后台"搬运工线程"慢慢写文件 —— 业务和磁盘 IO 解耦
//   ② 同步模式：init 时选同步、或队列满降级 → 调用线程直接写（一条不丢）
//   ③ 按天分文件：2026_09_17_ServerLog；单文件超 split_lines 行再开 .1 .2 …
//
// 用法（main 开头一次）：
//   Log::get_instance()->init("ServerLog", 5000000, 10000);  // 异步，队列 1 万条
//   Log::get_instance()->init("ServerLog", 5000000, 0);      // 同步直写
// 之后随便哪个线程：LOG_INFO("格式串", 参数...);
// 关停收尾：Log::get_instance()->flush();
//
// 关键设计：LOG_INFO 宏签名和 L19 版一模一样 →
//   main.cpp 22 处调用、sql_connection_pool.h 全部零改动（换引擎不换底盘）

#ifndef LOG_H
#define LOG_H

#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <ctime>
#include <mutex>
#include <string>
#include <thread>
#include "block_queue.h"

class Log
{
public:
    // C++11 局部静态变量单例：线程安全不用加锁（和 SqlConnPool 同款）
    static Log* get_instance()
    {
        static Log instance;
        return &instance;
    }

    // file_name：日志文件名（程序自动拼上日期前缀）
    // split_lines：单文件最大行数，超过开新文件（.1 .2 后缀）
    // max_queue_size：>0 = 异步模式（队列容量）；0 = 同步直写
    void init(const char* file_name, int split_lines = 5000000, int max_queue_size = 0)
    {
        strncpy(file_name_, file_name, sizeof(file_name_) - 1);
        split_lines_ = split_lines;
        if (max_queue_size > 0)
        {
            is_async_ = true;
            queue_ = new BlockQueue<std::string>(max_queue_size);
            flusher_ = std::thread(&Log::async_write_log, this);   // 搬运工上岗
        }
        today_ = 0;    // 0 = 还没开过文件，第一条日志时自动开今天的
        write_log(1, "[Log] 日志系统就位：%s 模式，按天分文件，单文件上限 %d 行",
          is_async_ ? "异步" : "同步", split_lines_);
    }

    void write_log(int level, const char* format, ...)
    {
        static const char* LEVELS[] = { "", "INFO", "WARN", "ERROR" };
        if (level < 1 || level > 3) level = 1;

        // ① 拼时间戳前缀（格式和 L19 版完全一致，终端观感不变）
        char buf[8192];
        char timebuf[32];
        time_t now = time(nullptr);
        struct tm t;
        localtime_r(&now, &t);
        strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", &t);
        int prefix = snprintf(buf, sizeof(buf), "[%s] %s ", timebuf, LEVELS[level]);

        // ② 拼用户消息（vsnprintf 限长防溢出）
        va_list ap;
        va_start(ap, format);
        vsnprintf(buf + prefix, sizeof(buf) - prefix, format, ap);
        va_end(ap);

        // ③ 补换行
        int len = (int)strlen(buf);
        if (len < (int)sizeof(buf) - 1) { buf[len] = '\n'; buf[len + 1] = '\0'; }

        // ④ 跨天 / 行数满 → 换新文件（原版同款逻辑，锁内干）
        std::unique_lock<std::mutex> lk(mtx_);
        bool new_day = (today_ != t.tm_mday);
        if (fp_ && (new_day || (count_ > 0 && count_ % split_lines_ == 0)))
        {
            fflush(fp_);
            fclose(fp_);
            fp_ = nullptr;
        }
        if (!fp_)
        {
            char name[256];
            long long part = 0;
            if (new_day || today_ == 0)
            {
                today_ = t.tm_mday;
                count_ = 0;
            }
            else
                part = count_ / split_lines_;   // 同一天写满：加 .1 .2 后缀

            if (part > 0)
                snprintf(name, sizeof(name), "%d_%02d_%02d_%s.%lld",
                         t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, file_name_, part);
            else
                snprintf(name, sizeof(name), "%d_%02d_%02d_%s",
                         t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, file_name_);
            fp_ = fopen(name, "a");
        }
        ++count_;

        // ⑤ 异步且队列没满 → 扔上传送带走人；否则（同步 / 队列满降级）直接写
        if (is_async_ && queue_->push(std::string(buf)))
        {
            // 已交棒，走人
        }
        else
        {
            fputs(buf, fp_);
            fflush(fp_);
        }
        lk.unlock();

        fputs(buf, stderr);   // 教学用：终端同步可见（生产环境可删）
    }

    // 关停收尾：传送带关门 → 等搬运工把剩的搬完 → 冲刷文件缓冲
    void flush()
    {
        if (is_async_ && queue_)
        {
            queue_->close();
            if (flusher_.joinable()) flusher_.join();
        }
        std::lock_guard<std::mutex> lk(mtx_);
        if (fp_) fflush(fp_);
    }

private:
    Log() { strncpy(file_name_, "ServerLog", sizeof(file_name_) - 1); }
    ~Log()
    {
        // 兜底：万一 main 忘了 flush（比如 exit(1) 硬退），也不能让线程悬着
        if (is_async_ && queue_)
        {
            queue_->close();
            if (flusher_.joinable()) flusher_.join();
        }
        if (fp_) { fflush(fp_); fclose(fp_); }
        delete queue_;
    }

    // 后台搬运工主循环：从队列搬日志进文件，关门且搬空才下班
    void async_write_log()
    {
        std::string single;
        while (queue_->pop(single))
        {
            std::lock_guard<std::mutex> lk(mtx_);
            fputs(single.c_str(), fp_);
            fflush(fp_);
        }
    }

    char file_name_[128];    // 日志文件名
    int  split_lines_ = 5000000;
    long long count_ = 0;    // 今天已写的行数
    int  today_ = 0;         // 今天几号（跨天检测）
    FILE* fp_ = nullptr;
    std::mutex mtx_;         // 保护 fp_ / count_ / 换文件
    bool is_async_ = false;
    BlockQueue<std::string>* queue_ = nullptr;
    std::thread flusher_;    // 搬运工
};

#define LOG_INFO(...)  Log::get_instance()->write_log(1, __VA_ARGS__)
#define LOG_WARN(...)  Log::get_instance()->write_log(2, __VA_ARGS__)
#define LOG_ERROR(...) Log::get_instance()->write_log(3, __VA_ARGS__)

#endif
