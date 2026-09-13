#pragma once
// sql_connection_pool.h  ——  数据库连接池（【L24】假钥匙换真 MYSQL* 版）
//
// L22 时钥匙是 std::string 假扮的；现在换成真的：
//   mysql_init          造一把钥匙（分配一个连接对象）
//   mysql_real_connect  拿钥匙开门（和数据库握手、登录）
//   mysql_query         用钥匙干活（执行 SQL）
//   mysql_close         销毁钥匙
// 借还逻辑（mutex + condvar）一字不改 —— 池子的骨架和假版完全一样，
// 这就是分层的好处：换引擎不换底盘。
//
// 关键不变量：任何时刻池里的钥匙数 + 厨师手上的钥匙数 = 总数（不漏不重）

#include <mysql/mysql.h>            // 【L24】真 MySQL 头文件（libmysqlclient-dev 装的）
#include <mutex>
#include <condition_variable>
#include <vector>
#include "log.h"

class SqlConnPool
{
public:
    static SqlConnPool& Instance()
    {
        static SqlConnPool p;   // 单例：进程里只有一个柜子
        return p;
    }

    // 【L24】参数从 1 个变 5 个：连哪家库、用谁的身份、什么密码、库名、几把钥匙
    void Init(const char* host, const char* user, const char* passwd, const char* dbname, int size)
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (int i = 0; i < size; ++i)
        {
            MYSQL* m = mysql_init(nullptr);   // 造一把钥匙
            if (!mysql_real_connect(m, host, user, passwd, dbname, 0, nullptr, 0))
            {
                // 连不上会带原因（比如密码错、库不存在、mysql 没启动）
                LOG_ERROR("[SQL Pool] 第 %d 把钥匙连不上：%s", i, mysql_error(m));
                exit(1);   // 启动时数据库都连不上，别硬撑，直接报错退出
            }
            pool_.push_back(m);
        }
        LOG_INFO("[SQL Pool] 真连接池初始化完毕，%d 把 MYSQL* 钥匙", size);
    }

    // 厨师线程调用：借一把。池空就阻塞等。（和 L22 一模一样）
    MYSQL* GetConn()
    {
        std::unique_lock<std::mutex> lk(mtx_);
        while (pool_.empty())
        {
            LOG_WARN("[SQL Pool] 钥匙借光了，厨师阻塞等待");
            cv_.wait(lk);   // 释放锁去睡觉，醒来再抢锁
        }
        MYSQL* conn = pool_.back();
        pool_.pop_back();
        return conn;
    }

    // 厨师线程调用：还一把。叫醒一个等钥匙的厨师。（和 L22 一模一样）
    void FreeConn(MYSQL* conn)
    {
        std::lock_guard<std::mutex> lk(mtx_);
        pool_.push_back(conn);
        cv_.notify_one();
    }

    // 关服务器时调用：把所有连接关掉
    void Close()
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto* c : pool_) mysql_close(c);   // 【L24】string 的 delete 换成 mysql_close
        pool_.clear();
        LOG_INFO("[SQL Pool] 清场，关闭全部连接");
    }

private:
    SqlConnPool() = default;   // 私有构造：外部没法 new 出第二个柜子
    std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<MYSQL*> pool_;   // 【L24】钥匙类型从 string* 换成 MYSQL*
};
