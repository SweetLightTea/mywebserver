#pragma once
// sql_connection_pool.h  ——  数据库连接池（教学版，用 std::string 假扮连接）
//
// 思路：服务器启动时一次性配 N 把"数据库钥匙"放进带锁的柜子。
//       厨师查数据 → GetConn 借一把 → 用完 → FreeConn 还回去。
//       池子空了？cv_.wait 让厨师睡觉；有人还钥匙就 notify_one 叫醒一个。
//
// 关键不变量：任何时刻池子里的钥匙数 + 厨师手上的钥匙数 = 总数（不漏不重）
//
// 用法：
//     SqlConnPool::Instance().Init(8);          // 启动时
//     string* conn = SqlConnPool::Instance().GetConn();   // 借
//     ... 用 conn 假装查表 ...
//     SqlConnPool::Instance().FreeConn(conn);             // 还
//     SqlConnPool::Instance().Close();         // 关服务器时

#include <string>
#include <mutex>
#include <condition_variable>
#include <vector>

class SqlConnPool 
{
public:
    static SqlConnPool& Instance() 
    {
        static SqlConnPool p;   // 单例：进程里只有一个柜子
        return p;
    }

    // 启动时主线程调用：一次性配 N 把钥匙
    void Init(int size) 
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (int i = 0; i < size; ++i) 
        {
            std::string* s = new std::string(
                "FAKE_DB_CONN_" + std::to_string(i));
            pool_.push_back(s);
        }
        LOG_INFO("[SQL Pool] 初始化完毕，配了 %d 把钥匙", size);
    }

    // 厨师线程调用：借一把。池空就阻塞等。
    std::string* GetConn() 
    {
        std::unique_lock<std::mutex> lk(mtx_);
        while (pool_.empty()) 
        {
            LOG_WARN("[SQL Pool] 钥匙借光了，厨师阻塞等待");
            cv_.wait(lk);   // 释放锁去睡觉，醒来再抢锁
        }
        std::string* conn = pool_.back();
        pool_.pop_back();
        LOG_INFO("[SQL Pool] 借出一把 %p", conn);
        return conn;
    }

    // 厨师线程调用：还一把。叫醒一个等钥匙的厨师。
    void FreeConn(std::string* conn) 
    {
        std::lock_guard<std::mutex> lk(mtx_);
        pool_.push_back(conn);
        LOG_INFO("[SQL Pool] 归还一把 %p", conn);
        cv_.notify_one();   // 有新钥匙了，叫醒一个在睡的厨师
    }

    // 关服务器时调用：把所有钥匙回收销毁
    void Close() 
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto* c : pool_) delete c;
        pool_.clear();
        LOG_INFO("[SQL Pool] 清场，销毁全部钥匙");
    }

private:
    SqlConnPool() = default; //私有构造函数：将默认构造函数声明为 private，意味着外部代码无法直接创建 SqlConnPool 对象
    std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::string*> pool_;
};
