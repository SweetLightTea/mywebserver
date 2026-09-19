// threadpool.h —— 线程池本尊（【加固】Locker/Cond 三件套上岗 + 队满阻塞）
// 编译命令走 cmake，自动被 web_server.cpp include

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>
#include <functional>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include "lock/locker.h"   // 【加固】L29 的教学资产正式上岗

const int QUEUE_SIZE = 1024;   // 小票队列容量

struct Task
{
    std::function<void()> func;  // 一张票 = 一段"待办代码"
};

struct ThreadPool
{
    Task queue[QUEUE_SIZE];
    int q_head = 0;     // 厨师从这里取
    int q_tail = 0;     // 老板从这里挂
    int q_count = 0;
    Locker lock;        // 钥匙（RAII：构造自动 init，析构自动 destroy）
    Cond   cond;        // 铃（一条铃两种叫醒理由：有活 / 有空位）
    std::vector<pthread_t> threads;
    bool shutdown = false;
};

// ========== 厨师入口 ==========
inline void* worker(void* arg)
{
    ThreadPool* pool = (ThreadPool*)arg;
    while (true)
    {
        // ① 锁门等活
        pool->lock.lock();
        while (pool->q_count == 0 && !pool->shutdown)
        {
            pool->cond.wait(pool->lock.get());
        }
        // ② 收到打烊指令 + 活干完 → 回家
        if (pool->shutdown && pool->q_count == 0)
        {
            pool->lock.unlock();
            break;
        }
        // ③ 抢一张小票
        Task t = pool->queue[pool->q_head];
        pool->q_head = (pool->q_head + 1) % QUEUE_SIZE;
        pool->q_count--;
        pool->cond.broadcast();   // 【加固】关键新增：腾出空位了，叫醒可能被堵的老板
        pool->lock.unlock();
        // ④ 锁外做菜（关键！做菜不能占着钥匙）
        t.func();
    }
    return nullptr;
}

// ========== 老板：雇厨师 ==========
inline ThreadPool* threadpool_create(int n)
{
    ThreadPool* pool = new ThreadPool();   // Locker/Cond 构造时自动初始化
    pool->threads.resize(n);
    for (int i = 0; i < n; i++)
    {
        if (pthread_create(&pool->threads[i], nullptr, worker, pool) != 0)
        {
            perror("pthread_create");
            exit(1);
        }
    }
    return pool;
}

// ========== 老板：挂小票（【加固】队满不再覆盖最老的票，改为排队等空位） ==========
inline void threadpool_add(ThreadPool* pool, Task t)
{
    pool->lock.lock();
    while (pool->q_count == QUEUE_SIZE)
    {
        pool->cond.wait(pool->lock.get());   // 满了：老板也排队，不覆盖、不丢票
    }
    pool->queue[pool->q_tail] = t;
    pool->q_tail = (pool->q_tail + 1) % QUEUE_SIZE;
    pool->q_count++;
    pool->cond.broadcast();   // 有新小票：叫醒等活的厨师
    pool->lock.unlock();
}

// ========== 老板：打烊 ==========
inline void threadpool_destroy(ThreadPool* pool)
{
    pool->lock.lock();
    pool->shutdown = true;
    pool->cond.broadcast();   // 叫醒所有厨师
    pool->lock.unlock();
    for (pthread_t t : pool->threads)
    {
        pthread_join(t, nullptr);   // 等所有厨师收摊
    }
    delete pool;   // Locker/Cond 析构时自动销毁
}

#endif
