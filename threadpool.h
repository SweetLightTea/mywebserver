// threadpool.h —— 线程池本尊
// 把迷你实验升级成可复用的真服务器版
// 编译命令走 cmake，自动被 main.cpp include

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>
#include <functional>
#include <vector>
#include <cstdio>
#include <cstdlib>

const int QUEUE_SIZE = 16;  // 最多挂 16 张小票（教学够用了）

struct Task 
{
    std::function<void()> func;  // 一张票 = 一段"待办代码"
};

struct ThreadPool 
{
    Task queue[QUEUE_SIZE];
    int q_head = 0;   // 厨师从这里取
    int q_tail = 0;   // 老板从这里挂
    int q_count = 0;
    pthread_mutex_t lock;   // 钥匙
    pthread_cond_t  cond;   // 铃
    std::vector<pthread_t> threads;
    bool shutdown = false;
};

// ========== 厨师入口（先放最前面，下面的函数要用 worker ==========
inline void* worker(void* arg) 
{
    ThreadPool* pool = (ThreadPool*)arg;
    while (true) 
    {
        // ① 锁门等活
        pthread_mutex_lock(&pool->lock);
        while (pool->q_count == 0 && !pool->shutdown) 
        {
            pthread_cond_wait(&pool->cond, &pool->lock);
        }
        // ② 收到打烊指令 + 活干完 → 回家
        if (pool->shutdown && pool->q_count == 0) 
        {
            pthread_mutex_unlock(&pool->lock);
            break;
        }
        // ③ 抢一张小票
        Task t = pool->queue[pool->q_head];
        pool->q_head = (pool->q_head + 1) % QUEUE_SIZE;
        pool->q_count--;
        pthread_mutex_unlock(&pool->lock);
        // ④ 锁外做菜（关键！做菜不能占着钥匙）
        t.func();
    }
    return nullptr;
}

// ========== 老板：雇厨师 ==========
inline ThreadPool* threadpool_create(int n) 
{
    ThreadPool* pool = new ThreadPool();
    pthread_mutex_init(&pool->lock, nullptr);
    pthread_cond_init(&pool->cond, nullptr);
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

// ========== 老板：挂小票 + 摇铃 ==========
inline void threadpool_add(ThreadPool* pool, Task t) 
{
    pthread_mutex_lock(&pool->lock);
    pool->queue[pool->q_tail] = t;
    pool->q_tail = (pool->q_tail + 1) % QUEUE_SIZE;
    pool->q_count++;
    pthread_cond_signal(&pool->cond);  // 摇醒一个厨师
    pthread_mutex_unlock(&pool->lock);
}

// ========== 老板：打烊 ==========
inline void threadpool_destroy(ThreadPool* pool) 
{
    pthread_mutex_lock(&pool->lock);
    pool->shutdown = true;
    pthread_cond_broadcast(&pool->cond);  // 叫醒所有厨师
    pthread_mutex_unlock(&pool->lock);
    for (pthread_t t : pool->threads) {
        pthread_join(t, nullptr);  // 等所有厨师收摊
    }
    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->cond);
    delete pool;
}

#endif
