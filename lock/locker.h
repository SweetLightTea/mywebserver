// lock/locker.h —— 【L29】pthread 三件套的 RAII 教学包
#ifndef LOCKER_H
#define LOCKER_H

#include <pthread.h>
#include <semaphore.h>
#include <exception>

// ====== 信号量 ======
class sem {
public:
    sem()            { if (sem_init(&m_sem, 0, 0) != 0) throw std::exception(); }
    sem(int num)     { if (sem_init(&m_sem, 0, num) != 0) throw std::exception(); }
    ~sem()           { sem_destroy(&m_sem); }
    bool wait()      { return sem_wait(&m_sem) == 0; }
    bool post()      { return sem_post(&m_sem) == 0; }

    // 【L29】禁用拷贝：内部持有 sem_t 资源，拷贝会 double init + double destroy 崩溃
    sem(const sem&)             = delete;
    sem& operator=(const sem&)  = delete;
private:
    sem_t m_sem;
};

// ====== 互斥锁 ======
class Locker {
public:
    Locker() { if (pthread_mutex_init(&m_mutex, NULL) != 0) throw std::exception(); }
    ~Locker()        { pthread_mutex_destroy(&m_mutex); }
    bool lock()      { return pthread_mutex_lock(&m_mutex) == 0; }
    bool unlock()    { return pthread_mutex_unlock(&m_mutex) == 0; }
    pthread_mutex_t* get() { return &m_mutex; }

    // 【L29】禁用拷贝：同上
    Locker(const Locker&)             = delete;
    Locker& operator=(const Locker&)  = delete;
private:
    pthread_mutex_t m_mutex;
};

// ====== 条件变量 ======
class Cond {
public:
    Cond()           { if (pthread_cond_init(&m_cond, NULL) != 0) throw std::exception(); }
    ~Cond()          { pthread_cond_destroy(&m_cond); }
    bool wait(pthread_mutex_t* m)            { return pthread_cond_wait(&m_cond, m) == 0; }
    bool timewait(pthread_mutex_t* m, struct timespec t)
 { return pthread_cond_timedwait(&m_cond, m, &t) == 0; }
    bool signal()                              { return pthread_cond_signal(&m_cond) == 0; }
    bool broadcast()                           { return pthread_cond_broadcast(&m_cond) == 0; }

    // 【L29】禁用拷贝：同上
    Cond(const Cond&)             = delete;
    Cond& operator=(const Cond&)  = delete;
private:
    pthread_cond_t m_cond;
};

#endif  // LOCKER_H
