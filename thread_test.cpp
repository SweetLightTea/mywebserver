#include <cstdio>
#include <pthread.h>
#include <unistd.h>

// 一张小票 = 一道菜（函数指针 + 菜的编号）
struct Task {
    void (*func)(int);   // 注意 func 后面是括号不是 o
    int arg;
};

const int QUEUE_SIZE = 16;          // 小票队列容量
Task tickets[QUEUE_SIZE];           // 环形队列本体
int q_head = 0;                     // 厨师从这头取
int q_tail = 0;                     // 老板从这头挂
int q_count = 0;                    // 现在挂着几张单

pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;      // 排队钥匙（静态初始化，不用手动的 init）
pthread_cond_t  g_bell = PTHREAD_COND_INITIALIZER;      // 床头铃

void push_task(void (*func)(int), int arg) 
{
    pthread_mutex_lock(&g_lock);          // 拿钥匙
    if (q_count == QUEUE_SIZE) 
    {
        printf("队列满了，丢单 %d\n", arg);
    } 
    else 
    {
        tickets[q_tail].func = func;
        tickets[q_tail].arg = arg;
        q_tail = (q_tail + 1) % QUEUE_SIZE;   // 尾巴转圈走，取余是环形的关键
        q_count++;
        pthread_cond_signal(&g_bell);          // 摇铃：有新单！
    }
    pthread_mutex_unlock(&g_lock);        // 还钥匙
}

void* chef_loop(void*) 
{                  // 厨师线程：进场后永远循环
    while (true) 
    {
        pthread_mutex_lock(&g_lock);
        while (q_count == 0) 
        {            // 用 while 判空！不是 if（自测题 3）
            pthread_cond_wait(&g_bell, &g_lock);   // 睡觉：自动撒手钥匙，被叫醒自动重新拿
        }
        Task t = tickets[q_head];         // 取走队头小票
        q_head = (q_head + 1) % QUEUE_SIZE;
        q_count--;
        pthread_mutex_unlock(&g_lock);    // 还钥匙

        t.func(t.arg);                    // 做菜——注意：在锁外面！
    }
    return nullptr;
}

void cook_dish(int dish) 
{                // 做菜动作：假装要 1 秒
    printf("厨师 %lu 开始做菜 %d\n", (unsigned long)pthread_self(), dish);
    sleep(1);
    printf("厨师 %lu 做好了菜 %d\n", (unsigned long)pthread_self(), dish);
}

int main() 
{
    const int CHEF_NUM = 3;
    pthread_t tid[CHEF_NUM];
    for (int i = 0; i < CHEF_NUM; i++)
        pthread_create(&tid[i], nullptr, chef_loop, nullptr);   // 雇 3 个厨师，进场就位

    for (int i = 1; i <= 10; i++) 
    {
        printf("老板挂上菜 %d 的小票\n", i);
        push_task(cook_dish, i);
    }

    sleep(5);    // 等厨师们干完活（实验用，真实线程池永不退出）
    printf("实验结束\n");
    return 0;
}
