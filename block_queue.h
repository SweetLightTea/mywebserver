#pragma once
// block_queue.h —— 【L28】循环数组阻塞队列（日志的传送带）
//
// 原版 TinyWebServer 的 log/block_queue.h 同款结构：
//   生产者（写日志的线程）push：满了直接返回 false —— 日志绝不让业务卡住
//   消费者（后台搬运工线程）pop：空了就睡（条件变量），有货被叫醒
// 区别：原版用 pthread 的 locker/cond，咱们沿用项目一贯的 std 风格
//
// 循环数组：front_ 指向最后出队的位置，back_ 指向最后入队的位置，
//           下标永远 (x + 1) % max_size 往前滚，滚到头绕回来

#include <mutex>
#include <condition_variable>
#include <vector>

template <class T>
class BlockQueue
{
public:
    explicit BlockQueue(int max_size)
        : max_size_(max_size), array_(max_size), size_(0), front_(-1), back_(-1), closed_(false)
    {}

    // 生产者：塞一条。满了/关门了 → false（调用方降级处理，绝不阻塞！）
    bool push(const T& item)
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (size_ >= max_size_ || closed_) return false;
        back_ = (back_ + 1) % max_size_;
        array_[back_] = item;
        ++size_;
        cv_.notify_all();     // 叫醒搬运工：来货了
        return true;
    }

    // 消费者：取一条。空了就睡，直到"有货或关门"
    // 返回 false = 关门且搬空了（该下班了）
    bool pop(T& item)
    {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [this] { return size_ > 0 || closed_; });
        if (size_ == 0) return false;    // 关门 + 空了
        front_ = (front_ + 1) % max_size_;
        item = array_[front_];
        --size_;
        return true;
    }

    // 关门：叫醒还睡着的搬运工，让它把剩下的搬完就下班
    void close()
    {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            closed_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<T> array_;    // 循环数组本体
    int max_size_;
    int size_;
    int front_;    // 最后出队的位置（-1 = 还没出过队）
    int back_;     // 最后入队的位置（-1 = 还没入过队）
    bool closed_;
};
