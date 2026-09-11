#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cerrno>
#include <mutex>
#include <string>              // 【L21】用 std::string 当请求缓冲
#include "log.h"               // 【L19】日志宏
#include "threadpool.h"        // 【L17】线程池
#include "sql_connection_pool.h" //预处理器是从上到下顺序展开的。当处理到 sql_connection_pool.h 时，LOG_INFO 这个宏已经在 log.h 里定义过了，所以编译器认识它。

const int TICK_SEC    = 3;
const int TIMEOUT_SEC = 6;
const int MAX_REQ_BUF = 8192;   // 【L21】单连接请求缓冲上限，防赖皮客人撑爆内存

volatile sig_atomic_t g_tick = 0;
void on_alarm(int) { g_tick = 1; alarm(TICK_SEC); }

const int PORT = 9007;
const int MAX_EVENTS = 10;
volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

time_t last_active[1024] = {0};
ThreadPool* g_pool = nullptr;
int g_epfd = -1;

std::mutex g_log_lock;    // log.h 里 extern 声明的锁，本体在这

// ========== 【L21】每张桌子一张"点单便签"：没凑成完整请求的碎片堆这儿 ==========
struct ConnCtx 
{
    std::string inbuf;
};
ConnCtx g_conns[1024];

// ========== 响应工厂（和以前一样）==========
int make_response(char* out, const char* status, const char* body, bool keep_alive) 
{
    return sprintf(out,
        "HTTP/1.1 %s\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"
        "\r\n"
        "%s",
        status, strlen(body),
        keep_alive ? "keep-alive" : "close",
        body);
}

// ========== 路由（和以前一样）==========
void handle_request(int fd, const char* method, const char* path, bool keep_alive) 
{
    char out[8192]; int len;
    if (strcmp(method, "GET") != 0)
        len = make_response(out, "405 Method Not Allowed", "<h1>405 我只听得懂 GET</h1>", keep_alive);
    else if (strcmp(path, "/") == 0)
        len = make_response(out, "200 OK", "<h1>欢迎光临首页！</h1>", keep_alive);
    else if (strcmp(path, "/hello") == 0)
        len = make_response(out, "200 OK", "<h1>你好，这里是 /hello</h1>", keep_alive);
    else if (strcmp(path, "/time") == 0) 
    {
        char tbuf[64], body[128];
        time_t t = time(nullptr);
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&t));
        snprintf(body, sizeof(body), "<h1>服务器时间：%s</h1>", tbuf);
        len = make_response(out, "200 OK", body, keep_alive);
    } 
    else if (strcmp(path, "/sql") == 0) 
    {
        std::string* conn = SqlConnPool::Instance().GetConn();
        std::string fake_row = "使用连接 " + *conn + " 查到：用户名=alice 积分=100";
        SqlConnPool::Instance().FreeConn(conn);
        std::string body = "<h1>SQL Pool OK</h1><p>" + fake_row + "</p>";
        len = make_response(out, "200 OK", body.c_str(), keep_alive);
    }
    else
        len = make_response(out, "404 Not Found", "<h1>404：菜单上没有这道菜</h1>", keep_alive);
    write(fd, out, len);
}

// ========== 把 fd 切成非阻塞（L20）==========
void set_nonblocking(int fd) 
{
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ========== 【L21】统一收桌：所有收桌路径只写一遍，便签一起清 ==========
void close_conn(int fd) 
{
    epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    last_active[fd] = 0;
    g_conns[fd].inbuf.clear();   // 不清 = 给下一位坐这个桌号的客人留垃圾
}

// ========== 【L21】发个错误响应再收桌 ==========
void send_error_and_close(int fd, const char* status, const char* body) 
{
    char out[1024];
    int len = make_response(out, status, body, false);
    write(fd, out, len);
    close_conn(fd);
}

// ========== 【L21 大改】厨师干活：先攒够，再切分 ==========
void do_read(int fd) 
{
    char buf[4096];

    // ----- 阶段一：能读多少读多少，全塞进便签（不解析！）-----
    while (true) 
    {
        int bytes = read(fd, buf, sizeof(buf));

        if (bytes == 0) {                       // 客人礼貌告别
            LOG_INFO("客人 %d 走了", fd);
            close_conn(fd);
            return;
        }
        if (bytes < 0) 
        {
            if (errno == EINTR) continue;        // 闹钟路过，重读
            if (errno == EAGAIN) break;          // 读干了 → 去切请求
            LOG_WARN("客人 %d 出状况（errno=%d），收桌", fd, errno);
            close_conn(fd);
            return;
        }

        g_conns[fd].inbuf.append(buf, bytes);    // 【关键】先攒着，别急着解析

        if (g_conns[fd].inbuf.size() > MAX_REQ_BUF) 
        {   // 【L21】防赖皮客人
            LOG_WARN("客人 %d 请求超长（%zu 字节），收桌", fd, g_conns[fd].inbuf.size());
            send_error_and_close(fd, "413 Payload Too Large", "<h1>413 你这单子太长了</h1>");
            return;
        }
    }

    // ----- 阶段二：从便签里按 \r\n\r\n 切完整请求（可能一次切出好几条）-----
    while (true) 
    {
        std::string& inb = g_conns[fd].inbuf;
        size_t pos = inb.find("\r\n\r\n");       // 找"空行" = 请求头结束
        if (pos == std::string::npos) break;     // 半包：还没凑齐，等下一批数据

        std::string req = inb.substr(0, pos + 4);   // 切出一条完整请求（含末尾 \r\n\r\n）
        inb.erase(0, pos + 4);                      // 从便签上划掉，剩下的留给下一轮

        char method[16] = {0}, path[256] = {0};
        int cnt = sscanf(req.c_str(), "%15s %255s", method, path);
        if (cnt != 2) 
        {
            LOG_WARN("客人 %d 请求格式看不懂，收桌", fd);
            send_error_and_close(fd, "400 Bad Request", "<h1>400 听不懂你在说什么</h1>");
            return;
        }

        bool keep_alive = (req.find("Connection: close") == std::string::npos);
        LOG_INFO("厨师做菜：客人 %d 点 %s %s", fd, method, path);
        handle_request(fd, method, path, keep_alive);
        last_active[fd] = time(nullptr);

        if (!keep_alive) 
        {                       // 【L21】客人说要走，服务端别赖着
            LOG_INFO("客人 %d 说了 close，收桌", fd);
            close_conn(fd);
            return;
        }
    }

    // ----- 阶段三：挂回（哪怕一条完整请求都没切出来，也必须挂回！）-----
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(g_epfd, EPOLL_CTL_MOD, fd, &ev);
}

int main() 
{
    SqlConnPool::Instance().Init(8);   // L22: 启动时配 8 把钥匙

    signal(SIGPIPE, SIG_IGN);
    signal(SIGALRM, on_alarm);
    alarm(TICK_SEC);

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    // ===== 开门 =====
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); exit(1); }
    set_nonblocking(listen_fd);
    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);
    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(listen_fd, 5) < 0) { perror("listen"); exit(1); }
    LOG_INFO("我的 epoll+线程池 服务器已启动！ 端口 %d", PORT);

    g_pool = threadpool_create(3);
    g_epfd = epoll_create1(0);

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd;
    epoll_ctl(g_epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    epoll_event events[MAX_EVENTS];

    while (!g_stop) 
    {
        if (g_tick) 
        {
            g_tick = 0;
            time_t now = time(nullptr);
            for (int fd = 0; fd < 1024; fd++) 
            {
                if (last_active[fd] != 0 && now - last_active[fd] >= TIMEOUT_SEC) 
                {
                    LOG_INFO("客人 %d 超时未点单，收桌", fd);
                    close_conn(fd);              // 【L21】改用它，便签一起清
                }
            }
        }

        int n = epoll_wait(g_epfd, events, 64, -1);
        if (n < 0) 
        {
            if (errno == EINTR) continue;
            LOG_ERROR("epoll_wait 出错 errno=%d，收摊", errno);
            break;
        }

        for (int i = 0; i < n; i++) 
        {
            if (events[i].data.fd == listen_fd) 
            {
                sockaddr_in cli{};
                socklen_t clen = sizeof(cli);
                while (true) 
                {                   // ET：一次报一批，循环 accept 到 EAGAIN
                    int fd = accept(listen_fd, (sockaddr*)&cli, &clen);
                    if (fd < 0) 
                    {
                        if (errno == EINTR) continue;
                        break;                   // EAGAIN = 接完了
                    }
                    if (fd >= 1024) { close(fd); continue; }
                    LOG_INFO("新客人来了！桌号 %d", fd);
                    set_nonblocking(fd);
                    g_conns[fd].inbuf.clear();   // 【L21 关键】桌号复用！新客人进门先清便签
                    ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                    ev.data.fd = fd;
                    epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev);
                    last_active[fd] = time(nullptr);
                }
            } 
            else 
            {
                int fd = events[i].data.fd;
                if (last_active[fd] == 0) continue;   // 保险丝：过期事件跳过
                Task t;
                t.func = [fd] { do_read(fd); };       // 派单给厨师
                threadpool_add(g_pool, t);
            }
        }
    }

    threadpool_destroy(g_pool);
    SqlConnPool::Instance().Close();   // L22: 关服务器时回收销毁钥匙
    close(listen_fd);
    close(g_epfd);
    LOG_INFO("我的 epoll+线程池 服务器已关闭！");
    return 0;
}
