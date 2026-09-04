#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cerrno>

const int PORT = 9007;  // 和原版的 9006 区分开，避免冲突
const int MAX_EVENTS = 10;
volatile sig_atomic_t g_stop = 0;

void on_signal(int)
{
    g_stop = 1;   
}

// 固定的 HTTP 响应。以后会学会自己解析请求、动态生成响应
const char* HTML =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: 32\r\n"
    "\r\n"
    "<h1>Hello, my first server!</h1>";

int main() 
{
    signal(SIGINT, SIG_IGN);

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    // ===== 第 1 段：开门准备（和昨天一模一样）=====
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); exit(1);} 

    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);  // 允许任何网卡接入
    addr.sin_port = htons(PORT);
    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {perror("bind"); exit(1); }
    if (listen(listen_fd, 5) < 0) {perror("listen"); exit(1); }
    printf("我的 epoll 服务器已启动！ 端口 %d\n", PORT);

    // ===== 第 2 段：装总台（新！）=====
    int epfd = epoll_create1(0);

    // ===== 第 3 段：把大门登记进总台（新！）=====
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    // ===== 第 4 段：主循环——坐着等名单（换掉了原来的循环）=====
    epoll_event events[MAX_EVENTS]; // 名单：今晚谁有事
    char buf[4096];
    
    while (!g_stop) 
    {
        // 坐着等。-1 = 没事就一直歇着；一有事立刻醒，n = 有几桌
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) 
        {
            if (errno == EINTR) continue; // 被信号打断了，继续等
            perror("epoll_wait");
            break;
        }
        
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;

            if (fd == listen_fd)
            {
                sockaddr_in cli{};
                socklen_t len = sizeof(cli);
                int conn_fd = accept(listen_fd, (sockaddr*)&cli, &len);
                printf("新客人来了！ 桌号 %d\n", conn_fd);

                // 把新桌子登记进总台（你答的那句"记录和监听"）
                epoll_event cev{};
                cev.events = EPOLLIN;
                cev.data.fd = conn_fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, conn_fd, &cev);
            }
            else
            {
                // 老客人开口说话了：读他的请求
                int bytes = read(fd, buf, sizeof(buf));

                if (bytes <= 0)
                {
                    // 读到 0 = 客人悄悄走了：注销桌位、收桌子
                    printf("客人 %d 走了\n", fd);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
                else
                {
                    // 正常点单：回固定页面，然后送客
                    printf("客人 %d 说了 %d 个字节\n", fd, bytes);
                    write(fd, HTML, strlen(HTML));
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            }
        }
    }

    // ===== 优雅打烊（新！）=====
    close(listen_fd);
    close(epfd);
    printf("我的 epoll 服务器已关闭！\n");
    return 0;
}