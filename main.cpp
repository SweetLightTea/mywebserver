#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <cstring>
#include <ctime>
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

// 响应工厂：把状态行和正文打包成合法的 HTTP 响应，返回总长度
int make_response(char* out, const char* status, const char* body, bool keep_alive) {
    return sprintf(out,
        "HTTP/1.1 %s\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"    // 分号在 html 和 charset 之间
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"                            // 冒号在 Connection 后面
        "\r\n"
        "%s",
        status, strlen(body),
        keep_alive ? "keep-alive" : "close",            // 留桌就回 keep-alive
        body);
}


// 路由：看客人要什么，决定端什么菜
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
    else
        len = make_response(out, "404 Not Found", "<h1>404：菜单上没有这道菜</h1>", keep_alive);
    write(fd, out, len);
}


int main() 
{
    signal(SIGPIPE, SIG_IGN);   // 客人拒收也保命：向断开的连接 write 不杀进程
 
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
                int bytes = read(fd, buf, sizeof(buf) - 1);    // 少读 1 字节，给结尾的 0 留位置
                if (bytes <= 0) {
                    printf("客人 %d 走了\n", fd);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                } 
                else 
                {
                    buf[bytes] = 0;    // 关键新增行！截断成 C 字符串，行尾分号
                    char method[16], path[256];
                    int cnt = sscanf(buf, "%15s %255s", method, path);
                    if (cnt == 2) 
                    {
                        bool keep_alive = (strstr(buf, "Connection: close") == nullptr);
                        printf("客人 %d 点单：%s %s\n", fd, method, path);
                        handle_request(fd, method, path, keep_alive);
                    } else 
                    {
                        char out[1024];
                        int len = make_response(out, "400 Bad Request", "<h1>400 听不懂你在说什么</h1>", false);
                        write(fd, out, len);
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                    }
                    // 注意：这里不再关桌！连接继续留在 epoll 里等下一单
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