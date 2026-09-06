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

const int TICK_SEC    = 3;   // 闹钟每 3 秒响一次
const int TIMEOUT_SEC = 6;   // 连续 6 秒没点单就收桌

//volatile：告诉编译器变量可能被外部修改，每次访问都从内存读取，避免优化。
volatile sig_atomic_t g_tick = 0;   // 闹钟旗：handler 立旗，主循环拔旗

void on_alarm(int) {
    g_tick = 1;            // 只立旗，别的什么都不干
    alarm(TICK_SEC);       // 续上下一次发条，闹钟才能一直响
}

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
    signal(SIGALRM, on_alarm);   // 行尾分号！SIG-ALRM（闹钟），别看成别的
    alarm(TICK_SEC);             // 上第一次发条

    time_t last_active[1024] = {0};   // 桌子登记簿：下标=桌号，值=最后点单时刻，0=空桌

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
        if (g_tick) 
        {                               // 保安挪到最顶上！先收桌再去等事件
            g_tick = 0;
            time_t now = time(nullptr);
            for (int fd = 0; fd < 1024; fd++) 
            {
                if (last_active[fd] != 0 && now - last_active[fd] >= TIMEOUT_SEC) 
                {
                    printf("客人 %d 超时未点单，收桌！\n", fd);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    last_active[fd] = 0;            // 销户
                }
            }
        }

        int n = epoll_wait(epfd, events, 64, -1);   // 门卫睡觉等事件

        if (n < 0) 
        {
            if (errno == EINTR) continue;           // 只是闹钟拍醒，回顶部扫一圈再睡
            printf("epoll_wait 出错 errno=%d，收摊\n", errno);   // 退出前必须留句话
            break;
        }

        for (int i = 0; i < n; i++) 
        {
            if (events[i].data.fd == listen_fd) 
            {
                struct sockaddr_in addr;
                socklen_t len = sizeof(addr);
                int fd = accept(listen_fd, (struct sockaddr*)&addr, &len);
                if (fd < 0 || fd >= 1024) continue;     // 【新增】accept 可能失败！拿 -1 或超大桌号直接跳过
                printf("新客人来了！桌号 %d\n", fd);
                ev.events = EPOLLIN;
                ev.data.fd = fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
                last_active[fd] = time(nullptr);
            } 
            else 
            {
                int fd = events[i].data.fd;
                if (last_active[fd] == 0) continue;     // 【新增】保险丝：桌号 0=已被保安收走，过期事件跳过

                int bytes = read(fd, buf, sizeof(buf) - 1);
                if (bytes == 0) 
                {                       // 客人礼貌告别
                    printf("客人 %d 走了\n", fd);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    last_active[fd] = 0;
                } 
                else if (bytes < 0) 
                {                 // 【新增】出事先验明正身，不再一律当走了
                    if (errno == EINTR) continue;       // 闹钟路过，客人还在
                    printf("客人 %d 出状况（errno=%d），收桌\n", fd, errno);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    last_active[fd] = 0;
                } 
                else 
                {
                    buf[bytes] = 0;
                    char method[16], path[256];
                    int cnt = sscanf(buf, "%15s %255s", method, path);
                    if (cnt == 2) 
                    {
                        bool keep_alive = (strstr(buf, "Connection: close") == nullptr);
                        printf("客人 %d 点单：%s %s\n", fd, method, path);
                        handle_request(fd, method, path, keep_alive);
                        last_active[fd] = time(nullptr);   // 点单续命
                    } 
                    else 
                    {
                        char out[1024];
                        int len = make_response(out, "400 Bad Request", "<h1>400 听不懂你在说什么</h1>", false);
                        write(fd, out, len);
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        last_active[fd] = 0;
                    }
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