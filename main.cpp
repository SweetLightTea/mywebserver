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
#include <map>                 // 【L24】装表单拆出来的键值对
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

// ========== 【L23】从请求头里找 Content-Length 的数值 ==========
int find_content_length(const std::string& headers)
{
    // 请求头里长这样：Content-Length: 25   （数字前有个空格，atoi 自己会跳过）
    const char* keys[2] = { "Content-Length:", "content-length:" };  // 大小写各试一遍
    for (int k = 0; k < 2; k++) 
    {
        size_t p = headers.find(keys[k]);
        if (p != std::string::npos) 
            return atoi(headers.c_str() + p + strlen(keys[k]));
    }
    return 0;   // 头里没写 = 当 0（GET 请求就是这样）
}

// ========== 【L23】URL 解码：把 %41 还原成字母 A，把 + 还原成空格 ==========
std::string url_decode(const std::string& s)
{
    // 浏览器发中文和特殊字符时会转义，比如 "张 三" -> "%E5%BC%A0+%E4%B8%89"
    std::string out;
    for (size_t i = 0; i < s.size(); i++) 
    {
        if (s[i] == '+') 
        {
            out += ' ';
        } 
        else if (s[i] == '%' && i + 2 < s.size()) 
        {
            char hex[3] = { s[i+1], s[i+2], 0 };           // 取 % 后面两个十六进制字符
            out += (char)strtol(hex, nullptr, 16);          // "41" -> 0x41 -> 字母 A
            i += 2;
        } 
        else 
        {
            out += s[i];
        }
    }
    return out;
}

// ========== 【L24】把 form-urlencoded 拆成键值对，塞进 map ==========
// 键和值都过一遍 url_decode（浏览器发的中文是 %E5%BC%A0 这种转义形态）
void parse_form(const std::string& body, std::map<std::string, std::string>& out)
{
    size_t start = 0;
    while (start < body.size())
    {
        size_t amp = body.find('&', start);              // & 分隔每对键值
        if (amp == std::string::npos) amp = body.size();
        if (amp > start)
        {
            std::string kv = body.substr(start, amp - start);
            size_t eq = kv.find('=');                    // = 分隔键和值
            std::string k = (eq == std::string::npos) ? kv : kv.substr(0, eq);
            std::string v = (eq == std::string::npos) ? "" : kv.substr(eq + 1);
            out[url_decode(k)] = url_decode(v);
        }
        start = amp + 1;
    }
}

// ========== 路由（【L23】多接一个 body 参数 + POST 路由）==========
void handle_request(int fd, const char* method, const char* path, const std::string& body, bool keep_alive)
{
    char out[8192]; int len;

    if (strcmp(method, "POST") == 0 && strcmp(path, "/echo") == 0)
    {
        // 【L23】拆 form-urlencoded：username=alice&passwd=123
        // 规则：& 分隔每对键值，= 分隔键和值
        std::string page = "<h1>收到 POST，拆出来的键值：</h1>"
                           "<table border='1'><tr><th>键</th><th>值</th></tr>";
        size_t start = 0;
        while (start < body.size()) 
        {
            size_t amp = body.find('&', start);              // 找下一对键值的起点
            if (amp == std::string::npos) amp = body.size();
            if (amp > start) 
            {
                std::string kv = body.substr(start, amp - start);
                size_t eq = kv.find('=');                    // 键和值的分界
                std::string k = (eq == std::string::npos) ? kv : kv.substr(0, eq);
                std::string v = (eq == std::string::npos) ? "" : kv.substr(eq + 1);
                page += "<tr><td>" + url_decode(k) + "</td><td>" + url_decode(v) + "</td></tr>";
            }
            start = amp + 1;
        }
        page += "</table>";
        len = make_response(out, "200 OK", page.c_str(), keep_alive);
        LOG_INFO("【L23】POST /echo 处理完毕，身子 %zu 字节", body.size());
    }
    else if (strcmp(method, "POST") == 0 && strcmp(path, "/register") == 0)
    {
        // 【L24】注册：先查有没有重名，没有就插进 user 表
        std::map<std::string, std::string> form;
        parse_form(body, form);
        std::string user = form.count("username") ? form["username"] : "";
        std::string pass = form.count("passwd") ? form["passwd"] : "";
        if (user.empty() || pass.empty())
            len = make_response(out, "400 Bad Request", "<h1>400 用户名和密码都得填</h1>", keep_alive);
        else
        {
            if (user.size() > 50) user.resize(50);   // 表里就 CHAR(50)，超长先剪
            if (pass.size() > 50) pass.resize(50);

            MYSQL* conn = SqlConnPool::Instance().GetConn();
            // 防注入核心：用户输入不能直接拼 SQL，先把 ' 引号等危险字符转义掉
            char eu[101] = {0}, ep[101] = {0};
            mysql_real_escape_string(conn, eu, user.c_str(), user.size());
            mysql_real_escape_string(conn, ep, pass.c_str(), pass.size());

            char sql[512];
            snprintf(sql, sizeof(sql), "SELECT passwd FROM user WHERE username='%s'", eu);
            std::string msg;
            if (mysql_query(conn, sql) != 0)
                msg = std::string("<h1>500 数据库出错</h1><p>") + mysql_error(conn) + "</p>";
            else
            {
                MYSQL_RES* res = mysql_store_result(conn);
                if (mysql_fetch_row(res))          // 查到行 = 这名字有人用了
                    msg = "<h1>注册失败：用户名 " + user + " 已经有人用了</h1>";
                else
                {
                    snprintf(sql, sizeof(sql),
                             "INSERT INTO user(username, passwd) VALUES('%s', '%s')", eu, ep);
                    if (mysql_query(conn, sql) == 0)
                    {
                        msg = "<h1>注册成功！</h1><p>用户名：" + user + "</p><p><a href='/user'>去登录</a></p>";
                        LOG_INFO("【L24】注册成功：%s", user.c_str());
                    }
                    else
                        msg = std::string("<h1>500 插入出错</h1><p>") + mysql_error(conn) + "</p>";
                }
                mysql_free_result(res);
            }
            SqlConnPool::Instance().FreeConn(conn);
            len = make_response(out, "200 OK", msg.c_str(), keep_alive);
        }
    }
    else if (strcmp(method, "POST") == 0 && strcmp(path, "/login") == 0)
    {
        // 【L24】登录：按用户名查密码，跟表单里密码对
        std::map<std::string, std::string> form;
        parse_form(body, form);
        std::string user = form.count("username") ? form["username"] : "";
        std::string pass = form.count("passwd") ? form["passwd"] : "";
        if (user.empty() || pass.empty())
            len = make_response(out, "400 Bad Request", "<h1>400 用户名和密码都得填</h1>", keep_alive);
        else
        {
            if (user.size() > 50) user.resize(50);

            MYSQL* conn = SqlConnPool::Instance().GetConn();
            char eu[101] = {0};
            mysql_real_escape_string(conn, eu, user.c_str(), user.size());
            char sql[512];
            snprintf(sql, sizeof(sql), "SELECT passwd FROM user WHERE username='%s'", eu);

            std::string msg;
            if (mysql_query(conn, sql) != 0)
                msg = std::string("<h1>500 数据库出错</h1><p>") + mysql_error(conn) + "</p>";
            else
            {
                MYSQL_RES* res = mysql_store_result(conn);
                MYSQL_ROW row = mysql_fetch_row(res);
                if (!row)
                    msg = "<h1>登录失败：查无此人 " + user + "</h1>";
                else if (pass == row[0])   // row[0] 是库里存的密码（教学版明文，生产得加密！）
                {
                    msg = "<h1>登录成功！欢迎回来，" + user + "</h1>";
                    LOG_INFO("【L24】登录成功：%s", user.c_str());
                }
                else
                    msg = "<h1>登录失败：密码不对</h1>";
                mysql_free_result(res);
            }
            SqlConnPool::Instance().FreeConn(conn);
            len = make_response(out, "200 OK", msg.c_str(), keep_alive);
        }
    }
    else if (strcmp(method, "GET") != 0)
        len = make_response(out, "405 Method Not Allowed", "<h1>405 我听得懂 GET 和 POST /echo</h1>", keep_alive);
    else if (strcmp(path, "/") == 0)
        len = make_response(out, "200 OK", "<h1>欢迎光临首页！</h1>", keep_alive);
    else if (strcmp(path, "/hello") == 0)
        len = make_response(out, "200 OK", "<h1>你好，这里是 /hello</h1>", keep_alive);
    else if (strcmp(path, "/time") == 0)
    {
        char tbuf[64], body2[128];
        time_t t = time(nullptr);
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&t));
        snprintf(body2, sizeof(body2), "<h1>服务器时间：%s</h1>", tbuf);
        len = make_response(out, "200 OK", body2, keep_alive);
    }
    else if (strcmp(path, "/sql") == 0)
    {
        // 【L24】真去数据库数一数 user 表里有几个人
        MYSQL* conn = SqlConnPool::Instance().GetConn();
        std::string msg;
        if (mysql_query(conn, "SELECT COUNT(*) FROM user") == 0)
        {
            MYSQL_RES* res = mysql_store_result(conn);   // 把查询结果抱回家
            MYSQL_ROW row = mysql_fetch_row(res);        // row[0] 就是人数（字符串形态）
            msg = "<h1>SQL Pool OK（真连接）</h1><p>user 表里现有 " + std::string(row[0]) + " 个用户</p>";
            mysql_free_result(res);                      // 结果用完必须还，不然内存漏
        }
        else
            msg = std::string("<h1>500 查询失败</h1><p>") + mysql_error(conn) + "</p>";
        SqlConnPool::Instance().FreeConn(conn);
        len = make_response(out, "200 OK", msg.c_str(), keep_alive);
    }
    else if (strcmp(path, "/post") == 0)
    {
        // 【L23】浏览器测试页：填完表单点提交，浏览器会自动 POST 到 /echo
        // 注意 HTML 属性用单引号 ' —— 跟 C 字符串的双引号 " 打架时会报编译错
        std::string form =
            "<h1>POST 测试表单</h1>"
            "<form method='POST' action='/echo'>"
            "用户名：<input name='username'><br>"
            "密码：<input name='passwd' type='password'><br>"
            "<button type='submit'>提交</button>"
            "</form>";
        len = make_response(out, "200 OK", form.c_str(), keep_alive);
    }
    else if (strcmp(path, "/user") == 0)
    {
        // 【L24】注册登录双表单测试页（HTML 属性用单引号 ' ，别用双引号——跟 C 字符串打架）
        std::string page =
            "<h1>用户系统测试页</h1>"
            "<h2>注册</h2>"
            "<form method='POST' action='/register'>"
            "用户名：<input name='username'><br>"
            "密码：<input name='passwd' type='password'><br>"
            "<button type='submit'>注册</button>"
            "</form>"
            "<h2>登录</h2>"
            "<form method='POST' action='/login'>"
            "用户名：<input name='username'><br>"
            "密码：<input name='passwd' type='password'><br>"
            "<button type='submit'>登录</button>"
            "</form>";
        len = make_response(out, "200 OK", page.c_str(), keep_alive);
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
// ========== 厨师干活：先攒够，再切分（【L23】POST 的身子也要攒齐）==========
void do_read(int fd)
{
    char buf[4096];

    // ----- 阶段一：能读多少读多少，全塞进便签（不解析！）-----（原样不动）
    while (true)
    {
        int bytes = read(fd, buf, sizeof(buf));
        if (bytes == 0) { LOG_INFO("客人 %d 走了", fd); close_conn(fd); return; }
        if (bytes < 0)
        {
            if (errno == EINTR) continue;
            if (errno == EAGAIN) break;
            LOG_WARN("客人 %d 出状况（errno=%d），收桌", fd, errno);
            close_conn(fd);
            return;
        }
        g_conns[fd].inbuf.append(buf, bytes);
        if (g_conns[fd].inbuf.size() > MAX_REQ_BUF)
        {
            LOG_WARN("客人 %d 请求超长（%zu 字节），收桌", fd, g_conns[fd].inbuf.size());
            send_error_and_close(fd, "413 Payload Too Large", "<h1>413 你这单子太长了</h1>");
            return;
        }
    }

    // ----- 阶段二：【L23 改这里】头齐 + 身子齐，缺一不可 -----
    while (true)
    {
        std::string& inb = g_conns[fd].inbuf;
        size_t pos = inb.find("\r\n\r\n");           // 找"空行" = 头结束
        if (pos == std::string::npos) break;         // 头都没齐，等下一批

        std::string headers = inb.substr(0, pos);    // 头部全文（不含空行）
        int clen = find_content_length(headers);     // 【L23】身子有多长？

        if (clen < 0)
        {   // 【L23】赖皮客人报负数 —— 不防的话 substr 会拿负数当巨大无符号数，直接崩
            LOG_WARN("客人 %d 的 Content-Length 是负数（%d），收桌", fd, clen);
            send_error_and_close(fd, "400 Bad Request", "<h1>400 别报假数</h1>");
            return;
        }

        if (inb.size() < pos + 4 + (size_t)clen)
        {   // 【L23 核心】头齐了但身子没到齐 → 半包，留在便签上接着等
            LOG_INFO("客人 %d 的身子还差 %zu 字节，接着等", fd, pos + 4 + (size_t)clen - inb.size());
            break;
        }

        std::string req  = inb.substr(0, pos + 4);               // 头（含空行）
        std::string body = inb.substr(pos + 4, (size_t)clen);     // 【L23】身子切出来
        inb.erase(0, pos + 4 + (size_t)clen);                     // 划掉整条：头+空行+身子

        char method[16] = {0}, path[256] = {0};
        int cnt = sscanf(req.c_str(), "%15s %255s", method, path);
        if (cnt != 2)
        {
            LOG_WARN("客人 %d 请求格式看不懂，收桌", fd);
            send_error_and_close(fd, "400 Bad Request", "<h1>400 听不懂你在说什么</h1>");
            return;
        }

        bool keep_alive = (req.find("Connection: close") == std::string::npos);
        LOG_INFO("厨师做菜：客人 %d 点 %s %s（身子 %d 字节）", fd, method, path, clen);
        handle_request(fd, method, path, body, keep_alive);        // 【L23】多传一个 body
        last_active[fd] = time(nullptr);

        if (!keep_alive)
        {
            LOG_INFO("客人 %d 说了 close，收桌", fd);
            close_conn(fd);
            return;
        }
    }

    // ----- 阶段三：挂回（哪怕什么都没切出来也必须挂回！）-----（原样不动）
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(g_epfd, EPOLL_CTL_MOD, fd, &ev);
}

int main() 
{
    SqlConnPool::Instance().Init("localhost", "web", "web123456", "tinywebdb", 8);   // 【L24】8 把真钥匙

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
