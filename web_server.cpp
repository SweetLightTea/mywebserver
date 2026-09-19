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
#include <sys/mman.h>          // 【L27】mmap/munmap：把文件映射进内存，大文件不占进程堆
#include <sys/stat.h>          // 【L27】stat：查文件大小、判断是不是目录
#include <sys/uio.h>           // 【L27】writev + struct iovec：一把发多块不相邻的内存
#include <strings.h>           // 【L27】strncasecmp：忽略大小写比较 HTTP 头
#include <mutex>
#include <string>              // 【L21】用 std::string 当请求缓冲
#include <map>                 // 【L24】装表单拆出来的键值对
#include "log.h"               // 【L19】日志宏
#include "threadpool.h"        // 【L17】线程池
#include "sql_connection_pool.h"
#include "lock/locker.h"           // 【L29】三件套教学资产
#include "config.h"         // 【L29】命令行 8 参数配置
#include "web_server.h"     // 【L30】自己的门面

const int TICK_SEC    = 3;
const int TIMEOUT_SEC = 6;
const int MAX_REQ_BUF = 8192;   // 【L21】单连接请求缓冲上限，防赖皮客人撑爆内存
const char* DOC_ROOT = "./root";   // 【L27】静态资源根目录（发文件的起点）

volatile sig_atomic_t g_tick = 0;
void on_alarm(int) { g_tick = 1; alarm(TICK_SEC); }

int PORT = 9007;
const int MAX_EVENTS = 1024;
const int MAX_FD = 65536;   // 【加固】桌子数上限：1024 → 65536，冲 10500 并发
volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

time_t last_active[MAX_FD] = {0};
ThreadPool* g_pool = nullptr;
int g_epfd = -1;
int g_actor = 1;    // 【L25】模式开关：0 = Reactor（厨师全包），1 = Proactor（主线程管读，厨师纯做菜）// 先写死变量，L29 学 config 时换成命令行参数 -a
int g_et_mode = 1;   // 【L26】LT/ET 四组合开关
                     //   bit0 = listenfd 用 ET？（0=LT  1=ET）
                     //   bit1 = connfd   用 ET？（0=LT  1=ET）
                     //   m=0 → 全LT m=1 → LT+ET（推荐，原版默认）
                     //   m=2 → ET+LT   m=3 → 全ET（你之前的写法）
                     // 先写死变量，L29 学 config 时换成命令行 -m

int g_log_async = 1;   // 【L28】日志模式开关：0 = 同步直写，1 = 异步队列
                       // 先写死变量，L29 学 config 时换成命令行参数 -l

int g_close_log = 0;   // 【L29】-c 开关

// ========== 【L26】connfd 的事件模板（按 g_et_mode 拼出 4 种之一）==========
uint32_t conn_events()
{
    uint32_t e = EPOLLIN | EPOLLONESHOT;
    if (g_et_mode & 2) e |= EPOLLET;   // bit1 决定 connfd 是 LT 还是 ET
    return e;
}

// ========== 【L27】解析状态机的"三态"和"三关" ==========
// 从状态机：切一行时，这一行现在是 完整 / 坏了 / 还没收完？
enum LineStatus { LINE_OK, LINE_BAD, LINE_OPEN };
// 主状态机：我现在在解 HTTP 的哪一段？
enum CheckState { CHECK_REQUESTLINE, CHECK_HEADER, CHECK_CONTENT };

// 返回值：数据不够继续等 / 完整请求到手 / 格式非法拒收
const int NO_REQUEST  = 0;
const int GET_REQUEST = 1;
const int BAD_REQUEST = 2;

// ========== 【L21】每张桌子一张"点单便签"（【L27】升级成"连接大脑"）==========
struct ConnCtx
{
    std::string inbuf;                  // 收进来的原始字节（可累加）
    size_t checked_idx = 0;             // 从状态机扫到哪了（下次接着扫，不回头）
    size_t start_line  = 0;             // 当前这行的起点
    size_t body_start  = 0;             // 身子的起点（头解析完才知道）
    int    check_state = CHECK_REQUESTLINE;   // 主状态机当前在哪一关
    int    content_len = 0;             // 头里读到的 Content-Length
    std::string method, url, body;      // 解析成果：方法 / 路径 / 身子
    bool   keep_alive  = true;          // 读到 Connection: close 就翻成 false
};
ConnCtx g_conns[MAX_FD];

// ========== 【L27】把一张桌子恢复成"刚坐下"（新客人进门 / 一条请求消费完都要调）==========
// 注意：这里不动 inbuf！粘包时 inbuf 里可能还躺着下一条请求，清了就丢数据
void reset_conn(ConnCtx& c)
{
    c.checked_idx = 0;
    c.start_line  = 0;
    c.body_start  = 0;
    c.check_state = CHECK_REQUESTLINE;
    c.content_len = 0;
    c.method.clear();
    c.url.clear();
    c.body.clear();
    c.keep_alive  = true;
}

// ========== 响应工厂（【L25】升级：返回 std::string，不再受栈缓冲 8K 上限）==========
std::string make_response_str(const char* status, const char* body, bool keep_alive)
{
    char head[256];
    int hl = snprintf(head, sizeof(head),
        "HTTP/1.1 %s\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"
        "\r\n",
        status, strlen(body),
        keep_alive ? "keep-alive" : "close");
    return std::string(head, hl) + body;    // 头（精确长度）拼接身子
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

// ======================================================================
// 【L27】主从状态机解 HTTP —— 原版 TinyWebServer 的核心解析器
//   从状态机 parse_line：只管"帮我切出一行完整的话"（逐字节扫 \r\n）
//   主状态机 process_read：按 请求行 → 头 → 身子 三关往前走
//   好处：① 数据分几次到都行（半包天然支持）② 一次收到多条能连着解（粘包）
// ======================================================================

// ---------- 从状态机：切出一行 ----------
// 返回 LINE_OK 时，[line_start, line_end) 就是这一行的内容
int parse_line(ConnCtx& c, size_t& line_start, size_t& line_end)
{
    for (size_t i = c.checked_idx; i < c.inbuf.size(); i++)
    {
        char ch = c.inbuf[i];
        if (ch == '\r')
        {
            if (i + 1 == c.inbuf.size())
                return LINE_OPEN;               // \r 是最后一个字节，\n 还没到，等下一趟
            if (c.inbuf[i + 1] == '\n')
            {
                line_start = c.start_line;      // 这行从 start_line 开始
                line_end   = i;                 // 到 \r 结束（\r\n 本身不算内容）
                c.checked_idx = i + 2;          // 下次从 \n 的后面接着扫
                c.start_line  = i + 2;
                return LINE_OK;
            }
            return LINE_BAD;                    // \r 后面不是 \n = 这不是合法 HTTP 行
        }
        else if (ch == '\n')
            return LINE_BAD;                    // 光有个 \n（前面没 \r）= 非法
    }
    return LINE_OPEN;                           // 扫到底也没见 \r\n，这行还没收完
}

// ---------- 主状态机第一关：请求行 "GET /hello HTTP/1.1" ----------
int parse_request_line(ConnCtx& c, const std::string& text)
{
    size_t sp1 = text.find(' ');                // 第一个空格：方法 | URL
    if (sp1 == std::string::npos) return BAD_REQUEST;
    size_t sp2 = text.find(' ', sp1 + 1);       // 第二个空格：URL | 版本
    if (sp2 == std::string::npos) return BAD_REQUEST;

    c.method = text.substr(0, sp1);
    c.url    = text.substr(sp1 + 1, sp2 - sp1 - 1);
    std::string version = text.substr(sp2 + 1);

    if (c.method != "GET" && c.method != "POST") return BAD_REQUEST;
    if (version != "HTTP/1.1" && version != "HTTP/1.0") return BAD_REQUEST;
    if (version == "HTTP/1.0") c.keep_alive = false;   // 【L30】1.0 默认说完就散，别挂着等下一条
    if (c.url.empty() || c.url[0] != '/') return BAD_REQUEST;

    c.check_state = CHECK_HEADER;               // 过关！进第二关
    return NO_REQUEST;
}

// ---------- 主状态机第二关：一行一个头 ----------
int parse_headers(ConnCtx& c, const std::string& text)
{
    if (text.empty())                           // 空行 = 头到头了
    {
        c.body_start = c.start_line;            // 身子从下一行开始（GET 时它=整条请求长度）
        if (c.content_len > 0)
        {
            c.check_state = CHECK_CONTENT;      // 过关！进第三关等身子
            return NO_REQUEST;
        }
        return GET_REQUEST;                     // 没身子 = 菜齐了，开做
    }
    if (strncasecmp(text.c_str(), "Content-Length:", 15) == 0)
    {
        c.content_len = atoi(text.c_str() + 15);
        if (c.content_len < 0) return BAD_REQUEST;   // 【L23 防赖皮】负数 = 假报告，拒收
    }
    else if (strncasecmp(text.c_str(), "Connection:", 11) == 0)
    {
        const char* v = text.c_str() + 11;
        while (*v == ' ' || *v == '\t') v++;    // 跳过 ":  close" 里的空格
        if (strncasecmp(v, "close", 5) == 0) c.keep_alive = false;
        else if (strncasecmp(v, "keep-alive", 10) == 0) c.keep_alive = true;
    }
    return NO_REQUEST;
}

// ---------- 主状态机第三关：身子（按 Content-Length 数字节，不看 \r\n！）----------
int parse_content(ConnCtx& c)
{
    if (c.inbuf.size() >= c.body_start + (size_t)c.content_len)
    {
        c.body = c.inbuf.substr(c.body_start, (size_t)c.content_len);
        return GET_REQUEST;
    }
    return NO_REQUEST;                          // 身子还差几字节，等着
}

// ---------- 主状态机：把三关串成一个循环 ----------
int process_read(ConnCtx& c)
{
    int line_status = LINE_OK;
    int ret = NO_REQUEST;
    size_t ls = 0, le = 0;

    while ((c.check_state == CHECK_CONTENT && line_status == LINE_OK)
           || (line_status = parse_line(c, ls, le)) == LINE_OK)
    {
        if (c.check_state == CHECK_REQUESTLINE)
        {
            ret = parse_request_line(c, c.inbuf.substr(ls, le - ls));
            if (ret == BAD_REQUEST) return BAD_REQUEST;
        }
        else if (c.check_state == CHECK_HEADER)
        {
            ret = parse_headers(c, c.inbuf.substr(ls, le - ls));
            if (ret == BAD_REQUEST) return BAD_REQUEST;
            if (ret == GET_REQUEST) return GET_REQUEST;
        }
        else if (c.check_state == CHECK_CONTENT)
        {
            ret = parse_content(c);
            if (ret == GET_REQUEST) return GET_REQUEST;
            line_status = LINE_OPEN;    // 身子不看行，这一趟到此为止
        }
    }
    return NO_REQUEST;   // 数据还不够，便签收好等下一趟（半包就是这么消化的）
}

// ========== 路由（【L25】只做菜不上菜：返回响应字符串，write 移出去）==========
std::string handle_request(const char* method, const char* path, const std::string& body, bool keep_alive)
{
    if (strcmp(method, "POST") == 0 && strcmp(path, "/echo") == 0)
    {
        std::string page = "<h1>收到 POST，拆出来的键值：</h1>"
                           "<table border='1'><tr><th>键</th><th>值</th></tr>";
        size_t start = 0;
        while (start < body.size())
        {
            size_t amp = body.find('&', start);
            if (amp == std::string::npos) amp = body.size();
            if (amp > start)
            {
                std::string kv = body.substr(start, amp - start);
                size_t eq = kv.find('=');
                std::string k = (eq == std::string::npos) ? kv : kv.substr(0, eq);
                std::string v = (eq == std::string::npos) ? "" : kv.substr(eq + 1);
                page += "<tr><td>" + url_decode(k) + "</td><td>" + url_decode(v) + "</td></tr>";
            }
            start = amp + 1;
        }
        page += "</table>";
        LOG_INFO("【L23】POST /echo 处理完毕，身子 %zu 字节", body.size());
        return make_response_str("200 OK", page.c_str(), keep_alive);
    }
    else if (strcmp(method, "POST") == 0 && strcmp(path, "/register") == 0)
    {
        std::map<std::string, std::string> form;
        parse_form(body, form);
        std::string user = form.count("username") ? form["username"] : "";
        std::string pass = form.count("passwd") ? form["passwd"] : "";
        if (user.empty() || pass.empty())
            return make_response_str("400 Bad Request", "<h1>400 用户名和密码都得填</h1>", keep_alive);
        if (user.size() > 50) user.resize(50);
        if (pass.size() > 50) pass.resize(50);

        MYSQL* conn = SqlConnPool::Instance().GetConn();
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
            if (mysql_fetch_row(res))
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
        return make_response_str("200 OK", msg.c_str(), keep_alive);
    }
    else if (strcmp(method, "POST") == 0 && strcmp(path, "/login") == 0)
    {
        std::map<std::string, std::string> form;
        parse_form(body, form);
        std::string user = form.count("username") ? form["username"] : "";
        std::string pass = form.count("passwd") ? form["passwd"] : "";
        if (user.empty() || pass.empty())
            return make_response_str("400 Bad Request", "<h1>400 用户名和密码都得填</h1>", keep_alive);
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
            else if (pass == row[0])
            {
                msg = "<h1>登录成功！欢迎回来，" + user + "</h1>";
                LOG_INFO("【L24】登录成功：%s", user.c_str());
            }
            else
                msg = "<h1>登录失败：密码不对</h1>";
            mysql_free_result(res);
        }
        SqlConnPool::Instance().FreeConn(conn);
        return make_response_str("200 OK", msg.c_str(), keep_alive);
    }
    else if (strcmp(method, "GET") != 0)
        return make_response_str("405 Method Not Allowed", "<h1>405 我听得懂 GET 和 POST /echo</h1>", keep_alive);
    else if (strcmp(path, "/") == 0)
        return make_response_str("200 OK", "<h1>欢迎光临首页！</h1>", keep_alive);
    else if (strcmp(path, "/hello") == 0)
        return make_response_str("200 OK", "<h1>你好，这里是 /hello</h1>", keep_alive);
    else if (strcmp(path, "/time") == 0)
    {
        char tbuf[64], body2[128];
        time_t t = time(nullptr);
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&t));
        snprintf(body2, sizeof(body2), "<h1>服务器时间：%s</h1>", tbuf);
        return make_response_str("200 OK", body2, keep_alive);
    }
    else if (strcmp(path, "/sql") == 0)
    {
        MYSQL* conn = SqlConnPool::Instance().GetConn();
        std::string msg;
        if (mysql_query(conn, "SELECT COUNT(*) FROM user") == 0)
        {
            MYSQL_RES* res = mysql_store_result(conn);
            MYSQL_ROW row = mysql_fetch_row(res);
            msg = "<h1>SQL Pool OK（真连接）</h1><p>user 表里现有 " + std::string(row[0]) + " 个用户</p>";
            mysql_free_result(res);
        }
        else
            msg = std::string("<h1>500 查询失败</h1><p>") + mysql_error(conn) + "</p>";
        SqlConnPool::Instance().FreeConn(conn);
        return make_response_str("200 OK", msg.c_str(), keep_alive);
    }
    else if (strcmp(path, "/post") == 0)
    {
        std::string form =
            "<h1>POST 测试表单</h1>"
            "<form method='POST' action='/echo'>"
            "用户名：<input name='username'><br>"
            "密码：<input name='passwd' type='password'><br>"
            "<button type='submit'>提交</button>"
            "</form>";
        return make_response_str("200 OK", form.c_str(), keep_alive);
    }
    else if (strcmp(path, "/user") == 0)
    {
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
        return make_response_str("200 OK", page.c_str(), keep_alive);
    }
    else
        return make_response_str("404 Not Found", "<h1>404：菜单上没有这道菜</h1>", keep_alive);
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
    reset_conn(g_conns[fd]);     // 【L27】状态机也归零，别带着上一位客人的解析进度
}

// ========== 【L27】循环写：把一段内存完整发出去（对付"部分写"）==========
// 为什么需要：write 语义是"尽力发"，socket 缓冲满了就只发一部分并返回已发字节数；
//             大响应一次 write 发不完，剩下的必须接着发，否则客户端收到的是半截 HTML。
bool send_all(int fd, const char* data, size_t len)
{
    size_t sent = 0;
    while (sent < len)
    {
        ssize_t n = write(fd, data + sent, len - sent);
        if (n > 0) { sent += n; continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            // 教学版：缓冲满了眯 1 毫秒再试（简单有效）
            // 原版是把"没发完的偏移"记下来、改成监听 EPOLLOUT，等内核说"能写了"再发
            usleep(1000);
            continue;
        }
        LOG_WARN("fd %d 发送失败 errno=%d", fd, errno);
        return false;
    }
    return true;
}

// ========== 【L21】发个错误响应再收桌（【L25】摆脱 1024 栈缓冲，【L27】改用循环写）==========
void send_error_and_close(int fd, const char* status, const char* body)
{
    std::string resp = make_response_str(status, body, false);
    send_all(fd, resp.data(), resp.size());
    close_conn(fd);
}

// ========== 【L27】发静态文件：mmap 映射 + writev 一次发"头 + 文件体" ==========
// 为什么用 mmap：39M 视频映射成一段内存，内核按需从磁盘投喂页，进程堆里不会出现 39M 的分配；
//               用 read + malloc 的老办法，得先申请 39M 再发。
// 为什么用 writev：响应头在用户态缓冲、文件体在内核页缓存，两块内存不相邻，
//                  writev 一次系统调用全发出去，不用 memcpy 拼成一大块（原版同款）。
bool serve_static(int fd, const char* url, bool keep_alive)
{
    std::string u = url;
    if (u.rfind("/static/", 0) != 0) return false;        // 只接管 /static/ 开头的
    if (u.find("..") != std::string::npos) return false;  // 防目录穿越：/static/../../etc/passwd
    std::string path = std::string(DOC_ROOT) + u.substr(7);  // "/static/a.bin" -> "./root/a.bin"

    struct stat st{};
    if (stat(path.c_str(), &st) < 0) return false;        // 文件不存在 -> 交回路由去 404
    if (S_ISDIR(st.st_mode)) return false;
    int ffd = open(path.c_str(), O_RDONLY);
    if (ffd < 0) return false;

    char* addr = (char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, ffd, 0);
    close(ffd);                     // 【原版同款】映射建好后 fd 就能关，映射还在
    if (addr == MAP_FAILED) { LOG_ERROR("mmap 失败"); return false; }

    char head[256];
    int hl = snprintf(head, sizeof(head),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/octet-stream\r\n"
        "Content-Length: %ld\r\n"
        "Connection: %s\r\n\r\n",
        (long)st.st_size, keep_alive ? "keep-alive" : "close");

    struct iovec iv[2];
    iv[0].iov_base = head;   iv[0].iov_len = hl;           // 第一块：响应头
    iv[1].iov_base = addr;   iv[1].iov_len = st.st_size;   // 第二块：文件体（映射来的）

    size_t total = hl + st.st_size, sent = 0;
    while (sent < total)
    {
        ssize_t n = writev(fd, iv, 2);                     // 一把发两块
        if (n < 0)
        {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
            break;
        }
        sent += n;
        // 【原版同款"剪枝"】把已经发出去的字节从 iovec 上剪掉，剩下的下次接着发
        if (sent >= iv[0].iov_len)
        {
            size_t body_sent = sent - iv[0].iov_len;
            iv[0].iov_len  = 0;
            iv[1].iov_base = addr + body_sent;
            iv[1].iov_len  = st.st_size - body_sent;
        }
        else
        {
            iv[0].iov_base = head + sent;
            iv[0].iov_len  = hl - sent;
        }
    }
    munmap(addr, st.st_size);       // 发完必须解映射，不然映射区越积越多
    LOG_INFO("【L27】静态文件 %s（%ld 字节）mmap+writev 发完", path.c_str(), (long)st.st_size);
    return true;
}

// ========== 【L25】阶段一单独抽出来：纯搬运（读 socket -> 便签）==========
// 谁调用它，谁就是"做 IO 读的人"——Reactor 里是厨师，Proactor 里是主线程
// 返回 false = 桌子已经收了（close_conn 已发生），调用方别再碰这个 fd
bool fetch_request(int fd)
{
    char buf[4096];
    while (true)
    {
        int bytes = read(fd, buf, sizeof(buf));
        if (bytes == 0) { LOG_INFO("客人 %d 走了", fd); close_conn(fd); return false; }
        if (bytes < 0)
        {
            if (errno == EINTR) continue;
            if (errno == EAGAIN) break;      // 读干净了
            LOG_WARN("客人 %d 出状况（errno=%d），收桌", fd, errno);
            close_conn(fd);
            return false;
        }
        g_conns[fd].inbuf.append(buf, bytes);
        if (g_conns[fd].inbuf.size() > MAX_REQ_BUF)
        {
            LOG_WARN("客人 %d 请求超长（%zu 字节），收桌", fd, g_conns[fd].inbuf.size());
            send_error_and_close(fd, "413 Payload Too Large", "<h1>413 你这单子太长了</h1>");
            return false;
        }
    }
    if (!g_conns[fd].inbuf.empty())
        LOG_INFO("【L25】%s 替客人 %d 把 %zu 字节读进便签了",
                 g_actor ? "主线程(Proactor)" : "厨师(Reactor)", fd, g_conns[fd].inbuf.size());
    return true;    // 读完了，数据在便签上
}

// ========== 【L25】阶段二 + 上菜：纯做菜（【L27】状态机切请求 -> 路由/静态文件 -> 发送）==========
void do_logic(int fd)
{
    ConnCtx& c = g_conns[fd];

    while (true)
    {
        int ret = process_read(c);

        if (ret == NO_REQUEST)                   // 数据不够
        {
            if (!c.inbuf.empty())
                LOG_INFO("客人 %d 的数据还没到齐（便签上 %zu 字节），等下一趟", fd, c.inbuf.size());
            break;
        }
        if (ret == BAD_REQUEST)
        {
            LOG_WARN("客人 %d 的请求格式不合法，收桌", fd);
            send_error_and_close(fd, "400 Bad Request", "<h1>400 听不懂你在说什么</h1>");
            return;
        }

        // ret == GET_REQUEST：菜齐了
        size_t total = c.body_start + (size_t)c.content_len;   // 这条请求一共占多少字节
        LOG_INFO("厨师做菜：客人 %d 点 %s %s（身子 %d 字节，这条共 %zu 字节）",
                 fd, c.method.c_str(), c.url.c_str(), c.content_len, total);

        // 【L27】先看是不是要静态文件（/static/xxx）；不是再走路由
        if (!serve_static(fd, c.url.c_str(), c.keep_alive))
        {
            std::string resp = handle_request(c.method.c_str(), c.url.c_str(), c.body, c.keep_alive);
            if (!send_all(fd, resp.data(), resp.size()))
            {
                LOG_WARN("客人 %d 发送失败，收桌", fd);
                close_conn(fd);
                return;
            }
        }

        last_active[fd] = time(nullptr);

        bool ka = c.keep_alive;      // 【坑】先存下来！下面 reset_conn 会把它刷回 true
        c.inbuf.erase(0, total);     // 消费掉这条请求占的字节
        reset_conn(c);               // 状态机归零，准备吃下一条（粘在一起的那条）

        if (!ka)
        {
            LOG_INFO("客人 %d 说了 close，收桌", fd);
            close_conn(fd);
            return;
        }
    }

    // ----- 挂回（哪怕什么都没切出来也必须挂回！）-----
    epoll_event ev{};
    ev.events = conn_events();   // 【L26】同 accept 时的模板
    ev.data.fd = fd;
    epoll_ctl(g_epfd, EPOLL_CTL_MOD, fd, &ev);
}

// ========== 【L25】Reactor 模式派单入口：厨师全包（读 + 做菜）==========
// 行为跟原来的 do_read 一模一样，等于没变
void do_reactor(int fd)
{
    if (!fetch_request(fd)) return;
    do_logic(fd);
}

void WebServer::init(Config& c)
{
    m_conf = c;    // 【L30】参数先存进成员，后面 run() 全用 m_conf

    LOG_INFO("[Config] 端口=%d 日志=%s 触发=%d linger=%d sql池=%d 线程=%d 关INFO=%d actor=%d",
            m_conf.PORT, m_conf.LOGWrite?"异步":"同步", m_conf.TRIGMode, m_conf.OPT_LINGER,
            m_conf.sql_num, m_conf.thread_num, m_conf.close_log, m_conf.actor_model);

    // 把参数灌进全局开关（这些全局变量现在只活在本文件里）
    PORT        = m_conf.PORT;
    g_log_async = m_conf.LOGWrite;
    g_et_mode   = m_conf.TRIGMode;
    g_actor     = m_conf.actor_model;
    g_close_log = m_conf.close_log;
}

void WebServer::run()
{
    // (1) Log init（替换 L667）
    Log::get_instance()->init("ServerLog", 5000000, m_conf.LOGWrite ? 10000 : 0);

    // (2) SqlConnPool Init（替换 L669）—— 末参 8 → m_conf.sql_num
    SqlConnPool::Instance().Init("localhost", "web", "web123456", "tinywebdb", m_conf.sql_num);
    
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
    // (3) listenfd 的 SO_LINGER（替换 L690 那段 setsockopt）—— 用 if (m_conf.OPT_LINGER) 包
    if (m_conf.OPT_LINGER) 
    {
        struct linger lg = {1, 0};
        setsockopt(listen_fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);
    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(listen_fd, SOMAXCONN) < 0) { perror("listen"); exit(1); }
    LOG_INFO("我的 epoll+线程池 服务器已启动！ 端口 %d", PORT);

    // (5) threadpool_create（替换 L700）—— 3 → m_conf.thread_num
    g_pool = threadpool_create(m_conf.thread_num);
    g_epfd = epoll_create1(0);

    epoll_event ev{};
    ev.events = EPOLLIN | ((g_et_mode & 1) ? EPOLLET : 0);   // 【L26】bit0 决定 listenfd 是 LT 还是 ET
    ev.data.fd = listen_fd;
    epoll_ctl(g_epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    epoll_event events[MAX_EVENTS];

    while (!g_stop) 
    {
        if (g_tick) 
        {
            g_tick = 0;
            time_t now = time(nullptr);
            for (int fd = 0; fd < MAX_FD; fd++) 
            {
                if (last_active[fd] != 0 && now - last_active[fd] >= TIMEOUT_SEC) 
                {
                    LOG_INFO("客人 %d 超时未点单，收桌", fd);
                    close_conn(fd);              // 【L21】改用它，便签一起清
                }
            }
        }

        int n = epoll_wait(g_epfd, events, MAX_EVENTS, -1);
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
                    if (fd >= MAX_FD) { close(fd); continue; }
                    LOG_INFO("新客人来了！桌号 %d", fd);
                    
                    set_nonblocking(fd);

                    // (4) connfd 的 SO_LINGER（替换 L755 那段）—— 同样 if (m_conf.OPT_LINGER) 包
                    if (m_conf.OPT_LINGER) 
                    {
                        struct linger lg = {1, 1};
                        setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
                    }

                    g_conns[fd].inbuf.clear();   // 【L21 关键】桌号复用！新客人进门先清便签
                    reset_conn(g_conns[fd]);     // 【L27】状态机归零

                    ev.events = conn_events();   // 【L26】按 4 组合之一挂监听
                    ev.data.fd = fd;
                    epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev);
                    last_active[fd] = time(nullptr);
                }
            } 
            else
            {
                int fd = events[i].data.fd;
                if (last_active[fd] == 0) continue;   // 保险丝：过期事件跳过
                if (g_actor == 0)
                {
                    // Reactor：桌号扔给厨师，读和做菜全他包
                    Task t;
                    t.func = [fd] { do_reactor(fd); };
                    threadpool_add(g_pool, t);
                }
                else
                {
                    // 【L25】Proactor：主线程自己把数据读干净，只把"做菜"派给厨师
                    if (!fetch_request(fd)) continue;   // 桌子在主线程手里就收了，不用派
                    Task t;
                    t.func = [fd] { do_logic(fd); };
                    threadpool_add(g_pool, t);
                }
            }

        }
    }

    // ===== 【L26】优雅关停：让正在服务的客人收到响应，再收摊 =====
    LOG_INFO("【L26】收到退出信号，开始优雅关停...");
    close(listen_fd);    // 1. 关门——不再接新客人（内核给后续 connect 发 RST）
    for (int fd = 0; fd < MAX_FD; fd++)    // 2. 给每张还开着的桌子发"半关闭"+close
    {
        if (last_active[fd] != 0)
        {
            shutdown(fd, SHUT_WR);    // 先告诉客人"我这边没数据要发了"，让 TA 能正常断开
            close_conn(fd);           // SO_LINGER(1) 让 close 等 1 秒把缓冲发完
        }
    }
    threadpool_destroy(g_pool);       // 3. 等所有厨师把手头的菜做完
    SqlConnPool::Instance().Close();   // 4. 销毁所有真钥匙
    close(g_epfd);                     // 5. 关掉 epoll
    LOG_INFO("【L26】服务器已优雅关闭！");
    Log::get_instance()->flush();   // 【L28】传送带排空 + 缓冲冲刷，一条日志都不丢
    return;
}