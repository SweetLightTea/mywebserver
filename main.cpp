#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

const int PORT = 9007;  // 和原版的 9006 区分开，避免冲突

// 固定的 HTTP 响应。以后会学会自己解析请求、动态生成响应
const char* HTML =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: 33\r\n"
    "\r\n"
    "<h1>Hello, my first server!</h1>";

int main() {
    // 1. 创建"听筒"：socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cerr << "socket 创建失败" << std::endl;
        return 1;
    }

    // 2. 绑定地址和端口：告诉系统"我在 9007 号门牌等客人"
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);  // 允许任何网卡接入
    addr.sin_port = htons(PORT);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind 失败（端口可能被占用）" << std::endl;
        return 1;
    }

    // 3. 开始"接听"：listen，最多 5 个客人排队
    if (listen(listen_fd, 5) < 0) {
        std::cerr << "listen 失败" << std::endl;
        return 1;
    }
    std::cout << "我的服务器已启动！浏览器访问 http://localhost:" << PORT << std::endl;

    // 4. 无限循环：接一个客人 -> 服务完 -> 再接下一个
    while (true) {
        int conn_fd = accept(listen_fd, nullptr, nullptr);  // 等一个客人上门
        if (conn_fd < 0) {
            std::cerr << "accept 失败" << std::endl;
            continue;
        }
        char buf[1024] = {0};
        read(conn_fd, buf, sizeof(buf));   // 收下请求（暂时不看内容）
        write(conn_fd, HTML, strlen(HTML));  // 回一个固定页面
        close(conn_fd);                    // 送客
    }
    return 0;
}
