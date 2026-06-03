#include <iostream>
#include <coroutine>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>
#include <cerrno>
#include <map>
#include <vector>

class EventLoop{
    private:
        int epfd;
        std::map<int, std::coroutine_handle<>> sleepers;
    public:
    EventLoop(){
        epfd = epoll_create1(0);
    }
    ~EventLoop() {
        close(epfd);
    }
    void add_socket(int fd, std::coroutine_handle<> handle){
        sleepers[fd] = handle;
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
            std::cerr << "epoll_ctl ADD failed for fd " << fd << ": " << strerror(errno) << std::endl;
        }
    }
    void remove_socket(int fd) {
        if (epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
            std::cerr << "epoll_ctl DEL failed for fd " << fd << ": " << strerror(errno) << std::endl;
        }
    }
    void run(){
        struct epoll_event events[64];
        while(true){
            int n = epoll_wait(epfd, events, 64, -1);
            for(int i = 0; i < n; ++i){
                int fd = events[i].data.fd;
                auto it = sleepers.find(fd);

                if(it != sleepers.end()){
                    auto handle = it->second;
                    sleepers.erase(it);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    handle.resume();
                } 
            }
        }
    }
};


struct SocketAwaiter{
    int fd;
    EventLoop& loop;

    SocketAwaiter(int socket_fd, EventLoop& loop_ref) : fd(socket_fd), loop(loop_ref) {}

    bool await_ready(){ 
        return false;
    }
    void await_suspend(std::coroutine_handle<> handle){
        loop.add_socket(fd, handle);
    }
    void await_resume() {}
};

struct Task {
    struct promise_type {
        Task get_return_object() {
            auto handle = std::coroutine_handle<promise_type>::from_promise(*this);
            return Task{handle};
        }
        
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
    
    std::coroutine_handle<promise_type> handle;
    
    Task(std::coroutine_handle<promise_type> h) : handle(h) {
    }
    ~Task() {
        if (handle) handle.destroy(); 
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) : handle(other.handle) {
        other.handle = nullptr;
    }
	
	Task& operator=(Task&& other) noexcept {
		if (this != &other) {
			if (handle) handle.destroy();
			handle = other.handle;
			other.handle = nullptr;
		}
		return *this;
	}
};

void make_nonblocking(int fd){
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int create_server_socket(int port){
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if(server_fd == -1){
        std::cerr << "socket err: " << strerror(errno) << std::endl;
        return -1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if(bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1){
        std::cerr << "bind err: " << strerror(errno) << std::endl;
        close(server_fd);
        return -1;
    }
    //10 - лимит входящих подключений

    //                   \/  
    if(listen(server_fd, 10) == -1){
        std::cerr << "listen err: " << strerror(errno) << std::endl;
        close(server_fd);
        return -1;
    } 
    make_nonblocking(server_fd);
    return server_fd;
}
Task handle_client(int client_fd, EventLoop& loop) {
    char buffer[4096];
    co_await SocketAwaiter{client_fd, loop};
    
    int n = read(client_fd, buffer, sizeof(buffer) - 1);
    if (n == -1) {
        std::cerr << "read error: " << strerror(errno) << std::endl;
        loop.remove_socket(client_fd);
        close(client_fd);
        co_return;
    }
    if (n == 0) {
        std::cout << "client disconnected" << std::endl;
        loop.remove_socket(client_fd);
        close(client_fd);
        co_return;
    }
    
    buffer[n] = '\0';
    std::cout << "received " << n << " bytes" << std::endl;
    
    const char* response = 
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 13\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        "Hello, World!";
    
    int written = write(client_fd, response, strlen(response));
    if (written == -1) {
        std::cerr << "write error: " << strerror(errno) << std::endl;
    }
    
    close(client_fd);
}
Task acceptor(int server_fd, EventLoop& loop){
    std::vector<Task> active_tasks;

    while(true) {
        co_await SocketAwaiter{server_fd, loop};
        
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        
        if (client_fd == -1) {
            std::cerr << "accept error: " << strerror(errno) << std::endl;
            continue;
        }

        make_nonblocking(client_fd);
        active_tasks.push_back(std::move(handle_client(client_fd, loop)));

        for (auto it = active_tasks.begin(); it != active_tasks.end(); ) {
            if (it->handle.done()) {
                it = active_tasks.erase(it);
            } else {
                ++it;
            }
        }
    }
}
int main() {
    EventLoop loop;
    int server_fd = create_server_socket(8080);
    if (server_fd == -1) {
        return 1;
    }
    Task accepter_task = acceptor(server_fd, loop);
    loop.run();
    return 0;
}
