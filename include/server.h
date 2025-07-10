#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <variant>
#include <deque>
#include <fstream>

class redisServer{
    
    public:
    int m_server_fd;
    int m_epoll_fd;
    
    struct ClientState{
        std::string buffer;
        std::string write_buffer;
        bool command_complete = false;
    };

    std::unordered_map<int, ClientState> m_clients;


    std::unordered_map<std::string, std::string> m_string_cache;
    std::unordered_map<std::string, std::deque<std::string>> m_list_cache;

    redisServer(int port = 5555){
        setup_server(port);
        setup_epoll();
    }


    void run_server(){
        
        struct epoll_event events[1024];
        std::cout << "Server Started...";
        while(true){
            int nfds = epoll_wait(m_epoll_fd, events, 1024, -1);

            if (nfds == -1){
                if(errno == EINTR){
                    continue;
                }
                perror("epoll_wait");
                break;
            }

            std::cout << "Data Received\n";

            for(int i = 0; i < nfds; i++){
                int fd = events[i].data.fd;

                if(fd == m_server_fd){
                    accept_new_clients();
                }else if(events[i].events & EPOLLIN){
                    read_from_client(fd);
                }else if(events[i].events & EPOLLOUT){
                    write_to_client(fd);
                }else if(events[i].events & (EPOLLHUP | EPOLLERR)){
                    cleanup_client(fd);
                }

            }
        }
    }

    private:

    void setup_server(int port){
        
        m_server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if(m_server_fd < 0){
            perror("Server Socket Failure");
            exit(EXIT_FAILURE);
        }

        struct sockaddr_in addr;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);

        if(bind(m_server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0){
            perror("Bind Failure");
            exit(EXIT_FAILURE);
        }

        if(listen(m_server_fd, SOMAXCONN) < 0){
            perror("Listen Failure");
            exit(EXIT_FAILURE);
        }

        make_non_blocking(m_server_fd);


    }

    void setup_epoll(){
        m_epoll_fd = epoll_create1(0);
        if (m_epoll_fd == -1){
            perror("Epoll Failure");
            exit(EXIT_FAILURE);
        }


        add_to_epoll(m_server_fd, EPOLLIN);
    }

    void make_non_blocking(int fd){
        int flags = fcntl(fd, F_GETFL, 0);
        if(flags == -1){
            perror("Failed to get socket flags");
            exit(EXIT_FAILURE);
        }

        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK)  < 0){                                                                    
            perror("Failed to set socket to non blocking");
            exit(EXIT_FAILURE);
        }
    }

    void add_to_epoll(int fd, uint32_t events){
        struct epoll_event ev;
        ev.events = events;
        ev.data.fd = fd;


        if(epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1){
            perror("Epoll Add Failure");
            exit(EXIT_FAILURE);
        }


    }

    void modify_epoll(int fd, uint32_t events){
        struct epoll_event ev;
        ev.events = events;
        ev.data.fd = fd;


        if(epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1){
            perror("Epoll Modify Failure");
            exit(EXIT_FAILURE);
        }


    }

    void accept_new_clients(){
        while(true){
            int client_fd = accept(m_server_fd, nullptr, nullptr);
            if(client_fd == -1){
                if(errno == EAGAIN || errno == EWOULDBLOCK){
                    break;
                }
                perror("accept");
                continue;
            }

            std::cout << "New Client Connected: " << client_fd << std::endl;

            make_non_blocking(client_fd);
            add_to_epoll(client_fd, EPOLLIN | EPOLLET);
            m_clients[client_fd] = ClientState{};
        }
    }

    void read_from_client(int fd){
        char buffer[1024];
        while(true){
            ssize_t bytes = read(fd, buffer, sizeof(buffer));

            if(bytes > 0){
                m_clients[fd].buffer.append(buffer, bytes);
                std::cout << m_clients[fd].buffer;
                process_complete_commands(fd);
            }
            else if(bytes == 0){
                std::cout << "Client " << fd << " Disconnected\n";
                cleanup_client(fd);
                break;
            }
            else if(bytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)){
                break;
            }
            else{
                perror("read");
                cleanup_client(fd);
                break;
            }
        }
    }

    void write_to_client(int fd){
        auto& client = m_clients[fd];

        while(!client.write_buffer.empty()){
            ssize_t bytes = write(fd, client.write_buffer.c_str(), client.write_buffer.size());

            if(bytes > 0){
                client.write_buffer.erase(0, bytes);
            }else if(errno == EAGAIN || errno == EWOULDBLOCK){
                break;
            }else{
                cleanup_client(fd);
                break;
            }
        }
    }

    void process_complete_commands(int fd){
        auto& client = m_clients[fd];
        while(true){
            auto cmd_end = client.buffer.find("\n");
            if (cmd_end == std::string::npos) break;
            
            std::string command = client.buffer.substr(0, cmd_end);
            client.buffer.erase(0, cmd_end + 1);


            std::cout << "Command To be Executed\n";
            std::string response = execute_command(command);
            std::cout << "Command Executed\n";
            send_response(fd, response);            
        }
    }

    std::string execute_command(const std::string& command){
        std::istringstream iss(command);
        std::string cmd;
        iss >> cmd;
        for(auto& c: cmd) c = std::toupper(c);

        if (cmd == "SET"){
            std::string key, value;
            iss >> key >> value;

            if(!key.empty() && !value.empty()){
                m_string_cache[key] = value;
                std::cout << "Value Stored\n";
                return "OK\n";
            }
            
            std::cout << "Wrong Args";
            return "ERR Wrong Number of Arguments\n";
            
        }else if (cmd == "GET"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                auto it = m_string_cache.find(key);
                if (it != m_string_cache.end()){
                    return it->second + "\n";
                }
                return "-1\n";
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "DEL"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                int deleted = m_string_cache.erase(key);
                return std::to_string(deleted) + "\n";
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "KEYS"){
            std::string result;
            for(auto&it: m_string_cache){
                result.append(it.first);
                result.append(" ");
            }
            result.append("\n");
            return result;
        }else if(cmd == "APP"){
            std::string key, value;
            iss >> key >> value;

            if(!key.empty() && !value.empty()){
                m_string_cache[key].append(value);
                return "OK\n";
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LSET"){
            std::string key;
            std::deque<std::string> values;
            std::string value;
            
            iss >> key;
            
            while(iss >> value){
                values.push_back(value);
                value.clear();
            }

            if(!key.empty() && !values.empty()){
                m_list_cache[key] = values;
                return "OK\n";
            }

            return "ERR Wrong Number of Arguments\n";

        }else if (cmd == "LGET"){
            std::string key;
            std::string value;

            iss >> key;
            if (!key.empty()){
                iss >> value;
                if(!value.empty()){

                    int v = std::stoi(value);

                    if (v > m_list_cache[key].size()-1){
                        return "ERR Index Out of Bounds\n";
                    }

                    std::string result = m_list_cache[key].at(v);
                    result.append("\n");
                    return result;
                    
                }

                std::string result;
                for(std::string& v: m_list_cache[key]){
                    result.append(v);
                    result.append(" ");
                }
                result.append("\n");
                std::cout << result << "\n";
                return result;
            }
            return "ERR Wrong Number of Arguments\n";

        }else if (cmd == "LDEL"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                int deleted = m_list_cache.erase(key);
                return std::to_string(deleted) + "\n";
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LPUSH"){
            std::string key;
            iss >> key;

            std::string value;
            if(!key.empty()){
                while(iss >> value){
                    m_list_cache[key].push_back(value);
                    value.clear();
                }
                return "OK\n";
            }

            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LPOP"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                std::string value;
                std::string result = "";
                while(iss >> value){
                    
                    int v = std::stoi(value);
                    if(v > m_list_cache[key].size()-1){
                        return "ERR Index Out of Bounds\n";
                    }

                    result.append(m_list_cache[key].at(v));
                    m_list_cache[key].erase(m_list_cache[key].begin() + v);
                    result.append(" ");

                }
                result.append("\n");
                return result;
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LEMPTY"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                if (m_list_cache[key].empty()){
                    return "TRUE\n";
                }else{
                    return "FALSE\n";
                }
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LKEYS"){
            std::string result;
            for(auto& it: m_list_cache){
                result.append(it.first);
                result.append(" ");
            }
            result.append("\n");
            return result;
        }else if (cmd == "LSIZE"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                int size = m_list_cache[key].size();
                std::string result = std::to_string(size);
                result.append("\n");
                return result;
            }
            
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LAPP"){
            std::string key, index, value;
            iss >> key >> index >> value;
            if(!key.empty() && !index.empty() && !value.empty()){
                m_list_cache[key].at(std::stoi(index)).append(value);
                return "OK\n";
            }
            return "ERR Wrong Number of Arguments\n";
        }else if(cmd == "STORE"){
            std::ofstream file("Redis_Cache", std::ios::ate);
            for(auto&i : m_string_cache){
                file << " ";
                file << i.first;
                file << " ";
                file << i.second;
                file << " ";
                
                file << ":";

            }
            file << ";";
            for(auto&i : m_list_cache){
                file << " ";
                file << i.first;
                file << " ";
                for(std::string& f: i.second){
                    file << " ";
                    file << f;
                    file << " ";
                }

                file << ":";
            }

            file << "; ";

            file.close();
            
            return "OK\n";
        }
        else if(cmd == "LOAD"){
            m_string_cache.clear();
            m_list_cache.clear();

            std::ifstream file("Redis_Cache");
            
            std::string value;

            bool key = true;
            bool string = true;
            bool list = false;
            std::string current_key = "";

            while(file >> value){

                if(current_key == ""){
                    current_key = value;
                    file >> value;
                }
                
                if(value == ":"){
                    file >> value;
                    current_key = value;
                    
                }else if(value == ":;"){
                    if(string) {
                        string = false;
                        list = true;       
                        current_key = "";                 
                    }
                    else if(!string && list){
                        return "OK\n";                            
                    }

                }else if (value == " "){

                }else{
                    if(string){
                        m_string_cache[current_key] = value;
                    }else if(list){
                        m_list_cache[current_key].push_back(value);
                    }
                }
            }
        }
        
        return "ERR Invalid Command\n";
    }
   
    void send_response(int fd, std::string response){
        auto& client = m_clients[fd];
        client.write_buffer += response;
        std::cout << "Sending Response\n";
        if(!client.write_buffer.empty()){
            modify_epoll(fd, EPOLLIN | EPOLLOUT | EPOLLET);
        }
    }
   
    void cleanup_client(int fd){
        std::cout << "Clean Up Called\n";
        if(epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1){
            perror("epoll_ctl DEL");
        }

        close(fd);
        m_clients.erase(fd);
    }
};