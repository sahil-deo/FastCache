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

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/reader.h"
#include "rapidjson/filereadstream.h"

#include "dict.h"
#include "llist.h"
#include "jsonReader.h"


class RedisServer{
    
    public:
    int m_server_fd;
    int m_epoll_fd;
    
    struct ClientState{
        std::string buffer;
        std::string write_buffer;
        bool command_complete = false;
    };

    std::unordered_map<int, ClientState> m_clients;

    // std::unordered_map<std::string, std::string> m_string_cache;
    // std::unordered_map<std::string, std::deque<std::string>> m_list_cache;

    RedisServer(int port = 5555){
        setupServer(port);
        setupEpoll();
    }


    void runServer(){
        
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

            // std::cout << "Data Received\n";

            for(int i = 0; i < nfds; i++){
                int fd = events[i].data.fd;

                if(fd == m_server_fd){
                    acceptNewClients();
                }else if(events[i].events & EPOLLIN){
                    readFromClient(fd);
                }else if(events[i].events & EPOLLOUT){
                    writeToClient(fd);
                }else if(events[i].events & (EPOLLHUP | EPOLLERR)){
                    cleanupClient(fd);
                }

            }
        }
    }

    private:

    void setupServer(int port){
        
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

        makeNonBlocking(m_server_fd);


    }

    void setupEpoll(){
        m_epoll_fd = epoll_create1(0);
        if (m_epoll_fd == -1){
            perror("Epoll Failure");
            exit(EXIT_FAILURE);
        }


        addToEpoll(m_server_fd, EPOLLIN);
    }

    void makeNonBlocking(int fd){
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

    void addToEpoll(int fd, uint32_t events){
        struct epoll_event ev;
        ev.events = events;
        ev.data.fd = fd;


        if(epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1){
            perror("Epoll Add Failure");
            exit(EXIT_FAILURE);
        }


    }

    void modifyEpoll(int fd, uint32_t events){
        struct epoll_event ev;
        ev.events = events;
        ev.data.fd = fd;


        if(epoll_ctl(m_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1){
            perror("Epoll Modify Failure");
            exit(EXIT_FAILURE);
        }


    }

    void acceptNewClients(){
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

            makeNonBlocking(client_fd);
            addToEpoll(client_fd, EPOLLIN | EPOLLET);
            m_clients[client_fd] = ClientState{};
        }
    }

    void readFromClient(int fd){
        char buffer[1024];
        while(true){
            ssize_t bytes = read(fd, buffer, sizeof(buffer));

            if(bytes > 0){
                m_clients[fd].buffer.append(buffer, bytes);
                std::cout << "COMMAND: " << m_clients[fd].buffer;
                processCompleteCommands(fd);
            }
            else if(bytes == 0){
                std::cout << "Client " << fd << " Disconnected\n";
                cleanupClient(fd);
                break;
            }
            else if(bytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)){
                break;
            }
            else{
                perror("read");
                cleanupClient(fd);
                break;
            }
        }
    }

    void writeToClient(int fd){
        auto& client = m_clients[fd];

        while(!client.write_buffer.empty()){
            ssize_t bytes = write(fd, client.write_buffer.c_str(), client.write_buffer.size());

            if(bytes > 0){
                client.write_buffer.erase(0, bytes);
            }else if(errno == EAGAIN || errno == EWOULDBLOCK){
                break;
            }else{
                cleanupClient(fd);
                break;
            }
        }

        
    }

    void processCompleteCommands(int fd){
        auto& client = m_clients[fd];
        while(true){
            auto cmd_end = client.buffer.find("\n");
            if (cmd_end == std::string::npos) break;
            
            std::string command = client.buffer.substr(0, cmd_end);
            client.buffer.erase(0, cmd_end + 1);


            // std::cout << "Command To be Executed\n";
            std::string response = executeCommand(std::move(command));
            // std::cout << "Command Executed\n";
            sendResponse(fd, response);            
        }
    }

    std::string executeCommand(std::string command){
        std::istringstream iss(std::move(command));
        std::string cmd;
        iss >> cmd;
        for(auto& c: cmd) c = std::toupper(c);
        
        if (cmd == "SET"){
            std::string key, value;
            iss >> key >> value;

            if(!key.empty() && !value.empty()){
                //Using unordered_map
                {
                    // m_string_cache[key] = value;
                }
                
                setString(std::move(key), std::move(value));
                // std::cout << "STRING SET\n";
                return "OK\n";
            }
            
            std::cout << "Wrong Args";
            return "ERR Wrong Number of Arguments\n";
            
        }else if (cmd == "GET"){
            std::string key;
            iss >> key;
            if(!key.empty()){

                //Using unordered_map
                {    
                    // auto it = m_string_cache.find(key);
                    // if (it != m_string_cache.end()){
                    //     return it->second + "\n";
                    // }
                    // return "-1\n";
                }
                        
                //using custom stringHash
                const char * val = getString(std::move(key));
                if(val == nullptr){
                    return "-1\n";
                }

                std::string result(val);
                result.append("\n");
                return result;

            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "DEL"){
            std::string key;
            iss >> key;
            if(!key.empty()){
                // int deleted = m_string_cache.erase(key); 
                // return std::to_string(deleted) + "\n";

                bool deleted = delKey(std::move(key));

                if(deleted){
                    return "1\n";
                }
                return "0\n";
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "KEYS"){
            // std::string result;
            // for(auto&it: m_string_cache){
            //     result.append(it.first);
            //     result.append(" ");
            // }
            // result.append("\n");

            std::string result = getKeys();
            result.append("\n");
            return result;
        }else if (cmd == "LSET"){
            std::string key;
            // std::deque<std::string> values;
            std::string value;
            
            iss >> key;
            
            
            if(!key.empty()){

                while(iss >> value){
                    if (value.empty()){break;}
                    pushBackList(key, std::move(value));
                }

                return "OK\n";
            }



            return "ERR Wrong Number of Arguments\n";

        }else if (cmd == "LGET"){



            std::string key;
            std::string value;
            
            
            iss >> key;
            iss >> value;
            
            try{
                
                if(!key.empty() && !value.empty()){
                    //add error check here to know if the value is int, if not call getList instead of getListR
                    return getListR(std::move(key), std::stoi(value));
                }
                
                if(!key.empty()){
                    return getList(std::move(key));
                }
            }catch(std::exception &e){
                std::cout << e.what();
                exit(EXIT_FAILURE); 
            }
            return "ERR Wrong Number of Arguments\n";

        }else if (cmd == "LDEL"){
            std::string key, index;
            iss >> key;
            if(!key.empty()){
                // int deleted = m_list_cache.erase(key);
                // return std::to_string(deleted) + "\n";
                bool result;
                if(iss >> index){
                    result = delListR(std::move(key), std::stoi(std::move(index))); // Randomly delete value at index in key list 
                }else{
                    result = delList(std::move(key)); // Delete entire list
                }

                if(result){
                    return "1\n";
                }else{
                    return "0\n";
                }
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LPUSHBACK"){
            std::string key;
            iss >> key;

            std::string value;
            if(!key.empty()){
                while(iss >> value){
                    pushBackList(key, std::move(value));
                }
                return "OK\n";
            }

            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LPOPBACK"){
            std::string key;
            iss >> key;
            if(!key.empty()){

                std::string result = popBackList(std::move(key));

                result.append("\n");
                return result;
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LPUSHFRONT"){
            std::string key;
            iss >> key;

            std::string value;
            if(!key.empty()){
                while(iss >> value){
                    // m_list_cache[key].push_back(value);
                    // value.clear();

                    pushFrontList(key, std::move(value));
                }
                return "OK\n";
            }

            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LPOPFRONT"){
            std::string key;
            iss >> key;
            if(!key.empty()){


                std::string result = popFrontList(std::move(key));

                result.append("\n");
                return result;
            }
            return "ERR Wrong Number of Arguments\n";
        }else if (cmd == "LKEYS"){
            std::string result;

            result = getListKeys();

            return result;
        }else if (cmd == "STORE"){
            std::string key;
            std::string fileName = "FastCache.json";
            if(iss>>key)
            {
                fileName = key;
                if(fileName == "" || fileName == " ") fileName = "FastCache";
                fileName.append(".json");
            }
            rapidjson::StringBuffer s;
            rapidjson::Writer<rapidjson::StringBuffer> writer(s);
            
            writer.StartObject();
            getSnapDict(writer);
            getSnapList(writer);
            writer.EndObject();

            std::ofstream file(fileName, std::ios::ate);
            // file.clear();
            file << s.GetString();
            file.close();

            return "OK\n";
        }
        else if (cmd == "LOAD"){
            std::string key;
            std::string fileName = "FastCache.json";
            if(iss>>key)
            {
                fileName = key;
                if(fileName == "" || fileName == " ") fileName = "FastCache";
                fileName.append(".json");
            }
            std::cout << "\nLoading: " << fileName << "\n";

            FILE* fp = fopen(fileName.c_str(), "r");

            if(!fp){
                return "ERR Unable To Open Cache File\n";
            }

            char buffer[65536];

            rapidjson::FileReadStream stream (fp, buffer, sizeof(buffer));

            rapidjson::Reader reader;
            
            JsonReader handler;

            if(!reader.Parse(stream, handler)){
                return "ERR Cannot Parse Json\n";
            }else{
                //convert unorderedmap <String, String> to Dict
                delAllStrings();
                for(auto& [key, value] : handler.kvMap)
                {
                    setString(std::move(key), std::move(value));
                }
                
                //convert unorderedmap <String, Vector<String>> to Lists
                
                delAllLists();
                for(auto& [key, value] : handler.kaMap)
                {
                    for(auto& item : value)
                    {
                        // delList(key);4
                        pushBackList(key, std::move(item));
                    }
                }
            }

            fclose(fp);
            return "OK\n";
        }
        else if(cmd == "DELALL"){
            delAllStrings();
            delAllLists();
            return "OK\n";
            
        }
        return "ERR Invalid Command\n";
    }
   
    void sendResponse(int fd, std::string response){
        auto& client = m_clients[fd];
        client.write_buffer += response;
        // std::cout << "Sending Response\n";
        if(!client.write_buffer.empty()){
            modifyEpoll(fd, EPOLLIN | EPOLLOUT | EPOLLET);
        }


        if(client.write_buffer.empty()){
           modifyEpoll(fd, EPOLLIN | EPOLLET); 
        }
    }
   
    void cleanupClient(int fd){
        std::cout << "Clean Up Called\n";
        if(epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1){
            perror("epoll_ctl DEL");
        }

        close(fd);
        m_clients.erase(fd);
    }
};