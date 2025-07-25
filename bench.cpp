#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <chrono>

int OPS = 50000;
int main()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0){
        std::cout << "ERR Socket not created\n";
        return 0;
    }

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(5555);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if(connect (sock, (sockaddr *)&addr, sizeof(addr)) < 0){
        std::cout << "ERR Could Not Connect";
        return 0;
    }

    std::cout << "Connected\n";

    char buffer[1024];

    auto start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < OPS; ++i)
    {
        std::string msg = "SET key";
        msg.append(std::to_string(i));
        msg.append(" VALUE\n");
        std::cout << "SENDING\n";
        if(send(sock, msg.c_str(), msg.size(), 0) < 0){
            std::cout << "SEND FAILED " << i << "\n";
            continue;
        }
        
        recv(sock, buffer, 1023, 0);

    }

    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds> (end-start).count();

    double f = duration;
    f/=1000000;

    std::cout << "Total Time: " << f << " s\n";
    std::cout << "OPS/SEC: " << OPS/f << "\n";

}