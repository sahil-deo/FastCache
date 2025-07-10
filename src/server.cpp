#include <server.h>

                                                
int main(){
    std::cout << "Starting Server on port 5555\n";
    redisServer server(5555);
    server.run_server();
    
}