#include <server.h>
                                    
int main(){
    std::cout << "Starting Server on port 5555\n";
    
    RedisServer server(5555);
    server.runServer();
    
}