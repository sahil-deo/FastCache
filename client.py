import socket
import time

class RedisClient:
    def __init__(self, host='192.168.1.101', port=5555):
        self.host = host
        self.port = port
        self.socket = None
        self.connect()
    
    def connect(self):
        """Connect to the Redis server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        print(f"Connected to {self.host}:{self.port}")
    
    def disconnect(self):
        """Disconnect from the Redis server"""
        if self.socket:
            self.socket.close()
            print("Disconnected")
    
    def send_command(self, command):
        """Send a command to the Redis server and return the response"""
        self.socket.send(command.encode() + b'\n')
        response = self.socket.recv(4096).decode().strip()
        return response
    
    def run_interactive(self):
        """Run interactive mode where user can input commands"""
        print("Redis Client - Type 'quit' to exit")
        print("Example commands: SET key value, GET key, LPUSH mylist item")
        
        while True:
            try:
                command = input("redis> ").strip()
                
                if command.lower() == 'quit':
                    break
                
                if not command:
                    continue
                
                start = time.time()
                response = self.send_command(command)
                end = time.time()
                print(end-start)
                print("\n")
                print(response)
                
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}")


# Example usage
if __name__ == "__main__":
    client = RedisClient()
    
    try:
        client.run_interactive()
    finally:
        client.disconnect()