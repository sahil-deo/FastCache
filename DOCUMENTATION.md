# FastCache Server Documentation

A high-performance, custom caching server implementation written in C++ using event-driven architecture with Linux epoll for handling concurrent client connections.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command Reference](#command-reference)
- [Data Persistence](#data-persistence)
- [Error Handling](#error-handling)
- [Performance Considerations](#performance-considerations)
- [API Reference](#api-reference)

## Overview

FastCache Server is a lightweight, single-threaded server that implements a custom caching protocol. It uses non-blocking I/O with epoll for efficient handling of multiple concurrent client connections.

### Important: Protocol
Commands must be sent as plain text terminated with a newline character (`\n`).

### Key Features
- **Event-driven architecture** using Linux epoll
- **Non-blocking sockets** with edge-triggered notifications
- **Custom data structures** for optimal memory usage
- **Persistent storage** with JSON serialization
- **Two data types**: Strings and Lists
- **Concurrent client support** without threading overhead

### Supported Data Types
- **Strings**: Key-value pairs for simple data storage
- **Lists**: Ordered collections with stack/queue operations

## Architecture

### Core Components
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RedisServer   │────│   ClientState   │────│  Data Storage   │
│                 │    │                 │    │                 │
│ • Socket Mgmt   │    │ • Read Buffer   │    │ • Custom Dict   │
│ • Epoll Events  │    │ • Write Buffer  │    │ • Custom Lists  │
│ • Command Proc  │    │ • State Flags   │    │ • JSON Persist  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Event Loop
1. **Accept Phase**: Accept new client connections
2. **Read Phase**: Read commands from ready clients
3. **Process Phase**: Execute commands and prepare responses
4. **Write Phase**: Send responses to clients
5. **Cleanup Phase**: Handle disconnected clients

## Installation

### Prerequisites
- Linux-based system (uses Linux-specific epoll)
- C++17 compatible compiler (g++ 7.0+)
- CMake 3.3 or above
### Build Instructions
```bash

# Build the server
mkdir build
cd build
cmake ..

# Compile the server
cmake --build . --config release

# Run the server
./server
```

## Quick Start

### Starting the Server
```bash
# Start server on default port (5555)
./server

# Server will output:
# Server Started...
```

### Connecting with a Client

```bash
# Using telnet
telnet localhost 5555

# Using netcat
nc localhost 5555

# Using a custom application that opens a TCP socket connection
```

### Protocol Format
All commands must be sent as plain text terminated with a newline character (`\n`):
```
COMMAND_NAME arg1 arg2 arg3\n
```

### Basic Usage Example
```bash
# Set a string value (note the \n terminator)
SET mykey myvalue\n
# Server responds: OK\n

# Get a string value  
GET mykey\n
# Server responds: myvalue\n

# Delete a key
DEL mykey\n
# Server responds: 1\n
```

## Command Reference

**Protocol Note**: All commands must end with `\n` (newline character). All responses from the server also end with `\n`.

### String Commands

#### `SET key value\n`
Store a string value under the specified key.
```
Client sends: SET username john_doe\n
Server responds: OK\n
```

#### `GET key\n`
Retrieve the string value for the specified key.
```
Client sends: GET username\n
Server responds: john_doe\n

Client sends: GET nonexistent\n
Server responds: -1\n
```

#### `DEL key\n`
Delete the specified key and its value.
```
Client sends: DEL username\n
Server responds: 1\n  (if key existed)
Server responds: 0\n  (if key didn't exist)
```

#### `KEYS\n`
List all string keys currently stored.
```
Client sends: KEYS\n
Server responds: key1 key2 key3\n
```

### List Commands

#### `LSET key value1 value2 ...\n`
Create a list with the specified values.
```
Client sends: LSET mylist apple banana cherry\n
Server responds: OK\n
```

#### `LGET key [index]\n`
Retrieve list contents or specific element.
```
# Get entire list
Client sends: LGET mylist\n
Server responds: apple banana cherry\n

# Get element at index 1
Client sends: LGET mylist 1\n
Server responds: banana\n
```

#### `LPUSHBACK key value1 value2 ...\n`
Add elements to the end of a list.
```
Client sends: LPUSHBACK mylist orange grape\n
Server responds: OK\n
```

#### `LPOPBACK key\n`
Remove and return the last element from a list.
```
Client sends: LPOPBACK mylist\n
Server responds: grape\n
```

#### `LPUSHFRONT key value1 value2 ...\n`
Add elements to the beginning of a list.
```
Client sends: LPUSHFRONT mylist mango kiwi\n
Server responds: OK\n
```

#### `LPOPFRONT key\n`
Remove and return the first element from a list.
```
Client sends: LPOPFRONT mylist\n
Server responds: mango\n
```

#### `LDEL key [index]\n`
Delete a list or specific element.
```
# Delete entire list
Client sends: LDEL mylist\n
Server responds: 1\n

# Delete element at index 2
Client sends: LDEL mylist 2\n
Server responds: 1\n  (if successful)
Server responds: 0\n  (if index out of bounds)
```

#### `LKEYS\n`
List all list keys currently stored.
```
Client sends: LKEYS\n
Server responds: list1 list2 list3\n
```

## Data Persistence

### `STORE [filename]`
Save all data to a JSON file.
```
# Save to default file (FastCache.json)
STORE
# Response: OK

# Save to custom file
STORE mybackup
# Response: OK (creates mybackup.json)
```

### `LOAD [filename]`
Load data from a JSON file.
```
# Load from default file
LOAD
# Response: OK

# Load from custom file
LOAD mybackup
# Response: OK

# File not found
LOAD nonexistent
# Response: ERR Unable To Open Cache File
```

### JSON Format
The persistence format stores both strings and lists:
```json
{
  "strings": {
    "key1": "value1",
    "key2": "value2"
  },
  "lists": {
    "mylist": ["item1", "item2", "item3"],
    "tasks": ["task1", "task2"]
  }
}
```

### `DELALL`
Clear all data from memory.
```
DELALL
# Response: OK
```

## Error Handling

### Common Error Responses
All error responses end with `\n`:
```
ERR Wrong Number of Arguments\n    # Missing required parameters
ERR Invalid Command\n             # Unknown command
ERR Unable To Open Cache File\n   # File I/O error during LOAD
ERR Cannot Parse Json\n          # JSON parsing error during LOAD
```

### Client Connection Errors
- **Connection refused**: Server not running or port blocked
- **Sudden disconnection**: Server handles gracefully, cleans up resources
- **Invalid commands**: Server responds with error, keeps connection alive

### Network Error Handling
The server implements robust error handling for:
- **EAGAIN/EWOULDBLOCK**: Non-blocking operation would block
- **EPIPE**: Client disconnected during write
- **ECONNRESET**: Connection reset by peer
- **EINTR**: System call interrupted by signal

## Performance Considerations

### Scalability
- **Single-threaded**: Eliminates synchronization overhead
- **Event-driven**: Handles thousands of connections efficiently
- **Edge-triggered epoll**: Minimizes system call overhead
- **Memory efficient**: Custom data structures optimized for usage patterns

## Client Application Example

Here's a simple Python client example for connecting to the server:

```python
import socket

class FastCacheClient:
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def send_command(self, command):
        # Send command with \n terminator
        self.socket.send((command + '\n').encode())
        
        # Read response until \n
        response = b''
        while True:
            char = self.socket.recv(1)
            if char == b'\n':
                break
            response += char
        
        return response.decode()
    
    def close(self):
        if self.socket:
            self.socket.close()

# Usage example
client = FastCacheClient()
client.connect()

# Set a value
response = client.send_command("SET mykey myvalue")
print(f"SET response: {response}")  # Output: OK

# Get a value
response = client.send_command("GET mykey")
print(f"GET response: {response}")  # Output: myvalue

client.close()
```

## API Reference

#### Constructor
```cpp
RedisServer(int port = 5555)
```
Creates server instance bound to specified port.

#### Public Methods
```cpp
void runServer()              // Start the event loop
```

#### Private Methods
```cpp
void setupServer(int port)           // Initialize server socket
void setupEpoll()                   // Initialize epoll instance
void acceptNewClients()             // Handle new connections
void readFromClient(int fd)         // Read data from client
void writeToClient(int fd)          // Write data to client
void processCompleteCommands(int fd) // Parse and execute commands
std::string executeCommand(std::string command) // Command dispatcher
void cleanupClient(int fd)          // Clean up disconnected client
```

### ClientState Structure
```cpp
struct ClientState {
    std::string buffer;        // Incoming data buffer
    std::string write_buffer;  // Outgoing data buffer
    bool command_complete;     // Command parsing state
};
```

### Error Handling Functions
```cpp
void makeNonBlocking(int fd)          // Set socket to non-blocking
void addToEpoll(int fd, uint32_t events)    // Add fd to epoll
void modifyEpoll(int fd, uint32_t events)   // Modify epoll events
```

## Development Notes

### Custom Data Structures
The server uses custom implementations instead of STL containers for:
- **Performance**: Optimized for specific access patterns
- **Memory control**: Precise memory management
- **Learning**: Educational value in implementing data structures

## Contributing

When contributing to this project:

1. Follow the existing error handling patterns
2. Maintain single-threaded design principles
3. Add appropriate documentation for new commands
4. Test with multiple concurrent clients
5. Ensure memory cleanup in error paths
