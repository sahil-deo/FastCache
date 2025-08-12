
# Fast Cache

Fast Cache is a caching server developed for local caching.


![Logo](https://raw.githubusercontent.com/sahil-deo/FastCache/refs/heads/master/FastLogo.jpg)


## Data Types

- String
- Lists

## Operations

- SET / GET
- LSET / LGET
- DEL / LDEL
- KEYS / LKEYS
- LPUSHFRONT / LPUSHBACK
- LPOPFRONT / LPOPBACK
- STORE / LOAD

## Running Tests

To run tests, run the following command

```bash
  python bench.py 
  
  #options
  -i --iterations <number of Iterations> (default: 1000)
  -H --host <host ip> (default: 192.168.1.101) 
  -w --workers <threads> (default: 5)
  -p --port <port> (default: 5555)
  -q --quick 
  -h --help 
```


## Build

To build this project run

```bash
  mkdir build
  cd build
  cmake ..
  cmake --build . --config release
```


## Deployment

To deploy this project run

```bash
    #from the build dir
    ./server
```

The Server would be deployed on the PORT ```55555```
