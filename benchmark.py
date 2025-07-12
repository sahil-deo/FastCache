#!/usr/bin/env python3

import socket
import time
import threading
import statistics
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
import argparse
import json

class RedisClient:
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port
        self.socket = None
        
    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        
    def disconnect(self):
        if self.socket:
            self.socket.close()
            
    def send_command(self, command: str) -> str:
        """Send command and get response"""
        try:
            self.socket.send((command + '\n').encode())
            response = self.socket.recv(4096).decode().strip()
            return response
        except Exception as e:
            return f"ERROR: {str(e)}"

class BenchmarkSuite:
    def __init__(self, host='localhost', port=5555, num_clients=10):
        self.host = host
        self.port = port
        self.num_clients = num_clients
        self.results = {}
        
    def generate_random_string(self, length=10):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        
    def generate_test_data(self, count=1000):
        """Generate test data for benchmarks"""
        return {
            'keys': [f'key_{i}' for i in range(count)],
            'values': [self.generate_random_string(20) for _ in range(count)],
            'list_keys': [f'list_{i}' for i in range(count // 10)],
            'list_values': [[self.generate_random_string(10) for _ in range(5)] for _ in range(count // 10)]
        }
    
    def benchmark_command(self, client: RedisClient, command: str, iterations: int = 100) -> Dict:
        """Benchmark a single command type"""
        times = []
        errors = 0
        
        for _ in range(iterations):
            start_time = time.perf_counter()
            try:
                response = client.send_command(command)
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)  # Convert to milliseconds
            
        return {
            'avg_time': statistics.mean(times),
            'min_time': min(times),
            'max_time': max(times),
            'median_time': statistics.median(times),
            'p95_time': statistics.quantiles(times, n=20)[18] if len(times) >= 20 else max(times),
            'p99_time': statistics.quantiles(times, n=100)[98] if len(times) >= 100 else max(times),
            'total_time': sum(times),
            'operations_per_second': 1000 * iterations / sum(times),
            'errors': errors,
            'success_rate': (iterations - errors) / iterations * 100
        }
    
    def benchmark_set_operations(self, client: RedisClient, test_data: Dict, iterations: int = 1000):
        """Benchmark SET operations"""
        print("Benchmarking SET operations...")
        times = []
        errors = 0
        
        for i in range(iterations):
            key = test_data['keys'][i % len(test_data['keys'])]
            value = test_data['values'][i % len(test_data['values'])]
            command = f"SET {key} {value}"
            
            start_time = time.perf_counter()
            try:
                response = client.send_command(command)
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        return self._calculate_stats(times, errors, iterations)
    
    def benchmark_get_operations(self, client: RedisClient, test_data: Dict, iterations: int = 1000):
        """Benchmark GET operations"""
        print("Benchmarking GET operations...")
        # First, populate some data
        for i in range(min(100, len(test_data['keys']))):
            client.send_command(f"SET {test_data['keys'][i]} {test_data['values'][i]}")
        
        times = []
        errors = 0
        
        for i in range(iterations):
            key = test_data['keys'][i % min(100, len(test_data['keys']))]
            command = f"GET {key}"
            
            start_time = time.perf_counter()
            try:
                response = client.send_command(command)
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        return self._calculate_stats(times, errors, iterations)
    
    def benchmark_list_operations(self, client: RedisClient, test_data: Dict, iterations: int = 500):
        """Benchmark list operations (LSET, LGET, LPUSH, LPOP, etc.)"""
        print("Benchmarking LIST operations...")
        results = {}
        
        # LSET operations
        times = []
        errors = 0
        for i in range(iterations):
            key = test_data['list_keys'][i % len(test_data['list_keys'])]
            values = ' '.join(test_data['list_values'][i % len(test_data['list_values'])])
            command = f"LSET {key} {values}"
            
            start_time = time.perf_counter()
            try:
                response = client.send_command(command)
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        results['LSET'] = self._calculate_stats(times, errors, iterations)
        
        # LGET operations
        times = []
        errors = 0
        for i in range(iterations):
            key = test_data['list_keys'][i % len(test_data['list_keys'])]
            command = f"LGET {key}"
            
            start_time = time.perf_counter()
            try:
                response = client.send_command(command)
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        results['LGET'] = self._calculate_stats(times, errors, iterations)
        
        # LPUSH operations
        times = []
        errors = 0
        for i in range(iterations):
            key = f"push_list_{i % 50}"
            values = ' '.join(random.choices(test_data['values'], k=3))
            command = f"LPUSH {key} {values}"
            
            start_time = time.perf_counter()
            try:
                response = client.send_command(command)
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        results['LPUSH'] = self._calculate_stats(times, errors, iterations)
        
        return results
    
    def benchmark_utility_operations(self, client: RedisClient, iterations: int = 100):
        """Benchmark utility operations (KEYS, LKEYS, DEL, LDEL, etc.)"""
        print("Benchmarking utility operations...")
        results = {}
        
        # KEYS operation
        times = []
        errors = 0
        for _ in range(iterations):
            start_time = time.perf_counter()
            try:
                response = client.send_command("KEYS")
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        results['KEYS'] = self._calculate_stats(times, errors, iterations)
        
        # LKEYS operation
        times = []
        errors = 0
        for _ in range(iterations):
            start_time = time.perf_counter()
            try:
                response = client.send_command("LKEYS")
                if response.startswith('ERROR') or response.startswith('ERR'):
                    errors += 1
            except Exception:
                errors += 1
            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)
            
        results['LKEYS'] = self._calculate_stats(times, errors, iterations)
        
        return results
    
    def _calculate_stats(self, times: List[float], errors: int, iterations: int) -> Dict:
        """Calculate statistics from timing data"""
        if not times:
            return {'error': 'No timing data available'}
            
        return {
            'avg_time_ms': round(statistics.mean(times), 3),
            'min_time_ms': round(min(times), 3),
            'max_time_ms': round(max(times), 3),
            'median_time_ms': round(statistics.median(times), 3),
            'p95_time_ms': round(statistics.quantiles(times, n=20)[18] if len(times) >= 20 else max(times), 3),
            'p99_time_ms': round(statistics.quantiles(times, n=100)[98] if len(times) >= 100 else max(times), 3),
            'total_time_ms': round(sum(times), 3),
            'operations_per_second': round(1000 * iterations / sum(times), 2),
            'errors': errors,
            'success_rate_percent': round((iterations - errors) / iterations * 100, 2)
        }
    
    def run_concurrent_benchmark(self, iterations_per_client: int = 100):
        """Run benchmark with multiple concurrent clients"""
        print(f"Running concurrent benchmark with {self.num_clients} clients...")
        
        def client_worker(client_id):
            client = RedisClient(self.host, self.port)
            try:
                client.connect()
                test_data = self.generate_test_data(iterations_per_client)
                
                # Run different operations
                set_results = self.benchmark_set_operations(client, test_data, iterations_per_client)
                get_results = self.benchmark_get_operations(client, test_data, iterations_per_client)
                #list_results = self.benchmark_list_operations(client, test_data, iterations_per_client // 2)
                
                return {
                    'client_id': client_id,
                    'SET': set_results,
                    'GET': get_results,
                    #'LIST': list_results
                }
            except Exception as e:
                return {'client_id': client_id, 'error': str(e)}
            finally:
                client.disconnect()
        
        # Run concurrent clients
        with ThreadPoolExecutor(max_workers=self.num_clients) as executor:
            futures = [executor.submit(client_worker, i) for i in range(self.num_clients)]
            results = [future.result() for future in as_completed(futures)]
        
        return results
    
    def run_single_client_benchmark(self, iterations: int = 1000):
        """Run comprehensive benchmark with single client"""
        print(f"Running single client benchmark with {iterations} iterations...")
        
        client = RedisClient(self.host, self.port)
        try:
            client.connect()
            test_data = self.generate_test_data(iterations)
            
            results = {}
            
            # Basic string operations
            results['SET'] = self.benchmark_set_operations(client, test_data, iterations)
            results['GET'] = self.benchmark_get_operations(client, test_data, iterations)
            
            # List operations
            list_results = self.benchmark_list_operations(client, test_data, iterations // 2)
            results.update(list_results)
            
            # Utility operations
            utility_results = self.benchmark_utility_operations(client, iterations // 10)
            results.update(utility_results)
            
            # APP operation (string append)
            print("Benchmarking APP operations...")
            times = []
            errors = 0
            for i in range(iterations // 10):
                key = test_data['keys'][i % len(test_data['keys'])]
                value = self.generate_random_string(5)
                command = f"APP {key} {value}"
                
                start_time = time.perf_counter()
                try:
                    response = client.send_command(command)
                    if response.startswith('ERROR') or response.startswith('ERR'):
                        errors += 1
                except Exception:
                    errors += 1
                end_time = time.perf_counter()
                times.append((end_time - start_time) * 1000)
                
            results['APP'] = self._calculate_stats(times, errors, iterations // 10)
            
            return results
            
        finally:
            client.disconnect()
    
    def print_results(self, results: Dict, title: str = "Benchmark Results"):
        """Print formatted benchmark results"""
        print(f"\n{'='*60}")
        print(f"{title:^60}")
        print(f"{'='*60}")
        
        for operation, stats in results.items():
            if isinstance(stats, dict) and 'avg_time_ms' in stats:
                print(f"\n{operation} Operation:")
                print(f"  Average Time: {stats['avg_time_ms']} ms")
                print(f"  Min Time: {stats['min_time_ms']} ms")
                print(f"  Max Time: {stats['max_time_ms']} ms")
                print(f"  Median Time: {stats['median_time_ms']} ms")
                print(f"  95th Percentile: {stats['p95_time_ms']} ms")
                print(f"  99th Percentile: {stats['p99_time_ms']} ms")
                print(f"  Operations/Second: {stats['operations_per_second']}")
                print(f"  Success Rate: {stats['success_rate_percent']}%")
                print(f"  Errors: {stats['errors']}")
    
    def save_results_to_file(self, results: Dict, filename: str = "benchmark_results.json"):
        """Save results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {filename}")

def main():
    start = time.time()
    parser = argparse.ArgumentParser(description='Redis Server Benchmark Tool')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=5555, help='Server port')
    parser.add_argument('--clients', type=int, default=1, help='Number of concurrent clients')
    parser.add_argument('--iterations', type=int, default=1000, help='Number of iterations per test')
    parser.add_argument('--concurrent', action='store_true', help='Run concurrent benchmark')
    parser.add_argument('--output', default='benchmark_results.json', help='Output file for results')
    
    args = parser.parse_args()
    
    benchmark = BenchmarkSuite(args.host, args.port, args.clients)
    
    try:
        if args.concurrent:
            results = benchmark.run_concurrent_benchmark(args.iterations // args.clients)
            print(f"\nConcurrent Benchmark Results ({args.clients} clients):")
            for result in results:
                if 'error' not in result:
                    print(f"\nClient {result['client_id']}:")
                    for op, stats in result.items():
                        if op != 'client_id' and isinstance(stats, dict):
                            print(f"  {op}: {stats.get('operations_per_second', 'N/A')} ops/sec")
        else:
            results = benchmark.run_single_client_benchmark(args.iterations)
            benchmark.print_results(results, "Single Client Benchmark")
            benchmark.save_results_to_file(results, args.output)
    
    except ConnectionRefusedError:
        print(f"Error: Could not connect to server at {args.host}:{args.port}")
        print("Make sure your Redis server is running.")
    except Exception as e:
        print(f"Error: {e}")
    end = time.time()
    print(end-start)

if __name__ == "__main__":
    main()