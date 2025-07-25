#!/usr/bin/env python3

import socket
import time
import random
import string
import statistics
import threading
import concurrent.futures
import argparse
import sys
from typing import List, Dict, Callable, Any
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    command: str
    total_ops: int
    successful_ops: int
    errors: int
    total_time: float
    times: List[float]
    
    @property
    def success_rate(self) -> float:
        return (self.successful_ops / self.total_ops * 100) if self.total_ops > 0 else 0
    
    @property
    def ops_per_sec(self) -> float:
        return self.successful_ops / self.total_time if self.total_time > 0 else 0
    
    @property
    def avg_time(self) -> float:
        return statistics.mean(self.times) if self.times else 0
    
    @property
    def min_time(self) -> float:
        return min(self.times) if self.times else 0
    
    @property
    def max_time(self) -> float:
        return max(self.times) if self.times else 0
    
    @property
    def p50(self) -> float:
        return statistics.median(self.times) if self.times else 0
    
    @property
    def p95(self) -> float:
        return statistics.quantiles(self.times, n=20)[18] if len(self.times) >= 20 else (max(self.times) if self.times else 0)
    
    @property
    def p99(self) -> float:
        return statistics.quantiles(self.times, n=100)[98] if len(self.times) >= 100 else (max(self.times) if self.times else 0)


class RedisConnection:
    def __init__(self, host: str = 'localhost', port: int = 6379, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        
    def connect(self):
        """Establish connection to Redis clone"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.timeout)
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Failed to connect to {self.host}:{self.port} - {e}")
            return False
    
    def send_command(self, command: str) -> str:
        """Send command and receive response"""
        if not self.socket:
            raise ConnectionError("Not connected to Redis")
        
        try:
            # Send command with newline
            self.socket.send((command + '\n').encode('utf-8'))
            
            # Receive response (assuming newline-terminated responses)
            response = b''
            while True:
                chunk = self.socket.recv(4096)
                if not chunk:
                    break
                response += chunk
                if b'\n' in chunk or b'\r' in chunk:
                    break
            
            return response.decode('utf-8').strip()
        except socket.timeout:
            raise TimeoutError(f"Command timeout: {command}")
        except Exception as e:
            raise ConnectionError(f"Command failed: {e}")
    
    def close(self):
        """Close connection"""
        if self.socket:
            self.socket.close()
            self.socket = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class RedisBenchmark:
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.host = host
        self.port = port
        self.results: Dict[str, BenchmarkResult] = {}
    
    def generate_random_string(self, length: int = 10) -> str:
        """Generate random string of specified length"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def generate_random_value(self) -> str:
        """Generate random value of various types and sizes"""
        value_types = ['short_string', 'long_string', 'number', 'mixed']
        value_type = random.choice(value_types)
        
        if value_type == 'short_string':
            return self.generate_random_string(8)
        elif value_type == 'long_string':
            return self.generate_random_string(100)
        elif value_type == 'number':
            return str(random.randint(1, 10000))
        else:  # mixed
            return f"{self.generate_random_string(5)}_{random.randint(1, 1000)}"
    
    def benchmark_command(self, 
                         command_name: str, 
                         command_generator: Callable[[int], str], 
                         iterations: int = 1000,
                         show_progress: bool = True) -> BenchmarkResult:
        """Benchmark a specific command"""
        if show_progress:
            print(f"\nBenchmarking {command_name} ({iterations:,} iterations)...")
        
        times = []
        errors = 0
        successful_ops = 0
        
        start_time = time.time()
        
        with RedisConnection(self.host, self.port) as conn:
            if not conn.socket:
                return BenchmarkResult(command_name, iterations, 0, iterations, 0, [])
            
            for i in range(iterations):
                try:
                    command = command_generator(i)
                    
                    # Time the command execution
                    cmd_start = time.perf_counter()
                    response = conn.send_command(command)
                    cmd_end = time.perf_counter()
                    
                    execution_time = (cmd_end - cmd_start) * 1000  # Convert to milliseconds
                    times.append(execution_time)
                    successful_ops += 1
                    
                    # Show progress
                    if show_progress and i % max(1, iterations // 20) == 0:
                        progress = (i + 1) / iterations * 100
                        print(f"\rProgress: {progress:.1f}% [{i+1:,}/{iterations:,}]", end='', flush=True)
                        
                except Exception as e:
                    errors += 1
                    if errors % 100 == 0:
                        print(f"\nErrors encountered: {errors}")
        
        total_time = time.time() - start_time
        
        if show_progress:
            print()  # New line after progress
        
        result = BenchmarkResult(
            command=command_name,
            total_ops=iterations,
            successful_ops=successful_ops,
            errors=errors,
            total_time=total_time,
            times=times
        )
        
        self.results[command_name] = result
        self.print_result(result)
        return result
    
    def print_result(self, result: BenchmarkResult):
        """Print benchmark results in a formatted way"""
        print(f"\n{'='*15} {result.command.upper()} RESULTS {'='*15}")
        print(f"Total Operations:    {result.total_ops:,}")
        print(f"Successful:          {result.successful_ops:,}")
        print(f"Errors:              {result.errors:,}")
        print(f"Success Rate:        {result.success_rate:.2f}%")
        print(f"Total Time:          {result.total_time:.3f}s")
        print(f"Throughput:          {result.ops_per_sec:.0f} ops/sec")
        
        if result.times:
            print(f"Average Latency:     {result.avg_time:.3f}ms")
            print(f"Min Latency:         {result.min_time:.3f}ms")
            print(f"Max Latency:         {result.max_time:.3f}ms")
            print(f"P50 Latency:         {result.p50:.3f}ms")
            print(f"P95 Latency:         {result.p95:.3f}ms")
            print(f"P99 Latency:         {result.p99:.3f}ms")
    
    def run_basic_benchmarks(self, iterations: int = 1000):
        """Run benchmarks for basic Redis commands"""
        print("\n" + "="*50)
        print("BASIC COMMAND BENCHMARKS")
        print("="*50)
        
        # SET benchmark
        self.benchmark_command(
            'SET',
            lambda i: f"SET key_{i} {self.generate_random_value()}",
            iterations
        )
        
        # GET benchmark (using keys from SET)
        self.benchmark_command(
            'GET',
            lambda i: f"GET key_{i % iterations}",
            iterations
        )
        
        # DEL benchmark
        self.benchmark_command(
            'DEL',
            lambda i: f"DEL key_{i}",
            min(iterations, 500)
        )
        
        # KEYS benchmark (expensive operation, fewer iterations)
        self.benchmark_command(
            'KEYS',
            lambda i: "KEYS",
            min(50, iterations // 20)
        )
    
    def run_list_benchmarks(self, iterations: int = 1000):
        """Run benchmarks for list commands"""
        print("\n" + "="*50)
        print("LIST COMMAND BENCHMARKS")
        print("="*50)
        
        # LSET benchmark
        self.benchmark_command(
            'LSET',
            lambda i: f"LSET list_{i} " + " ".join([self.generate_random_value() 
                                                   for _ in range(random.randint(1, 5))]),
            iterations
        )
        
        # LGET benchmark
        self.benchmark_command(
            'LGET',
            lambda i: f"LGET list_{i % iterations}",
            iterations
        )
        
        # LDEL with index benchmark
        self.benchmark_command(
            'LDEL_INDEX',
            lambda i: f"LDEL list_{i % iterations} {random.randint(0, 3)}",
            min(iterations, 500)
        )
        
        # LDEL full list benchmark
        self.benchmark_command(
            'LDEL_FULL',
            lambda i: f"LDEL list_{i}",
            min(iterations, 300)
        )
        
        # LKEYS benchmark
        self.benchmark_command(
            'LKEYS',
            lambda i: "LKEYS",
            min(50, iterations // 20)
        )
    
    def run_persistence_benchmarks(self):
        """Run benchmarks for persistence commands"""
        print("\n" + "="*50)
        print("PERSISTENCE COMMAND BENCHMARKS")
        print("="*50)
        
        # STORE benchmark
        self.benchmark_command(
            'STORE',
            lambda i: "STORE",
            20  # Storage operations are typically slower
        )
        
        # LOAD benchmark
        self.benchmark_command(
            'LOAD',
            lambda i: "LOAD",
            20
        )
    
    def run_mixed_workload(self, iterations: int = 1000):
        """Run mixed workload benchmark"""
        print("\n" + "="*50)
        print("MIXED WORKLOAD BENCHMARK")
        print("="*50)
        
        def mixed_command_generator(i):
            commands = [
                lambda: f"SET mixed_{random.randint(1, 100)} {self.generate_random_value()}",
                lambda: f"GET mixed_{random.randint(1, 100)}",
                lambda: f"LSET mixed_list_{random.randint(1, 50)} {self.generate_random_value()} {self.generate_random_value()}",
                lambda: f"LGET mixed_list_{random.randint(1, 50)}",
                lambda: f"DEL mixed_{random.randint(1, 100)}",
            ]
            return random.choice(commands)()
        
        self.benchmark_command(
            'MIXED_WORKLOAD',
            mixed_command_generator,
            iterations
        )
    
    def run_concurrency_test(self, num_workers: int = 5, ops_per_worker: int = 200):
        """Run concurrency test with multiple workers"""
        print("\n" + "="*50)
        print(f"CONCURRENCY TEST ({num_workers} workers, {ops_per_worker} ops each)")
        print("="*50)
        
        def worker_task(worker_id: int) -> Dict[str, int]:
            successful = 0
            errors = 0
            
            try:
                with RedisConnection(self.host, self.port) as conn:
                    if not conn.socket:
                        return {'successful': 0, 'errors': ops_per_worker}
                    
                    for i in range(ops_per_worker):
                        try:
                            commands = [
                                f"SET worker_{worker_id}_{i} value_{i}",
                                f"GET worker_{worker_id}_{random.randint(0, max(1, i))}",
                                f"LSET worker_list_{worker_id}_{i} item1 item2",
                                f"LGET worker_list_{worker_id}_{random.randint(0, max(1, i))}"
                            ]
                            
                            command = commands[i % len(commands)]
                            conn.send_command(command)
                            successful += 1
                            
                        except Exception:
                            errors += 1
                            
            except Exception:
                errors += ops_per_worker
            
            return {'successful': successful, 'errors': errors}
        
        start_time = time.time()
        
        # Run workers concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker_task, worker_id) for worker_id in range(num_workers)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        
        # Calculate results
        total_successful = sum(r['successful'] for r in results)
        total_errors = sum(r['errors'] for r in results)
        total_time = end_time - start_time
        throughput = total_successful / total_time if total_time > 0 else 0
        
        print(f"\n{'='*20} CONCURRENCY RESULTS {'='*20}")
        print(f"Workers:             {num_workers}")
        print(f"Ops per worker:      {ops_per_worker:,}")
        print(f"Total operations:    {total_successful + total_errors:,}")
        print(f"Successful:          {total_successful:,}")
        print(f"Errors:              {total_errors:,}")
        print(f"Success rate:        {(total_successful / (total_successful + total_errors) * 100):.2f}%")
        print(f"Total time:          {total_time:.3f}s")
        print(f"Throughput:          {throughput:.0f} ops/sec")
        print(f"Avg per worker:      {total_successful / num_workers:.1f} ops")
    
    def run_full_benchmark(self, iterations: int = 1000, concurrency_workers: int = 5):
        """Run the complete benchmark suite"""
        print("Redis Clone Benchmark Suite")
        print("="*60)
        print(f"Target:              {self.host}:{self.port}")
        print(f"Iterations per test: {iterations:,}")
        print(f"Concurrency workers: {concurrency_workers}")
        print("="*60)
        
        overall_start = time.time()
        
        try:
            # Test connection first
            with RedisConnection(self.host, self.port) as conn:
                if not conn.socket:
                    print(f"❌ Failed to connect to {self.host}:{self.port}")
                    return
                print(f"✅ Connected to Redis clone at {self.host}:{self.port}")
            
            # Run all benchmark suites
            self.run_basic_benchmarks(iterations)
            self.run_list_benchmarks(iterations)
            self.run_persistence_benchmarks()
            self.run_mixed_workload(iterations)
            self.run_concurrency_test(concurrency_workers, 200)
            
        except KeyboardInterrupt:
            print("\n\n❌ Benchmark interrupted by user")
            return
        except Exception as e:
            print(f"\n\n❌ Benchmark failed: {e}")
            return
        
        overall_time = time.time() - overall_start
        
        # Print summary
        self.print_summary(overall_time)
    
    def print_summary(self, total_time: float):
        """Print benchmark summary"""
        print("\n" + "="*60)
        print("BENCHMARK SUMMARY")
        print("="*60)
        
        if not self.results:
            print("No results to display")
            return
        
        # Print results table
        print(f"{'Command':<20} {'Ops/sec':<12} {'Avg (ms)':<12} {'P95 (ms)':<12} {'Success %':<10}")
        print("-" * 70)
        
        for command, result in self.results.items():
            print(f"{command:<20} {result.ops_per_sec:<12.0f} {result.avg_time:<12.3f} "
                  f"{result.p95:<12.3f} {result.success_rate:<10.2f}")
        
        print("-" * 70)
        print(f"Total benchmark time: {total_time:.2f} seconds")
        
        # Find best and worst performing commands
        if len(self.results) > 1:
            best_command = max(self.results.items(), key=lambda x: x[1].ops_per_sec)
            worst_command = min(self.results.items(), key=lambda x: x[1].ops_per_sec)
            
            print(f"\n🏆 Best performance:  {best_command[0]} ({best_command[1].ops_per_sec:.0f} ops/sec)")
            print(f"🐌 Worst performance: {worst_command[0]} ({worst_command[1].ops_per_sec:.0f} ops/sec)")


def main():
    parser = argparse.ArgumentParser(description='Redis Clone Benchmark Tool')
    parser.add_argument('--host', '-H', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', '-p', type=int, default=5555, help='Redis port (default: 5555)')
    parser.add_argument('--iterations', '-i', type=int, default=1000, help='Iterations per test (default: 1000)')
    parser.add_argument('--workers', '-w', type=int, default=5, help='Concurrency workers (default: 5)')
    parser.add_argument('--quick', '-q', action='store_true', help='Run quick benchmark (fewer iterations)')
    
    args = parser.parse_args()
    
    if args.quick:
        args.iterations = 100
        args.workers = 3
        print("🚀 Running quick benchmark...")
    
    benchmark = RedisBenchmark(args.host, args.port)
    benchmark.run_full_benchmark(args.iterations, args.workers)


if __name__ == '__main__':
    main()