# Performance Testing Guide

This guide provides comprehensive instructions for using the Gen3 SDK performance testing tools to benchmark and optimize download performance.

## Overview

The performance testing module allows you to:

- Compare different download methods (Gen3 SDK async vs CDIS Data Client)
- Analyze performance bottlenecks
- Monitor system resources during downloads
- Generate detailed performance reports
- Optimize download configurations

## Quick Start

### 1. Basic Performance Test

```bash
# Install dependencies
pip install -r performance_testing/requirements.txt

# Run basic test
python performance_testing/async_comparison.py
```

### 2. Custom Configuration

```bash
# Set environment variables
export PERF_NUM_RUNS=3
export PERF_MAX_CONCURRENT_ASYNC=300
export PERF_CREDENTIALS_PATH="~/Downloads/credentials.json"

# Run test
python performance_testing/async_comparison.py
```

### 3. Using Configuration File

```bash
# Create default config
python -c "from performance_testing.config import create_default_config_file; create_default_config_file()"

# Edit config file
nano performance_config.json

# Run with config
python performance_testing/async_comparison.py --config performance_config.json
```

## Configuration Options

### Environment Variables

The performance testing module supports extensive configuration via environment variables:

#### Test Configuration

```bash
# Number of test runs per method
export PERF_NUM_RUNS=2

# Enable/disable profiling
export PERF_ENABLE_PROFILING=true
export PERF_ENABLE_MONITORING=true

# Monitoring interval
export PERF_MONITORING_INTERVAL=1.0

# Filter for medium-sized files (1-100MB)
export PERF_FILTER_MEDIUM_FILES=false
```

#### Concurrency Settings

```bash
# Max concurrent requests for async downloads
export PERF_MAX_CONCURRENT_ASYNC=200

# Number of CDIS workers
export PERF_NUM_WORKERS_CDIS=8
```


#### Paths and Endpoints

```bash
# Path to gen3-client executable
export GEN3_CLIENT_PATH="/path/to/gen3-client"

# Credentials file
export PERF_CREDENTIALS_PATH="~/Downloads/credentials.json"

# Gen3 endpoint
export PERF_ENDPOINT="https://data.midrc.org"

# Custom manifest file
export PERF_MANIFEST_PATH="/path/to/manifest.json"

# Results directory
export PERF_RESULTS_DIR="/path/to/results"
```

#### Test Methods

```bash
# Test specific methods
export PERF_TEST_METHODS="async,cdis"

# Test only async
export PERF_TEST_METHODS="async"

# Test only CDIS
export PERF_TEST_METHODS="cdis"
```

### Configuration File

Create a JSON configuration file for more complex setups:

```json
{
  "num_runs": 3,
  "enable_profiling": true,
  "enable_real_time_monitoring": true,
  "monitoring_interval": 1.0,
  "max_concurrent_requests_async": 300,
  "num_workers_cdis": 8,
  "test_methods": ["async", "cdis"],
  "endpoint": "https://data.midrc.org",
  "credentials_path": "~/Downloads/credentials.json",
  "manifest_path": "/path/to/manifest.json",
  "results_dir": "/path/to/results",
  "enable_line_profiling": true,
  "enable_memory_profiling": true,
  "enable_network_monitoring": true,
  "enable_disk_io_monitoring": true,
  "memory_warning_threshold_mb": 2000,
  "cpu_warning_threshold_percent": 90,
  "throughput_warning_threshold_mbps": 10,
  "success_rate_warning_threshold": 90,
  "log_level": "INFO",
  "generate_html_report": true,
  "open_report_in_browser": true,
  "save_detailed_metrics": true
}
```

## Usage Examples

### 1. Quick Performance Assessment

For a quick performance check with minimal overhead:

```bash
# Single run, minimal profiling
export PERF_NUM_RUNS=1
export PERF_ENABLE_PROFILING=false
export PERF_ENABLE_MONITORING=true
export PERF_MAX_CONCURRENT_ASYNC=100

python performance_testing/async_comparison.py
```

### 2. Comprehensive Benchmark

For detailed performance analysis:

```bash
# Multiple runs, full profiling
export PERF_NUM_RUNS=3
export PERF_ENABLE_PROFILING=true
export PERF_ENABLE_LINE_PROFILING=true
export PERF_ENABLE_MEMORY_PROFILING=true
export PERF_MAX_CONCURRENT_ASYNC=500
export PERF_ENABLE_NETWORK_MONITORING=true
export PERF_ENABLE_DISK_IO_MONITORING=true

python performance_testing/async_comparison.py
```

### 3. Custom Manifest Testing

Test with your own manifest file:

```bash
# Use custom manifest
export PERF_MANIFEST_PATH="/path/to/your/manifest.json"
export PERF_RESULTS_DIR="/custom/results/path"

python performance_testing/async_comparison.py
```

### 4. Method-Specific Testing

Test only specific download methods:

```bash
# Test only Gen3 SDK async
export PERF_TEST_METHODS="async"
export PERF_MAX_CONCURRENT_ASYNC=300

python performance_testing/async_comparison.py
```

```bash
# Test only CDIS Data Client
export PERF_TEST_METHODS="cdis"
export PERF_NUM_WORKERS_CDIS=16

python performance_testing/async_comparison.py
```

### 5. Performance Optimization Testing

Test different concurrency levels:

```bash
# Low concurrency
export PERF_MAX_CONCURRENT_ASYNC=50
export PERF_NUM_WORKERS_CDIS=4
python performance_testing/async_comparison.py

# Medium concurrency
export PERF_MAX_CONCURRENT_ASYNC=200
export PERF_NUM_WORKERS_CDIS=8
python performance_testing/async_comparison.py

# High concurrency
export PERF_MAX_CONCURRENT_ASYNC=500
export PERF_NUM_WORKERS_CDIS=16
python performance_testing/async_comparison.py
```

## Understanding Results

### Output Files

The performance test generates several output files:

- **HTML Report**: `async_comparison_results/performance_report_YYYYMMDD_HHMMSS.html`
- **JSON Results**: `async_comparison_results/async_comparison_results_YYYYMMDD_HHMMSS.json`
- **Log File**: `async_comparison_results/async_comparison_YYYYMMDD_HHMMSS.log`
- **Status File**: `async_comparison_results/test_status.json`

### Key Metrics Explained

#### Performance Metrics

- **Throughput (MB/s)**: Download speed in megabytes per second
- **Success Rate (%)**: Percentage of files successfully downloaded
- **Download Time (s)**: Total time for all downloads
- **Files per Second**: Number of files downloaded per second

#### System Metrics

- **Peak Memory (MB)**: Maximum memory usage during test
- **Peak CPU (%)**: Maximum CPU usage during test
- **Network I/O (MB)**: Total network data transferred
- **Disk I/O (MB)**: Total disk operations performed

#### Profiling Metrics

- **Function Timing**: Time spent in each function
- **Line Profiling**: Line-by-line execution time
- **Memory Profiling**: Memory allocation patterns
- **Bottleneck Analysis**: Performance bottleneck identification

### Reading the HTML Report

The HTML report provides:

1. **Summary Cards**: Quick overview of each method's performance
2. **Comparison Charts**: Visual comparison of throughput, success rate, and time
3. **Detailed Tables**: Comprehensive metrics for each test run
4. **Profiling Analysis**: Code-level performance breakdown
5. **Bottleneck Analysis**: Performance recommendations

## Performance Optimization

### For High-Throughput Scenarios

```bash
# Increase concurrency
export PERF_MAX_CONCURRENT_ASYNC=500
export PERF_NUM_WORKERS_CDIS=16

# Disable profiling for pure performance measurement
export PERF_ENABLE_PROFILING=false
export PERF_ENABLE_LINE_PROFILING=false
```

### For Memory-Constrained Systems

```bash
# Reduce concurrency
export PERF_MAX_CONCURRENT_ASYNC=50
export PERF_NUM_WORKERS_CDIS=4

# Enable memory monitoring
export PERF_ENABLE_MEMORY_PROFILING=true
export PERF_MEMORY_WARNING_THRESHOLD_MB=1000
```

### For Network-Constrained Systems

```bash
# Reduce concurrent requests
export PERF_MAX_CONCURRENT_ASYNC=10
export PERF_NUM_WORKERS_CDIS=2

# Enable network monitoring
export PERF_ENABLE_NETWORK_MONITORING=true
```

### For CPU-Constrained Systems

```bash
# Reduce workers
export PERF_NUM_WORKERS_CDIS=2
export PERF_MAX_CONCURRENT_ASYNC=50

# Enable CPU monitoring
export PERF_CPU_WARNING_THRESHOLD_PERCENT=80
```
## Additional Resources

- [Gen3 SDK Documentation](../)
- [CDIS Data Client Documentation](https://github.com/uc-cdis/cdis-data-client)
- [Performance Testing Best Practices](https://github.com/uc-cdis/gen3sdk-python/wiki/Performance-Testing)
- [Configuration Reference](../performance_testing/config.py)
