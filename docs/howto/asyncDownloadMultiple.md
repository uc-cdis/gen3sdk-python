## Asynchronous Multiple File Downloads

The Gen3 SDK provides an optimized asynchronous download method `async_download_multiple` for efficiently downloading large numbers of files with high throughput and memory efficiency.

## Overview

The `async_download_multiple` method implements a hybrid architecture combining:

- **Multiprocessing**: Multiple Python subprocesses for CPU utilization
- **Asyncio**: High I/O concurrency within each process
- **Queue-based memory management**: Efficient handling of large file sets
- **Just-in-time presigned URL generation**: Optimized authentication flow

## Architecture

### Concurrency Model

The implementation uses a three-tier architecture:

1. **Producer Thread**: Feeds GUIDs to worker processes via bounded queues
2. **Worker Processes**: Multiple Python subprocesses with asyncio event loops
3. **Queue System**: Memory-efficient streaming of work items

```python
# Architecture overview
Producer Thread → Input Queue → Worker Processes → Output Queue → Results
     (1)           (configurable)   (configurable)    (configurable)   (Final)
```

### Key Features

- **Memory Efficiency**: Bounded queues prevent memory explosion with large file sets
- **True Parallelism**: Multiprocessing bypasses Python GIL limitations
- **High Concurrency**: Configurable concurrent downloads per process
- **Resume Support**: Skip completed files with `--skip-completed` flag
- **Progress Tracking**: Real-time progress bars and detailed reporting

## Usage

### Command Line Interface

Download multiple files using a manifest:

```bash
gen3 --endpoint my-commons.org --auth credentials.json download-multiple-async \
    --manifest files.json \
    --download-path ./downloads \
    --max-concurrent-requests 10 \
    --filename-format original \
    --skip-completed \
    --no-prompt
```

### Python API

The `async_download_multiple` method is available in the `Gen3File` class for programmatic use. Refer to the Python SDK documentation for the complete API reference.

## Parameters

For detailed parameter information and current default values, run:

```bash
gen3 download-multiple-async --help
```

The command supports various options for customizing download behavior, including concurrency settings, file naming strategies, and progress controls.

## Performance Characteristics

### Throughput Optimization

The method is optimized for high-throughput scenarios:

- **Concurrent Downloads**: Configurable number of simultaneous downloads
- **Memory Usage**: Bounded by queue sizes (typically < 100MB)
- **CPU Utilization**: Leverages multiple CPU cores
- **Network Efficiency**: Just-in-time presigned URL generation

### Scalability

Performance scales with:

- **File Count**: Linear time complexity with constant memory usage
- **File Size**: Independent of individual file sizes
- **Network Bandwidth**: Limited by available bandwidth and concurrent connections
- **System Resources**: Scales with available CPU cores and memory

## Error Handling

### Robust Error Recovery

The implementation includes comprehensive error handling:

- **Network Failures**: Automatic retry with exponential backoff
- **Authentication Errors**: Token refresh and retry
- **File System Errors**: Graceful handling of permission and space issues
- **Process Failures**: Automatic worker process restart

### Result Reporting

The method returns a structured result object containing lists of succeeded, failed, and skipped downloads with detailed information about each operation.

## Best Practices

### Configuration Recommendations

For optimal performance, adjust the concurrency and process settings based on your specific use case:

- **Small files**: Use higher concurrent request limits
- **Large files**: Use lower concurrent request limits to avoid overwhelming the system
- **High-bandwidth networks**: Increase the number of worker processes
- **Limited memory**: Reduce queue sizes to manage memory usage

### Memory Management

- **Queue Size**: Adjust based on available system memory
- **Batch Size**: Balance between memory usage and processing overhead
- **Process Count**: Match available CPU cores for optimal performance

### Network Optimization

- **Concurrent Requests**: Match your network capacity and server limits
- **Protocol Selection**: Use the appropriate protocol for your environment
- **Resume Support**: Enable skip-completed functionality for interrupted downloads

## Comparison with Synchronous Downloads

### Performance Advantages

| Metric             | Synchronous                  | Asynchronous                 |
| ------------------ | ---------------------------- | ---------------------------- |
| Memory Usage       | O(n) - grows with file count | O(1) - bounded by queue size |
| CPU Utilization    | Single core                  | Multiple cores               |
| Network Efficiency | Sequential                   | Parallel                     |
| Scalability        | Limited by GIL               | Scales with CPU cores        |

## Troubleshooting

### Common Issues

**Slow Downloads:**

- Check network bandwidth and server limits
- Reduce concurrent request limits if server is overwhelmed
- Verify authentication token is valid

**Memory Issues:**

- Reduce queue sizes and batch sizes
- Lower the number of worker processes if system memory is limited
- Monitor system memory usage during downloads

**Authentication Errors:**

- Verify credentials file is valid and not expired
- Check endpoint URL is correct
- Ensure proper permissions for target files

**Process Failures:**

- Check system resources (CPU, memory, file descriptors)
- Verify network connectivity to Gen3 commons
- Review logs for specific error messages

### Debugging

Enable verbose logging for detailed debugging:

```bash
gen3 -vv --endpoint my-commons.org --auth credentials.json download-multiple-async \
    --manifest files.json \
    --download-path ./downloads
```

## Examples

### Basic Usage

```bash
# Download files with default settings
gen3 --endpoint data.commons.io --auth creds.json download-multiple-async \
    --manifest my_files.json \
    --download-path ./data
```

### High-Performance Configuration

```bash
# Optimized for high-throughput downloads
gen3 --endpoint data.commons.io --auth creds.json download-multiple-async \
    --manifest large_dataset.json \
    --download-path ./large_downloads \
    --max-concurrent-requests 20 \
    --no-progress \
    --skip-completed
```

**Note**: The specific values shown in examples (like `--max-concurrent-requests 20`) are for demonstration only. For current parameter options and default values, always refer to the command line help: `gen3 download-multiple-async --help`
