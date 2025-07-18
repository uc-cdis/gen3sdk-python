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
     (1)           (1000)         (4×15)           (1000)       (Final)
```

### Key Features

- **Memory Efficiency**: Bounded queues prevent memory explosion with large file sets
- **True Parallelism**: Multiprocessing bypasses Python GIL limitations
- **High Concurrency**: Up to 60 concurrent downloads (4 processes × 15 requests each)
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

```python
from gen3.auth import Gen3Auth
from gen3.file import Gen3File

# Initialize authentication
auth = Gen3Auth(refresh_file="credentials.json")
file_client = Gen3File(auth_provider=auth)

# Manifest data
manifest_data = [
    {"guid": "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"},
    {"guid": "dg.XXTS/6f9a924f-9d83-4597-8f66-fe7d3021729f"},
    {"object_id": "dg.XXTS/181af989-5d66-4139-91e7-69f4570ccd41"}
]

# Download files
import asyncio
result = asyncio.run(file_client.async_download_multiple(
    manifest_data=manifest_data,
    download_path="./downloads",
    filename_format="original",
    max_concurrent_requests=10,
    num_processes=4,
    skip_completed=True,
    no_progress=False
))

print(f"Succeeded: {len(result['succeeded'])}")
print(f"Failed: {len(result['failed'])}")
print(f"Skipped: {len(result['skipped'])}")
```

## Parameters

### Required Parameters

- **manifest_data**: List of dictionaries containing file information
  - Each item must have either `guid` or `object_id` field
  - Additional metadata fields are supported but optional

### Optional Parameters

- **download_path** (str, default: "."): Directory to save downloaded files
- **filename_format** (str, default: "original"): File naming strategy
  - `"original"`: Use original filename from metadata
  - `"guid"`: Use GUID as filename
  - `"combined"`: Combine original name with GUID
- **protocol** (str, optional): Preferred download protocol (e.g., "s3")
- **max_concurrent_requests** (int, default: 10): Maximum concurrent downloads per process
- **num_processes** (int, default: 4): Number of worker processes
- **queue_size** (int, default: 1000): Maximum items in input queue
- **batch_size** (int, default: 100): Number of GUIDs per batch
- **skip_completed** (bool, default: False): Skip files that already exist
- **rename** (bool, default: False): Rename files on conflicts
- **no_progress** (bool, default: False): Disable progress display

## Performance Characteristics

### Throughput Optimization

The method is optimized for high-throughput scenarios:

- **Concurrent Downloads**: Up to 60 simultaneous downloads
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

Detailed results are returned with:

```python
{
    "succeeded": [
        {"guid": "guid1", "filepath": "/path/file1.txt", "size": 1024},
        {"guid": "guid2", "filepath": "/path/file2.txt", "size": 2048}
    ],
    "failed": [
        {"guid": "guid3", "error": "Network timeout", "attempts": 3}
    ],
    "skipped": [
        {"guid": "guid4", "reason": "File already exists"}
    ]
}
```

## Best Practices

### Configuration Recommendations

For optimal performance:

- **Small files (< 1MB)**: Use higher `max_concurrent_requests` (15-20)
- **Large files (> 100MB)**: Use lower `max_concurrent_requests` (5-10)
- **Mixed file sizes**: Use moderate settings (10-15 concurrent requests)
- **High-bandwidth networks**: Increase `num_processes` to 6-8
- **Limited memory**: Reduce `queue_size` and `batch_size`

### Memory Management

- **Queue Size**: Adjust based on available memory (500-2000 items)
- **Batch Size**: Balance between memory usage and overhead (50-200 items)
- **Process Count**: Match available CPU cores (typically 4-8)

### Network Optimization

- **Concurrent Requests**: Match network capacity and server limits
- **Protocol Selection**: Use appropriate protocol for your environment
- **Resume Support**: Enable `skip_completed` for interrupted downloads

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
- Reduce `max_concurrent_requests` if server is overwhelmed
- Verify authentication token is valid

**Memory Issues:**

- Reduce `queue_size` and `batch_size`
- Lower `num_processes` if system memory is limited
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


### Workflow Integration

Common integration patterns:

1. **Query-based downloads**: Generate manifest from Gen3 queries
2. **Batch processing**: Process large datasets in chunks
3. **Resume workflows**: Skip completed files for interrupted downloads
4. **Validation pipelines**: Download and validate file integrity

