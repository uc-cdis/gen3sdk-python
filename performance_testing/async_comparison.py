#!/usr/bin/env python3
"""
Multiple Download Performance Test - Async Comparison
Comparing CDIS Data Client and Gen3 SDK async download-multiple
With configurable test methods and performance monitoring
"""

import json
import logging
import os
import subprocess
import time
import psutil
import shutil
import webbrowser
import cProfile
import pstats
import io
import threading
import asyncio
import sys
import functools
import tracemalloc
import line_profiler
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, field
from statistics import mean, stdev
import zipfile
import math

# Add the parent directory to the path to import config
GEN3_SDK_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, GEN3_SDK_PATH)

# Import config functions first (these should always be available)
try:
    from performance_testing.config import (
        get_config,
        print_config_help,
        create_default_config_file,
    )

    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False
    logging.warning("Performance testing config not available")

# Try to import Gen3 SDK modules
try:
    from gen3.auth import Gen3Auth
    from gen3.file import Gen3File

    GEN3_SDK_AVAILABLE = True
except ImportError:
    GEN3_SDK_AVAILABLE = False
    logging.warning("Gen3 SDK not available for direct API testing")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "async_comparison_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

STATUS_FILE = os.path.join(RESULTS_DIR, "test_status.json")


@dataclass
class CodePerformanceMetrics:
    """Detailed code-level performance metrics."""

    function_name: str
    total_time: float
    total_calls: int
    average_time_per_call: float
    percentage_of_total: float
    line_by_line_timing: Optional[Dict[int, float]] = None
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None


@dataclass
class PerformanceMetrics:
    """Detailed performance metrics for a single test run."""

    tool_name: str
    run_number: int
    workers: int
    total_files: int
    successful_downloads: int
    success_rate: float
    total_download_time: float
    total_size_mb: float
    average_throughput_mbps: float
    files_per_second: float
    peak_memory_mb: float
    avg_memory_mb: float
    peak_cpu_percent: float
    avg_cpu_percent: float
    setup_time: float
    download_time: float
    verification_time: float
    return_code: int
    file_details: List[Dict] = field(default_factory=list)
    profiling_stats: Optional[str] = None
    profiling_analysis: Optional[str] = None
    error_details: Optional[str] = None
    code_performance_metrics: List[CodePerformanceMetrics] = field(default_factory=list)
    memory_timeline: List[Dict[str, float]] = field(default_factory=list)
    cpu_timeline: List[Dict[str, float]] = field(default_factory=list)
    network_io_metrics: Optional[Dict[str, float]] = None
    disk_io_metrics: Optional[Dict[str, float]] = None
    bottleneck_analysis: Optional[str] = None


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Gen3 SDK Performance Testing Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python async_comparison.py

  # Use configuration file
  python async_comparison.py --config performance_config.json

  # Quick test with environment variables
  PERF_NUM_RUNS=1 python async_comparison.py

  # Show configuration help
  python async_comparison.py --config-help

  # Create default config file
  python async_comparison.py --create-config
        """,
    )

    parser.add_argument(
        "--config", type=str, help="Path to configuration file (JSON format)"
    )

    parser.add_argument(
        "--config-help",
        action="store_true",
        help="Show configuration options and environment variables",
    )

    parser.add_argument(
        "--create-config",
        action="store_true",
        help="Create a default configuration file",
    )

    parser.add_argument(
        "--manifest", type=str, help="Path to manifest file (overrides config)"
    )

    parser.add_argument(
        "--credentials", type=str, help="Path to credentials file (overrides config)"
    )

    parser.add_argument(
        "--endpoint", type=str, help="Gen3 endpoint URL (overrides config)"
    )

    parser.add_argument(
        "--results-dir", type=str, help="Results directory (overrides config)"
    )

    parser.add_argument(
        "--num-runs", type=int, help="Number of test runs (overrides config)"
    )

    parser.add_argument(
        "--max-concurrent-async",
        type=int,
        help="Max concurrent requests for async (overrides config)",
    )

    parser.add_argument(
        "--num-workers-cdis", type=int, help="Number of CDIS workers (overrides config)"
    )

    parser.add_argument(
        "--test-methods",
        type=str,
        help="Comma-separated list of test methods (overrides config)",
    )

    parser.add_argument(
        "--enable-profiling",
        action="store_true",
        help="Enable profiling (overrides config)",
    )

    parser.add_argument(
        "--disable-profiling",
        action="store_true",
        help="Disable profiling (overrides config)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level (overrides config)",
    )

    return parser.parse_args()


def setup_configuration(args):
    """Setup configuration from arguments and environment."""
    # Get base configuration
    config = get_config(args.config)

    # Override with command line arguments
    if args.manifest:
        config.manifest_path = args.manifest
    if args.credentials:
        config.credentials_path = args.credentials
    if args.endpoint:
        config.endpoint = args.endpoint
    if args.results_dir:
        config.results_dir = args.results_dir
    if args.num_runs:
        config.num_runs = args.num_runs
    if args.max_concurrent_async:
        config.max_concurrent_requests_async = args.max_concurrent_async
    if args.num_workers_cdis:
        config.num_workers_cdis = args.num_workers_cdis
    if args.test_methods:
        config.test_methods = [
            method.strip() for method in args.test_methods.split(",")
        ]
    if args.log_level:
        config.log_level = args.log_level

    # Handle profiling flags
    if args.enable_profiling:
        config.enable_profiling = True
    elif args.disable_profiling:
        config.enable_profiling = False

    return config


def setup_logging(config):
    """Set up logging configuration."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if config.log_file:
        log_file = config.log_file
    else:
        log_file = f"{RESULTS_DIR}/async_comparison_{timestamp}.log"

    # Create results directory if specified
    if config.results_dir:
        os.makedirs(config.results_dir, exist_ok=True)
        log_file = os.path.join(config.results_dir, f"async_comparison_{timestamp}.log")

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )

    logger = logging.getLogger(__name__)
    logger.info(f"📝 Logging to: {log_file}")
    return logger


class TestConfiguration:
    """Configuration for the performance test."""

    def __init__(self, config):
        self.num_runs = config.num_runs
        self.enable_profiling = config.enable_profiling
        self.enable_real_time_monitoring = config.enable_real_time_monitoring
        self.monitoring_interval = config.monitoring_interval
        self.filter_medium_files = config.filter_medium_files
        self.force_uncompressed_cdis = config.force_uncompressed_cdis
        self.auto_extract_cdis = config.auto_extract_cdis

        self.max_concurrent_requests_async = config.max_concurrent_requests_async
        self.num_workers_cdis = config.num_workers_cdis

        self.enable_line_profiling = config.enable_line_profiling
        self.enable_memory_profiling = config.enable_memory_profiling
        self.enable_network_monitoring = config.enable_network_monitoring
        self.enable_disk_io_monitoring = config.enable_disk_io_monitoring
        self.profile_specific_functions = config.profile_specific_functions

        self.test_methods = config.test_methods

        # Add missing attributes
        self.manifest_path = config.manifest_path
        self.credentials_path = config.credentials_path
        self.endpoint = config.endpoint
        self.gen3_client_path = config.gen3_client_path
        self.results_dir = config.results_dir

        self.AVAILABLE_METHODS = ["async", "cdis"]


class PerformanceProfiler:
    """Performance profiler with detailed code analysis."""

    def __init__(self, config: TestConfiguration):
        self.config = config
        self.profiler = cProfile.Profile()
        self.line_profiler = None
        self.memory_snapshots = []
        self.function_timings = {}
        self.start_time = None

        if config.enable_line_profiling:
            try:
                self.line_profiler = line_profiler.LineProfiler()
            except ImportError:
                logging.warning("line_profiler not available, line profiling disabled")

        if config.enable_memory_profiling:
            tracemalloc.start()

    def start_profiling(self):
        """Start performance profiling."""
        if not self.config.enable_profiling:
            return

        try:
            # Disable any existing profilers
            cProfile._current_profiler = None
            import sys

            if hasattr(sys, "setprofile"):
                sys.setprofile(None)

            self.profiler = cProfile.Profile()
            self.profiler.enable()

            if self.config.enable_memory_profiling:
                tracemalloc.start()
                self.memory_start_snapshot = tracemalloc.take_snapshot()
        except Exception as e:
            logging.warning(f"Failed to start profiling: {e}")
            # Continue without profiling
            self.config.enable_profiling = False

    def stop_profiling(self) -> Dict[str, Any]:
        """Stop profiling and return analysis."""
        if not self.config.enable_profiling:
            return {}

        try:
            self.profiler.disable()

            # Get profiling stats
            stats_stream = io.StringIO()
            stats = pstats.Stats(self.profiler, stream=stats_stream)
            stats.sort_stats("cumulative")
            stats.print_stats(20)

            # Memory profiling
            memory_analysis = {}
            if self.config.enable_memory_profiling and hasattr(
                self, "memory_start_snapshot"
            ):
                try:
                    current_snapshot = tracemalloc.take_snapshot()
                    memory_analysis = self._analyze_memory_usage(current_snapshot)
                    tracemalloc.stop()
                except Exception as e:
                    logging.warning(f"Memory profiling error: {e}")

            # Extract function metrics
            function_metrics = self._extract_function_metrics(stats)

            return {
                "stats_text": stats_stream.getvalue(),
                "function_metrics": function_metrics,
                "memory_analysis": memory_analysis,
                "line_profiling": self._get_line_profiling()
                if self.config.enable_line_profiling
                else {},
            }
        except Exception as e:
            logging.warning(f"Error stopping profiling: {e}")
            return {}

    def _extract_function_metrics(
        self, stats: pstats.Stats
    ) -> List[CodePerformanceMetrics]:
        """Extract detailed metrics for each function."""
        metrics = []
        total_time = stats.total_tt

        try:
            stats_list = []
            for func, (cc, nc, tt, ct, callers) in stats.stats.items():
                if tt > 0.01:  # Only include functions taking more than 10ms
                    percentage = (tt / total_time) * 100 if total_time > 0 else 0

                    metric = CodePerformanceMetrics(
                        function_name=str(func),
                        total_time=tt,
                        total_calls=nc,
                        average_time_per_call=tt / nc if nc > 0 else 0,
                        percentage_of_total=percentage,
                    )
                    metrics.append(metric)
        except Exception as e:
            print(f"Profiling extraction failed: {e}")
            if total_time > 0:
                metric = CodePerformanceMetrics(
                    function_name="total_execution",
                    total_time=total_time,
                    total_calls=1,
                    average_time_per_call=total_time,
                    percentage_of_total=100.0,
                )
                metrics.append(metric)

        return sorted(metrics, key=lambda x: x.total_time, reverse=True)

    def _analyze_memory_usage(self, final_snapshot) -> Dict[str, Any]:
        """Analyze memory usage patterns."""
        if not final_snapshot or not self.memory_snapshots:
            return {}

        initial_snapshot = self.memory_snapshots[0]
        stats = final_snapshot.compare_to(initial_snapshot, "lineno")

        memory_analysis = {
            "total_memory_allocated": final_snapshot.statistics("traceback")[0].size,
            "memory_growth": final_snapshot.statistics("traceback")[0].size
            - initial_snapshot.statistics("traceback")[0].size,
            "top_memory_consumers": [],
        }

        for stat in stats[:10]:
            memory_analysis["top_memory_consumers"].append(
                {
                    "file": stat.traceback.format()[-1],
                    "size_diff": stat.size_diff,
                    "count_diff": stat.count_diff,
                }
            )

        return memory_analysis

    def _get_line_profiling(self) -> Dict[str, Any]:
        """Get line-by-line profiling data."""
        if not self.line_profiler:
            return {}

        line_profiling = {}
        for func_name, (
            code,
            first_lineno,
            func,
        ) in self.line_profiler.code_map.items():
            if func_name in self.config.profile_specific_functions:
                line_stats = self.line_profiler.get_stats()
                if func_name in line_stats:
                    line_profiling[str(func_name)] = {
                        "line_timings": line_stats[func_name].timings,
                        "line_hits": line_stats[func_name].hits,
                    }

        return line_profiling


class NetworkIOMonitor:
    """Monitor network I/O during downloads."""

    def __init__(self):
        self.start_stats = None
        self.end_stats = None

    def start_monitoring(self):
        """Start network monitoring."""
        self.start_stats = psutil.net_io_counters()

    def stop_monitoring(self) -> Dict[str, float]:
        """Stop monitoring and return network metrics."""
        if not self.start_stats:
            return {}

        self.end_stats = psutil.net_io_counters()

        bytes_sent = self.end_stats.bytes_sent - self.start_stats.bytes_sent
        bytes_recv = self.end_stats.bytes_recv - self.start_stats.bytes_recv
        packets_sent = self.end_stats.packets_sent - self.start_stats.packets_sent
        packets_recv = self.end_stats.packets_recv - self.start_stats.packets_recv

        return {
            "bytes_sent_mb": bytes_sent / (1024 * 1024),
            "bytes_received_mb": bytes_recv / (1024 * 1024),
            "packets_sent": packets_sent,
            "packets_received": packets_recv,
            "total_network_io_mb": (bytes_sent + bytes_recv) / (1024 * 1024),
        }


class DiskIOMonitor:
    """Monitor disk I/O during downloads."""

    def __init__(self):
        self.start_stats = None
        self.end_stats = None

    def start_monitoring(self):
        """Start disk I/O monitoring."""
        self.start_stats = psutil.disk_io_counters()

    def stop_monitoring(self) -> Dict[str, float]:
        """Stop monitoring and return disk I/O metrics."""
        if not self.start_stats:
            return {}

        self.end_stats = psutil.disk_io_counters()

        read_bytes = self.end_stats.read_bytes - self.start_stats.read_bytes
        write_bytes = self.end_stats.write_bytes - self.start_stats.write_bytes
        read_count = self.end_stats.read_count - self.start_stats.read_count
        write_count = self.end_stats.write_count - self.start_stats.write_count

        return {
            "read_bytes_mb": read_bytes / (1024 * 1024),
            "write_bytes_mb": write_bytes / (1024 * 1024),
            "read_count": read_count,
            "write_count": write_count,
            "total_disk_io_mb": (read_bytes + write_bytes) / (1024 * 1024),
        }


def performance_timer(func):
    """Decorator to time function execution."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        if not hasattr(wrapper, "timings"):
            wrapper.timings = []
        wrapper.timings.append(
            {
                "function": func.__name__,
                "execution_time": end_time - start_time,
                "timestamp": datetime.now().isoformat(),
            }
        )

        return result

    return wrapper


def analyze_bottlenecks(metrics: PerformanceMetrics) -> str:
    """Analyze performance bottlenecks from collected metrics."""
    analysis = []

    if metrics.code_performance_metrics:
        analysis.append("🔍 FUNCTION-LEVEL BOTTLENECKS:")
        for metric in metrics.code_performance_metrics[:5]:  # Top 5
            analysis.append(
                f"  • {metric.function_name}: {metric.total_time:.3f}s ({metric.percentage_of_total:.1f}%)"
            )

    if metrics.memory_timeline:
        peak_memory = max(m["memory_mb"] for m in metrics.memory_timeline)
        avg_memory = mean(m["memory_mb"] for m in metrics.memory_timeline)
        analysis.append(f"\n💾 MEMORY ANALYSIS:")
        analysis.append(f"  • Peak Memory: {peak_memory:.1f} MB")
        analysis.append(f"  • Average Memory: {avg_memory:.1f} MB")

        if peak_memory > 2000:  # 2GB threshold
            analysis.append(
                "  ⚠️ High memory usage detected - consider optimizing memory usage"
            )

    if metrics.cpu_timeline:
        peak_cpu = max(m["cpu_percent"] for m in metrics.cpu_timeline)
        avg_cpu = mean(m["cpu_percent"] for m in metrics.cpu_timeline)
        analysis.append(f"\n🖥️ CPU ANALYSIS:")
        analysis.append(f"  • Peak CPU: {peak_cpu:.1f}%")
        analysis.append(f"  • Average CPU: {avg_cpu:.1f}%")

        if peak_cpu > 90:
            analysis.append(
                "  ⚠️ High CPU usage detected - consider reducing concurrency"
            )

    if metrics.network_io_metrics:
        analysis.append(f"\n🌐 NETWORK I/O ANALYSIS:")
        analysis.append(
            f"  • Data Received: {metrics.network_io_metrics.get('bytes_received_mb', 0):.1f} MB"
        )
        analysis.append(
            f"  • Data Sent: {metrics.network_io_metrics.get('bytes_sent_mb', 0):.1f} MB"
        )
        analysis.append(
            f"  • Total Network I/O: {metrics.network_io_metrics.get('total_network_io_mb', 0):.1f} MB"
        )

    if metrics.disk_io_metrics:
        analysis.append(f"\n💿 DISK I/O ANALYSIS:")
        analysis.append(
            f"  • Data Read: {metrics.disk_io_metrics.get('read_bytes_mb', 0):.1f} MB"
        )
        analysis.append(
            f"  • Data Written: {metrics.disk_io_metrics.get('write_bytes_mb', 0):.1f} MB"
        )
        analysis.append(
            f"  • Total Disk I/O: {metrics.disk_io_metrics.get('total_disk_io_mb', 0):.1f} MB"
        )

    analysis.append(f"\n💡 PERFORMANCE RECOMMENDATIONS:")

    if metrics.average_throughput_mbps < 10:
        analysis.append(
            "  • Low throughput detected - check network connection and server performance"
        )

    if metrics.success_rate < 90:
        analysis.append(
            "  • Low success rate - check authentication and file availability"
        )

    if metrics.peak_memory_mb > 2000:
        analysis.append(
            "  • High memory usage - consider reducing concurrent downloads"
        )

    if metrics.peak_cpu_percent > 90:
        analysis.append("  • High CPU usage - consider reducing worker count")

    return "\n".join(analysis)


def update_status(status: str, current_tool: str = "", progress: float = 0.0):
    """Update status file for monitoring."""
    status_data = {
        "timestamp": datetime.now().isoformat(),
        "status": status,
        "current_tool": current_tool,
        "progress_percent": progress,
        "pid": os.getpid(),
    }
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump(status_data, f, indent=2)
    except Exception as e:
        logging.warning(f"Failed to update status file: {e}")


class RealTimeMonitor:
    """Real-time system monitoring during downloads."""

    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.monitoring = False
        self.metrics = []
        self.thread = None

    def start_monitoring(self):
        """Start real-time monitoring."""
        self.monitoring = True
        self.metrics = []
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.daemon = True
        self.thread.start()

    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop monitoring and return aggregated metrics."""
        self.monitoring = False
        if self.thread:
            self.thread.join(timeout=2.0)

        if not self.metrics:
            return {}

        cpu_values = [m["cpu_percent"] for m in self.metrics]
        memory_values = [m["memory_mb"] for m in self.metrics]

        return {
            "peak_memory_mb": max(memory_values),
            "avg_memory_mb": mean(memory_values),
            "peak_cpu_percent": max(cpu_values),
            "avg_cpu_percent": mean(cpu_values),
            "sample_count": len(self.metrics),
            "duration": len(self.metrics) * self.interval,
        }

    def _monitor_loop(self):
        """Internal monitoring loop."""
        while self.monitoring:
            try:
                memory_info = psutil.virtual_memory()
                cpu_percent = psutil.cpu_percent()

                self.metrics.append(
                    {
                        "timestamp": time.time(),
                        "cpu_percent": cpu_percent,
                        "memory_mb": memory_info.used / (1024 * 1024),
                        "memory_percent": memory_info.percent,
                    }
                )

                time.sleep(self.interval)
            except Exception:
                break


def filter_medium_files(
    manifest_data: List[Dict], logger: logging.Logger
) -> List[Dict]:
    """Filter manifest for medium-sized files (1MB - 100MB)."""
    filtered_files = []
    min_size = 1 * 1024 * 1024  # 1MB
    max_size = 100 * 1024 * 1024  # 100MB

    for file_entry in manifest_data:
        file_size = file_entry.get("file_size", 0)
        if min_size <= file_size <= max_size:
            filtered_files.append(file_entry)

    logger.info(
        f"🎯 Filtered to {len(filtered_files)} medium-sized files ({min_size / (1024 * 1024):.0f}MB - {max_size / (1024 * 1024):.0f}MB) from {len(manifest_data)} total files"
    )
    return filtered_files


def extract_cdis_files(
    download_dir: str, config: TestConfiguration, logger: logging.Logger
) -> int:
    """Extract CDIS zip files for fair comparison and return total extracted size."""
    if not config.auto_extract_cdis or not os.path.exists(download_dir):
        return 0

    total_extracted_size = 0
    zip_files = []

    for root, dirs, files in os.walk(download_dir):
        for file in files:
            if file.endswith(".zip"):
                zip_files.append(os.path.join(root, file))

    logger.info(f"🗜️ Extracting {len(zip_files)} CDIS zip files for fair comparison...")

    for zip_path in zip_files:
        try:
            extract_dir = zip_path.replace(".zip", "_extracted")
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

                for extracted_file in zip_ref.namelist():
                    extracted_path = os.path.join(extract_dir, extracted_file)
                    if os.path.isfile(extracted_path):
                        total_extracted_size += os.path.getsize(extracted_path)

            logger.debug(f"✅ Extracted: {os.path.basename(zip_path)}")

        except Exception as e:
            logger.warning(f"⚠️ Failed to extract {os.path.basename(zip_path)}: {e}")

    logger.info(
        f"📊 CDIS extraction complete - Total uncompressed size: {total_extracted_size / 1024 / 1024:.2f}MB"
    )
    return total_extracted_size


def verify_prerequisites(
    logger: logging.Logger,
    gen3_client_path: str,
    credentials_path: str,
    manifest_path: str,
) -> bool:
    """Verify that all required tools and files are available."""
    logger.info("🔍 Verifying prerequisites...")

    if not os.path.exists(gen3_client_path):
        logger.error(f"❌ Missing CDIS client: {gen3_client_path}")
        return False
    else:
        logger.info("✅ CDIS client is available")

    if not os.path.exists(credentials_path):
        logger.error(f"❌ Missing credentials: {credentials_path}")
        return False
    else:
        logger.info("✅ Credentials file found")

    if not os.path.exists(manifest_path):
        logger.error(f"❌ Missing manifest: {manifest_path}")
        return False
    else:
        logger.info("✅ Manifest file found")

    try:
        result = subprocess.run(
            [
                "python",
                "-c",
                "import gen3.auth; import gen3.file; print('✅ Gen3 SDK imports successful')",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=GEN3_SDK_PATH,
            env={"PYTHONPATH": GEN3_SDK_PATH},
        )
        if result.returncode == 0:
            logger.info("✅ Gen3 SDK core modules are importable")
        else:
            logger.warning(f"⚠️ Gen3 SDK import issues: {result.stderr}")
    except Exception as e:
        logger.warning(f"⚠️ Gen3 SDK import test failed: {e}")

    return True


def verify_credentials(
    logger: logging.Logger, credentials_path: str, endpoint: str
) -> bool:
    """Verify that credentials are working by testing authentication."""
    logger.info("🔐 Verifying credentials...")

    try:
        result = subprocess.run(
            [
                "python",
                "-c",
                f"import gen3.auth; "
                f"auth = gen3.auth.Gen3Auth(refresh_file='{credentials_path}', endpoint='{endpoint}'); "
                f"print('✅ Auth successful' if auth.get_access_token() else '❌ Auth failed')",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0 and "✅ Auth successful" in result.stdout:
            logger.info("✅ Credentials are valid and working")
            return True
        else:
            logger.warning(
                f"⚠️ Credential verification failed, but continuing with tests: {result.stderr or result.stdout}"
            )
            return True

    except Exception as e:
        logger.warning(f"⚠️ Credential verification error, but continuing: {e}")
        return True


def cleanup_previous_downloads(logger: logging.Logger) -> None:
    """Clean up all previously downloaded files."""
    logger.info("🧹 Cleaning up previously downloaded files...")

    download_dirs = [
        f"{RESULTS_DIR}/cdis_client",
        f"{RESULTS_DIR}/sdk_download_async",
    ]

    for dir_path in download_dirs:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path)
                logger.info(f"🗑️ Removed {dir_path}")
            except Exception as e:
                logger.warning(f"⚠️ Could not clean {dir_path}: {e}")


def analyze_profiling_stats(
    profiler: cProfile.Profile, tool_name: str, run_number: int, logger: logging.Logger
) -> str:
    """Analyze profiling statistics and return detailed breakdown."""
    if not profiler:
        return ""

    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)

    total_calls = ps.total_calls
    total_time = ps.total_tt

    ps.sort_stats("cumulative")
    ps.print_stats(15)  # Top 15 by cumulative time
    cumulative_output = s.getvalue()

    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.sort_stats("tottime")
    ps.print_stats(15)  # Top 15 by total time
    tottime_output = s.getvalue()

    analysis = f"""
{tool_name} Profiling (Run {run_number})
Total Function Calls: {total_calls:,} in {total_time:.3f} seconds

Top Performance Bottlenecks (Cumulative Time):"""

    cumulative_lines = cumulative_output.split("\n")
    bottleneck_count = 0
    for line in cumulative_lines:
        if any(
            keyword in line.lower()
            for keyword in [
                "subprocess.py",
                "selectors.py",
                "time.sleep",
                "select.poll",
                "psutil",
                "communicate",
                "socket",
                "ssl",
                "urllib",
                "requests",
                "threading",
                "asyncio",
                "concurrent.futures",
            ]
        ):
            if any(char.isdigit() for char in line) and bottleneck_count < 10:
                cleaned_line = " ".join(line.split())
                if "seconds" in cleaned_line or any(
                    c.isdigit() for c in cleaned_line.split()[:3]
                ):
                    analysis += f"\n  {cleaned_line}"
                    bottleneck_count += 1

    analysis += f"\n\nTop Time Consumers (Total Time):"
    tottime_lines = tottime_output.split("\n")
    time_count = 0
    for line in tottime_lines:
        if any(char.isdigit() for char in line) and time_count < 5:
            parts = line.split()
            if len(parts) >= 4:
                try:
                    time_val = float(parts[3]) if len(parts) > 3 else 0
                    if time_val > 0.1:  # Only show functions taking > 0.1s
                        cleaned_line = " ".join(line.split())
                        analysis += f"\n  {cleaned_line}"
                        time_count += 1
                except (ValueError, IndexError):
                    continue

    analysis += f"\n\nPerformance Insights:"
    if "subprocess" in cumulative_output.lower():
        analysis += f"\n  • High subprocess overhead detected - consider optimizing external calls"
    if "time.sleep" in cumulative_output.lower():
        analysis += (
            f"\n  • Sleep/wait operations found - potential for async optimization"
        )
    if (
        "selectors" in cumulative_output.lower()
        or "select" in cumulative_output.lower()
    ):
        analysis += (
            f"\n  • I/O blocking detected - async operations could improve performance"
        )
    if "psutil" in cumulative_output.lower():
        analysis += (
            f"\n  • System monitoring overhead - consider reducing monitoring frequency"
        )

    if total_time > 0:
        calls_per_second = total_calls / total_time
        analysis += (
            f"\n  • Function calls efficiency: {calls_per_second:,.0f} calls/second"
        )

    return analysis


def find_matching_files_improved(
    download_dir: str,
    manifest_data: List[Dict],
    logger: logging.Logger,
) -> Tuple[List[str], List[Dict]]:
    """Improved file matching that handles CDIS client's nested directory structure and Gen3 SDK GUID-based files."""
    if not os.path.exists(download_dir):
        logger.warning(f"Download directory does not exist: {download_dir}")
        return [], []

    all_files = []

    for root, dirs, files in os.walk(download_dir):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)

    logger.debug(f"Found {len(all_files)} total files in download directory")

    matched_files = []
    file_details = []

    for entry in manifest_data:
        object_id = entry.get("object_id", "")
        expected_filename = entry.get("file_name", "")
        expected_size = entry.get("file_size", 0)

        if "/" in object_id:
            guid = object_id.split("/")[-1]
        else:
            guid = object_id

        logger.debug(
            f"Looking for file with GUID: {guid}, expected filename: {expected_filename}"
        )

        best_match = None
        best_score = 0

        for file_path in all_files:
            file_basename = os.path.basename(file_path)
            file_dirname = os.path.dirname(file_path)
            score = 0

            if guid and guid.lower() == file_basename.lower():
                score += 1000  # Very high priority for exact GUID match
                logger.debug(f"Exact GUID match found: {file_basename}")
            elif guid and guid.lower() in file_basename.lower():
                score += 800  # GUID appears in filename
                logger.debug(f"GUID in filename: {file_basename}")

            if expected_filename and expected_filename.lower() == file_basename.lower():
                score += 500
                logger.debug(f"Exact filename match: {file_basename}")
            elif (
                expected_filename and expected_filename.lower() in file_basename.lower()
            ):
                score += 300
            elif (
                expected_filename and file_basename.lower() in expected_filename.lower()
            ):
                score += 200

            if guid and guid.lower() in file_path.lower():
                score += 100
                logger.debug(f"GUID in path: {file_path}")

            if object_id and object_id.lower() in file_path.lower():
                score += 80

            try:
                file_size = os.path.getsize(file_path)
                if file_size == expected_size:
                    score += 50
                    logger.debug(f"Exact size match: {file_size} bytes")
                elif abs(file_size - expected_size) < max(
                    1024 * 1024, expected_size * 0.1
                ):
                    score += 20  # Within 1MB or 10% of expected size
                    logger.debug(
                        f"Close size match: {file_size} vs {expected_size} bytes"
                    )
            except:
                pass

            if "_extracted" in file_path and not file_path.endswith(".zip"):
                score += 10

            if "dg.MD1R" in file_path and guid:
                if guid.lower() in file_path.lower():
                    score += 30

            if expected_filename and any(
                ext in expected_filename.lower() for ext in [".nii.gz", ".nii", ".dcm"]
            ):
                if any(
                    ext in file_basename.lower() for ext in [".nii.gz", ".nii", ".dcm"]
                ):
                    score += 15

            if score > best_score:
                best_score = score
                best_match = file_path

        if best_match and best_score >= 50:
            matched_files.append(best_match)

            try:
                actual_size = os.path.getsize(best_match)
                size_match_percent = (
                    (min(actual_size, expected_size) / max(actual_size, expected_size))
                    * 100
                    if max(actual_size, expected_size) > 0
                    else 0
                )

                guid_verified = guid and guid.lower() in best_match.lower()

            except:
                actual_size = 0
                size_match_percent = 0
                guid_verified = False

            file_details.append(
                {
                    "object_id": object_id,
                    "guid": guid,
                    "expected_filename": expected_filename,
                    "actual_path": os.path.relpath(best_match, download_dir),
                    "expected_size": expected_size,
                    "actual_size": actual_size,
                    "size_match_percent": size_match_percent,
                    "match_score": best_score,
                    "match_type": "improved_guid_scoring",
                    "guid_verified": guid_verified,
                }
            )

            logger.debug(
                f"✅ Matched (score={best_score}, GUID verified={guid_verified}): {expected_filename} -> {os.path.relpath(best_match, download_dir)}"
            )
        else:
            logger.warning(
                f"❌ No match found for: {expected_filename} (object_id: {object_id}, guid: {guid}) - best score: {best_score}"
            )

            file_details.append(
                {
                    "object_id": object_id,
                    "guid": guid,
                    "expected_filename": expected_filename,
                    "actual_path": "NOT_FOUND",
                    "expected_size": expected_size,
                    "actual_size": 0,
                    "size_match_percent": 0,
                    "match_score": best_score,
                    "match_type": "failed_match",
                    "guid_verified": False,
                }
            )

    guid_verified_count = sum(
        1 for detail in file_details if detail.get("guid_verified", False)
    )
    logger.info(
        f"✅ Successfully matched {len(matched_files)}/{len(manifest_data)} files, "
        f"GUID verified: {guid_verified_count}/{len(manifest_data)}"
    )

    return matched_files, file_details


def create_filtered_manifest(
    original_manifest: str, filtered_data: List[Dict], logger: logging.Logger
) -> str:
    """Create a filtered manifest file with only the selected data."""
    filtered_manifest_path = f"{RESULTS_DIR}/filtered_manifest.json"
    with open(filtered_manifest_path, "w") as f:
        json.dump(filtered_data, f, indent=2)
    logger.info(f"📝 Created filtered manifest with {len(filtered_data)} files")
    return filtered_manifest_path


def run_tool_with_profiling(
    cmd: List[str],
    download_dir: str,
    manifest_path: str,
    tool_name: str,
    config: TestConfiguration,
    run_number: int,
    logger: logging.Logger,
    working_dir: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    gen3_client_path: str = None,
    credentials_path: str = None,
    endpoint: str = None,
) -> PerformanceMetrics:
    """Run a tool with detailed performance metrics and profiling."""

    monitor = (
        RealTimeMonitor(config.monitoring_interval)
        if config.enable_real_time_monitoring
        else None
    )

    profiler = PerformanceProfiler(config) if config.enable_profiling else None

    network_monitor = NetworkIOMonitor() if config.enable_network_monitoring else None
    disk_monitor = DiskIOMonitor() if config.enable_disk_io_monitoring else None

    total_start_time = time.time()

    with open(manifest_path, "r") as f:
        manifest_data = json.load(f)

    setup_start_time = time.time()

    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    if "gen3-client" in cmd[0] and gen3_client_path and credentials_path and endpoint:
        configure_cmd = [
            gen3_client_path,
            "configure",
            f"--profile=midrc",
            f"--cred={credentials_path}",
            f"--apiendpoint={endpoint}",
        ]
        try:
            subprocess.run(configure_cmd, capture_output=True, text=True, timeout=30)
        except Exception as e:
            logger.warning(f"Configuration warning: {e}")

    setup_time = time.time() - setup_start_time

    logger.info(
        f"🔧 {tool_name} Run {run_number}: Starting download of {len(manifest_data)} files..."
    )

    update_status("Running tests", tool_name, 0.0)

    if monitor:
        monitor.start_monitoring()

    if profiler:
        profiler.start_profiling()

    if network_monitor:
        network_monitor.start_monitoring()

    if disk_monitor:
        disk_monitor.start_monitoring()

    download_start_time = time.time()

    try:
        run_env = os.environ.copy()
        if env:
            run_env.update(env)

        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=working_dir, env=run_env
        )

        download_end_time = time.time()

        monitoring_stats = monitor.stop_monitoring() if monitor else {}
        profiling_results = profiler.stop_profiling() if profiler else {}
        network_metrics = network_monitor.stop_monitoring() if network_monitor else {}
        disk_metrics = disk_monitor.stop_monitoring() if disk_monitor else {}

        if result.returncode != 0 or result.stderr:
            logger.warning(
                f"⚠️ {tool_name} Run {run_number} had issues: "
                f"return_code={result.returncode}, "
                f"stderr='{result.stderr[:500]}...'"
                if len(result.stderr) > 500
                else f"stderr='{result.stderr}'"
            )

        if result.stdout and "Failed" in result.stdout:
            logger.warning(
                f"⚠️ {tool_name} Run {run_number} stdout indicates failures: "
                f"'{result.stdout[:500]}...'"
                if len(result.stdout) > 500
                else f"'{result.stdout}'"
            )

        verification_start_time = time.time()

        if "gen3-client" in cmd[0] and config.auto_extract_cdis:
            extract_cdis_files(download_dir, config, logger)

        matched_files, file_details = find_matching_files_improved(
            download_dir, manifest_data, logger
        )
        verification_time = time.time() - verification_start_time

        if file_details:
            total_size_mb = sum(
                d.get("actual_size_for_calc", d.get("actual_size", 0))
                for d in file_details
            ) / (1024 * 1024)
        else:
            total_size_mb = sum(
                os.path.getsize(f) for f in matched_files if os.path.exists(f)
            ) / (1024 * 1024)

        download_time = download_end_time - download_start_time
        total_time = time.time() - total_start_time
        throughput = total_size_mb / download_time if download_time > 0 else 0
        files_per_second = (
            len(matched_files) / download_time if download_time > 0 else 0
        )
        success_rate = (len(matched_files) / len(manifest_data)) * 100

        profiling_stats = None
        profiling_analysis = ""
        if profiler and profiling_results:
            profiling_stats = profiling_results.get(
                "stats_text", "No profiling data available"
            )
            profiling_analysis = f"""
Gen3 SDK (async) Profiling (Run {run_number})
Total Function Calls: {len(profiling_results.get("function_metrics", []))} functions analyzed

Top Performance Bottlenecks:
{profiling_stats[:1000]}"""

        code_performance_metrics = []
        if profiling_results and "function_metrics" in profiling_results:
            code_performance_metrics = profiling_results["function_metrics"]

        memory_timeline = []
        cpu_timeline = []
        if monitoring_stats:
            memory_timeline = [
                {
                    "timestamp": m.get("timestamp", 0),
                    "memory_mb": m.get("memory_mb", 0),
                    "memory_percent": m.get("memory_percent", 0),
                }
                for m in monitoring_stats.get("metrics", [])
            ]
            cpu_timeline = [
                {
                    "timestamp": m.get("timestamp", 0),
                    "cpu_percent": m.get("cpu_percent", 0),
                }
                for m in monitoring_stats.get("metrics", [])
            ]

        bottleneck_analysis = analyze_bottlenecks(
            PerformanceMetrics(
                tool_name=tool_name,
                run_number=run_number,
                workers=config.num_workers_cdis,
                total_files=len(manifest_data),
                successful_downloads=len(matched_files),
                success_rate=success_rate,
                total_download_time=total_time,
                total_size_mb=total_size_mb,
                average_throughput_mbps=throughput,
                files_per_second=files_per_second,
                peak_memory_mb=monitoring_stats.get("peak_memory_mb", 0),
                avg_memory_mb=monitoring_stats.get("avg_memory_mb", 0),
                peak_cpu_percent=monitoring_stats.get("peak_cpu_percent", 0),
                avg_cpu_percent=monitoring_stats.get("avg_cpu_percent", 0),
                setup_time=setup_time,
                download_time=download_time,
                verification_time=verification_time,
                return_code=result.returncode,
                file_details=file_details,
                profiling_stats=profiling_stats,
                profiling_analysis=profiling_analysis,
                code_performance_metrics=code_performance_metrics,
                memory_timeline=memory_timeline,
                cpu_timeline=cpu_timeline,
                network_io_metrics=network_metrics,
                disk_io_metrics=disk_metrics,
                bottleneck_analysis=None,  # Will be set below
            )
        )

        logger.info(
            f"📊 {tool_name} Run {run_number}: {len(matched_files)}/{len(manifest_data)} files, "
            f"{success_rate:.1f}% success, {throughput:.2f} MB/s, {download_time:.1f}s"
        )

        if code_performance_metrics:
            logger.info(f"🔍 Top performance bottlenecks for {tool_name}:")
            for metric in code_performance_metrics[:3]:
                logger.info(
                    f"  • {metric.function_name}: {metric.total_time:.3f}s ({metric.percentage_of_total:.1f}%)"
                )

        if network_metrics:
            logger.info(
                f"🌐 Network I/O: {network_metrics.get('total_network_io_mb', 0):.1f} MB"
            )

        if disk_metrics:
            logger.info(
                f"💿 Disk I/O: {disk_metrics.get('total_disk_io_mb', 0):.1f} MB"
            )

        return PerformanceMetrics(
            tool_name=tool_name,
            run_number=run_number,
            workers=config.num_workers_cdis,
            total_files=len(manifest_data),
            successful_downloads=len(matched_files),
            success_rate=success_rate,
            total_download_time=total_time,
            total_size_mb=total_size_mb,
            average_throughput_mbps=throughput,
            files_per_second=files_per_second,
            peak_memory_mb=monitoring_stats.get("peak_memory_mb", 0),
            avg_memory_mb=monitoring_stats.get("avg_memory_mb", 0),
            peak_cpu_percent=monitoring_stats.get("peak_cpu_percent", 0),
            avg_cpu_percent=monitoring_stats.get("avg_cpu_percent", 0),
            setup_time=setup_time,
            download_time=download_time,
            verification_time=verification_time,
            return_code=result.returncode,
            file_details=file_details,
            profiling_stats=profiling_stats,
            profiling_analysis=profiling_analysis,
            code_performance_metrics=code_performance_metrics,
            memory_timeline=memory_timeline,
            cpu_timeline=cpu_timeline,
            network_io_metrics=network_metrics,
            disk_io_metrics=disk_metrics,
            bottleneck_analysis=bottleneck_analysis,
        )

    except Exception as e:
        logger.error(f"❌ {tool_name} Run {run_number} failed: {e}")
        if monitor:
            monitor.stop_monitoring()
        if profiler:
            profiler.stop_profiling()
        return PerformanceMetrics(
            tool_name=tool_name,
            run_number=run_number,
            workers=config.num_workers_cdis,
            total_files=len(manifest_data),
            successful_downloads=0,
            success_rate=0,
            total_download_time=0,
            total_size_mb=0,
            average_throughput_mbps=0,
            files_per_second=0,
            peak_memory_mb=0,
            avg_memory_mb=0,
            peak_cpu_percent=0,
            avg_cpu_percent=0,
            setup_time=setup_time,
            download_time=0,
            verification_time=0,
            return_code=-1,
            error_details=str(e),
        )


def calculate_aggregated_metrics(
    metrics_list: List[PerformanceMetrics],
) -> Dict[str, Any]:
    """Calculate aggregated statistics from multiple test runs."""
    if not metrics_list:
        return {
            "total_runs": 0,
            "successful_runs": 0,
            "overall_success_rate": 0,
            "avg_throughput": 0,
            "std_throughput": 0,
            "min_throughput": 0,
            "max_throughput": 0,
            "avg_download_time": 0,
            "std_download_time": 0,
            "avg_peak_memory": 0,
            "avg_peak_cpu": 0,
            "total_files_attempted": 0,
            "total_files_successful": 0,
        }

    successful_runs = [m for m in metrics_list if m.success_rate > 0]

    if not successful_runs:
        return {
            "total_runs": len(metrics_list),
            "successful_runs": 0,
            "overall_success_rate": 0,
            "avg_throughput": 0,
            "std_throughput": 0,
            "min_throughput": 0,
            "max_throughput": 0,
            "avg_download_time": 0,
            "std_download_time": 0,
            "avg_peak_memory": 0,
            "avg_peak_cpu": 0,
            "total_files_attempted": sum(m.total_files for m in metrics_list),
            "total_files_successful": sum(m.successful_downloads for m in metrics_list),
        }

    throughputs = [
        m.average_throughput_mbps
        for m in successful_runs
        if m.average_throughput_mbps > 0
    ]
    download_times = [m.download_time for m in successful_runs if m.download_time > 0]
    success_rates = [m.success_rate for m in metrics_list]
    memory_values = [m.peak_memory_mb for m in successful_runs if m.peak_memory_mb > 0]
    cpu_values = [m.peak_cpu_percent for m in successful_runs if m.peak_cpu_percent > 0]

    return {
        "total_runs": len(metrics_list),
        "successful_runs": len(successful_runs),
        "overall_success_rate": mean(success_rates) if success_rates else 0,
        "avg_throughput": mean(throughputs) if throughputs else 0,
        "std_throughput": stdev(throughputs) if len(throughputs) > 1 else 0,
        "min_throughput": min(throughputs) if throughputs else 0,
        "max_throughput": max(throughputs) if throughputs else 0,
        "avg_download_time": mean(download_times) if download_times else 0,
        "std_download_time": stdev(download_times) if len(download_times) > 1 else 0,
        "avg_peak_memory": mean(memory_values) if memory_values else 0,
        "avg_peak_cpu": mean(cpu_values) if cpu_values else 0,
        "total_files_attempted": sum(m.total_files for m in metrics_list),
        "total_files_successful": sum(m.successful_downloads for m in metrics_list),
    }


def create_html_report(
    all_metrics: List[PerformanceMetrics],
    config: TestConfiguration,
    logger: logging.Logger,
    manifest_path: str = None,
) -> str:
    """Create a comprehensive HTML report with detailed metrics."""

    def safe_value(value, default=0, precision=2):
        """Safely format a value, handling NaN, inf, None, and missing values."""
        if value is None or (
            isinstance(value, (int, float)) and (math.isnan(value) or math.isinf(value))
        ):
            return default
        try:
            if isinstance(value, (int, float)):
                return round(float(value), precision)
            return value
        except (ValueError, TypeError):
            return default

    tool_groups = {}
    for metric in all_metrics:
        if metric.tool_name not in tool_groups:
            tool_groups[metric.tool_name] = []
        tool_groups[metric.tool_name].append(metric)

    tool_aggregates = {}
    for tool_name, tool_metrics in tool_groups.items():
        tool_aggregates[tool_name] = calculate_aggregated_metrics(tool_metrics)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    tested_methods = list(set(m.tool_name for m in all_metrics))

    manifest_data = []
    if manifest_path:
        try:
            with open(manifest_path, "r") as f:
                manifest_data = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load manifest for file details: {e}")
            manifest_data = []

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Performance Report - Gen3 SDK</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
                color: #333;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .config-note {{
                background-color: #e8f5e8;
                border-left: 4px solid #4caf50;
                padding: 15px;
                margin: 20px;
                border-radius: 4px;
            }}
            .performance-note {{
                background-color: #fff3cd;
                border-left: 4px solid #ffc107;
                padding: 15px;
                margin: 20px;
                border-radius: 4px;
            }}
            .file-info {{
                background-color: #f8f9fa;
                padding: 15px;
                margin: 20px;
                border-radius: 8px;
                border-left: 4px solid #007bff;
            }}
            .file-info h3 {{
                margin-top: 0;
                color: #007bff;
            }}
            .file-details {{
                font-family: 'Courier New', monospace;
                font-size: 0.9em;
                background-color: white;
                padding: 10px;
                border-radius: 4px;
                border: 1px solid #dee2e6;
                margin-top: 10px;
            }}
            .guid {{
                color: #007bff;
                font-weight: bold;
            }}
            .summary-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                padding: 20px;
            }}
            .summary-card {{
                background: #f8f9fa;
                border-radius: 8px;
                padding: 20px;
                text-align: center;
                border: 1px solid #dee2e6;
            }}
            .summary-card h3 {{
                margin: 0 0 10px 0;
                color: #495057;
                font-size: 18px;
            }}
            .value {{
                font-size: 2em;
                font-weight: bold;
                color: #007bff;
                margin: 10px 0;
            }}
            .comparison-section {{
                padding: 20px;
                margin: 20px 0;
                background-color: #f8f9fa;
                border-radius: 8px;
            }}
            .chart-wrapper {{
                position: relative;
                height: 400px;
                margin: 20px 0;
            }}
            .data-table {{
                margin: 20px;
                overflow-x: auto;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #f8f9fa;
                font-weight: 600;
                color: #495057;
            }}
            .success-high {{ color: #28a745; }}
            .success-medium {{ color: #ffc107; }}
            .success-low {{ color: #dc3545; }}
            .bottleneck-section {{
                background-color: #fff5f5;
                border-left: 4px solid #dc3545;
                padding: 15px;
                margin: 20px;
                border-radius: 4px;
            }}
            .performance-metrics {{
                background-color: #f0f8ff;
                border-left: 4px solid #007bff;
                padding: 15px;
                margin: 20px;
                border-radius: 4px;
            }}
            .code-performance {{
                background-color: #f8f9fa;
                padding: 15px;
                margin: 20px;
                border-radius: 8px;
                border: 1px solid #dee2e6;
            }}
            .function-metric {{
                display: flex;
                justify-content: space-between;
                padding: 8px 0;
                border-bottom: 1px solid #eee;
            }}
            .function-metric:last-child {{
                border-bottom: none;
            }}
            .metric-bar {{
                background-color: #007bff;
                height: 20px;
                border-radius: 10px;
                margin-top: 5px;
            }}
            .io-metrics {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin: 15px 0;
            }}
            .io-card {{
                background: white;
                padding: 15px;
                border-radius: 8px;
                border: 1px solid #dee2e6;
                text-align: center;
            }}
            .io-value {{
                font-size: 1.5em;
                font-weight: bold;
                color: #007bff;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Performance Report - Gen3 SDK</h1>
                <p>Testing Methods: {", ".join(tested_methods)}</p>
                <p>Generated: {timestamp}</p>
            </div>
            
            <div class="config-note">
                <strong>⚡ Performance Configuration:</strong>
                <ul>
                    <li><strong>Async:</strong> {config.max_concurrent_requests_async} concurrent requests</li>
                    <li><strong>CDIS:</strong> {config.num_workers_cdis} parallel workers</li>
                    <li><strong>Profiling:</strong> Line-by-line profiling, memory tracking, I/O monitoring</li>
                </ul>
            </div>
            
            <div class="performance-note">
                <strong>📊 Test Configuration:</strong> {config.num_runs} runs per method, 
                Real-time monitoring enabled, Advanced profiling with bottleneck analysis.
            </div>"""

    if manifest_data:
        total_size_mb = sum(entry.get("file_size", 0) for entry in manifest_data) / (
            1024 * 1024
        )
        html_content += f"""
            <div class="file-info">
                <h3>📁 Test Files Information</h3>
                <p><strong>Total Files:</strong> {len(manifest_data)} | <strong>Total Size:</strong> {total_size_mb:.2f} MB</p>
                <div class="file-details">
                    <table style="width: 100%; font-size: 0.9em;">
                        <thead>
                            <tr>
                                <th style="text-align: left;">#</th>
                                <th style="text-align: left;">GUID</th>
                                <th style="text-align: left;">Object ID</th>
                                <th style="text-align: left;">File Name</th>
                                <th style="text-align: right;">Size (MB)</th>
                            </tr>
                        </thead>
                        <tbody>"""

        for i, entry in enumerate(manifest_data, 1):
            guid = (
                entry.get("object_id", "").split("/")[-1]
                if "/" in entry.get("object_id", "")
                else entry.get("object_id", "")
            )
            object_id = entry.get("object_id", "")
            file_name = entry.get("file_name", "")
            file_size_mb = entry.get("file_size", 0) / (1024 * 1024)

            html_content += f"""
                            <tr>
                                <td>{i}</td>
                                <td class="guid">{guid}</td>
                                <td style="font-family: 'Courier New', monospace; font-size: 0.8em;">{object_id}</td>
                                <td>{file_name}</td>
                                <td style="text-align: right;">{file_size_mb:.2f}</td>
                            </tr>"""

        html_content += """
                        </tbody>
                    </table>
                </div>
            </div>"""

    html_content += """
            <div class="summary-grid">"""

    for tool_name in tested_methods:
        agg = tool_aggregates.get(tool_name, {})
        throughput = safe_value(agg.get("avg_throughput", 0))
        success = safe_value(agg.get("overall_success_rate", 0))

        html_content += f"""
                <div class="summary-card">
                    <h3>{tool_name}</h3>
                    <div class="value">{throughput:.2f}</div>
                    <div>MB/s avg throughput</div>
                    <div>Success: {success:.1f}%</div>
                    <div>{config.num_runs} runs</div>
                </div>"""

    html_content += """
            </div>
            
            <div class="comparison-section">
                <h2>📈 Performance Comparison Charts</h2>
                
                <div class="chart-wrapper">
                    <canvas id="throughputChart"></canvas>
                </div>
                
                <div class="chart-wrapper">
                    <canvas id="successChart"></canvas>
                </div>
                
                <div class="chart-wrapper">
                    <canvas id="timeChart"></canvas>
                </div>
            </div>"""

    html_content += """
                <div class="data-table">
                    <h3>Detailed Performance Data</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Tool</th>
                                <th>Run</th>
                                <th>Success Rate</th>
                                <th>Files</th>
                                <th>Throughput (MB/s)</th>
                                <th>Download Time (s)</th>
                                <th>Total Size (MB)</th>
                                <th>Peak Memory (MB)</th>
                                <th>Peak CPU (%)</th>
                                <th>Network I/O (MB)</th>
                                <th>Disk I/O (MB)</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>"""

    for metric in all_metrics:
        success_class = (
            "success-high"
            if metric.success_rate >= 90
            else "success-medium"
            if metric.success_rate >= 70
            else "success-low"
        )
        status = (
            "✅ Success"
            if metric.success_rate > 80
            else "⚠️ Issues"
            if metric.success_rate > 50
            else "❌ Failed"
        )

        network_io = (
            metric.network_io_metrics.get("total_network_io_mb", 0)
            if metric.network_io_metrics
            else 0
        )
        disk_io = (
            metric.disk_io_metrics.get("total_disk_io_mb", 0)
            if metric.disk_io_metrics
            else 0
        )

        html_content += f"""
                    <tr>
                        <td><strong>{metric.tool_name}</strong></td>
                        <td>{metric.run_number}</td>
                        <td class="{success_class}">{metric.success_rate:.1f}%</td>
                        <td>{metric.successful_downloads}/{metric.total_files}</td>
                        <td>{metric.average_throughput_mbps:.2f}</td>
                        <td>{metric.download_time:.1f}</td>
                        <td>{metric.total_size_mb:.1f}</td>
                        <td>{metric.peak_memory_mb:.1f}</td>
                        <td>{metric.peak_cpu_percent:.1f}</td>
                        <td>{network_io:.1f}</td>
                        <td>{disk_io:.1f}</td>
                        <td class="{success_class}">{status}</td>
                    </tr>"""

    html_content += """
                </tbody>
            </table>
            
            <h2>📈 Aggregated Performance Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Tool</th>
                        <th>Runs</th>
                        <th>Overall Success</th>
                        <th>Avg Throughput</th>
                        <th>Std Dev</th>
                        <th>Min-Max Throughput</th>
                        <th>Avg Download Time</th>
                        <th>Total Files</th>
                    </tr>
                </thead>
                <tbody>"""

    for tool_name, agg_data in tool_aggregates.items():
        if agg_data and agg_data.get("total_runs", 0) > 0:
            success_class = (
                "success-high"
                if agg_data.get("overall_success_rate", 0) >= 90
                else "success-medium"
                if agg_data.get("overall_success_rate", 0) >= 70
                else "success-low"
            )

            min_max_throughput = f"{safe_value(agg_data.get('min_throughput', 0)):.2f} - {safe_value(agg_data.get('max_throughput', 0)):.2f}"

            html_content += f"""
                    <tr>
                        <td><strong>{tool_name}</strong></td>
                        <td>{safe_value(agg_data.get("total_runs", 0))}</td>
                        <td class="{success_class}">{safe_value(agg_data.get("overall_success_rate", 0)):.1f}%</td>
                        <td>{safe_value(agg_data.get("avg_throughput", 0)):.2f} MB/s</td>
                        <td>±{safe_value(agg_data.get("std_throughput", 0)):.2f}</td>
                        <td>{min_max_throughput} MB/s</td>
                        <td>{safe_value(agg_data.get("avg_download_time", 0)):.1f}s</td>
                        <td>{safe_value(agg_data.get("total_files_successful", 0))}/{safe_value(agg_data.get("total_files_attempted", 0))}</td>
                    </tr>"""

    html_content += """
                </tbody>
            </table>
            
            <h2>🔍 Detailed Profiling Analysis</h2>
            <div class="profiling-section">"""

    for metric in all_metrics:
        if metric.profiling_analysis:
            html_content += f"""
                <div class="profiling-card">
                    <h3>{metric.tool_name} - Run {metric.run_number}</h3>
                    <pre style="background: #f8f9fa; padding: 15px; border-radius: 8px; overflow-x: auto; font-size: 0.9em;">
{metric.profiling_analysis}
                    </pre>
                </div>"""

    chart_labels = list(tested_methods)
    chart_throughputs = [
        safe_value(tool_aggregates.get(tool, {}).get("avg_throughput", 0))
        for tool in chart_labels
    ]
    chart_success = [
        safe_value(tool_aggregates.get(tool, {}).get("overall_success_rate", 0))
        for tool in chart_labels
    ]
    chart_times = [
        safe_value(tool_aggregates.get(tool, {}).get("avg_download_time", 0))
        for tool in chart_labels
    ]

    html_content += f"""
            </div>
        </div>
    </div>

    <script>
        // Throughput Chart
        const throughputCtx = document.getElementById('throughputChart').getContext('2d');
        new Chart(throughputCtx, {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [{{
                    label: 'Average Throughput (MB/s)',
                    data: {chart_throughputs},
                    backgroundColor: ['rgba(155, 89, 182, 0.8)', 'rgba(39, 174, 96, 0.8)', 'rgba(52, 152, 219, 0.8)'],
                    borderColor: ['rgba(155, 89, 182, 1)', 'rgba(39, 174, 96, 1)', 'rgba(52, 152, 219, 1)'],
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{ y: {{ beginAtZero: true }} }},
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Higher is Better'
                    }}
                }}
            }}
        }});

        // Success Rate Chart
        const successCtx = document.getElementById('successChart').getContext('2d');
        new Chart(successCtx, {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [{{
                    label: 'Success Rate (%)',
                    data: {chart_success},
                    backgroundColor: ['rgba(155, 89, 182, 0.8)', 'rgba(39, 174, 96, 0.8)', 'rgba(52, 152, 219, 0.8)'],
                    borderColor: ['rgba(155, 89, 182, 1)', 'rgba(39, 174, 96, 1)', 'rgba(52, 152, 219, 1)'],
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{ y: {{ beginAtZero: true, max: 100 }} }},
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Higher is Better'
                    }}
                }}
            }}
        }});

        // Download Time Chart
        const timeCtx = document.getElementById('timeChart').getContext('2d');
        new Chart(timeCtx, {{
            type: 'bar',
            data: {{
                labels: {chart_labels},
                datasets: [{{
                    label: 'Average Download Time (s)',
                    data: {chart_times},
                    backgroundColor: ['rgba(155, 89, 182, 0.8)', 'rgba(39, 174, 96, 0.8)', 'rgba(52, 152, 219, 0.8)'],
                    borderColor: ['rgba(155, 89, 182, 1)', 'rgba(39, 174, 96, 1)', 'rgba(52, 152, 219, 1)'],
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{ y: {{ beginAtZero: true }} }},
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Lower is Better'
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>"""

    timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{RESULTS_DIR}/performance_report_{timestamp_file}.html"

    with open(report_path, "w") as f:
        f.write(html_content)

    logger.info(f"📊 Performance report saved to: {report_path}")
    return report_path


async def main():
    """Main function to run the performance comparison test."""
    # Parse command line arguments
    args = parse_arguments()

    # Handle special commands
    if args.config_help:
        print_config_help()
        return

    if args.create_config:
        create_default_config_file()
        return

    # Setup configuration
    config = setup_configuration(args)

    # Setup logging
    logger = setup_logging(config)

    logger.info("🚀 Starting Download Performance Comparison")

    update_status("Initializing", "", 0.0)

    cleanup_previous_downloads(logger)

    # Create test configuration from config
    test_config = TestConfiguration(config)

    logger.info(f"📋 Test Configuration:")
    logger.info(f"  • Methods to test: {', '.join(test_config.test_methods)}")
    logger.info(f"  • Runs per method: {test_config.num_runs}")
    logger.info(
        f"  • Async concurrent requests: {test_config.max_concurrent_requests_async}"
    )
    logger.info(f"  • CDIS workers: {test_config.num_workers_cdis}")

    # Use configuration values for paths
    manifest_path = test_config.manifest_path or os.path.join(
        GEN3_SDK_PATH, "performance_testing", "custom_manifest.json"
    )
    credentials_path = test_config.credentials_path
    endpoint = test_config.endpoint
    gen3_client_path = test_config.gen3_client_path

    if not verify_prerequisites(
        logger, gen3_client_path, credentials_path, manifest_path
    ):
        logger.error("❌ Prerequisites not met. Exiting.")
        update_status("Failed - Prerequisites not met", "", 0.0)
        return

    if not verify_credentials(logger, credentials_path, endpoint):
        logger.warning("⚠️ Credentials verification failed, but continuing.")

    with open(manifest_path, "r") as f:
        original_manifest_data = json.load(f)

    if test_config.filter_medium_files:
        filtered_manifest_data = filter_medium_files(original_manifest_data, logger)
        if not filtered_manifest_data:
            logger.error("❌ No medium-sized files found in manifest. Exiting.")
            update_status("Failed - No files found", "", 0.0)
            return

        filtered_manifest_path = create_filtered_manifest(
            manifest_path, filtered_manifest_data, logger
        )
        manifest_to_use = os.path.abspath(filtered_manifest_path)
        manifest_data = filtered_manifest_data
    else:
        manifest_to_use = os.path.abspath(manifest_path)
        manifest_data = original_manifest_data
        logger.info(f"📋 Using custom manifest with {len(manifest_data)} files")

    all_metrics = []

    test_configs = []

    if "async" in test_config.test_methods:
        test_configs.append(
            {
                "name": "Gen3 SDK (async)",
                "cmd": [
                    "python",
                    "-m",
                    "gen3.cli",
                    "--auth",
                    credentials_path,
                    "--endpoint",
                    endpoint,
                    "download-multiple-async",
                    "--manifest",
                    manifest_to_use,
                    "--download-path",
                    f"{os.path.abspath(RESULTS_DIR)}/sdk_download_async",
                    "--max-concurrent-requests",
                    str(test_config.max_concurrent_requests_async),
                    "--filename-format",
                    "original",
                    "--skip-completed",
                    "--rename",
                    "--no-prompt",
                    "--no-progress",
                ],
                "download_dir": f"{RESULTS_DIR}/sdk_download_async",
                "working_dir": GEN3_SDK_PATH,
                "env": {"PYTHONPATH": GEN3_SDK_PATH},
            }
        )

    if "cdis" in test_config.test_methods:
        test_configs.append(
            {
                "name": "CDIS Data Client",
                "cmd": [
                    gen3_client_path,
                    "download-multiple",
                    "--profile=midrc",
                    f"--manifest={manifest_to_use}",
                    f"--download-path={os.path.abspath(RESULTS_DIR)}/cdis_client",
                    f"--numparallel={test_config.num_workers_cdis}",
                    "--skip-completed",
                    "--no-prompt",
                ],
                "download_dir": f"{RESULTS_DIR}/cdis_client",
                "working_dir": None,
                "env": None,
            }
        )

    total_tests = len(test_configs) * test_config.num_runs
    current_test = 0

    for test_config_item in test_configs:
        logger.info(f"🔧 Testing {test_config_item['name']}...")
        for run in range(1, test_config.num_runs + 1):
            current_test += 1
            progress = (current_test / total_tests) * 100

            update_status("Running tests", test_config_item["name"], progress)

            metrics = run_tool_with_profiling(
                test_config_item["cmd"],
                test_config_item["download_dir"],
                manifest_to_use,
                test_config_item["name"],
                test_config,
                run,
                logger,
                working_dir=test_config_item["working_dir"],
                env=test_config_item["env"],
                gen3_client_path=gen3_client_path,
                credentials_path=credentials_path,
                endpoint=endpoint,
            )
            all_metrics.append(metrics)

    update_status("Generating report", "", 95.0)
    logger.info("📊 Generating performance comparison report...")
    report_path = create_html_report(all_metrics, test_config, logger, manifest_path)

    logger.info("📊 === PERFORMANCE RESULTS ===")
    tested_methods = list(set(m.tool_name for m in all_metrics))
    for tool_name in tested_methods:
        tool_metrics = [m for m in all_metrics if m.tool_name == tool_name]
        if tool_metrics:
            agg = calculate_aggregated_metrics(tool_metrics)
            logger.info(
                f"{tool_name}: {agg.get('overall_success_rate', 0):.1f}% success, "
                f"{agg.get('avg_throughput', 0):.2f} MB/s avg throughput, "
                f"{agg.get('avg_download_time', 0):.1f}s avg time"
            )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"{RESULTS_DIR}/async_comparison_results_{timestamp}.json"

    results_data = {
        "timestamp": timestamp,
        "config": {
            "num_runs": test_config.num_runs,
            "test_methods": test_config.test_methods,
            "max_concurrent_requests_async": test_config.max_concurrent_requests_async,
            "num_workers_cdis": test_config.num_workers_cdis,
            "enable_profiling": test_config.enable_profiling,
            "enable_real_time_monitoring": test_config.enable_real_time_monitoring,
        },
        "test_focus": "Performance comparison with configurable methods",
        "metrics": [
            {
                "tool_name": m.tool_name,
                "run_number": m.run_number,
                "success_rate": m.success_rate,
                "throughput": m.average_throughput_mbps,
                "download_time": m.download_time,
                "files_downloaded": m.successful_downloads,
                "total_files": m.total_files,
                "total_size_mb": m.total_size_mb,
                "peak_memory_mb": m.peak_memory_mb,
                "peak_cpu_percent": m.peak_cpu_percent,
                "error_details": m.error_details,
            }
            for m in all_metrics
        ],
    }

    with open(results_file, "w") as f:
        json.dump(results_data, f, indent=2)

    update_status("Completed", "", 100.0)

    logger.info(f"💾 Detailed results saved to: {results_file}")
    logger.info(f"📊 HTML report generated: {report_path}")

    if config.open_report_in_browser:
        try:
            webbrowser.open(f"file://{os.path.abspath(report_path)}")
            logger.info("🌐 Opened report in browser")
        except Exception as e:
            logger.warning(f"⚠️ Could not open browser: {e}")

    if test_config.filter_medium_files and os.path.exists(
        f"{RESULTS_DIR}/filtered_manifest.json"
    ):
        os.remove(f"{RESULTS_DIR}/filtered_manifest.json")
        logger.info("🧹 Cleaned up filtered manifest file")

    logging.info("🎉 Performance comparison test completed!")


if __name__ == "__main__":
    asyncio.run(main())
