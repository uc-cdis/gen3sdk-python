"""
Configuration system for Gen3 SDK Performance Testing.

This module provides a centralized configuration system that supports:
- Environment variables
- Configuration files
- Default values
- Validation and type conversion
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class PerformanceConfig:
    """Configuration for performance testing."""

    # Test Configuration
    num_runs: int = 2
    enable_profiling: bool = True
    enable_real_time_monitoring: bool = True
    monitoring_interval: float = 1.0
    filter_medium_files: bool = False
    force_uncompressed_cdis: bool = True
    auto_extract_cdis: bool = True

    # Concurrency Settings
    max_concurrent_requests_async: int = 200
    num_workers_cdis: int = 8

    # Profiling Settings
    enable_line_profiling: bool = True
    enable_memory_profiling: bool = True
    enable_network_monitoring: bool = True
    enable_disk_io_monitoring: bool = True

    # Test Methods
    test_methods: List[str] = field(default_factory=lambda: ["async", "cdis"])

    # Paths and Endpoints
    gen3_client_path: str = "gen3-client"
    credentials_path: str = "~/Downloads/credentials.json"
    endpoint: str = "https://data.midrc.org"
    manifest_path: Optional[str] = None
    results_dir: Optional[str] = None

    # File Processing
    profile_specific_functions: List[str] = field(
        default_factory=lambda: [
            "download_single",
            "async_download_multiple",
            "get_presigned_url",
            "find_matching_files_improved",
            "extract_cdis_files",
        ]
    )

    # Performance Thresholds
    memory_warning_threshold_mb: float = 2000.0
    cpu_warning_threshold_percent: float = 90.0
    throughput_warning_threshold_mbps: float = 10.0
    success_rate_warning_threshold: float = 90.0

    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None

    # Report Settings
    generate_html_report: bool = True
    open_report_in_browser: bool = True
    save_detailed_metrics: bool = True

    @classmethod
    def from_env(cls) -> "PerformanceConfig":
        """Create configuration from environment variables."""
        config = cls()

        # Test Configuration
        config.num_runs = int(os.getenv("PERF_NUM_RUNS", config.num_runs))
        config.enable_profiling = (
            os.getenv("PERF_ENABLE_PROFILING", "true").lower() == "true"
        )
        config.enable_real_time_monitoring = (
            os.getenv("PERF_ENABLE_MONITORING", "true").lower() == "true"
        )
        config.monitoring_interval = float(
            os.getenv("PERF_MONITORING_INTERVAL", config.monitoring_interval)
        )
        config.filter_medium_files = (
            os.getenv("PERF_FILTER_MEDIUM_FILES", "false").lower() == "true"
        )
        config.force_uncompressed_cdis = (
            os.getenv("PERF_FORCE_UNCOMPRESSED_CDIS", "true").lower() == "true"
        )
        config.auto_extract_cdis = (
            os.getenv("PERF_AUTO_EXTRACT_CDIS", "true").lower() == "true"
        )

        # Concurrency Settings
        config.max_concurrent_requests_async = int(
            os.getenv("PERF_MAX_CONCURRENT_ASYNC", config.max_concurrent_requests_async)
        )
        config.num_workers_cdis = int(
            os.getenv("PERF_NUM_WORKERS_CDIS", config.num_workers_cdis)
        )

        # Profiling Settings
        config.enable_line_profiling = (
            os.getenv("PERF_ENABLE_LINE_PROFILING", "true").lower() == "true"
        )
        config.enable_memory_profiling = (
            os.getenv("PERF_ENABLE_MEMORY_PROFILING", "true").lower() == "true"
        )
        config.enable_network_monitoring = (
            os.getenv("PERF_ENABLE_NETWORK_MONITORING", "true").lower() == "true"
        )
        config.enable_disk_io_monitoring = (
            os.getenv("PERF_ENABLE_DISK_IO_MONITORING", "true").lower() == "true"
        )

        # Test Methods
        test_methods_str = os.getenv("PERF_TEST_METHODS", "async,cdis")
        config.test_methods = [method.strip() for method in test_methods_str.split(",")]

        # Paths and Endpoints
        config.gen3_client_path = os.getenv("GEN3_CLIENT_PATH", config.gen3_client_path)
        config.credentials_path = os.path.expanduser(
            os.getenv("PERF_CREDENTIALS_PATH", config.credentials_path)
        )
        config.endpoint = os.getenv("PERF_ENDPOINT", config.endpoint)
        config.manifest_path = os.getenv("PERF_MANIFEST_PATH", config.manifest_path)
        config.results_dir = os.getenv("PERF_RESULTS_DIR", config.results_dir)

        # Performance Thresholds
        config.memory_warning_threshold_mb = float(
            os.getenv(
                "PERF_MEMORY_WARNING_THRESHOLD_MB", config.memory_warning_threshold_mb
            )
        )
        config.cpu_warning_threshold_percent = float(
            os.getenv(
                "PERF_CPU_WARNING_THRESHOLD_PERCENT",
                config.cpu_warning_threshold_percent,
            )
        )
        config.throughput_warning_threshold_mbps = float(
            os.getenv(
                "PERF_THROUGHPUT_WARNING_THRESHOLD_MBPS",
                config.throughput_warning_threshold_mbps,
            )
        )
        config.success_rate_warning_threshold = float(
            os.getenv(
                "PERF_SUCCESS_RATE_WARNING_THRESHOLD",
                config.success_rate_warning_threshold,
            )
        )

        # Logging
        config.log_level = os.getenv("PERF_LOG_LEVEL", config.log_level)
        config.log_file = os.getenv("PERF_LOG_FILE", config.log_file)

        # Report Settings
        config.generate_html_report = (
            os.getenv("PERF_GENERATE_HTML_REPORT", "true").lower() == "true"
        )
        config.open_report_in_browser = (
            os.getenv("PERF_OPEN_REPORT_IN_BROWSER", "true").lower() == "true"
        )
        config.save_detailed_metrics = (
            os.getenv("PERF_SAVE_DETAILED_METRICS", "true").lower() == "true"
        )

        return config

    @classmethod
    def from_file(cls, config_path: str) -> "PerformanceConfig":
        """Create configuration from JSON file."""
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)
            return cls.from_dict(config_data)
        except Exception as e:
            logging.warning(f"Failed to load config from {config_path}: {e}")
            return cls.from_env()

    def save_to_file(self, config_path: str) -> None:
        """Save configuration to JSON file."""
        try:
            with open(config_path, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save config to {config_path}: {e}")

    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []

        # Validate numeric values
        if self.num_runs < 1:
            errors.append("num_runs must be at least 1")

        if self.max_concurrent_requests_async < 1:
            errors.append("max_concurrent_requests_async must be at least 1")

        if self.num_workers_cdis < 1:
            errors.append("num_workers_cdis must be at least 1")

        if self.monitoring_interval <= 0:
            errors.append("monitoring_interval must be positive")

        # Validate paths
        if self.credentials_path and not os.path.exists(
            os.path.expanduser(self.credentials_path)
        ):
            errors.append(f"Credentials file not found: {self.credentials_path}")

        if self.manifest_path and not os.path.exists(self.manifest_path):
            errors.append(f"Manifest file not found: {self.manifest_path}")

        # Validate test methods
        valid_methods = ["async", "cdis"]
        for method in self.test_methods:
            if method not in valid_methods:
                errors.append(
                    f"Invalid test method: {method}. Valid methods: {valid_methods}"
                )

        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            errors.append(
                f"Invalid log level: {self.log_level}. Valid levels: {valid_log_levels}"
            )

        return errors


def get_config(config_file: Optional[str] = None) -> PerformanceConfig:
    """
    Get configuration with fallback order:
    1. Config file (if provided)
    2. Environment variables
    3. Default values
    """
    if config_file and os.path.exists(config_file):
        config = PerformanceConfig.from_file(config_file)
    else:
        config = PerformanceConfig.from_env()

    # Validate configuration
    errors = config.validate()
    if errors:
        logging.warning("Configuration validation errors:")
        for error in errors:
            logging.warning(f"  - {error}")

    return config


def create_default_config_file(config_path: str = "performance_config.json") -> None:
    """Create a default configuration file."""
    config = PerformanceConfig()
    config.save_to_file(config_path)
    print(f"Default configuration saved to: {config_path}")


def print_config_help() -> None:
    """Print help information about configuration options."""
    help_text = """
Performance Testing Configuration Options
=======================================

Environment Variables:
---------------------

Test Configuration:
  PERF_NUM_RUNS                    Number of test runs per method (default: 2)
  PERF_ENABLE_PROFILING            Enable code profiling (default: true)
  PERF_ENABLE_MONITORING           Enable real-time monitoring (default: true)
  PERF_MONITORING_INTERVAL         Monitoring interval in seconds (default: 1.0)
  PERF_FILTER_MEDIUM_FILES         Filter for medium-sized files (default: false)
  PERF_FORCE_UNCOMPRESSED_CDIS     Force uncompressed CDIS downloads (default: true)
  PERF_AUTO_EXTRACT_CDIS           Auto-extract CDIS files (default: true)

Concurrency Settings:
  PERF_MAX_CONCURRENT_ASYNC        Max concurrent requests for async (default: 200)
  PERF_NUM_WORKERS_CDIS            Number of CDIS workers (default: 8)

Profiling Settings:
  PERF_ENABLE_LINE_PROFILING       Enable line-by-line profiling (default: true)
  PERF_ENABLE_MEMORY_PROFILING     Enable memory profiling (default: true)
  PERF_ENABLE_NETWORK_MONITORING   Enable network I/O monitoring (default: true)
  PERF_ENABLE_DISK_IO_MONITORING   Enable disk I/O monitoring (default: true)

Test Methods:
  PERF_TEST_METHODS                Comma-separated list of methods (default: "async,cdis")

Paths and Endpoints:
  GEN3_CLIENT_PATH                 Path to gen3-client executable
  PERF_CREDENTIALS_PATH            Path to credentials file (default: ~/Downloads/credentials.json)
  PERF_ENDPOINT                    Gen3 endpoint URL (default: https://data.midrc.org)
  PERF_MANIFEST_PATH               Path to manifest file
  PERF_RESULTS_DIR                 Directory for results

Performance Thresholds:
  PERF_MEMORY_WARNING_THRESHOLD_MB     Memory warning threshold in MB (default: 2000)
  PERF_CPU_WARNING_THRESHOLD_PERCENT   CPU warning threshold in % (default: 90)
  PERF_THROUGHPUT_WARNING_THRESHOLD_MBPS  Throughput warning threshold in MB/s (default: 10)
  PERF_SUCCESS_RATE_WARNING_THRESHOLD    Success rate warning threshold in % (default: 90)

Logging:
  PERF_LOG_LEVEL                  Log level (default: INFO)
  PERF_LOG_FILE                   Log file path

Report Settings:
  PERF_GENERATE_HTML_REPORT       Generate HTML report (default: true)
  PERF_OPEN_REPORT_IN_BROWSER     Open report in browser (default: true)
  PERF_SAVE_DETAILED_METRICS      Save detailed metrics (default: true)

Configuration File:
------------------
You can also use a JSON configuration file:

{
  "num_runs": 2,
  "enable_profiling": true,
  "max_concurrent_requests_async": 200,
  "test_methods": ["async", "cdis"],
  "endpoint": "https://data.midrc.org"
}

Usage Examples:
--------------
# Basic usage with environment variables
export PERF_NUM_RUNS=3
export PERF_MAX_CONCURRENT_ASYNC=300
python async_comparison.py

# Using configuration file
python async_comparison.py --config performance_config.json

# Quick test with minimal profiling
export PERF_ENABLE_PROFILING=false
export PERF_NUM_RUNS=1
python async_comparison.py
"""
    print(help_text)
