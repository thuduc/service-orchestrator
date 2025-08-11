import logging
import time
from typing import Dict, Any, Callable, List
from collections import defaultdict
from framework.middleware import Middleware


class MetricsMiddleware(Middleware):
    """Middleware for collecting execution metrics"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the metrics middleware
        
        Args:
            config: Configuration dictionary with options:
                - collect_latency: Whether to collect latency metrics
                - collect_errors: Whether to collect error metrics
                - collect_throughput: Whether to collect throughput metrics
                - export_interval: Interval for exporting metrics (seconds)
                - percentiles: List of percentiles to calculate for latency
        """
        self.config = config or {}
        self.collect_latency = self.config.get('collect_latency', True)
        self.collect_errors = self.config.get('collect_errors', True)
        self.collect_throughput = self.config.get('collect_throughput', True)
        self.export_interval = self.config.get('export_interval', 60)
        self.percentiles = self.config.get('percentiles', [50, 90, 95, 99])
        
        # Metrics storage
        self.metrics = {
            'requests_total': defaultdict(int),
            'requests_success': defaultdict(int),
            'requests_failed': defaultdict(int),
            'latency_samples': defaultdict(list),
            'error_types': defaultdict(lambda: defaultdict(int)),
            'throughput': defaultdict(list)
        }
        
        # Timing for export
        self.last_export = time.time()
        
        self.logger = logging.getLogger(__name__)
    
    def process(self, context: Dict[str, Any], next_handler: Callable) -> Dict[str, Any]:
        """
        Collect metrics for request processing
        
        Args:
            context: The request context
            next_handler: The next handler in the chain
            
        Returns:
            The response from the next handler with metrics
        """
        service_id = context.get('service_id', 'unknown')
        start_time = time.time()
        
        # Add start time to context for other middleware to use
        context['_metrics_start_time'] = start_time
        
        result = None
        error = None
        
        try:
            # Execute next handler
            result = next_handler(context)
            
            # Record success
            self._record_success(service_id, start_time)
            
            return result
            
        except Exception as e:
            error = e
            # Record failure
            self._record_failure(service_id, start_time, e)
            raise
            
        finally:
            # Check if we should export metrics
            if time.time() - self.last_export > self.export_interval:
                self._export_metrics()
    
    def _record_success(self, service_id: str, start_time: float):
        """Record successful request metrics"""
        execution_time = time.time() - start_time
        
        # Update counters
        self.metrics['requests_total'][service_id] += 1
        self.metrics['requests_success'][service_id] += 1
        
        # Record latency
        if self.collect_latency:
            self.metrics['latency_samples'][service_id].append(execution_time)
            # Keep only last 1000 samples per service
            if len(self.metrics['latency_samples'][service_id]) > 1000:
                self.metrics['latency_samples'][service_id] = \
                    self.metrics['latency_samples'][service_id][-1000:]
        
        # Record throughput
        if self.collect_throughput:
            self.metrics['throughput'][service_id].append(time.time())
            # Keep only last hour of throughput data
            cutoff = time.time() - 3600
            self.metrics['throughput'][service_id] = [
                t for t in self.metrics['throughput'][service_id] if t > cutoff
            ]
    
    def _record_failure(self, service_id: str, start_time: float, error: Exception):
        """Record failed request metrics"""
        execution_time = time.time() - start_time
        
        # Update counters
        self.metrics['requests_total'][service_id] += 1
        self.metrics['requests_failed'][service_id] += 1
        
        # Record error type
        if self.collect_errors:
            error_type = type(error).__name__
            self.metrics['error_types'][service_id][error_type] += 1
        
        # Still record latency for failed requests
        if self.collect_latency:
            self.metrics['latency_samples'][service_id].append(execution_time)
    
    def _calculate_percentile(self, samples: List[float], percentile: int) -> float:
        """Calculate percentile from samples"""
        if not samples:
            return 0.0
        
        sorted_samples = sorted(samples)
        index = int(len(sorted_samples) * percentile / 100)
        index = min(index, len(sorted_samples) - 1)
        return sorted_samples[index]
    
    def _calculate_throughput(self, timestamps: List[float]) -> float:
        """Calculate requests per second from timestamps"""
        if len(timestamps) < 2:
            return 0.0
        
        # Calculate over last minute
        cutoff = time.time() - 60
        recent = [t for t in timestamps if t > cutoff]
        
        if len(recent) < 2:
            return 0.0
        
        duration = recent[-1] - recent[0]
        if duration == 0:
            return 0.0
        
        return len(recent) / duration
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get a summary of collected metrics
        
        Returns:
            Dictionary containing metrics summary
        """
        summary = {}
        
        for service_id in self.metrics['requests_total'].keys():
            service_metrics = {
                'total_requests': self.metrics['requests_total'][service_id],
                'successful_requests': self.metrics['requests_success'][service_id],
                'failed_requests': self.metrics['requests_failed'][service_id]
            }
            
            # Calculate success rate
            if service_metrics['total_requests'] > 0:
                service_metrics['success_rate'] = (
                    service_metrics['successful_requests'] / 
                    service_metrics['total_requests'] * 100
                )
            else:
                service_metrics['success_rate'] = 0
            
            # Calculate latency percentiles
            if self.collect_latency and self.metrics['latency_samples'][service_id]:
                samples = self.metrics['latency_samples'][service_id]
                service_metrics['latency'] = {
                    'min': min(samples),
                    'max': max(samples),
                    'avg': sum(samples) / len(samples)
                }
                
                for p in self.percentiles:
                    service_metrics['latency'][f'p{p}'] = \
                        self._calculate_percentile(samples, p)
            
            # Calculate throughput
            if self.collect_throughput and self.metrics['throughput'][service_id]:
                service_metrics['throughput_rps'] = \
                    self._calculate_throughput(self.metrics['throughput'][service_id])
            
            # Add error breakdown
            if self.collect_errors and self.metrics['error_types'][service_id]:
                service_metrics['errors'] = dict(self.metrics['error_types'][service_id])
            
            summary[service_id] = service_metrics
        
        return summary
    
    def _export_metrics(self):
        """Export metrics (log them for now)"""
        summary = self.get_metrics_summary()
        
        self.logger.info("=== Metrics Export ===")
        for service_id, metrics in summary.items():
            self.logger.info(f"Service: {service_id}")
            self.logger.info(f"  Total: {metrics['total_requests']}, "
                           f"Success: {metrics['successful_requests']}, "
                           f"Failed: {metrics['failed_requests']}, "
                           f"Success Rate: {metrics['success_rate']:.1f}%")
            
            if 'latency' in metrics:
                lat = metrics['latency']
                self.logger.info(f"  Latency - Avg: {lat['avg']:.3f}s, "
                               f"P50: {lat.get('p50', 0):.3f}s, "
                               f"P99: {lat.get('p99', 0):.3f}s")
            
            if 'throughput_rps' in metrics:
                self.logger.info(f"  Throughput: {metrics['throughput_rps']:.1f} req/s")
        
        self.last_export = time.time()
    
    def reset_metrics(self):
        """Reset all collected metrics"""
        self.metrics = {
            'requests_total': defaultdict(int),
            'requests_success': defaultdict(int),
            'requests_failed': defaultdict(int),
            'latency_samples': defaultdict(list),
            'error_types': defaultdict(lambda: defaultdict(int)),
            'throughput': defaultdict(list)
        }
        self.logger.info("Metrics reset")