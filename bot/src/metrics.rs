use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

/// Basic metrics collection system for telemetry
#[derive(Debug, Default)]
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Arc<AtomicU64>>>,
    histograms: RwLock<HashMap<String, Arc<RwLock<Vec<u64>>>>>,
    gauges: RwLock<HashMap<String, Arc<AtomicU64>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment a counter by 1
    pub fn increment_counter(&self, name: &str) {
        self.add_to_counter(name, 1);
    }

    /// Add a value to a counter
    pub fn add_to_counter(&self, name: &str, value: u64) {
        let counters = self.counters.read().unwrap();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
        } else {
            drop(counters);
            let mut counters = self.counters.write().unwrap();
            let counter = counters
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(AtomicU64::new(0)));
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Set gauge value
    pub fn set_gauge(&self, name: &str, value: u64) {
        let gauges = self.gauges.read().unwrap();
        if let Some(gauge) = gauges.get(name) {
            gauge.store(value, Ordering::Relaxed);
        } else {
            drop(gauges);
            let mut gauges = self.gauges.write().unwrap();
            let gauge = gauges
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(AtomicU64::new(0)));
            gauge.store(value, Ordering::Relaxed);
        }
    }

    /// Record histogram value (duration in milliseconds)
    pub fn record_histogram(&self, name: &str, duration: Duration) {
        let millis = duration.as_millis() as u64;
        let histograms = self.histograms.read().unwrap();
        if let Some(histogram) = histograms.get(name) {
            let mut hist = histogram.write().unwrap();
            hist.push(millis);
            // Keep only last 1000 values to prevent unbounded growth
            if hist.len() > 1000 {
                hist.drain(0..500);
            }
        } else {
            drop(histograms);
            let mut histograms = self.histograms.write().unwrap();
            let histogram = histograms
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(Vec::new())));
            let mut hist = histogram.write().unwrap();
            hist.push(millis);
        }
    }

    /// Get counter value
    pub fn get_counter(&self, name: &str) -> u64 {
        self.counters
            .read()
            .unwrap()
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get gauge value
    pub fn get_gauge(&self, name: &str) -> u64 {
        self.gauges
            .read()
            .unwrap()
            .get(name)
            .map(|g| g.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get histogram statistics
    pub fn get_histogram_stats(&self, name: &str) -> Option<HistogramStats> {
        let histograms = self.histograms.read().unwrap();
        histograms.get(name).and_then(|h| {
            let hist = h.read().unwrap();
            if hist.is_empty() {
                return None;
            }
            let mut sorted = hist.clone();
            sorted.sort_unstable();
            let len = sorted.len();
            Some(HistogramStats {
                count: len as u64,
                min: sorted[0],
                max: sorted[len - 1],
                p50: sorted[len / 2],
                p95: sorted[len * 95 / 100],
                p99: sorted[len * 99 / 100],
            })
        })
    }

    /// Export all metrics in a simple format
    pub fn export_metrics(&self) -> MetricsSnapshot {
        let counters = self
            .counters
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();

        let gauges = self
            .gauges
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();

        let histograms = self
            .histograms
            .read()
            .unwrap()
            .iter()
            .filter_map(|(k, _v)| {
                self.get_histogram_stats(k)
                    .map(|stats| (k.clone(), stats))
            })
            .collect();

        MetricsSnapshot {
            counters,
            gauges,
            histograms,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HistogramStats {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, u64>,
    pub histograms: HashMap<String, HistogramStats>,
}

/// Global metrics registry instance
static GLOBAL_METRICS: std::sync::OnceLock<MetricsRegistry> = std::sync::OnceLock::new();

/// Get global metrics registry
pub fn metrics() -> &'static MetricsRegistry {
    GLOBAL_METRICS.get_or_init(MetricsRegistry::new)
}

/// Timer helper for measuring duration
pub struct Timer {
    name: String,
    start: Instant,
}

impl Timer {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            start: Instant::now(),
        }
    }

    pub fn finish(self) {
        let duration = self.start.elapsed();
        metrics().record_histogram(&self.name, duration);
    }
}

/// Convenience macro for timing code blocks
#[macro_export]
macro_rules! time_block {
    ($name:expr, $block:expr) => {{
        let _timer = $crate::metrics::Timer::new($name);
        let result = $block;
        _timer.finish();
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_operations() {
        let registry = MetricsRegistry::new();
        registry.increment_counter("test_counter");
        registry.add_to_counter("test_counter", 5);
        assert_eq!(registry.get_counter("test_counter"), 6);
    }

    #[test]
    fn test_gauge_operations() {
        let registry = MetricsRegistry::new();
        registry.set_gauge("test_gauge", 42);
        assert_eq!(registry.get_gauge("test_gauge"), 42);
        registry.set_gauge("test_gauge", 100);
        assert_eq!(registry.get_gauge("test_gauge"), 100);
    }

    #[test]
    fn test_histogram_operations() {
        let registry = MetricsRegistry::new();
        registry.record_histogram("test_hist", Duration::from_millis(100));
        registry.record_histogram("test_hist", Duration::from_millis(200));
        registry.record_histogram("test_hist", Duration::from_millis(150));

        let stats = registry.get_histogram_stats("test_hist").unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, 100);
        assert_eq!(stats.max, 200);
    }

    #[test]
    fn test_timer() {
        {
            let timer = Timer::new("test_timer");
            std::thread::sleep(Duration::from_millis(10));
            timer.finish();
        }
        let stats = metrics().get_histogram_stats("test_timer").unwrap();
        assert!(stats.min >= 10); // Should be at least 10ms
    }
}