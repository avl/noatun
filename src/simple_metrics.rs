//! Simple metrics recorder
//!
//! Noatun uses the `metrics` crate to provide metrics in a flexible way.
//!
//! This crate contains a very simple concrete metrics implementation that can be used
//! to get at the values. For real deployments, you may want to check out something like
//! prometheus.
use indexmap::IndexMap;
use metrics::CounterFn;
use metrics::GaugeFn;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Default)]
struct InnerState {
    counters: IndexMap<Key, Arc<AtomicU64>>,
    gauges: IndexMap<Key, Arc<AtomicU64>>,
}

/// A simple 'metrics' recorder for noatun.
///
/// You don't need to use this, you can use any `metrics`-crate compatible mechanism.
#[derive(Default, Clone)]
pub struct SimpleMetricsRecorder {
    inner: Arc<Mutex<InnerState>>,
}
struct TestCounter {
    inner: Arc<AtomicU64>,
}
struct TestGauge {
    inner: Arc<AtomicU64>,
}
impl CounterFn for TestCounter {
    fn increment(&self, value: u64) {
        self.inner.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.inner.store(value, Ordering::Relaxed);
    }
}

impl TestGauge {
    fn update(&self, mut updater: impl FnMut(f64) -> f64) {
        for _ in 0..10 {
            // If we race 10 times, rather than racing yet again, we just continue.
            // Better to have invalid statistics than weird perf problems.
            let prev_value_bits = self.inner.load(Ordering::Relaxed);
            let prev_value = f64::from_bits(prev_value_bits);
            let new_value = updater(prev_value);
            let new_value_bits = new_value.to_bits();
            if self
                .inner
                .compare_exchange(
                    prev_value_bits,
                    new_value_bits,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return;
            }
        }
    }
}

impl GaugeFn for TestGauge {
    fn increment(&self, value: f64) {
        self.update(|prev| prev + value)
    }

    fn decrement(&self, value: f64) {
        self.update(|prev| prev - value)
    }

    fn set(&self, value: f64) {
        self.inner.store(value.to_bits(), Ordering::Relaxed);
    }
}
impl Recorder for SimpleMetricsRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        todo!()
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(TestCounter {
            inner: self
                .inner
                .lock()
                .unwrap()
                .counters
                .entry(key.clone())
                .or_default()
                .clone(),
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(TestGauge {
            inner: self
                .inner
                .lock()
                .unwrap()
                .gauges
                .entry(key.clone())
                .or_default()
                .clone(),
        }))
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        todo!()
    }
}

impl SimpleMetricsRecorder {
    /// Register a thread local metrics recorder (useful for tests)
    pub fn register_local(&self) -> metrics::LocalRecorderGuard<'_> {
        metrics::set_default_local_recorder(self)
    }
    /// Register a thread local global recorder
    pub fn register_global(self) {
        metrics::set_global_recorder(self).unwrap();
    }

    /// Get all recorded metrics
    pub fn metrics_items(&self) -> Vec<(String, String)> {
        let mut ret = Vec::new();
        let guard = self.inner.lock().unwrap();
        for (key, val) in guard.counters.iter() {
            ret.push((
                key.name().to_string(),
                val.load(Ordering::Relaxed).to_string(),
            ));
        }
        for (key, val) in guard.gauges.iter() {
            ret.push((
                key.name().to_string(),
                f64::from_bits(val.load(Ordering::Relaxed)).to_string(),
            ));
        }

        ret
    }
    /// Get all recorded metrics as a big string
    pub fn get_metrics_text(&self) -> String {
        use std::fmt::Write;
        let guard = self.inner.lock().unwrap();
        let mut output = String::new();
        for (key, val) in guard.counters.iter() {
            writeln!(&mut output, "{key}: {}", val.load(Ordering::Relaxed)).unwrap();
        }
        for (key, val) in guard.gauges.iter() {
            writeln!(
                &mut output,
                "{key}: {}",
                f64::from_bits(val.load(Ordering::Relaxed))
            )
            .unwrap();
        }
        output
    }
}
