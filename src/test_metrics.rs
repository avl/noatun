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
#[derive(Default)]
pub(crate) struct TestRecorder {
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
impl Recorder for TestRecorder {
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

impl TestRecorder {
    pub fn register(&self) -> metrics::LocalRecorderGuard<'_> {
        metrics::set_default_local_recorder(self)
    }
    pub fn get_metrics(&self) -> String {
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
