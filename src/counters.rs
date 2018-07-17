use chrono;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

pub struct PerfCounters {
    req: AtomicUsize,
    lat: AtomicUsize,
    lat_max: AtomicUsize,
    req_current: AtomicUsize,
}

pub struct RequestStat {
    timestamp: Instant,
}

impl PerfCounters {
    pub fn new() -> PerfCounters {
        PerfCounters {
            req: AtomicUsize::new(0),
            lat: AtomicUsize::new(0),
            lat_max: AtomicUsize::new(0),
            req_current: AtomicUsize::new(0),
        }
    }

    pub fn request_count(&self) -> usize {
        self.req.load(Ordering::SeqCst)
    }

    pub fn request_current(&self) -> usize {
        self.req_current.load(Ordering::SeqCst)
    }

    pub fn latency_ns(&self) -> u64 {
        self.lat.load(Ordering::SeqCst) as u64
    }

    pub fn pull_latency_max_ns(&self) -> u64 {
        self.lat_max.swap(0, Ordering::SeqCst) as u64
    }

    pub fn start_request(&self) -> RequestStat {
        self.req_current.fetch_add(1, Ordering::SeqCst);
        RequestStat { timestamp: Instant::now() }
    }

    pub fn stop_request(&self, req: RequestStat) {
        self.req.fetch_add(1, Ordering::SeqCst);
        self.req_current.fetch_sub(1, Ordering::SeqCst);
        let duration = req.timestamp.elapsed();
        let nanos = duration.as_secs() as usize * 1_000_000_000 + duration.subsec_nanos() as usize;
        self.lat.fetch_add(nanos as usize, Ordering::SeqCst);
        loop {
            let current = self.lat_max.load(Ordering::SeqCst);
            if current >= nanos || self.lat_max.compare_and_swap(current, nanos, Ordering::SeqCst) == current {
                break;
            }
        }
    }
}

pub fn setup_monitor(counters: Arc<PerfCounters>, warmup_seconds: u64, sample_rate: u64) -> thread::JoinHandle<()> {
    let monitor_thread = thread::Builder::new()
        .name("monitor".to_string())
        .spawn(move || {
            thread::sleep(Duration::from_secs(warmup_seconds));
            println!("warm up past");
            let mut prev_reqs = 0;
            let mut prev_lat = 0;
            loop {
                let reqs = counters.request_count();
                {
                    //if reqs > prev_reqs {
                    let reqs_current = counters.request_current();
                    let latency = counters.latency_ns();
                    let latency_max = counters.pull_latency_max_ns();
                    let req_count = (reqs - prev_reqs) as u64;
                    let latency_diff = latency - prev_lat;
                    println!(
                        "rate: {}, latency: {}, latency max: {}, in-flight: {}",
                        req_count / sample_rate,
                        chrono::Duration::nanoseconds((if req_count > 0 { latency_diff / req_count } else { 0 }) as i64),
                        chrono::Duration::nanoseconds(latency_max as i64),
                        reqs_current
                    );
                    prev_reqs = reqs;
                    prev_lat = latency;
                }
                thread::sleep(Duration::from_secs(sample_rate));
            }
        })
        .unwrap();
    monitor_thread
}
