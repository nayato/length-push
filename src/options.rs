use num_cpus;
use std::{cmp, net::SocketAddr, time::Duration};

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "length-push", about = "Applies load to Length-prefixed echo server")]
pub struct Opt {
    #[structopt(short = "a", long = "address", parse(try_from_str))]
    pub address: SocketAddr,
    #[structopt(short = "s", long = "size", help = "size of payload to send in KB")]
    size_kb: Option<usize>,
    #[structopt(short = "p", long = "precise-size", help = "size of payload to send in bytes")]
    size: Option<usize>,
    #[structopt(short = "c", long = "concurrency", help = "# of connections to open concurrently", default_value = "1")]
    pub concurrency: u64,
    #[structopt(short = "t", long = "threads", help = "number of threads to use")]
    threads: Option<u64>,
    #[structopt(short = "d", long = "delay", help = "delay (ms) between two calls made for the same connection", default_value = "0")]
    delay_millis: u64,
    #[structopt(short = "q", long = "connection-rate", help = "number of connections allowed to open concurrently (per thread)")]
    connection_rate: Option<usize>,
    #[structopt(short = "r", long = "sample-rate", help = "seconds between average reports", default_value = "1")]
    pub sample_rate_seconds: u64,
    #[structopt(short = "w", long = "warm-up", help = "seconds before counter values are considered for reporting", default_value = "2")]
    pub warm_up_seconds: u64,
}
impl Opt {
    pub fn threads(&self) -> u64 {
        cmp::min(self.concurrency, self.threads.unwrap_or_else(|| num_cpus::get() as u64))
    }

    pub fn connections_per_thread(&self) -> u64 {
        cmp::max(self.concurrency / self.threads(), 1)
    }

    pub fn connection_rate(&self) -> usize {
        self.connection_rate.unwrap_or_else(|| self.connections_per_thread() as usize)
    }

    pub fn payload_size(&self) -> usize {
        match (self.size_kb, self.size) {
            (Some(s), _) => s * 1024, // -s takes precedence
            (None, Some(p)) => p,
            (None, None) => 0,
        }
    }

    pub fn delay(&self) -> Duration {
        Duration::from_millis(self.delay_millis)
    }
}