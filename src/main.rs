#![feature(proc_macro, conservative_impl_trait, generators, vec_resize_default)]

extern crate bytes;
extern crate clap;
extern crate futures_await as futures;
extern crate num_cpus;
extern crate time;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;
extern crate tokio_tls;
extern crate native_tls;
#[macro_use]
extern crate error_chain;

use futures::prelude::*;
use std::net::SocketAddr;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use std::{cmp, io, thread};
use futures::{future, Future, Sink, Stream};
use bytes::{Bytes, BytesMut, BufMut, BigEndian, ByteOrder};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use native_tls::TlsConnector;
use tokio_tls::{TlsConnectorExt, TlsStream};
use tokio_io::{AsyncRead, AsyncWrite};
use std::rc::Rc;

mod error;
mod codec;
use error::*;
use codec::LengthCodec;

static PAYLOAD_SOURCE: &[u8] = include_bytes!("lorem.txt");

#[async]
fn tokio_delay(val: Duration, loop_handle: Handle) -> Result<()> {
    await!(tokio_core::reactor::Timeout::new(val, &loop_handle)?)?;
    Ok(())
}

fn main() {
    let matches = clap::App::new("Length Push")
        .version("0.1")
        .about("Applies load to Length-prefixed echo server")
        .args_from_usage(
            "<address> 'IP address and port to push'
                -s, --size=[NUMBER] 'size of PUBLISH packet payload to send in KB'
                -p, --precise-size=[NUMBER] 'size of PUBLISH packet payload to send in bytes'
                -c, --concurrency=[NUMBER] 'number of MQTT connections to open and use concurrently for sending'
                -w, --warm-up=[SECONDS] 'seconds before counter values are considered for reporting'
                -r, --sample-rate=[SECONDS] 'seconds between average reports'
                -d, --delay=[MILLISECONDS] 'delay in milliseconds between two calls made for the same connection'
                --connection-rate=[COUNT] 'number of connections allowed to open concurrently (per thread)'
                -t, --threads=[NUMBER] 'number of threads to use'",
        )
        .get_matches();

    let addr: SocketAddr = matches.value_of("address").unwrap().parse().unwrap();
    let payload_size: usize = match (matches.value_of("size"), matches.value_of("precise-size")) {
        (Some(s), _) => parse_u64_default(Some(s), 0) as usize * 1024, // -s takes precedence
        (None, Some(p)) => parse_u64_default(Some(p), 0) as usize,
        (None, None) => 0
    };
    let concurrency = parse_u64_default(matches.value_of("concurrency"), 1);
    let threads = cmp::min(
        concurrency,
        parse_u64_default(matches.value_of("threads"), num_cpus::get() as u64),
    );
    let warmup_seconds = parse_u64_default(matches.value_of("warm-up"), 2) as u64;
    let sample_rate = parse_u64_default(matches.value_of("sample-rate"), 1) as u64;
    let delay = Duration::from_millis(parse_u64_default(matches.value_of("delay"), 0));

    let connections_per_thread = cmp::max(concurrency / threads, 1);
    let connection_rate = parse_u64_default(matches.value_of("connection-rate"), connections_per_thread) as usize;
    let perf_counters = Arc::new(PerfCounters::new());
    let threads = (0..threads)
        .map(|i| {
            let counters = perf_counters.clone();
            thread::Builder::new()
                .name(format!("worker{}", i))
                .spawn(move || {
                    if addr.port() / 10u16 % 10 == 8u16 { // port is *8?, e.g. 21082
                        push(
                            connections_per_thread as usize,
                            (i * connections_per_thread) as usize,
                            connection_rate,
                            payload_size,
                            delay,
                            &counters,
                            Rc::new(move |h| connect_tcp(addr, h))
                        )
                    }
                    else {
                        push(
                            connections_per_thread as usize,
                            (i * connections_per_thread) as usize,
                            connection_rate,
                            payload_size,
                            delay,
                            &counters,
                            Rc::new(move |h| connect_tls(addr, h))
                        )
                    }
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let counters = perf_counters.clone();
    let monitor_thread = thread::Builder::new()
        .name("monitor".to_string())
        .spawn(move || {
            thread::sleep(Duration::from_secs(warmup_seconds));
            println!("warm up past");
            let mut prev_reqs = 0;
            let mut prev_lat = 0;
            loop {
                let reqs = counters.request_count();
                if reqs > prev_reqs {
                    let latency = counters.latency_ns();
                    let latency_max = counters.pull_latency_max_ns();
                    let req_count = (reqs - prev_reqs) as u64;
                    let latency_diff = latency - prev_lat;
                    println!(
                        "rate: {}, latency: {}, latency max: {}",
                        req_count / sample_rate,
                        time::Duration::nanoseconds((latency_diff / req_count) as i64),
                        time::Duration::nanoseconds(latency_max as i64)
                    );
                    prev_reqs = reqs;
                    prev_lat = latency;
                }
                thread::sleep(Duration::from_secs(sample_rate));
            }
        })
        .unwrap();

    for thread in threads {
        thread.join().unwrap();
    }
    monitor_thread.join().unwrap();
}

fn parse_u64_default(input: Option<&str>, default: u64) -> u64 {
    input
        .map(|v| v.parse().expect(&format!("not a valid number: {}", v)))
        .unwrap_or(default)
}

fn push<Io, F>(connections: usize, offset: usize, rate: usize, payload_size: usize, delay: Duration, perf_counters: &Arc<PerfCounters>, new_transport: Rc<F>)
    where Io: AsyncRead + AsyncWrite + 'static, F: Fn(Handle) -> Box<Future<Item=Io, Error=Error>> + 'static
{
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let payload = Bytes::from_static(&PAYLOAD_SOURCE[..payload_size]);

    let timestamp = time::precise_time_ns();
    println!("now connecting at rate {}", rate);
    let conn_stream = futures::stream::iter_ok(0..connections)
        .map(|i| connect_with_retry(handle.clone(), new_transport.clone()))
        .buffered(rate)
        .collect()
        .and_then(|connections| {
            println!(
                "done connecting {} in {}",
                connections.len(),
                time::Duration::nanoseconds((time::precise_time_ns() - timestamp) as i64)
            );
            future::join_all(connections.into_iter().map(|conn| {
                run_connection(conn, handle.clone(), Bytes::from(payload.as_ref()), delay, perf_counters.clone())
            }))
        })
        .map_err(|e| {
            println!("error: {:?}", e);
            e
        })
        .and_then(|_| Ok(()));
    core.run(conn_stream).unwrap();
}

#[async]
fn connect_with_retry<Io, F>(handle: Handle, new_transport: Rc<F>) -> Result<Io>
    where Io: AsyncRead + AsyncWrite + 'static, F: Fn(Handle) -> Box<Future<Item=Io, Error=Error>> + 'static
{
    #[async]
    for _ in futures::stream::repeat::<_, io::Error>(0) {
        match await!(new_transport(handle.clone())) {
            Ok(c) => {
                return Ok(c);
            }
            Err(e) => {
                println!("{:?}", e); // todo: log e?
                print!("!"); // todo: log e?
                await!(tokio_delay(Duration::from_secs(20), handle.clone()))?;
            }
        }
    }
    Err(io::Error::from(io::ErrorKind::NotConnected).into())
}

#[async(boxed)]
fn connect_tcp(addr: SocketAddr, handle: Handle) -> Result<TcpStream> {
    let socket = await!(TcpStream::connect(&addr, &handle))?;
    socket.set_nodelay(true).unwrap();
    Ok(socket)
}

#[async(boxed)]
fn connect_tls(addr: SocketAddr, handle: Handle) -> Result<TlsStream<TcpStream>> {
    let socket = await!(connect_tcp(addr, handle))?;
    let tls_context = TlsConnector::builder()
        .unwrap()
        .build()
        .unwrap();
    Ok(await!(tls_context.connect_async("gateway.tests.com", socket).map_err(|e| {
        Error::with_chain(e, ErrorKind::Msg("TLS handshake failed".into()))
    }))?)
}

#[async]
pub fn run_connection<Io: AsyncRead + AsyncWrite + 'static>(io: Io, handle: Handle, payload: Bytes, delay: Duration, perf_counters: Arc<PerfCounters>) -> Result<()> {
    let perf_counters = perf_counters.clone();
    //let mut connection = io.framed(LengthCodec::new());
    let len = payload.len();
    let mut req = BytesMut::with_capacity(len + 4);
    req.put_u32::<bytes::BigEndian>(len as u32);

    req.put_slice(payload.as_ref());
    let mut req = req.freeze();
    let mut io = io;
    let mut resp = vec![0; len + 4];
    for _ in std::iter::repeat(0) {
        if delay > Duration::default() {
            await!(tokio_delay(delay, handle.clone()))?;
        }
        let timestamp = time::precise_time_ns();
        let (io1, req1) = await!(tokio_io::io::write_all(io, req))?; // sending payload
        req = req1;
        let (io2, resp1) = await!(tokio_io::io::read_exact(io1, resp))?;
        io = io2;
        resp = resp1;
        //resp.clear();
        //connection = await!(connection.send(payload.clone()))?; // sending payload
        // let (_, conn1) = await!(connection.into_future()).map_err(|e| e.0)?; // waiting for echo response
        // connection = conn1;
        perf_counters.register_request();
        perf_counters.register_latency(time::precise_time_ns() - timestamp);
    }
    Ok(())
}

pub struct PerfCounters {
    req: AtomicUsize,
    lat: AtomicUsize,
    lat_max: AtomicUsize
}

impl PerfCounters {
    pub fn new() -> PerfCounters {
        PerfCounters {
            req: AtomicUsize::new(0),
            lat: AtomicUsize::new(0),
            lat_max: AtomicUsize::new(0),
        }
    }

    pub fn request_count(&self) -> usize {
        self.req.load(Ordering::SeqCst)
    }

    pub fn latency_ns(&self) -> u64 {
        self.lat.load(Ordering::SeqCst) as u64
    }

    pub fn pull_latency_max_ns(&self) -> u64 {
        self.lat_max.swap(0, Ordering::SeqCst) as u64
    }

    pub fn register_request(&self) {
        self.req.fetch_add(1, Ordering::SeqCst);
    }

    pub fn register_latency(&self, nanos: u64) {
        let nanos = nanos as usize;
        self.lat.fetch_add(nanos, Ordering::SeqCst);
        loop {
            let current = self.lat_max.load(Ordering::SeqCst);
            if current >= nanos || self.lat_max.compare_and_swap(current, nanos, Ordering::SeqCst) == current {
                break;
            }
        }
    }
}
