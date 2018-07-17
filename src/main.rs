extern crate bytes;
#[macro_use]
extern crate structopt;
extern crate futures;
extern crate num_cpus;
extern crate chrono;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;
extern crate tokio_tls;
extern crate native_tls;
#[macro_use]
extern crate error_chain;

use std::net::SocketAddr;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use std::{io, thread};
use futures::{future, Future, Stream};
use bytes::{Bytes, BytesMut, BufMut};
use std::sync::Arc;
use std::time::Duration;
use native_tls::TlsConnector;
use tokio_tls::{TlsConnectorExt, TlsStream};
use tokio_io::{AsyncRead, AsyncWrite};
use std::rc::Rc;
use structopt::StructOpt;

mod error;
mod codec;
mod counters;
mod options;
use error::*;
use codec::LengthCodec;
use counters::PerfCounters;
use options::Opt;

static PAYLOAD_SOURCE: &[u8] = include_bytes!("lorem.txt");

fn tokio_delay(val: Duration) -> impl Future<Item = (), Error = io::Error> {
    tokio_timer::sleep(val).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

fn main() {
    let opt = Arc::new(Opt::from_args());
    let perf_counters = Arc::new(PerfCounters::new());

    let threads = (0..opt.threads())
        .map(|i| {
            let counters = perf_counters.clone();
            let opt = opt.clone();
            thread::Builder::new()
                .name(format!("worker{}", i))
                .spawn(move || {
                    if opt.address.port() / 10u16 % 10 == 8u16 { // port is *8?, e.g. 21082
                        push(
                            opt.connections_per_thread() as usize,
                            (i * opt.connections_per_thread()) as usize,
                            opt.connection_rate(),
                            opt.payload_size(),
                            opt.delay(),
                            &counters,
                            Rc::new(move |h| connect_tcp(opt.address, h))
                        )
                    }
                    else {
                        push(
                            opt.connections_per_thread() as usize,
                            (i * opt.connections_per_thread()) as usize,
                            opt.connection_rate(),
                            opt.payload_size(),
                            opt.delay(),
                            &counters,
                            Rc::new(move |h| connect_tls(opt.address, h))
                        )
                    }
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let monitor_thread = counters::setup_monitor(perf_counters, opt.warm_up_seconds, opt.sample_rate_seconds);

    for thread in threads {
        thread.join().unwrap();
    }
    monitor_thread.join().unwrap();
}

fn push<Io, F, Ft>(connections: usize, offset: usize, rate: usize, payload_size: usize, delay: Duration, perf_counters: &Arc<PerfCounters>, new_transport: Rc<F>)
    where Io: AsyncRead + AsyncWrite + 'static, F: 'static + Fn(Handle) -> Ft, Ft: Future<Item=Io, Error=Error> + 'static
{
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let payload = Bytes::from_static(&PAYLOAD_SOURCE[..payload_size]);

    let timestamp = ::std::time::Instant::now();
    println!("now connecting at rate {}", rate);
    let conn_stream = futures::stream::iter_ok(0..connections)
        .map(|_| connect_with_retry(handle.clone(), new_transport.clone()))
        .buffered(rate)
        .collect()
        .and_then(|connections| {
            println!(
                "done connecting {} in {}",
                connections.len(),
                chrono::Duration::from_std(timestamp.elapsed()).unwrap()
            );
            for conn in connections {
                handle.spawn(run_connection(conn, handle.clone(), Bytes::from(payload.as_ref()), delay, perf_counters.clone())
                        .map_err(|e| println!("error: {:?}", e)));
            }
            future::empty::<(), _>()
        });
    core.run(conn_stream).expect("Worker failed");
}

fn connect_with_retry<Io, F, Ft>(handle: Handle, new_transport: Rc<F>) -> Box<Future<Item = Io, Error = Error>>
    where Io: AsyncRead + AsyncWrite + 'static, F: 'static + Fn(Handle) -> Ft, Ft: Future<Item=Io, Error=Error> + 'static
{
    Box::new(new_transport.clone()(handle.clone())
        .or_else(move |_e| {
            // println!("{:?}", _e);
            print!("!");
            tokio_delay(Duration::from_secs(20))
                .from_err()
                .and_then(move |_| connect_with_retry(handle, new_transport))
        })
    )
}

fn connect_tcp(addr: SocketAddr, handle: Handle) -> impl Future<Item = TcpStream, Error = Error> {
    TcpStream::connect(&addr, &handle)
        .from_err()
        .and_then(|socket| {
            socket.set_nodelay(true)?;
            Ok(socket)
        })
}

fn connect_tls(addr: SocketAddr, handle: Handle) -> impl Future<Item = TlsStream<TcpStream>, Error = Error> {
    connect_tcp(addr, handle)
        .from_err()
        .and_then(|socket| {
            let tls_context = TlsConnector::builder()
                .unwrap()
                .build()
                .unwrap();
            tls_context.connect_async("gateway.tests.com", socket).map_err(|e| {
                Error::with_chain(e, ErrorKind::Msg("TLS handshake failed".into()))
            })
        })
}

pub fn run_connection<Io: AsyncRead + AsyncWrite + 'static>(io: Io, handle: Handle, payload: Bytes, delay: Duration, perf_counters: Arc<PerfCounters>
) -> impl Future<Item = (), Error = Error> {
    let perf_counters = perf_counters.clone();
    //let mut connection = io.framed(LengthCodec::new());
    let len = payload.len();
    let mut req = BytesMut::with_capacity(len + 4);
    req.put_u32_be(len as u32);
    req.put_slice(payload.as_ref());
    let req = req.freeze();
    let resp = vec![0; len + 4];
    futures::future::loop_fn(
        (io, req, resp, perf_counters, handle),
        move |(io, req, resp, perf_counters, handle)| {
            let rec = perf_counters.start_request();
            tokio_io::io::write_all(io, req) // sending payload
                .from_err()
                .and_then(move |(io, req)| {
                    tokio_io::io::read_exact(io, resp)
                        .from_err()
                        .and_then(move |(io, resp)| {
                            perf_counters.stop_request(rec);
                            let fut = if delay > Duration::default() {
                                future::Either::A(tokio_delay(delay).from_err())
                            }
                            else {
                                future::Either::B(future::ok(()))
                            };
                            fut.map(move |_| future::Loop::Continue((io, req, resp, perf_counters, handle)))
                        })
                })
        })
}

// pub struct PerfCounters {
//     req: AtomicUsize,
//     lat: AtomicUsize,
//     lat_max: AtomicUsize
// }

// impl PerfCounters {
//     pub fn new() -> PerfCounters {
//         PerfCounters {
//             req: AtomicUsize::new(0),
//             lat: AtomicUsize::new(0),
//             lat_max: AtomicUsize::new(0),
//         }
//     }

//     pub fn request_count(&self) -> usize {
//         self.req.load(Ordering::SeqCst)
//     }

//     pub fn latency_ns(&self) -> u64 {
//         self.lat.load(Ordering::SeqCst) as u64
//     }

//     pub fn pull_latency_max_ns(&self) -> u64 {
//         self.lat_max.swap(0, Ordering::SeqCst) as u64
//     }

//     pub fn register_request(&self) {
//         self.req.fetch_add(1, Ordering::SeqCst);
//     }

//     pub fn register_latency(&self, duration: Duration) {
//         let nanos = duration.as_secs() as usize * 1_000_000_000 + duration.subsec_nanos() as usize;
//         self.lat.fetch_add(nanos as usize, Ordering::SeqCst);
//         loop {
//             let current = self.lat_max.load(Ordering::SeqCst);
//             if current >= nanos || self.lat_max.compare_and_swap(current, nanos, Ordering::SeqCst) == current {
//                 break;
//             }
//         }
//     }
// }
