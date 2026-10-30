//! libzmq's `tests/test_shutdown_stress.cpp`: many threads each connect a
//! SUB to a PUB and close it while the connection is underway, then the
//! PUB closes and the context terminates, on a context with seven I/O
//! threads. libzmq runs it over TCP; it runs here over every transport.

use std::thread;
use std::time::Duration;

use zmq::{Context, Pub, Sub};
use zmq_test_ports::free_endpoint;

use crate::support::within;

const THREAD_COUNT: usize = 100;

fn test(address: fn() -> String) {
    within(Duration::from_secs(60), move || {
        for _ in 0..10 {
            let context = Context::with_io_threads(7).unwrap();
            let mut socket = context.socket::<Pub>();
            let endpoint = socket.bind(&address()).unwrap().to_string();
            let workers: Vec<_> = (0..THREAD_COUNT)
                .map(|_| {
                    let context = context.clone();
                    let endpoint = endpoint.clone();
                    thread::spawn(move || {
                        let mut socket = context.socket::<Sub>();
                        socket.connect(&endpoint).unwrap();
                        // Start closing the socket while the connecting
                        // process is underway.
                        drop(socket);
                    })
                })
                .collect();
            for worker in workers {
                worker.join().unwrap();
            }
            drop(socket);
            drop(context);
        }
    });
}

#[test]
fn tcp() {
    test(free_endpoint);
}

#[cfg(unix)]
#[test]
fn ipc() {
    test(|| "ipc://*".to_owned());
}

#[test]
fn inproc() {
    test(|| "inproc://libzmq-shutdown-stress".to_owned());
}
