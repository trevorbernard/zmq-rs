//! libzmq's `tests/test_proxy.cpp`: asynchronous clients (DEALER) talk to
//! a pool of workers (DEALER) through a ROUTER-to-DEALER proxy, all
//! steered by a PUB control socket in a context of its own.
//!
//! libzmq stops `zmq_proxy` by terminating its context; here the proxy is
//! stopped through its `ProxyControl`. libzmq hands each client the
//! frontend's endpoint over an inproc PAIR so as not to fix a port; here
//! the endpoint is passed to the thread directly.

use std::hash::{BuildHasher, RandomState};
use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Identity, Linger, Message, Proxy, Pub, Router, SocketOptions, Sub};

use crate::support::within;

const CONTENT_SIZE: usize = 13;
const ROUTING_ID_SIZE: usize = 10;
pub(crate) const QT_WORKERS: usize = 5;
pub(crate) const QT_CLIENTS: usize = 3;

fn rand(n: u64) -> u64 {
    RandomState::new().hash_one(()) % n
}

pub(crate) fn no_linger() -> SocketOptions {
    SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    }
}

fn control_socket(control_context: &Context) -> zmq::Socket<Sub> {
    let mut control = control_context.socket_with::<Sub>(no_linger());
    control.subscribe("");
    control.connect("inproc://control").unwrap();
    control
}

/// Sends a request every 200 ms and checks each reply, until told to STOP
/// sending and then to TERMINATE.
pub(crate) fn client_task(context: Context, control_context: Context, endpoint: String) {
    // A random routing id, to make tracing easier; libzmq's includes the
    // terminating NUL, making it ROUTING_ID_SIZE octets.
    let routing_id = format!("{:04X}-{:04X}\0", rand(0xFFFF), rand(0xFFFF));
    let mut client = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new(routing_id.into_bytes()).unwrap(),
        ..no_linger()
    });
    client.connect(&endpoint).unwrap();
    let mut control = control_socket(&control_context);

    let mut request_nbr = 0;
    let mut keep_sending = true;
    'run: loop {
        // Tick once per 200 ms, pulling in arriving messages
        for _ in 0..20 {
            let ready = zmq::poll_timeout(
                [client.readable(), control.readable()],
                Duration::from_millis(10),
            );
            let Some([reply, command]) = ready else {
                continue;
            };
            if reply {
                let content = client.try_recv().expect("readable");
                assert_eq!(content.len().get(), 1, "a single part");
                let body = content.last();
                assert_eq!(body.len(), CONTENT_SIZE);
                assert!(body.starts_with(b"request #"));
            }
            if command {
                let command = control.try_recv().expect("readable");
                let command = command.last();
                if command.starts_with(b"TERMINATE") {
                    break 'run;
                }
                if command.starts_with(b"STOP") {
                    keep_sending = false;
                    break;
                }
            }
        }
        if keep_sending {
            request_nbr += 1;
            // CONTENT_SIZE octets: libzmq sends the C string's NUL too
            client.send(format!("request #{request_nbr:03}\0"));
        }
    }
}

/// Handles one request at a time, sending 0 to 4 replies back with random
/// delays, until told to STOP replying and then to TERMINATE.
pub(crate) fn server_worker(context: Context, control_context: Context) {
    let mut worker = context.socket_with::<Dealer>(no_linger());
    worker.connect("inproc://backend").unwrap();
    let mut control = control_socket(&control_context);

    let mut keep_sending = true;
    loop {
        if let Some(command) = control.try_recv() {
            let command = command.last();
            if command.starts_with(b"TERMINATE") {
                return;
            }
            if command.starts_with(b"STOP") {
                keep_sending = false;
            }
        }
        // The DEALER gives us the reply envelope and the request
        let Some(request) = worker.try_recv() else {
            thread::sleep(Duration::from_millis(1));
            continue;
        };
        let (routing_id, rest) = request.into_parts();
        assert_eq!(routing_id.len(), ROUTING_ID_SIZE);
        let content = Message::try_from(rest).expect("a request after the envelope");
        assert_eq!(content.last().len(), CONTENT_SIZE);
        if keep_sending {
            for _ in 0..rand(5) {
                thread::sleep(Duration::from_millis(rand(10) + 1));
                let mut reply = Message::new(routing_id.clone());
                reply.push(content.last().clone());
                worker.send(reply);
            }
        }
    }
}

#[test]
fn proxy() {
    within(Duration::from_secs(30), || {
        let context = Context::new().unwrap();
        let control_context = Context::new().unwrap();
        let mut control = control_context.socket_with::<Pub>(no_linger());
        control.bind("inproc://control").unwrap();

        // Frontend talks to clients over TCP, backend to workers over inproc
        let mut frontend = context.socket_with::<Router>(no_linger());
        let endpoint = frontend.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let mut backend = context.socket_with::<Dealer>(no_linger());
        backend.bind("inproc://backend").unwrap();

        let workers: Vec<_> = (0..QT_WORKERS)
            .map(|_| {
                let (context, control_context) = (context.clone(), control_context.clone());
                thread::spawn(move || server_worker(context, control_context))
            })
            .collect();
        let clients: Vec<_> = (0..QT_CLIENTS)
            .map(|_| {
                let (context, control_context) = (context.clone(), control_context.clone());
                let endpoint = endpoint.clone();
                thread::spawn(move || client_task(context, control_context, endpoint))
            })
            .collect();

        let mut proxy = Proxy::new(&mut frontend, &mut backend);
        let proxy_control = proxy.control();
        let steering = thread::spawn(move || {
            // Run for 500 ms, then stop and terminate everyone
            thread::sleep(Duration::from_millis(500));
            control.send("STOP");
            thread::sleep(Duration::from_millis(500));
            control.send("TERMINATE");
            thread::sleep(Duration::from_millis(500));
            proxy_control.terminate();
        });
        let statistics = proxy.run();
        steering.join().unwrap();
        for thread in workers.into_iter().chain(clients) {
            thread.join().unwrap();
        }
        assert!(
            statistics.frontend.received.messages > 0,
            "clients sent requests"
        );
    });
}
