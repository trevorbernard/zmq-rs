//! Our proxy between libzmq clients and workers.

use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Proxy, Router};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

/// Terminates the proxy when dropped, so a failing test stops it instead
/// of waiting on it forever.
struct StopOnDrop(zmq::ProxyControl);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.terminate();
    }
}

#[test]
fn libzmq_clients_reach_libzmq_workers_through_our_proxy() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Router>();
    let clients_at = frontend.bind(&free_endpoint()).unwrap();
    let mut backend = context.socket::<Dealer>();
    let workers_at = backend.bind(&free_endpoint()).unwrap();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();

    let worker = Socket::new(Kind::Rep);
    worker.set_receive_timeout(FIVE);
    worker.connect(workers_at);
    let serving = thread::spawn(move || {
        for _ in 0..3 {
            let request = worker.recv().unwrap();
            let mut reply = b"re: ".to_vec();
            reply.extend_from_slice(&request[0]);
            worker.send(&[&reply]).unwrap();
        }
    });

    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        for n in 0..3 {
            let client = Socket::new(Kind::Req);
            client.set_receive_timeout(FIVE);
            client.connect(clients_at.clone());
            let request = format!("request {n}");
            client.send(&[request.as_bytes()]).unwrap();
            assert_eq!(
                client.recv().unwrap(),
                [format!("re: request {n}").into_bytes()]
            );
        }
        control.terminate();
        let statistics = running.join().unwrap();
        assert_eq!(statistics.frontend.received.messages, 9);
        assert_eq!(statistics.frontend.sent.messages, 9);
    });
    serving.join().unwrap();
}
