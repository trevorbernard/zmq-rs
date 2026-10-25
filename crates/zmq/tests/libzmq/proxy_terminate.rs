//! libzmq's `tests/test_proxy_terminate.cpp`, for issue #1382: a SUB-to-PUSH
//! proxy whose PUSH has no peer must still stop when told to, though it
//! holds a message it cannot send.
//!
//! libzmq stops `zmq_proxy` by terminating its context; here it is stopped
//! through its `ProxyControl`.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Proxy, Pub, Push, Sub};

use crate::support::{FIVE, settle, within};

#[test]
fn proxy_terminate() {
    let context = Context::new().unwrap();
    let (endpoints, endpoint) = mpsc::channel();
    let (controls, control) = mpsc::channel();
    let server = thread::spawn({
        let context = context.clone();
        move || {
            let mut frontend = context.socket::<Sub>();
            frontend.subscribe("");
            let bound = frontend.bind("tcp://127.0.0.1:*").unwrap();
            // A PUSH that is never read
            let mut backend = context.socket::<Push>();
            backend.bind("tcp://127.0.0.1:*").unwrap();
            endpoints.send(bound.to_string()).unwrap();

            let mut proxy = Proxy::new(&mut frontend, &mut backend);
            controls.send(proxy.control()).unwrap();
            proxy.run()
        }
    });
    let endpoint = endpoint.recv().unwrap();
    let control = control.recv().unwrap();

    thread::sleep(Duration::from_millis(500));
    let mut publisher = context.socket::<Pub>();
    publisher.connect(&endpoint).unwrap();
    settle();
    for _ in 0..3 {
        publisher.send("This is a test");
        thread::sleep(Duration::from_millis(50));
    }

    control.terminate();
    let statistics = within(FIVE, move || server.join().unwrap());
    assert!(
        statistics.frontend.received.messages > 0,
        "the proxy took a message"
    );
    assert_eq!(
        statistics.backend.sent.messages, 0,
        "with nobody to send it to"
    );
}
