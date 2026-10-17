//! The built-in proxy, as libzmq's `zmq_proxy_steerable`: shared queues,
//! forwarders and streamers, with capture and steering.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Message, Pair, Proxy, ProxyStatistics, Pub, Pull, Push, Rep, Req, Router, Sub,
    XPub, XSub,
};

const FIVE: Duration = Duration::from_secs(5);

/// Terminates the proxy when dropped, so a failing test stops it instead
/// of waiting on it forever.
struct StopOnDrop(zmq::ProxyControl);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.terminate();
    }
}

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

#[test]
fn a_router_dealer_proxy_is_a_shared_queue() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Router>();
    frontend.bind("inproc://clients").unwrap();
    let mut backend = context.socket::<Dealer>();
    backend.bind("inproc://workers").unwrap();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();

    let workers: Vec<_> = (0..2)
        .map(|n| {
            let mut rep = context.socket::<Rep>();
            rep.connect("inproc://workers").unwrap();
            thread::spawn(move || {
                for _ in 0..2 {
                    let request = rep.recv();
                    let text = String::from_utf8_lossy(request.body().get(0).unwrap());
                    let answer = format!("worker {n}: {text}");
                    request.reply(answer);
                }
            })
        })
        .collect();
    let clients: Vec<_> = (0..4)
        .map(|n| {
            let mut req = context.socket::<Req>();
            req.connect("inproc://clients").unwrap();
            thread::spawn(move || req.request(format!("request {n}")).unwrap())
        })
        .collect();

    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        let answers: Vec<Message> = clients
            .into_iter()
            .map(|client| within(FIVE, move || client.join().unwrap()))
            .collect();
        for (n, answer) in answers.iter().enumerate() {
            let text = String::from_utf8_lossy(answer.get(0).unwrap()).into_owned();
            assert!(text.ends_with(&format!("request {n}")), "{text}");
        }
        control.terminate();
        let statistics = running.join().unwrap();
        // Each request is three parts at the ROUTER (id, delimiter, body)
        // and two at the DEALER (delimiter, body), and so is each reply.
        assert_eq!(statistics.frontend.received.messages, 12);
        assert_eq!(statistics.backend.sent.messages, 12);
        assert_eq!(statistics.backend.received.messages, 12);
        assert_eq!(statistics.frontend.sent.messages, 12);
    });
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn an_xsub_xpub_proxy_forwards_subscriptions_up_and_messages_down() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://upstream").unwrap();
    let mut frontend = context.socket::<XSub>();
    frontend.connect("inproc://upstream").unwrap();
    let mut backend = context.socket::<XPub>();
    backend.bind("inproc://downstream").unwrap();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();

    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        let mut subscriber = context.socket::<Sub>();
        subscriber.connect("inproc://downstream").unwrap();
        subscriber.subscribe("news");
        let started = Instant::now();
        while publisher.send("news: forwarded") == 0 {
            assert!(
                started.elapsed() < FIVE,
                "the subscription never got upstream"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            publisher.send("sports"),
            0,
            "only wanted topics go upstream"
        );
        assert_eq!(
            within(FIVE, move || subscriber.recv()),
            Message::from("news: forwarded")
        );
        control.terminate();
        running.join().unwrap();
    });
}

#[test]
fn a_pull_push_proxy_is_a_streamer() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Pull>();
    frontend.bind("inproc://in").unwrap();
    let mut backend = context.socket::<Push>();
    backend.bind("inproc://out").unwrap();
    let mut source = context.socket::<Push>();
    source.connect("inproc://in").unwrap();
    let mut sink = context.socket::<Pull>();
    sink.connect("inproc://out").unwrap();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();
    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        for i in 0..1000 {
            source.send(format!("{i}"));
        }
        let received = within(FIVE, move || {
            (0..1000).map(|_| sink.recv()).collect::<Vec<_>>()
        });
        let expected: Vec<Message> = (0..1000).map(|i| Message::from(format!("{i}"))).collect();
        assert_eq!(received, expected, "in order, none lost");
        control.terminate();
        let statistics = running.join().unwrap();
        assert_eq!(statistics.frontend.received.messages, 1000);
        assert_eq!(statistics.backend.sent.messages, 1000);
        assert_eq!(statistics.frontend.sent.messages, 0);
    });
}

#[test]
fn capture_sees_both_directions() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Pair>();
    frontend.bind("inproc://left").unwrap();
    let mut backend = context.socket::<Pair>();
    backend.bind("inproc://right").unwrap();
    let mut capture = context.socket::<Pub>();
    capture.bind("inproc://tap").unwrap();
    let mut tap = context.socket::<Sub>();
    tap.subscribe("");
    tap.connect("inproc://tap").unwrap();
    let mut left = context.socket::<Pair>();
    left.connect("inproc://left").unwrap();
    let mut right = context.socket::<Pair>();
    right.connect("inproc://right").unwrap();

    let mut proxy = Proxy::new(&mut frontend, &mut backend).capture(&mut capture);
    let control = proxy.control();
    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        left.send("to the right");
        assert_eq!(
            within(FIVE, move || {
                let message = right.recv();
                right.send("to the left");
                message
            }),
            Message::from("to the right")
        );
        assert_eq!(
            within(FIVE, move || left.recv()),
            Message::from("to the left")
        );
        let copies = within(FIVE, move || [tap.recv(), tap.recv()]);
        assert_eq!(
            copies,
            [Message::from("to the right"), Message::from("to the left")]
        );
        control.terminate();
        running.join().unwrap();
    });
}

#[test]
fn pausing_holds_messages_until_resumed() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Pull>();
    frontend.bind("inproc://paused-in").unwrap();
    let mut backend = context.socket::<Push>();
    backend.bind("inproc://paused-out").unwrap();
    let mut source = context.socket::<Push>();
    source.connect("inproc://paused-in").unwrap();
    let mut sink = context.socket::<Pull>();
    sink.connect("inproc://paused-out").unwrap();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();
    control.pause();
    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        source.send("held");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(sink.try_recv(), None, "nothing moves while paused");
        assert_eq!(control.statistics(), ProxyStatistics::default());
        control.resume();
        assert_eq!(within(FIVE, move || sink.recv()), Message::from("held"));
        let statistics = control.statistics();
        assert_eq!(statistics.frontend.received.messages, 1);
        assert_eq!(statistics.frontend.received.bytes, 4);
        control.terminate();
        assert_eq!(running.join().unwrap(), statistics);
    });
}

#[test]
fn terminating_an_idle_proxy_returns_at_once() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Router>();
    let mut backend = context.socket::<Dealer>();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();
    let started = Instant::now();
    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        thread::sleep(Duration::from_millis(50));
        control.terminate();
        running.join().unwrap();
    });
    assert!(started.elapsed() < FIVE);
}

#[test]
fn a_slow_backend_holds_up_only_its_own_direction() {
    // Replies keep flowing frontwards while the backend cannot take more.
    let context = Context::new().unwrap();
    let one = zmq::Hwm::Messages(std::num::NonZeroUsize::MIN);
    let mut frontend = context.socket::<Pair>();
    frontend.bind("inproc://busy-left").unwrap();
    let mut backend = context.socket_with::<Pair>(zmq::SocketOptions {
        send_hwm: one,
        ..zmq::SocketOptions::default()
    });
    backend.bind("inproc://busy-right").unwrap();
    let mut left = context.socket::<Pair>();
    left.connect("inproc://busy-left").unwrap();
    let mut right = context.socket_with::<Pair>(zmq::SocketOptions {
        recv_hwm: one,
        ..zmq::SocketOptions::default()
    });
    right.connect("inproc://busy-right").unwrap();
    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();
    thread::scope(|scope| {
        let running = scope.spawn(|| proxy.run());
        let _stop = StopOnDrop(control.clone());
        for i in 0..10 {
            left.send(format!("{i}"));
        }
        // Nobody reads `right`: its queue fills and the proxy holds the rest.
        thread::sleep(Duration::from_millis(50));
        right.send("meanwhile");
        let (heard, mut left) = within(FIVE, move || (left.recv(), left));
        assert_eq!(heard, Message::from("meanwhile"));
        let received = within(FIVE, move || {
            (0..10).map(|_| right.recv()).collect::<Vec<_>>()
        });
        assert_eq!(received.len(), 10, "nothing lost while held");
        let _ = &mut left;
        control.terminate();
        running.join().unwrap();
    });
}
