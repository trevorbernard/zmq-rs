//! The sockets' `async` methods: each returns a future that waits without
//! blocking its thread, and wakes its task once it can go on. No runtime is
//! needed, so these tests poll the futures by hand.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Hwm, Identity, ManualXPub, Message, Pair, Proxy, Pub, Pull, Push, Rep, Req,
    Router, RoutingId, SocketOptions, Sub, XPub, XPubEvent, XSub,
};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

struct Flag {
    woken: AtomicBool,
    thread: Thread,
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
        self.thread.unpark();
    }
}

/// A task run by hand on the test's thread. It polls a future again only
/// once the future has woken it, as a runtime would, so a lost wakeup
/// fails the test.
struct Task {
    flag: Arc<Flag>,
    waker: Waker,
}

impl Task {
    fn new() -> Self {
        let flag = Arc::new(Flag {
            woken: AtomicBool::new(false),
            thread: thread::current(),
        });
        let waker = Waker::from(flag.clone());
        Self { flag, waker }
    }

    fn poll<F: Future>(&self, future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut TaskContext::from_waker(&self.waker))
    }

    /// Waits until a future this task polled wakes it.
    fn woken(&self) {
        let deadline = Instant::now() + FIVE;
        while !self.flag.woken.swap(false, Ordering::SeqCst) {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "never woken");
            thread::park_timeout(left);
        }
    }

    /// The output of a future that was waiting, once it wakes this task.
    fn resume<F: Future>(&self, mut future: Pin<&mut F>) -> F::Output {
        loop {
            self.woken();
            if let Poll::Ready(output) = self.poll(future.as_mut()) {
                return output;
            }
        }
    }
}

/// Runs `future` to its end.
fn run<F: Future>(future: F) -> F::Output {
    let task = Task::new();
    let mut future = pin!(future);
    match task.poll(future.as_mut()) {
        Poll::Ready(output) => output,
        Poll::Pending => task.resume(future),
    }
}

/// Queues of one message each way, so a peer that does not read soon
/// leaves no room.
fn tiny() -> SocketOptions {
    let one = Hwm::Messages(NonZeroUsize::MIN);
    SocketOptions {
        send_hwm: one,
        recv_hwm: one,
        ..SocketOptions::default()
    }
}

#[test]
fn a_pull_wakes_for_a_message_over_tcp() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind(&free_endpoint()).unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();

    let task = Task::new();
    let mut receiving = pin!(pull.recv_async());
    assert!(task.poll(receiving.as_mut()).is_pending());
    assert_eq!(run(push.send_async("work")), Ok(()));
    assert_eq!(task.resume(receiving), Ok(Message::from("work")));
}

#[test]
fn a_push_waits_for_room() {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(tiny());
    pull.bind("inproc://push-room").unwrap();
    let mut push = context.socket_with::<Push>(tiny());
    push.connect("inproc://push-room").unwrap();
    while push.try_send("fill").is_ok() {}

    let task = Task::new();
    let mut sending = pin!(push.send_async("last"));
    assert!(task.poll(sending.as_mut()).is_pending());
    assert_eq!(pull.try_recv().unwrap(), Some(Message::from("fill")));
    assert_eq!(task.resume(sending), Ok(()));
    let mut received = Vec::new();
    while let Some(message) = pull.try_recv().unwrap() {
        received.push(message);
    }
    assert_eq!(received.last(), Some(&Message::from("last")));
}

#[test]
fn a_pair_sends_and_receives() {
    let context = Context::new().unwrap();
    let mut left = context.socket_with::<Pair>(tiny());
    left.bind("inproc://pair").unwrap();
    let mut right = context.socket_with::<Pair>(tiny());
    right.connect("inproc://pair").unwrap();

    let task = Task::new();
    {
        let mut receiving = pin!(right.recv_async());
        assert!(task.poll(receiving.as_mut()).is_pending());
        assert_eq!(run(left.send_async("ping")), Ok(()));
        assert_eq!(task.resume(receiving), Ok(Message::from("ping")));
    }

    while left.try_send("fill").is_ok() {}
    let mut sending = pin!(left.send_async("last"));
    assert!(task.poll(sending.as_mut()).is_pending());
    assert_eq!(right.try_recv().unwrap(), Some(Message::from("fill")));
    assert_eq!(task.resume(sending), Ok(()));
}

#[test]
fn a_dealer_sends_and_receives() {
    let context = Context::new().unwrap();
    let mut left = context.socket_with::<Dealer>(tiny());
    left.bind("inproc://dealer").unwrap();
    let mut right = context.socket_with::<Dealer>(tiny());
    right.connect("inproc://dealer").unwrap();

    let task = Task::new();
    {
        let mut receiving = pin!(right.recv_async());
        assert!(task.poll(receiving.as_mut()).is_pending());
        assert_eq!(run(left.send_async("ping")), Ok(()));
        assert_eq!(task.resume(receiving), Ok(Message::from("ping")));
    }

    while left.try_send("fill").is_ok() {}
    let mut sending = pin!(left.send_async("last"));
    assert!(task.poll(sending.as_mut()).is_pending());
    assert_eq!(right.try_recv().unwrap(), Some(Message::from("fill")));
    assert_eq!(task.resume(sending), Ok(()));
}

#[test]
fn a_router_receives_and_answers() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("inproc://router").unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("d").unwrap(),
        ..SocketOptions::default()
    });
    dealer.connect("inproc://router").unwrap();

    let task = Task::new();
    let from = {
        let mut receiving = pin!(router.recv_async());
        assert!(task.poll(receiving.as_mut()).is_pending());
        dealer.send("hello").unwrap();
        let (from, message) = task.resume(receiving).unwrap();
        assert_eq!(message, Message::from("hello"));
        from
    };
    assert_eq!(from, RoutingId::new("d").unwrap());
    assert_eq!(run(router.send_async(&from, "hi")), Ok(()));
    assert_eq!(dealer.try_recv().unwrap(), Some(Message::from("hi")));
}

#[test]
fn a_request_and_its_reply() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    rep.bind("inproc://reqrep").unwrap();
    let mut req = context.socket::<Req>();
    req.connect("inproc://reqrep").unwrap();

    let server = Task::new();
    let client = Task::new();
    let mut serving = pin!(rep.recv_async());
    assert!(server.poll(serving.as_mut()).is_pending());
    let mut requesting = pin!(req.request_async("question"));
    assert!(client.poll(requesting.as_mut()).is_pending());

    let request = server.resume(serving).unwrap();
    assert_eq!(request.body(), &Message::from("question"));
    request.reply("answer");
    assert_eq!(client.resume(requesting), Ok(Message::from("answer")));
}

#[test]
fn a_sub_wakes_for_a_publication() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://pubsub").unwrap();
    let mut sub = context.socket::<Sub>();
    sub.subscribe("a");
    sub.connect("inproc://pubsub").unwrap();

    let task = Task::new();
    let mut receiving = pin!(sub.recv_async());
    assert!(task.poll(receiving.as_mut()).is_pending());
    assert_eq!(publisher.send("b: unmatched").unwrap(), 0);
    assert_eq!(publisher.send("a: matched").unwrap(), 1);
    assert_eq!(task.resume(receiving), Ok(Message::from("a: matched")));
}

#[test]
fn send_all_waits_for_every_subscriber_to_have_room() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket_with::<Pub>(tiny());
    publisher.bind("inproc://send-all").unwrap();
    let mut sub = context.socket_with::<Sub>(tiny());
    sub.subscribe("");
    sub.connect("inproc://send-all").unwrap();
    assert_eq!(run(publisher.send_all_async("first")), Ok(1));
    while publisher.try_send_all("fill").is_ok() {}

    let task = Task::new();
    let mut sending = pin!(publisher.send_all_async("last"));
    assert!(task.poll(sending.as_mut()).is_pending());
    assert_eq!(sub.try_recv().unwrap(), Some(Message::from("first")));
    assert_eq!(task.resume(sending), Ok(1));
}

#[test]
fn an_xpub_wakes_for_a_subscription() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://xpub").unwrap();
    let mut sub = context.socket::<Sub>();
    sub.connect("inproc://xpub").unwrap();

    let task = Task::new();
    let mut receiving = pin!(xpub.recv_async());
    assert!(task.poll(receiving.as_mut()).is_pending());
    sub.subscribe("topic");
    assert_eq!(
        task.resume(receiving),
        Ok(XPubEvent::Subscribe("topic".into()))
    );
}

#[test]
fn a_manual_xpub_wakes_for_a_request_to_subscribe() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<ManualXPub>();
    xpub.bind("inproc://manual").unwrap();
    assert!(matches!(xpub.try_recv(), Ok(None)));
    let mut sub = context.socket::<Sub>();
    sub.connect("inproc://manual").unwrap();

    let task = Task::new();
    {
        let mut receiving = pin!(xpub.recv_async());
        assert!(task.poll(receiving.as_mut()).is_pending());
        sub.subscribe("topic");
        let mut request = task.resume(receiving).unwrap();
        assert_eq!(request.event(), &XPubEvent::Subscribe("topic".into()));
        assert!(request.accept());
    }
    assert_eq!(xpub.send("topic: now subscribed").unwrap(), 1);

    sub.subscribe("other");
    let mut request = xpub.try_recv().unwrap().expect("the second request");
    assert_eq!(request.event(), &XPubEvent::Subscribe("other".into()));
    assert!(request.accept());
}

#[test]
fn an_xsub_wakes_for_a_publication() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://xsub").unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.subscribe("a");
    xsub.connect("inproc://xsub").unwrap();

    let task = Task::new();
    let mut receiving = pin!(xsub.recv_async());
    assert!(task.poll(receiving.as_mut()).is_pending());
    assert_eq!(publisher.send("a: matched").unwrap(), 1);
    assert_eq!(task.resume(receiving), Ok(Message::from("a: matched")));
}

#[test]
fn a_proxy_runs_as_a_task_until_it_is_terminated() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Pull>();
    frontend.bind("inproc://proxy-front").unwrap();
    let mut backend = context.socket::<Push>();
    backend.bind("inproc://proxy-back").unwrap();
    let mut source = context.socket::<Push>();
    source.connect("inproc://proxy-front").unwrap();
    let mut sink = context.socket::<Pull>();
    sink.connect("inproc://proxy-back").unwrap();

    let mut proxy = Proxy::new(&mut frontend, &mut backend);
    let control = proxy.control();
    let task = Task::new();
    let mut running = pin!(proxy.run_async());
    assert!(task.poll(running.as_mut()).is_pending());

    source.send("through").unwrap();
    task.woken();
    assert!(task.poll(running.as_mut()).is_pending());
    assert_eq!(sink.try_recv().unwrap(), Some(Message::from("through")));

    control.terminate();
    let statistics = task.resume(running);
    assert_eq!(statistics.frontend.received.messages, 1);
    assert_eq!(statistics.backend.sent.messages, 1);
}

#[cfg(feature = "draft")]
mod draft {
    use zmq::{
        Channel, Client, ClientId, Dish, Frame, Gather, Group, Peer, PeerId, Radio, Scatter,
        SendError, Server,
    };

    use super::*;

    #[test]
    fn a_dish_wakes_for_its_group() {
        let news = Group::new("news").unwrap();
        let context = Context::new().unwrap();
        let mut radio = context.socket::<Radio>();
        radio.bind("inproc://radio").unwrap();
        let mut dish = context.socket::<Dish>();
        dish.connect("inproc://radio").unwrap();
        assert!(dish.join(&news));

        let task = Task::new();
        let mut receiving = pin!(dish.recv_async());
        assert!(task.poll(receiving.as_mut()).is_pending());
        assert_eq!(radio.send(&news, "rain").unwrap(), 1);
        assert_eq!(task.resume(receiving), Ok((news, Frame::from("rain"))));
    }

    #[test]
    fn a_client_and_a_server_talk() {
        let context = Context::new().unwrap();
        let mut server = context.socket_with::<Server>(tiny());
        server.bind("inproc://server").unwrap();
        let mut client = context.socket_with::<Client>(tiny());
        client.connect("inproc://server").unwrap();

        let task = Task::new();
        let from = {
            let mut receiving = pin!(server.recv_async());
            assert!(task.poll(receiving.as_mut()).is_pending());
            assert_eq!(run(client.send_async("hello")), Ok(()));
            let (from, frame) = task.resume(receiving).unwrap();
            assert_eq!(frame, Frame::from("hello"));
            from
        };
        {
            let mut receiving = pin!(client.recv_async());
            assert!(task.poll(receiving.as_mut()).is_pending());
            assert_eq!(run(server.send_async(from, "hi")), Ok(()));
            assert_eq!(task.resume(receiving), Ok(Frame::from("hi")));
        }

        while server.try_send(from, "fill").is_ok() {}
        let mut sending = pin!(server.send_async(from, "last"));
        assert!(task.poll(sending.as_mut()).is_pending());
        assert_eq!(client.try_recv().unwrap(), Some(Frame::from("fill")));
        assert_eq!(task.resume(sending), Ok(()));
    }

    #[test]
    fn a_server_refuses_an_unknown_client_at_once() {
        let context = Context::new().unwrap();
        let mut server = context.socket::<Server>();
        let nobody = ClientId::new(7).unwrap();
        assert_eq!(
            run(server.send_async(nobody, "lost")),
            Err(SendError::UnknownPeer(Frame::from("lost")))
        );
    }

    #[test]
    fn peers_talk() {
        let context = Context::new().unwrap();
        let mut alice = context.socket_with::<Peer>(tiny());
        alice.bind("inproc://peer").unwrap();
        let mut bob = context.socket_with::<Peer>(tiny());
        let to_alice = bob.connect_peer("inproc://peer").unwrap();

        let task = Task::new();
        let to_bob = {
            let mut receiving = pin!(alice.recv_async());
            assert!(task.poll(receiving.as_mut()).is_pending());
            assert_eq!(run(bob.send_async(to_alice, "hello")), Ok(()));
            let (from, frame) = task.resume(receiving).unwrap();
            assert_eq!(frame, Frame::from("hello"));
            from
        };

        while alice.try_send(to_bob, "fill").is_ok() {}
        let mut sending = pin!(alice.send_async(to_bob, "last"));
        assert!(task.poll(sending.as_mut()).is_pending());
        assert_eq!(
            bob.try_recv().unwrap(),
            Some((to_alice, Frame::from("fill")))
        );
        assert_eq!(task.resume(sending), Ok(()));
    }

    #[test]
    fn a_peer_refuses_an_unknown_peer_at_once() {
        let context = Context::new().unwrap();
        let mut peer = context.socket::<Peer>();
        let nobody = PeerId::new(7).unwrap();
        assert_eq!(
            run(peer.send_async(nobody, "lost")),
            Err(SendError::UnknownPeer(Frame::from("lost")))
        );
    }

    #[test]
    fn a_scatter_and_a_gather() {
        let context = Context::new().unwrap();
        let mut gather = context.socket_with::<Gather>(tiny());
        gather.bind("inproc://gather").unwrap();
        let mut scatter = context.socket_with::<Scatter>(tiny());
        scatter.connect("inproc://gather").unwrap();

        let task = Task::new();
        {
            let mut receiving = pin!(gather.recv_async());
            assert!(task.poll(receiving.as_mut()).is_pending());
            assert_eq!(run(scatter.send_async("piece")), Ok(()));
            assert_eq!(task.resume(receiving), Ok(Frame::from("piece")));
        }

        while scatter.try_send("fill").is_ok() {}
        let mut sending = pin!(scatter.send_async("last"));
        assert!(task.poll(sending.as_mut()).is_pending());
        assert_eq!(gather.try_recv().unwrap(), Some(Frame::from("fill")));
        assert_eq!(task.resume(sending), Ok(()));
    }

    #[test]
    fn a_channel_sends_and_receives() {
        let context = Context::new().unwrap();
        let mut left = context.socket_with::<Channel>(tiny());
        left.bind("inproc://channel").unwrap();
        let mut right = context.socket_with::<Channel>(tiny());
        right.connect("inproc://channel").unwrap();

        let task = Task::new();
        {
            let mut receiving = pin!(right.recv_async());
            assert!(task.poll(receiving.as_mut()).is_pending());
            assert_eq!(run(left.send_async("ping")), Ok(()));
            assert_eq!(task.resume(receiving), Ok(Frame::from("ping")));
        }

        while left.try_send("fill").is_ok() {}
        let mut sending = pin!(left.send_async("last"));
        assert!(task.poll(sending.as_mut()).is_pending());
        assert_eq!(right.try_recv().unwrap(), Some(Frame::from("fill")));
        assert_eq!(task.resume(sending), Ok(()));
    }
}
