//! Every pair of socket types that talk to each other, between ours and
//! libzmq's: each as either type, with either of the two binding, over
//! `tcp` on IPv4 and IPv6 and over `ipc`, by path and by abstract name.

use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Frame, Message, Pair, Pub, Pull, Push, Rep, Req, Router, RoutingId,
    SocketOptions, SocketType, Sub, XPub, XPubEvent, XSub,
};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_test_ports::free_port;

const FIVE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
enum Binds {
    Ours,
    Libzmq,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Transport {
    Tcp,
    Tcp6,
    Ipc,
    /// A name in Linux's abstract socket namespace.
    IpcAbstract,
}

/// How a socket of ours and one of libzmq's are joined.
#[derive(Debug, Clone, Copy)]
struct Link {
    binds: Binds,
    transport: Transport,
}

/// Every link this host can make: IPv6 only where its loopback can be
/// bound, `ipc` only on Unix.
fn links() -> Vec<Link> {
    let mut transports = vec![Transport::Tcp];
    if TcpListener::bind("[::1]:0").is_ok() {
        transports.push(Transport::Tcp6);
    }
    if cfg!(unix) {
        transports.push(Transport::Ipc);
    }
    if cfg!(any(target_os = "linux", target_os = "android")) {
        transports.push(Transport::IpcAbstract);
    }
    transports
        .into_iter()
        .flat_map(|transport| [Binds::Ours, Binds::Libzmq].map(|binds| Link { binds, transport }))
        .collect()
}

/// The start of the `ipc` paths libzmq binds for this test.
fn ipc_prefix() -> String {
    format!("zmq-interop-pairs-{}-", std::process::id())
}

/// Removes, when dropped, the socket files libzmq leaves: its context
/// outlives the test, and with it the listeners that would remove them.
struct Sweep;

impl Drop for Sweep {
    fn drop(&mut self) {
        let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
            return;
        };
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(&ipc_prefix())
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

impl Link {
    /// What a socket of ours binds, to be told where it is bound.
    fn wildcard(self) -> String {
        match self.transport {
            Transport::Tcp => "tcp://127.0.0.1:*".to_owned(),
            Transport::Tcp6 => "tcp://[::1]:*".to_owned(),
            Transport::Ipc => "ipc://*".to_owned(),
            // An abstract name has no wildcard.
            Transport::IpcAbstract => self.free(),
        }
    }

    /// An endpoint nothing is bound to, for libzmq to bind.
    fn free(self) -> String {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        match self.transport {
            Transport::Tcp => format!("tcp://{}", free_port()),
            Transport::Tcp6 => format!("tcp://[::1]:{}", free_port().port()),
            Transport::Ipc => format!(
                "ipc://{}/{}{}",
                std::env::temp_dir().display(),
                ipc_prefix(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ),
            Transport::IpcAbstract => format!(
                "ipc://@{}{}",
                ipc_prefix(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ),
        }
    }

    /// A socket of libzmq's, able to use this link's transport.
    fn theirs(self, kind: Kind) -> Socket {
        let socket = Socket::new(kind);
        if self.transport == Transport::Tcp6 {
            socket.set_ipv6();
        }
        socket
    }

    /// Joins `ours` and `theirs`, neither bound nor connected yet.
    fn join<T: SocketType>(self, ours: &mut zmq::Socket<T>, theirs: &Socket) {
        match self.binds {
            Binds::Ours => {
                let bound = ours.bind(self.wildcard()).unwrap();
                theirs.connect_str(&bound.to_string());
            }
            Binds::Libzmq => {
                let endpoint = self.free();
                theirs.bind(&endpoint);
                ours.connect(&endpoint).unwrap();
            }
        }
    }
}

/// Runs `f` on a thread of its own, for calls of ours that block.
fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
}

/// Calls `attempt` until it says it is done, failing after five seconds.
///
/// A socket of either library takes up a new connection, and sends it its
/// subscriptions, only when it is next called. So an attempt calls both
/// sockets: libzmq's with [`Socket::events`] if with nothing else.
fn until(what: &str, mut attempt: impl FnMut() -> bool) {
    let started = Instant::now();
    while !attempt() {
        assert!(started.elapsed() < FIVE, "{what}");
        thread::sleep(Duration::from_millis(5));
    }
}

fn identity(id: &str) -> SocketOptions {
    SocketOptions::new().identity(zmq::Identity::new(id.to_owned()).unwrap())
}

fn id(text: &str) -> RoutingId {
    RoutingId::new(text.to_owned()).unwrap()
}

fn message<const N: usize>(frames: [&'static str; N]) -> Message {
    Message::from(frames)
}

#[test]
fn req_and_rep() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();

        let rep = link.theirs(Kind::Rep);
        let mut req = context.socket::<Req>().unwrap();
        link.join(&mut req, &rep);
        let answer = spawn(move || req.request("question"));
        assert_eq!(rep.recv().unwrap(), [b"question"], "{link:?}");
        rep.send(&[b"answer"]).unwrap();
        assert_eq!(
            answer.recv_timeout(FIVE).unwrap(),
            Ok(Message::from("answer")),
            "{link:?}"
        );

        let req = link.theirs(Kind::Req);
        let mut rep = context.socket::<Rep>().unwrap();
        link.join(&mut rep, &req);
        req.send(&[b"question"]).unwrap();
        let request = rep.recv_timeout(FIVE).unwrap().expect("a request");
        assert_eq!(*request.body(), Message::from("question"), "{link:?}");
        request.reply("answer").unwrap();
        assert_eq!(req.recv().unwrap(), [b"answer"], "{link:?}");
    }
}

#[test]
fn req_and_router() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();

        let router = link.theirs(Kind::Router);
        let mut req = context.socket_with::<Req>(identity("req")).unwrap();
        link.join(&mut req, &router);
        let answer = spawn(move || req.request("question"));
        assert_eq!(
            router.recv().unwrap(),
            [&b"req"[..], b"", b"question"],
            "{link:?}"
        );
        router.send(&[b"req", b"", b"answer"]).unwrap();
        assert_eq!(
            answer.recv_timeout(FIVE).unwrap(),
            Ok(Message::from("answer")),
            "{link:?}"
        );

        let req = link.theirs(Kind::Req);
        req.set_routing_id(b"req");
        let mut router = context.socket::<Router>().unwrap();
        link.join(&mut router, &req);
        req.send(&[b"question"]).unwrap();
        let (from, request) = router.recv_timeout(FIVE).unwrap().expect("a request");
        assert_eq!((&from, request), (&id("req"), message(["", "question"])));
        router.send(&from, message(["", "answer"])).unwrap();
        assert_eq!(req.recv().unwrap(), [b"answer"], "{link:?}");
    }
}

#[test]
fn dealer_and_rep() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();

        let rep = link.theirs(Kind::Rep);
        let mut dealer = context.socket::<Dealer>().unwrap();
        link.join(&mut dealer, &rep);
        dealer.send(message(["", "question"])).unwrap();
        assert_eq!(rep.recv().unwrap(), [b"question"], "{link:?}");
        rep.send(&[b"answer"]).unwrap();
        assert_eq!(
            dealer.recv_timeout(FIVE).unwrap(),
            Some(message(["", "answer"])),
            "{link:?}"
        );

        let dealer = link.theirs(Kind::Dealer);
        let mut rep = context.socket::<Rep>().unwrap();
        link.join(&mut rep, &dealer);
        dealer.send(&[b"", b"question"]).unwrap();
        let request = rep.recv_timeout(FIVE).unwrap().expect("a request");
        assert_eq!(*request.body(), Message::from("question"), "{link:?}");
        request.reply("answer").unwrap();
        assert_eq!(dealer.recv().unwrap(), [&b""[..], b"answer"], "{link:?}");
    }
}

#[test]
fn dealer_and_router() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();

        let router = link.theirs(Kind::Router);
        let mut dealer = context.socket_with::<Dealer>(identity("dealer")).unwrap();
        link.join(&mut dealer, &router);
        dealer.send("hello").unwrap();
        assert_eq!(
            router.recv().unwrap(),
            [&b"dealer"[..], b"hello"],
            "{link:?}"
        );
        router.send(&[b"dealer", b"reply"]).unwrap();
        assert_eq!(
            dealer.recv_timeout(FIVE).unwrap(),
            Some(Message::from("reply")),
            "{link:?}"
        );

        let dealer = link.theirs(Kind::Dealer);
        dealer.set_routing_id(b"dealer");
        let mut router = context.socket::<Router>().unwrap();
        link.join(&mut router, &dealer);
        dealer.send(&[b"hello"]).unwrap();
        let (from, hello) = router.recv_timeout(FIVE).unwrap().expect("a message");
        assert_eq!((&from, hello), (&id("dealer"), Message::from("hello")));
        router.send(&from, "reply").unwrap();
        assert_eq!(dealer.recv().unwrap(), [b"reply"], "{link:?}");
    }
}

#[test]
fn dealer_and_dealer() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();
        let theirs = link.theirs(Kind::Dealer);
        let mut ours = context.socket::<Dealer>().unwrap();
        link.join(&mut ours, &theirs);
        ours.send(message(["from", "ours"])).unwrap();
        assert_eq!(theirs.recv().unwrap(), [b"from", b"ours"], "{link:?}");
        theirs.send(&[b"from", b"libzmq"]).unwrap();
        assert_eq!(
            ours.recv_timeout(FIVE).unwrap(),
            Some(message(["from", "libzmq"])),
            "{link:?}"
        );
    }
}

/// Neither ROUTER can send until it has heard the other's identity, so
/// each tries until the other receives.
#[test]
fn router_and_router() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();
        let theirs = link.theirs(Kind::Router);
        theirs.set_routing_id(b"theirs");
        theirs.set_receive_timeout(Duration::from_millis(5));
        let mut ours = context.socket_with::<Router>(identity("ours")).unwrap();
        link.join(&mut ours, &theirs);

        until("our ROUTER never reached libzmq's", || {
            let _ = ours.try_send(&id("theirs"), "from ours");
            theirs.recv().is_ok_and(|heard| {
                assert_eq!(heard, [&b"ours"[..], b"from ours"], "{link:?}");
                true
            })
        });
        until("libzmq's ROUTER never reached ours", || {
            theirs.send(&[b"ours", b"from libzmq"]).unwrap();
            ours.try_recv().unwrap().is_some_and(|heard| {
                assert_eq!(heard, (id("theirs"), Message::from("from libzmq")));
                true
            })
        });
    }
}

/// How each side of a publish-subscribe pair is one of ours.
#[derive(Debug, Clone, Copy)]
enum Publisher {
    Pub,
    XPub,
}

#[derive(Debug, Clone, Copy)]
enum Subscriber {
    Sub,
    XSub,
}

/// Our publisher reaches libzmq's subscriber, on the topic it subscribed
/// to and no other.
fn we_publish(link: Link, publisher: Publisher, subscriber: Subscriber) {
    let context = Context::new().unwrap();
    let theirs = link.theirs(match subscriber {
        Subscriber::Sub => Kind::Sub,
        Subscriber::XSub => Kind::XSub,
    });
    let subscribe = |theirs: &Socket| match subscriber {
        Subscriber::Sub => theirs.subscribe(b"news"),
        Subscriber::XSub => theirs.send(&[b"\x01news"]).unwrap(),
    };
    let what = format!("{publisher:?} to {subscriber:?} over {link:?}");
    match publisher {
        Publisher::Pub => {
            let mut ours = context.socket::<Pub>().unwrap();
            link.join(&mut ours, &theirs);
            subscribe(&theirs);
            until(&what, || {
                theirs.events();
                ours.send("news: first").unwrap() == 1
            });
            assert_eq!(ours.send("sports").unwrap(), 0, "{what}");
            assert_eq!(ours.send(message(["news", "second"])).unwrap(), 1);
        }
        Publisher::XPub => {
            let mut ours = context.socket::<XPub>().unwrap();
            link.join(&mut ours, &theirs);
            subscribe(&theirs);
            until(&what, || {
                theirs.events();
                let heard = ours.try_recv().unwrap();
                heard.is_some() && {
                    assert_eq!(heard, Some(XPubEvent::Subscribe(Frame::from("news"))));
                    true
                }
            });
            assert_eq!(ours.send("news: first").unwrap(), 1, "{what}");
            assert_eq!(ours.send("sports").unwrap(), 0, "{what}");
            assert_eq!(ours.send(message(["news", "second"])).unwrap(), 1);
        }
    }
    assert_eq!(theirs.recv().unwrap(), [b"news: first"], "{what}");
    assert_eq!(theirs.recv().unwrap(), [&b"news"[..], b"second"], "{what}");
}

/// libzmq's publisher reaches our subscriber, on the topic it subscribed
/// to and no other.
fn libzmq_publishes(link: Link, publisher: Publisher, subscriber: Subscriber) {
    let context = Context::new().unwrap();
    let theirs = link.theirs(match publisher {
        Publisher::Pub => Kind::Pub,
        Publisher::XPub => Kind::XPub,
    });
    let what = format!("{publisher:?} to {subscriber:?} over {link:?}");
    theirs.set_receive_timeout(Duration::from_millis(5));
    // An XPUB hands its application the subscription first.
    let mut heard = matches!(publisher, Publisher::Pub);
    let mut publish = |theirs: &Socket| {
        if !heard {
            heard = theirs.recv().is_ok_and(|subscription| {
                assert_eq!(subscription, [b"\x01news"], "{what}");
                true
            });
            return;
        }
        theirs.send(&[b"sports"]).unwrap();
        theirs.send(&[b"news", b"two frames"]).unwrap();
    };
    let expected = Some(message(["news", "two frames"]));
    match subscriber {
        Subscriber::Sub => {
            let mut ours = context.socket::<Sub>().unwrap();
            ours.subscribe("news");
            link.join(&mut ours, &theirs);
            until(&what, || {
                publish(&theirs);
                let taken = ours.try_recv().unwrap();
                taken.is_some() && {
                    assert_eq!(taken, expected, "{what}");
                    true
                }
            });
        }
        Subscriber::XSub => {
            let mut ours = context.socket::<XSub>().unwrap();
            ours.subscribe("news");
            link.join(&mut ours, &theirs);
            until(&what, || {
                publish(&theirs);
                let taken = ours.try_recv().unwrap();
                taken.is_some() && {
                    assert_eq!(taken, expected, "{what}");
                    true
                }
            });
        }
    }
}

#[test]
fn publishers_and_subscribers() {
    let _sweep = Sweep;
    for link in links() {
        for publisher in [Publisher::Pub, Publisher::XPub] {
            for subscriber in [Subscriber::Sub, Subscriber::XSub] {
                we_publish(link, publisher, subscriber);
                libzmq_publishes(link, publisher, subscriber);
            }
        }
    }
}

#[test]
fn push_and_pull() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();

        let pull = link.theirs(Kind::Pull);
        let mut push = context.socket::<Push>().unwrap();
        link.join(&mut push, &pull);
        push.send(message(["from", "ours"])).unwrap();
        assert_eq!(pull.recv().unwrap(), [b"from", b"ours"], "{link:?}");

        let push = link.theirs(Kind::Push);
        let mut pull = context.socket::<Pull>().unwrap();
        link.join(&mut pull, &push);
        push.send(&[b"from", b"libzmq"]).unwrap();
        assert_eq!(
            pull.recv_timeout(FIVE).unwrap(),
            Some(message(["from", "libzmq"])),
            "{link:?}"
        );
    }
}

#[test]
fn pair_and_pair() {
    let _sweep = Sweep;
    for link in links() {
        let context = Context::new().unwrap();
        let theirs = link.theirs(Kind::Pair);
        let mut ours = context.socket::<Pair>().unwrap();
        link.join(&mut ours, &theirs);
        ours.send(message(["from", "ours"])).unwrap();
        assert_eq!(theirs.recv().unwrap(), [b"from", b"ours"], "{link:?}");
        theirs.send(&[b"from", b"libzmq"]).unwrap();
        assert_eq!(
            ours.recv_timeout(FIVE).unwrap(),
            Some(message(["from", "libzmq"])),
            "{link:?}"
        );
    }
}

#[cfg(feature = "draft")]
mod draft {
    use zmq::{Channel, Client, Dish, Gather, Group, Peer, Radio, Scatter, Server};
    use zmq_interop::libzmq;

    use super::*;

    #[test]
    fn client_and_server() {
        if !libzmq::has_drafts() {
            return;
        }
        let _sweep = Sweep;
        for link in links() {
            let context = Context::new().unwrap();

            let server = link.theirs(Kind::Server);
            let mut client = context.socket::<Client>().unwrap();
            link.join(&mut client, &server);
            client.send("ping").unwrap();
            let (from, ping) = server.recv_routed().unwrap();
            assert_eq!(ping, b"ping", "{link:?}");
            server.send_routed(from, b"pong").unwrap();
            assert_eq!(
                client.recv_timeout(FIVE).unwrap(),
                Some(Frame::from("pong")),
                "{link:?}"
            );

            let client = link.theirs(Kind::Client);
            let mut server = context.socket::<Server>().unwrap();
            link.join(&mut server, &client);
            client.send(&[b"ping"]).unwrap();
            let (from, ping) = server.recv_timeout(FIVE).unwrap().expect("a message");
            assert_eq!(ping, Frame::from("ping"), "{link:?}");
            server.send(from, "pong").unwrap();
            assert_eq!(client.recv().unwrap(), [b"pong"], "{link:?}");
        }
    }

    #[test]
    fn radio_and_dish() {
        if !libzmq::has_drafts() {
            return;
        }
        let weather = Group::new("weather").unwrap();
        let sports = Group::new("sports").unwrap();
        let _sweep = Sweep;
        for link in links() {
            let context = Context::new().unwrap();

            let dish = link.theirs(Kind::Dish);
            dish.join("weather");
            let mut radio = context.socket::<Radio>().unwrap();
            link.join(&mut radio, &dish);
            until(
                &format!("libzmq's JOIN never arrived over {link:?}"),
                || {
                    dish.events();
                    radio.send(&weather, "sunny").unwrap() == 1
                },
            );
            assert_eq!(radio.send(&sports, "not joined").unwrap(), 0, "{link:?}");
            assert_eq!(
                dish.recv_from_group().unwrap(),
                ("weather".to_owned(), b"sunny".to_vec()),
                "{link:?}"
            );

            let radio = link.theirs(Kind::Radio);
            let mut dish = context.socket::<Dish>().unwrap();
            dish.join(&weather);
            link.join(&mut dish, &radio);
            until(&format!("our JOIN never arrived over {link:?}"), || {
                radio.send_to_group("sports", b"not joined").unwrap();
                radio.send_to_group("weather", b"sunny").unwrap();
                let taken = dish.try_recv().unwrap();
                taken.is_some() && {
                    assert_eq!(taken, Some((weather.clone(), Frame::from("sunny"))));
                    true
                }
            });
        }
    }

    #[test]
    fn scatter_and_gather() {
        if !libzmq::has_drafts() {
            return;
        }
        let _sweep = Sweep;
        for link in links() {
            let context = Context::new().unwrap();

            let gather = link.theirs(Kind::Gather);
            let mut scatter = context.socket::<Scatter>().unwrap();
            link.join(&mut scatter, &gather);
            scatter.send("from ours").unwrap();
            assert_eq!(gather.recv().unwrap(), [b"from ours"], "{link:?}");

            let scatter = link.theirs(Kind::Scatter);
            let mut gather = context.socket::<Gather>().unwrap();
            link.join(&mut gather, &scatter);
            scatter.send(&[b"from libzmq"]).unwrap();
            assert_eq!(
                gather.recv_timeout(FIVE).unwrap(),
                Some(Frame::from("from libzmq")),
                "{link:?}"
            );
        }
    }

    /// The PEER that connects learns the other's id by doing so, and
    /// speaks first.
    #[test]
    fn peer_and_peer() {
        if !libzmq::has_drafts() {
            return;
        }
        let _sweep = Sweep;
        for link in links() {
            let context = Context::new().unwrap();
            let theirs = link.theirs(Kind::Peer);
            let mut ours = context.socket::<Peer>().unwrap();
            match link.binds {
                Binds::Ours => {
                    let bound = ours.bind(link.wildcard()).unwrap();
                    let to_ours = theirs.try_connect_peer(&bound.to_string()).unwrap();
                    theirs.send_routed(to_ours, b"from libzmq").unwrap();
                    let (to_theirs, heard) = ours.recv_timeout(FIVE).unwrap().expect("a message");
                    assert_eq!(heard, Frame::from("from libzmq"), "{link:?}");
                    ours.send(to_theirs, "from ours").unwrap();
                    assert_eq!(
                        theirs.recv_routed().unwrap(),
                        (to_ours, b"from ours".to_vec()),
                        "{link:?}"
                    );
                }
                Binds::Libzmq => {
                    let endpoint = link.free();
                    theirs.bind(&endpoint);
                    let to_theirs = ours.connect_peer(&endpoint).unwrap();
                    ours.send(to_theirs, "from ours").unwrap();
                    let (to_ours, heard) = theirs.recv_routed().unwrap();
                    assert_eq!(heard, b"from ours", "{link:?}");
                    theirs.send_routed(to_ours, b"from libzmq").unwrap();
                    assert_eq!(
                        ours.recv_timeout(FIVE).unwrap(),
                        Some((to_theirs, Frame::from("from libzmq"))),
                        "{link:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn channel_and_channel() {
        if !libzmq::has_drafts() {
            return;
        }
        let _sweep = Sweep;
        for link in links() {
            let context = Context::new().unwrap();
            let theirs = link.theirs(Kind::Channel);
            let mut ours = context.socket::<Channel>().unwrap();
            link.join(&mut ours, &theirs);
            ours.send("from ours").unwrap();
            assert_eq!(theirs.recv().unwrap(), [b"from ours"], "{link:?}");
            theirs.send(&[b"from libzmq"]).unwrap();
            assert_eq!(
                ours.recv_timeout(FIVE).unwrap(),
                Some(Frame::from("from libzmq")),
                "{link:?}"
            );
        }
    }
}
