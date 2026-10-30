//! Teardown racing work on other threads: sockets dropped with messages
//! still queued, the last handle on a context dropped on whichever thread
//! gets there last, and subscribers coming and going while a publisher
//! sends. Each runs several rounds over every transport, on a context
//! with several I/O threads, and must finish in time.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;
use std::{panic, thread};

use zmq::{Context, Dealer, Linger, Message, Pub, Pull, Push, Router, SocketOptions, Sub};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);
const ROUNDS: usize = 5;

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a teardown that hangs fails instead of hanging the suite.
fn within(limit: Duration, f: impl FnOnce() + Send + 'static) {
    let (done, result) = mpsc::channel();
    let running = thread::spawn(move || {
        f();
        let _ = done.send(());
    });
    match result.recv_timeout(limit) {
        Ok(()) => {}
        Err(RecvTimeoutError::Timeout) => panic!("timed out after {limit:?}"),
        Err(RecvTimeoutError::Disconnected) => {
            panic::resume_unwind(running.join().expect_err("f panicked"))
        }
    }
}

/// Which sender a message came from, and its place in that sender's run.
fn origin(message: &Message) -> (usize, usize) {
    let text = std::str::from_utf8(message.split_first().0).unwrap();
    let (sender, index) = text.split_once(' ').unwrap();
    (sender.parse().unwrap(), index.parse().unwrap())
}

/// Senders on their own threads, each with its own handle on the context,
/// queue a run of messages and drop their sockets at once, lingering
/// forever; the receiver gets every message, each sender's in order, and
/// the context ends with its last handle, the receiver.
fn lingering_messages_survive_concurrent_drops(address: fn() -> String) {
    const SENDERS: usize = 16;
    const MESSAGES: usize = 200;
    within(Duration::from_secs(60), move || {
        for _ in 0..ROUNDS {
            let context = Context::with_io_threads(4).unwrap();
            let mut pull = context.socket::<Pull>();
            let endpoint = pull.bind(&address()).unwrap().to_string();
            let senders: Vec<_> = (0..SENDERS)
                .map(|sender| {
                    let context = context.clone();
                    let endpoint = endpoint.clone();
                    thread::spawn(move || {
                        let mut push = context.socket::<Push>();
                        push.connect(&endpoint).unwrap();
                        for index in 0..MESSAGES {
                            push.send(format!("{sender} {index}"));
                        }
                    })
                })
                .collect();
            drop(context);
            let mut next = [0; SENDERS];
            for _ in 0..SENDERS * MESSAGES {
                let message = pull.recv_timeout(FIVE).expect("every message, in time");
                let (sender, index) = origin(&message);
                assert_eq!(index, next[sender], "sender {sender}'s messages in order");
                next[sender] += 1;
            }
            for sender in senders {
                sender.join().unwrap();
            }
            drop(pull);
        }
    });
}

/// The thread that set things up drops its socket and its handle first;
/// workers still connecting, each holding a handle, send a message that
/// can no longer be delivered and drop theirs. Whichever drops last ends
/// the context, waiting out the bounded linger.
fn the_last_handle_ends_the_context_on_any_thread(address: fn() -> String) {
    const WORKERS: usize = 32;
    within(Duration::from_secs(60), move || {
        for _ in 0..ROUNDS {
            let context = Context::with_io_threads(4).unwrap();
            let mut router = context.socket::<Router>();
            let endpoint = router.bind(&address()).unwrap().to_string();
            let workers: Vec<_> = (0..WORKERS)
                .map(|worker| {
                    let context = context.clone();
                    let endpoint = endpoint.clone();
                    thread::spawn(move || {
                        let options = SocketOptions {
                            linger: Linger::For(Duration::from_millis(50)),
                            ..SocketOptions::default()
                        };
                        let mut dealer = context.socket_with::<Dealer>(options);
                        dealer.connect(&endpoint).unwrap();
                        let _ = dealer.try_send(format!("{worker}"));
                        thread::yield_now();
                    })
                })
                .collect();
            drop(router);
            drop(context);
            for worker in workers {
                worker.join().unwrap();
            }
        }
    });
}

/// Subscribers on several threads connect, subscribe, take a message and
/// drop, over and over, while a publisher on another sends without pause.
/// Each subscriber gets a message, and everything ends once the publisher
/// stops.
fn subscribers_come_and_go_while_a_publisher_sends(address: fn() -> String) {
    const SUBSCRIBERS: usize = 8;
    const VISITS: usize = 20;
    within(Duration::from_secs(60), move || {
        for _ in 0..ROUNDS {
            let context = Context::with_io_threads(4).unwrap();
            let mut publisher = context.socket::<Pub>();
            let endpoint = publisher.bind(&address()).unwrap().to_string();
            let stop = Arc::new(AtomicBool::new(false));
            let publishing = {
                let stop = stop.clone();
                thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        publisher.send("news");
                        thread::yield_now();
                    }
                })
            };
            let subscribers: Vec<_> = (0..SUBSCRIBERS)
                .map(|_| {
                    let context = context.clone();
                    let endpoint = endpoint.clone();
                    thread::spawn(move || {
                        for _ in 0..VISITS {
                            let mut subscriber = context.socket::<Sub>();
                            subscriber.connect(&endpoint).unwrap();
                            subscriber.subscribe("");
                            assert_eq!(subscriber.recv_timeout(FIVE), Some("news".into()));
                        }
                    })
                })
                .collect();
            drop(context);
            for subscriber in subscribers {
                subscriber.join().unwrap();
            }
            stop.store(true, Ordering::Relaxed);
            publishing.join().unwrap();
        }
    });
}

mod tcp {
    use super::free_endpoint as address;

    #[test]
    fn lingering_messages_survive_concurrent_drops() {
        super::lingering_messages_survive_concurrent_drops(address);
    }

    #[test]
    fn the_last_handle_ends_the_context_on_any_thread() {
        super::the_last_handle_ends_the_context_on_any_thread(address);
    }

    #[test]
    fn subscribers_come_and_go_while_a_publisher_sends() {
        super::subscribers_come_and_go_while_a_publisher_sends(address);
    }
}

#[cfg(unix)]
mod ipc {
    fn address() -> String {
        "ipc://*".to_owned()
    }

    #[test]
    fn lingering_messages_survive_concurrent_drops() {
        super::lingering_messages_survive_concurrent_drops(address);
    }

    #[test]
    fn the_last_handle_ends_the_context_on_any_thread() {
        super::the_last_handle_ends_the_context_on_any_thread(address);
    }

    #[test]
    fn subscribers_come_and_go_while_a_publisher_sends() {
        super::subscribers_come_and_go_while_a_publisher_sends(address);
    }
}

mod inproc {
    fn address() -> String {
        "inproc://shutdown-stress".to_owned()
    }

    #[test]
    fn lingering_messages_survive_concurrent_drops() {
        super::lingering_messages_survive_concurrent_drops(address);
    }

    #[test]
    fn the_last_handle_ends_the_context_on_any_thread() {
        super::the_last_handle_ends_the_context_on_any_thread(address);
    }

    #[test]
    fn subscribers_come_and_go_while_a_publisher_sends() {
        super::subscribers_come_and_go_while_a_publisher_sends(address);
    }
}
