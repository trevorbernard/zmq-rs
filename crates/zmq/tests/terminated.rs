//! Shutting a context down, as libzmq's `zmq_ctx_shutdown`: every send and
//! receive on its sockets fails with `Terminated` (`ETERM`) from then on,
//! and threads waiting in one are woken to fail.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::task::{Context as TaskContext, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Error, Message, Pub, Pull, Push, Rep, ReplyError, Req, RequestError, Router,
    RoutingId, SendError, Sub, Terminated, TrySendError, XSub,
};

const SHORT: Duration = Duration::from_millis(100);
const FIVE: Duration = Duration::from_secs(5);

/// Runs `blocked` on a thread, shuts `context` down once the thread has
/// had time to block, and returns what the call came to.
fn woken<T: Send + 'static>(context: &Context, blocked: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    let thread = thread::spawn(move || done.send(blocked()));
    assert!(
        result.recv_timeout(SHORT).is_err(),
        "the call returned before the shutdown"
    );
    context.shutdown(FIVE).unwrap();
    let result = result
        .recv_timeout(FIVE)
        .expect("the shutdown woke the call");
    thread.join().unwrap().unwrap();
    result
}

#[test]
fn a_blocked_recv_fails_once_the_context_is_shut_down() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://recv").unwrap();
    assert_eq!(woken(&context, move || pull.recv()), Err(Terminated));
}

#[test]
fn a_blocked_recv_timeout_fails_before_its_timeout() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://recv-timeout").unwrap();
    let result = woken(&context, move || pull.recv_timeout(Duration::from_secs(60)));
    assert_eq!(result, Err(Terminated));
}

#[test]
fn a_blocked_send_fails_once_the_context_is_shut_down() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.bind("inproc://send").unwrap();
    assert_eq!(
        woken(&context, move || push.send("nobody")),
        Err(Terminated)
    );
}

#[test]
fn a_blocked_send_timeout_hands_its_message_back() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.bind("inproc://send-timeout").unwrap();
    let result = woken(&context, move || {
        push.send_timeout("nobody", Duration::from_secs(60))
    });
    assert_eq!(result, Err(TrySendError::Terminated("nobody".into())));
}

#[test]
fn a_request_waiting_for_its_reply_fails_once_the_context_is_shut_down() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>().unwrap();
    rep.bind("inproc://request").unwrap();
    let mut req = context.socket::<Req>().unwrap();
    req.connect("inproc://request").unwrap();
    let result = woken(&context, move || req.request("anyone?"));
    assert_eq!(result, Err(RequestError::Terminated));
    drop(rep);
}

#[test]
fn a_blocked_poll_fails_once_the_context_is_shut_down() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://poll").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.bind("inproc://poll-out").unwrap();
    let result = woken(&context, move || {
        zmq::poll([pull.readable(), push.writable()])
    });
    assert_eq!(result, Err(Terminated));
}

#[test]
fn a_shutdown_through_one_clone_reaches_sockets_made_through_another() {
    let context = Context::new().unwrap();
    let clone = context.clone();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://clones").unwrap();
    assert!(!context.is_terminated());
    assert_eq!(woken(&clone, move || pull.recv()), Err(Terminated));
    assert!(context.is_terminated());
}

#[test]
fn a_message_already_waiting_is_not_received_after_a_shutdown() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://waiting").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://waiting").unwrap();
    push.send("too late").unwrap();
    context.shutdown(FIVE).unwrap();
    // As libzmq, which checks for termination before anything else.
    assert_eq!(pull.try_recv(), Err(Terminated));
    assert_eq!(pull.recv(), Err(Terminated));
    assert_eq!(pull.recv_timeout(SHORT), Err(Terminated));
}

#[test]
fn every_kind_of_send_fails_after_a_shutdown() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://sends").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://sends").unwrap();
    let mut publisher = context.socket::<Pub>().unwrap();
    let mut xsub = context.socket::<XSub>().unwrap();
    let mut router = context.socket::<Router>().unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    let mut req = context.socket::<Req>().unwrap();
    context.shutdown(FIVE).unwrap();

    let kept = || Message::from("kept");
    assert_eq!(push.send(kept()), Err(Terminated));
    assert_eq!(push.try_send(kept()), Err(TrySendError::Terminated(kept())));
    assert_eq!(
        push.send_timeout(kept(), SHORT),
        Err(TrySendError::Terminated(kept()))
    );
    assert_eq!(dealer.send(kept()), Err(Terminated));
    assert_eq!(publisher.send(kept()), Err(Terminated));
    assert_eq!(publisher.send_all(kept()), Err(Terminated));
    assert_eq!(
        publisher.try_send_all(kept()),
        Err(TrySendError::Terminated(kept()))
    );
    assert_eq!(xsub.send(kept()), Err(Terminated));
    let nobody = RoutingId::new("nobody").unwrap();
    assert_eq!(
        router.send(&nobody, kept()),
        Err(SendError::Terminated(kept()))
    );
    assert_eq!(
        router.try_send(&nobody, kept()),
        Err(SendError::Terminated(kept()))
    );
    assert_eq!(
        router.send_timeout(&nobody, kept(), SHORT),
        Err(SendError::Terminated(kept()))
    );
    assert_eq!(req.request(kept()), Err(RequestError::Terminated));
    assert_eq!(
        req.request_timeout(kept(), SHORT),
        Err(RequestError::Terminated)
    );
}

#[test]
fn a_reply_fails_after_a_shutdown() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>().unwrap();
    rep.bind("inproc://late-reply").unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    dealer.connect("inproc://late-reply").unwrap();
    let mut question = Message::new("");
    question.push("question");
    dealer.send(question).unwrap();
    let request = rep.recv_timeout(FIVE).unwrap().expect("a request");
    context.shutdown(FIVE).unwrap();
    assert_eq!(request.reply("late"), Err(ReplyError::Terminated));
}

#[test]
fn binding_and_connecting_fail_after_a_shutdown() {
    let context = Context::new().unwrap();
    let mut sub = context.socket::<Sub>().unwrap();
    context.shutdown(FIVE).unwrap();
    assert!(matches!(sub.bind("inproc://late"), Err(Error::Terminated)));
    assert!(matches!(
        sub.connect("inproc://late"),
        Err(Error::Terminated)
    ));
}

/// libzmq's `test_ctx_shutdown_socket_opened_after` and
/// `test_ctx_shutdown_only_socket_opened_after`, from its
/// `tests/test_ctx_destroy.cpp`: a socket cannot be made once the context
/// is shut down, whether or not it has had one.
#[test]
fn a_socket_cannot_be_made_after_a_shutdown() {
    for opened_before in [true, false] {
        let context = Context::new().unwrap();
        if opened_before {
            drop(context.socket::<Pull>().unwrap());
        }
        context.shutdown(FIVE).unwrap();
        assert!(
            matches!(context.socket::<Pull>(), Err(Error::Terminated)),
            "opened before: {opened_before}"
        );
        assert!(matches!(
            context.socket_with::<Pull>(zmq::SocketOptions::new()),
            Err(Error::Terminated)
        ));
    }
}

#[test]
fn a_poll_with_a_timeout_fails_after_a_shutdown() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    context.shutdown(FIVE).unwrap();
    assert_eq!(zmq::poll_timeout([pull.readable()], SHORT), Err(Terminated));
}

#[test]
fn a_shutdown_wakes_a_task_waiting_to_receive() {
    struct Flag(AtomicBool);

    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://task").unwrap();
    let woken = Arc::new(Flag(AtomicBool::new(false)));
    let waker = Waker::from(woken.clone());
    let mut cx = TaskContext::from_waker(&waker);
    let mut receiving = pin!(pull.recv_async());
    assert!(receiving.as_mut().poll(&mut cx).is_pending());
    assert!(!woken.0.load(Ordering::SeqCst));

    context.shutdown(FIVE).unwrap();
    assert!(woken.0.load(Ordering::SeqCst), "the shutdown woke the task");
    assert_eq!(
        receiving.as_mut().poll(&mut cx),
        Poll::Ready(Err(Terminated))
    );
}

#[test]
fn a_proxy_stops_once_its_context_is_shut_down() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Router>().unwrap();
    frontend.bind("inproc://proxy-front").unwrap();
    let mut backend = context.socket::<Dealer>().unwrap();
    backend.bind("inproc://proxy-back").unwrap();
    woken(&context, move || {
        zmq::Proxy::new(&mut frontend, &mut backend).run();
    });
}

#[test]
fn a_shutdown_delivers_what_was_queued_before_it() {
    let context = Context::new().unwrap();
    let receivers = Context::new().unwrap();
    let mut pull = receivers.socket::<Pull>().unwrap();
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(&endpoint).unwrap();
    push.send("last words").unwrap();
    context.shutdown(FIVE).unwrap();
    assert_eq!(pull.recv_timeout(FIVE), Ok(Some("last words".into())));
}

#[test]
fn a_bound_socket_is_dropped_without_waiting_after_a_shutdown() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(&endpoint).unwrap();
    push.send("connected").unwrap();
    assert_eq!(pull.recv(), Ok("connected".into()));
    context.shutdown(FIVE).unwrap();
    let (done, dropped) = mpsc::channel();
    thread::spawn(move || {
        drop((pull, push, context));
        done.send(()).unwrap();
    });
    dropped
        .recv_timeout(FIVE)
        .expect("the sockets and the context drop after a shutdown");
}

#[test]
fn a_connection_still_shaking_hands_does_not_hold_a_shutdown_up() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(&endpoint).unwrap();
    let started = std::time::Instant::now();
    assert_eq!(context.shutdown(FIVE), Ok(()));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn a_blocked_poll_of_a_vec_fails_once_the_context_is_shut_down() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://poll-vec").unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.bind("inproc://poll-vec-out").unwrap();
    let result = woken(&context, move || {
        zmq::poll(vec![pull.readable(), push.writable()])
    });
    assert_eq!(result, Err(Terminated));
}

#[test]
fn a_blocked_poll_of_a_descriptor_fails_once_the_context_is_shut_down() {
    use std::os::fd::AsFd;
    use std::os::unix::net::UnixStream;

    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://poll-fd").unwrap();
    let result = woken(&context, move || {
        let (_ours, theirs) = UnixStream::pair().unwrap();
        zmq::poll([
            pull.readable(),
            zmq::PollItem::fd_readable(theirs.as_fd()).unwrap(),
        ])
    });
    assert_eq!(result, Err(Terminated));
}
