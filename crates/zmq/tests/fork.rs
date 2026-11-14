//! A forked child has the parent's sockets and context but none of its
//! I/O threads: closing them there waits for nothing, as in libzmq.
#![cfg(target_os = "linux")]
// fork has no safe wrapper in the standard library.
#![allow(unsafe_code)]

use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Message, Pull, Push, Router};

const FIVE: Duration = Duration::from_secs(5);

/// The exit status of `child`, killed if it has not exited within five
/// seconds.
fn wait_for(child: libc::pid_t) -> Option<i32> {
    let started = Instant::now();
    let mut status = 0;
    loop {
        // SAFETY: `status` is a valid int for waitpid to fill in.
        let exited = unsafe { libc::waitpid(child, &raw mut status, libc::WNOHANG) };
        if exited == child {
            return libc::WIFEXITED(status).then(|| libc::WEXITSTATUS(status));
        }
        if started.elapsed() > FIVE {
            // SAFETY: `child` is our own child, not yet waited for.
            unsafe {
                libc::kill(child, libc::SIGKILL);
                libc::waitpid(child, &raw mut status, 0);
            }
            return None;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_forked_child_closes_what_it_inherited_and_the_parent_goes_on() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("tcp://127.0.0.1:*").unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>();
    push.connect(&bound).unwrap();

    // SAFETY: the child drops what it inherited, uses sockets of its own
    // and exits.
    let child = unsafe { libc::fork() };
    assert!(child >= 0);
    if child == 0 {
        drop(router);
        drop(push);
        drop(pull);
        drop(context);
        // As libzmq's test_fork: a context of its own works.
        let own = Context::new().unwrap();
        let mut push = own.socket::<Push>();
        push.connect(&bound).unwrap();
        push.send("from the child").unwrap();
        drop(push);
        drop(own);
        // SAFETY: ends the child without running the test harness's exit.
        unsafe { libc::_exit(0) };
    }
    assert_eq!(wait_for(child), Some(0), "the child hung");
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("from the child"))
    );

    push.send("still here").unwrap();
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("still here"))
    );
}
