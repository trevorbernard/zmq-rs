//! A listener that runs out of file descriptors accepts the waiting
//! connection once some are free again, without another arriving first.
#![cfg(target_os = "linux")]
// setrlimit has no safe wrapper in the standard library.
#![allow(unsafe_code)]

use std::fs;
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, EventKind, Monitor, Pull};

const FIVE: Duration = Duration::from_secs(5);

fn fd_limit() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid rlimit for getrlimit to fill in.
    let failed = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
    assert_eq!(failed, 0);
    limit
}

fn set_fd_limit(limit: libc::rlimit) {
    // SAFETY: `limit` is a valid rlimit, read by setrlimit only.
    let failed = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit) };
    assert_eq!(failed, 0);
}

/// File descriptors open in this process, not counting the one that
/// lists them.
fn open_fds() -> u64 {
    fs::read_dir("/proc/self/fd").unwrap().count() as u64 - 1
}

fn expect(monitor: &mut Monitor, wanted: impl Fn(&EventKind) -> bool) {
    let started = Instant::now();
    let mut seen = Vec::new();
    loop {
        if let Ok(event) = monitor.try_recv() {
            if wanted(&event.kind) {
                return;
            }
            seen.push(event.kind);
            continue;
        }
        assert!(started.elapsed() < FIVE, "not among {seen:#?}");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_listener_out_of_file_descriptors_accepts_once_some_are_free() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut monitor = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let address = bound.tcp_addr().unwrap();
    // Once listening, the I/O thread opens nothing more until it accepts.
    expect(&mut monitor, |kind| *kind == EventKind::Listening);
    let original = fd_limit();
    // Room for the client's socket and nothing more, so the listener
    // cannot accept it. Counted again if a descriptor opened meanwhile took
    // the client's room.
    let _client = (0..10)
        .find_map(|_| {
            set_fd_limit(libc::rlimit {
                rlim_cur: open_fds() + 1,
                ..original
            });
            let client = TcpStream::connect(address).ok();
            if client.is_none() {
                set_fd_limit(original);
            }
            client
        })
        .expect("room for the client");
    expect(&mut monitor, |kind| {
        matches!(kind, EventKind::AcceptFailed(_))
    });
    set_fd_limit(original);
    expect(&mut monitor, |kind| *kind == EventKind::Accepted);
}
