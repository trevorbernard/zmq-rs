//! libzmq's `tests/test_reqrep_ipc.cpp`: REQ-REP over ipc, and a reply to
//! a requester that has gone.
#![cfg(unix)]

use std::time::Duration;

use zmq::{Context, Rep, Req};

use crate::support::{FIVE, bounce_req_rep, msg, settle};

#[test]
fn simple() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>();
    let endpoint = sb.bind("ipc://*").unwrap().to_string();
    let mut sc = context.socket::<Req>();
    sc.connect(&endpoint).unwrap();
    bounce_req_rep(&mut sb, sc);
}

/// libzmq checks that replying after the requester closed leaks nothing;
/// here the reply must be dropped quietly.
#[test]
fn leak() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>();
    let endpoint = sb.bind("ipc://*").unwrap().to_string();
    let mut sc = context.socket::<Req>();
    sc.connect(&endpoint).unwrap();

    // REQ sends and gives up waiting, then closes.
    let requester = std::thread::spawn(move || {
        let _ = sc.request_timeout("leakymsg", Duration::from_millis(100));
    });
    let request = sb.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &msg(&["leakymsg"]));
    requester.join().unwrap();
    settle();

    request.reply("leakymsg");
}
