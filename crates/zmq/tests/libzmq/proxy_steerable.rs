//! libzmq's `tests/test_proxy_steerable.cpp`: the clients and workers of
//! `test_proxy` behind a proxy steered through its control: STATISTICS,
//! PAUSE, RESUME and TERMINATE.
//!
//! libzmq steers over a REP control socket and reads STATISTICS as eight
//! 64-bit frames; here `ProxyControl` does both, with
//! `ProxyControl::statistics` returning them as a struct. libzmq only
//! prints the statistics; this port also checks that they never go back
//! and that traffic flowed.

use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Proxy, ProxyStatistics, Pub, Router};
use zmq_test_ports::free_endpoint;

use crate::proxy::{QT_CLIENTS, QT_WORKERS, client_task, no_linger, server_worker};
use crate::support::within;

fn moved(statistics: ProxyStatistics) -> u64 {
    let ProxyStatistics { frontend, backend } = statistics;
    frontend.received.messages
        + frontend.sent.messages
        + backend.received.messages
        + backend.sent.messages
}

#[test]
fn proxy_steerable() {
    within(Duration::from_secs(30), || {
        let context = Context::new().unwrap();
        let control_context = Context::new().unwrap();
        let mut control = control_context.socket_with::<Pub>(no_linger());
        control.bind("inproc://control").unwrap();

        let mut frontend = context.socket_with::<Router>(no_linger());
        let endpoint = frontend.bind(&free_endpoint()).unwrap().to_string();
        let mut backend = context.socket_with::<Dealer>(no_linger());
        backend.bind("inproc://backend").unwrap();

        let workers: Vec<_> = (0..QT_WORKERS)
            .map(|_| {
                let (context, control_context) = (context.clone(), control_context.clone());
                thread::spawn(move || server_worker(context, control_context))
            })
            .collect();
        let clients: Vec<_> = (0..QT_CLIENTS)
            .map(|_| {
                let (context, control_context) = (context.clone(), control_context.clone());
                let endpoint = endpoint.clone();
                thread::spawn(move || client_task(context, control_context, endpoint))
            })
            .collect();

        let mut proxy = Proxy::new(&mut frontend, &mut backend);
        let proxy_control = proxy.control();
        let steering = thread::spawn(move || {
            let pause = || thread::sleep(Duration::from_millis(500));
            pause();
            pause();
            let started = proxy_control.statistics();
            proxy_control.pause();
            pause();
            proxy_control.resume();
            pause();
            let ran = proxy_control.statistics();
            control.send("STOP");
            let stopped = proxy_control.statistics();
            pause();
            control.send("TERMINATE");
            pause();
            let terminated = proxy_control.statistics();
            pause();
            proxy_control.terminate();
            [started, ran, stopped, terminated]
        });
        proxy.run();
        let steered = steering.join().unwrap();
        for thread in workers.into_iter().chain(clients) {
            thread.join().unwrap();
        }

        let moved = steered.map(moved);
        assert!(moved.is_sorted(), "statistics went back: {moved:?}");
        assert!(
            moved[1] > moved[0],
            "traffic flowed after resuming: {moved:?}"
        );
    });
}
