//! File transfer model #1: the server sends the whole file in chunks, as
//! fast as it can, without flow control.
//!
//! Reads `testdata` in the current directory; mktestdata makes it.

use std::fs::File;
use std::io::Read;
use std::thread;

use zmq::{Context, Dealer, Hwm, Router, SocketOptions};

const CHUNK_SIZE: u64 = 250_000;

fn client_thread(context: Context) -> Result<(), zmq::Error> {
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("tcp://127.0.0.1:6000")?;

    dealer.send("fetch");
    let mut total = 0; // Total bytes received
    let mut chunks = 0; // Total chunks received

    loop {
        let size = dealer.recv().last().len();
        chunks += 1;
        total += size;
        if size == 0 {
            break; // Whole file received
        }
    }
    println!("{chunks} chunks received, {total} bytes");
    Ok(())
}

// The server thread reads the file from disk in chunks, and sends each
// chunk to the client as a separate message. We only have one test file,
// so open that once and then serve it out as needed.
fn server_thread(context: Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut file = File::open("testdata")?;

    let mut router = context.socket_with::<Router>(SocketOptions {
        send_hwm: Hwm::Unlimited,
        recv_hwm: Hwm::Unlimited,
        ..SocketOptions::default()
    });
    router.bind("tcp://*:6000")?;
    loop {
        // First frame in each message is the sender identity, which the
        // ROUTER hands us separately
        let (identity, command) = router.recv();
        assert_eq!(&**command.last(), b"fetch");

        loop {
            let mut data = Vec::new();
            let size = (&mut file).take(CHUNK_SIZE).read_to_end(&mut data)?;
            router.send(&identity, data)?;
            if size == 0 {
                break; // Always end with a zero-size frame
            }
        }
    }
}

// The main task starts the client and server threads; it's easier to
// test this as a single process with threads, than as multiple processes.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    thread::spawn({
        let context = context.clone();
        move || {
            if let Err(err) = server_thread(context) {
                eprintln!("E: server: {err}");
            }
        }
    });
    thread::spawn(move || client_thread(context))
        .join()
        .expect("client panicked")?;
    Ok(())
}
