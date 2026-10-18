//! File transfer model #3: the client keeps up to PIPELINE chunk requests
//! in flight, a credit-based flow control.
//!
//! Reads `testdata` in the current directory; mktestdata makes it.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::thread;

use zguide::text;
use zmq::{Context, Dealer, Hwm, Message, Router, SocketOptions};

const CHUNK_SIZE: usize = 250_000;
const PIPELINE: usize = 10;

fn client_thread(context: Context) -> Result<(), zmq::Error> {
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("tcp://127.0.0.1:6000")?;

    // Up to this many chunks in transit
    let mut credit = PIPELINE;

    let mut total = 0; // Total bytes received
    let mut chunks = 0; // Total chunks received
    let mut offset = 0; // Offset of next chunk request

    loop {
        while credit > 0 {
            // Ask for next chunk
            let mut request = Message::new("fetch");
            request.push(offset.to_string());
            request.push(CHUNK_SIZE.to_string());
            dealer.send(request);
            offset += CHUNK_SIZE;
            credit -= 1;
        }
        let size = dealer.recv().last().len();
        chunks += 1;
        credit += 1;
        total += size;
        if size < CHUNK_SIZE {
            break; // Last chunk received; exit
        }
    }
    println!("{chunks} chunks received, {total} bytes");
    Ok(())
}

// The rest of the code is exactly the same as in model 2, except that we
// set the HWM on the server's ROUTER socket to PIPELINE * 2 to act as a
// sanity check.
fn server_thread(context: Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut file = File::open("testdata")?;

    let hwm = Hwm::Messages(NonZeroUsize::new(PIPELINE * 2).expect("nonzero"));
    let mut router = context.socket_with::<Router>(SocketOptions {
        send_hwm: hwm,
        recv_hwm: hwm,
        ..SocketOptions::default()
    });
    router.bind("tcp://*:6000")?;
    loop {
        let (identity, request) = router.recv();
        let [command, offset, chunksz] = request.iter().collect::<Vec<_>>()[..] else {
            continue;
        };
        assert_eq!(&**command, b"fetch");
        let offset: u64 = text(offset).parse()?;
        let chunksz: u64 = text(chunksz).parse()?;

        // Read chunk of data from file
        file.seek(SeekFrom::Start(offset))?;
        let mut data = Vec::new();
        (&mut file).take(chunksz).read_to_end(&mut data)?;

        // Send resulting chunk to client
        router.send(&identity, data)?;
    }
}

// The main task is just the same as in the first model.
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
