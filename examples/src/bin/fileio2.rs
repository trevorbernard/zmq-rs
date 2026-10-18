//! File transfer model #2: the client asks for each chunk in turn, by
//! offset and size, so the server only sends what was asked for.
//!
//! Reads `testdata` in the current directory; mktestdata makes it.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::thread;

use zguide::text;
use zmq::{Context, Dealer, Hwm, Message, Router, SocketOptions};

const CHUNK_SIZE: usize = 250_000;

fn one() -> SocketOptions {
    let hwm = Hwm::Messages(NonZeroUsize::MIN);
    SocketOptions {
        send_hwm: hwm,
        recv_hwm: hwm,
        ..SocketOptions::default()
    }
}

fn client_thread(context: Context) -> Result<(), zmq::Error> {
    let mut dealer = context.socket_with::<Dealer>(one());
    dealer.connect("tcp://127.0.0.1:6000")?;

    let mut total = 0; // Total bytes received
    let mut chunks = 0; // Total chunks received

    loop {
        // Ask for next chunk
        let mut request = Message::new("fetch");
        request.push(total.to_string());
        request.push(CHUNK_SIZE.to_string());
        dealer.send(request);

        let size = dealer.recv().last().len();
        chunks += 1;
        total += size;
        if size < CHUNK_SIZE {
            break; // Last chunk received; exit
        }
    }
    println!("{chunks} chunks received, {total} bytes");
    Ok(())
}

// The server thread waits for a chunk request from a client, reads that
// chunk and sends it back to the client.
fn server_thread(context: Context) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut file = File::open("testdata")?;

    let mut router = context.socket_with::<Router>(one());
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
