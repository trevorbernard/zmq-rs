//! Titanic service: implements the server side of
//! <http://rfc.zeromq.org/spec:9>.
//!
//! The request thread hands UUIDs to the dispatcher over a channel rather
//! than a CZMQ pipe; the dispatcher waits on it with a timeout, as the
//! guide polls its pipe.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::Duration;

use zguide::mdcliapi::MdClient;
use zguide::mdp::message;
use zguide::mdwrkapi::MdWorker;
use zguide::{randof, text};
use zmq::{Context, Frame, Message};

const TITANIC_DIR: &str = ".titanic";
const BROKER: &str = "tcp://localhost:5555";

/// A new UUID as 32 hex digits.
fn generate_uuid() -> String {
    format!("{:016X}{:016X}", randof(u64::MAX), randof(u64::MAX))
}

fn request_filename(uuid: &str) -> PathBuf {
    PathBuf::from(format!("{TITANIC_DIR}/{uuid}.req"))
}

fn reply_filename(uuid: &str) -> PathBuf {
    PathBuf::from(format!("{TITANIC_DIR}/{uuid}.rep"))
}

/// Saves `msg` to `path`, each frame prefixed by its length, as CZMQ's
/// `zmsg_save`.
fn save(msg: &Message, path: &PathBuf) -> io::Result<()> {
    let mut file = File::create(path)?;
    for frame in msg {
        file.write_all(
            &u32::try_from(frame.len())
                .expect("small frame")
                .to_be_bytes(),
        )?;
        file.write_all(frame)?;
    }
    Ok(())
}

/// Loads a message `save` wrote, as CZMQ's `zmsg_load`.
fn load(path: &PathBuf) -> io::Result<Message> {
    let data = fs::read(path)?;
    let mut frames = Vec::new();
    let mut rest = data.as_slice();
    while let Some((len, tail)) = rest.split_first_chunk::<4>() {
        let len = u32::from_be_bytes(*len) as usize;
        let (frame, tail) = tail.split_at(len.min(tail.len()));
        frames.push(Frame::copy_from_slice(frame));
        rest = tail;
    }
    Message::try_from(frames).map_err(|_| io::Error::other("empty message"))
}

/// The titanic.request service: writes each request to disk and returns
/// a UUID to the client, who picks up the reply asynchronously with
/// titanic.reply.
fn titanic_request(
    context: Context,
    pipe: Sender<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = MdWorker::new(&context, BROKER, "titanic.request", false)?;
    let mut reply = None;
    loop {
        // Send reply if it's not null, and then get next request from
        // broker
        let request = worker.recv(reply.take());

        // Ensure message directory exists
        fs::create_dir_all(TITANIC_DIR)?;

        // Generate UUID and save message to disk
        let uuid = generate_uuid();
        save(&request, &request_filename(&uuid))?;

        // Send UUID through to message queue
        pipe.send(uuid.clone())?;

        // Now send UUID back to client, done by the recv at the top of the
        // loop
        reply = Some(message(vec![Frame::from("200"), Frame::from(uuid)]));
    }
}

/// The titanic.reply service: returns the reply for the request named by
/// its UUID with 200 (OK), or 300 (Pending) or 400 (Unknown).
fn titanic_reply(context: Context) -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = MdWorker::new(&context, BROKER, "titanic.reply", false)?;
    let mut reply = None;
    loop {
        let request = worker.recv(reply.take());
        let uuid = text(request.split_first().0);
        let rep_filename = reply_filename(&uuid);
        reply = Some(if rep_filename.exists() {
            let mut frames = vec![Frame::from("200")];
            frames.extend(load(&rep_filename)?);
            message(frames)
        } else if request_filename(&uuid).exists() {
            Message::new("300") // Pending
        } else {
            Message::new("400") // Unknown
        });
    }
}

/// The titanic.close service: removes any waiting replies for the request
/// named by its UUID. It's idempotent, so it is safe to call more than
/// once in a row.
fn titanic_close(context: Context) -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = MdWorker::new(&context, BROKER, "titanic.close", false)?;
    let mut reply = None;
    loop {
        let request = worker.recv(reply.take());
        let uuid = text(request.split_first().0);
        let _ = fs::remove_file(request_filename(&uuid));
        let _ = fs::remove_file(reply_filename(&uuid));
        reply = Some(Message::new("200"));
    }
}

/// Tries to call the service a stored request is for: first checks, with
/// an MMI lookup to the broker, that the service exists, then sends the
/// request and waits for a reply with the conventional MDP client API.
/// True once the request is answered.
fn service_success(context: &Context, uuid: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // Load request message, service will be first frame. If the client
    // already closed request, treat as successful
    let Ok(request) = load(&request_filename(uuid)) else {
        return Ok(true);
    };
    let (service, body) = request.into_parts();
    let service_name = text(&service);

    // Create MDP client session with short timeout
    let mut client = MdClient::new(context, BROKER, false)?;
    client.set_timeout(Duration::from_secs(1));
    client.set_retries(1);

    // Use MMI protocol to check if service is available
    let mmi_reply = client.send("mmi.service", Message::new(service));
    let service_ok = mmi_reply.is_some_and(|reply| **reply.split_first().0 == *b"200");
    if !service_ok {
        return Ok(false);
    }
    let Ok(body) = Message::try_from(body) else {
        return Ok(true); // Nothing to send; drop the request
    };
    match client.send(&service_name, body) {
        Some(reply) => {
            save(&reply, &reply_filename(uuid))?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Throws each pending request in the queue file at its service, marking
/// the answered ones processed.
fn dispatch(context: &Context, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mut file) = OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{TITANIC_DIR}/queue"))
    else {
        return Ok(());
    };
    let mut queue = String::new();
    file.read_to_string(&mut queue)?;
    let mut offset = 0;
    for line in queue.split_inclusive('\n') {
        // UUID is prefixed with '-' if still waiting
        if let Some(uuid) = line.trim_end().strip_prefix('-') {
            if verbose {
                println!("I: processing request {uuid}");
            }
            if service_success(context, uuid)? {
                // Mark queue entry as processed
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(b"+")?;
            }
        }
        offset += line.len() as u64;
    }
    Ok(())
}

/// Starts the request, reply, and close services, then dispatches stored
/// requests to workers using a simple brute force disk queue.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;

    let (pipe, request_pipe) = mpsc::channel();
    thread::spawn({
        let context = context.clone();
        move || titanic_request(context, pipe).map_err(|err| err.to_string())
    });
    thread::spawn({
        let context = context.clone();
        move || titanic_reply(context).map_err(|err| err.to_string())
    });
    thread::spawn({
        let context = context.clone();
        move || titanic_close(context).map_err(|err| err.to_string())
    });

    // Main dispatcher loop
    loop {
        // We'll dispatch once per second, if there's no activity
        match request_pipe.recv_timeout(Duration::from_secs(1)) {
            Ok(uuid) => {
                // Ensure message directory exists
                fs::create_dir_all(TITANIC_DIR)?;
                // Append UUID to queue, prefixed with '-' for pending
                let mut queue = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(format!("{TITANIC_DIR}/queue"))?;
                writeln!(queue, "-{uuid}")?;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err("request service stopped".into()),
        }
        // Brute force dispatcher
        dispatch(&context, verbose)?;
    }
}
