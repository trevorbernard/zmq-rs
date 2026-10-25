//! libzmq's `tests/test_client_server.cpp`: SERVER answers CLIENT by the
//! id it gave the client.
//!
//! `test_client_sndmore_fails` and `test_server_sndmore_fails` have no
//! port: both sockets' `send` takes one `Frame`, so a multipart send does
//! not compile.

use zmq::{Client, Context, Frame, Server, Socket};

use crate::support::{FIVE, within};

fn inproc_client_server_pair(context: &Context, name: &str) -> (Socket<Server>, Socket<Client>) {
    let mut server = context.socket::<Server>();
    server.bind(&format!("inproc://{name}")).unwrap();
    let mut client = context.socket::<Client>();
    client.connect(&format!("inproc://{name}")).unwrap();
    (server, client)
}

#[test]
fn routing_id() {
    let context = Context::new().unwrap();
    let (mut server, mut client) = inproc_client_server_pair(&context, "test-client-server");

    client.send("X");
    let (routing_id, frame, mut server) = within(FIVE, move || {
        let (id, frame) = server.recv();
        (id, frame, server)
    });
    assert_eq!(frame, Frame::from("X"));

    server.send(routing_id, vec![2]).unwrap();
    // A client's messages carry no routing id: it has one peer.
    assert_eq!(within(FIVE, move || client.recv()), Frame::from(vec![2]));
}
