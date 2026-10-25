//! Simple request-reply broker: binds ROUTER to tcp://*:5559 for clients
//! and DEALER to tcp://*:5560 for services, and switches messages between
//! them.

use zmq::{Context, Dealer, Message, Router, RoutingId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Router>();
    frontend.bind("tcp://*:5559")?;
    let mut backend = context.socket::<Dealer>();
    backend.bind("tcp://*:5560")?;

    // Switch messages between sockets
    loop {
        let [from_client, from_service] = zmq::poll([frontend.readable(), backend.readable()]);
        if from_client {
            // The client's routing id goes first, so the reply finds it
            let (client, request) = frontend.recv();
            let mut message = Message::new(client.as_bytes().to_vec());
            message.extend(request);
            backend.send(message);
        }
        if from_service {
            let (client, reply) = backend.recv().into_parts();
            frontend.send(&RoutingId::new(client)?, Message::try_from(reply)?)?;
        }
    }
}
