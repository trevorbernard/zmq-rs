//! Hand-built connections for unit-testing routing strategies without
//! threads or sockets.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

use zmq_pipe::{Hwm, Reader, Writer, pipe};
use zmtp::{Established, Frame, Message, Metadata, SocketType, Version};

use crate::Connection;
use crate::context::PeerSlot;
use crate::socket::PeerList;

/// The session's ends of a hand-built connection.
pub(crate) struct FakeSession {
    pub(crate) to_socket: Writer<Message>,
    pub(crate) from_socket: Reader<Message>,
    pub(crate) connection_peer: PeerSlot,
}

impl FakeSession {
    /// Completes the handshake, announcing `identity`.
    pub(crate) fn establish(&self, identity: &'static str) {
        *self.connection_peer.lock().unwrap() = Some(Established {
            version: Version::V3_1,
            peer_socket_type: Some(SocketType::Dealer),
            peer_identity: Frame::from(identity),
            metadata: Metadata::new(),
            authenticated: None,
            implicit_subscription: false,
        });
    }
}

pub(crate) fn connection(hwm: usize) -> (Connection, FakeSession) {
    let hwm = Hwm::Messages(NonZeroUsize::new(hwm).unwrap());
    let (tx, from_socket) = pipe(hwm);
    let (to_socket, rx) = pipe(hwm);
    let peer = PeerSlot::default();
    (
        Connection {
            tx,
            rx,
            peer: peer.clone(),
        },
        FakeSession {
            to_socket,
            from_socket,
            connection_peer: peer,
        },
    )
}

pub(crate) fn list(connections: Vec<Connection>) -> PeerList {
    let mut list = PeerList::default();
    for connection in connections {
        list.push(connection);
    }
    list
}

#[derive(Default)]
pub(crate) struct Count(pub(crate) AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) fn waker() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count::default());
    (count.clone(), Waker::from(count))
}

pub(crate) fn msg(text: &'static str) -> Message {
    Message::new(Frame::from(text))
}

pub(crate) fn drain(session: &mut FakeSession) -> Vec<Message> {
    std::iter::from_fn(|| session.from_socket.try_recv().ok()).collect()
}
