//! Properties of ROUTER's routing table, checked against a model of
//! libzmq's `router_t` over generated arrivals, handshakes, departures and
//! sends.

use proptest::prelude::*;
use zmtp::{Established, Metadata, SocketType, Version};

use super::*;
use crate::test_support::*;

/// An identity a peer announces, or a routing id sent to.
#[derive(Debug, Clone, Copy)]
enum Id {
    /// Nothing: the table generates one.
    Anonymous,
    /// One of a few names, so that duplicates are common.
    Named(u8),
    /// What the table's `n`th generated id is: a peer may announce it,
    /// as libzmq never stops one.
    LikeGenerated(u32),
}

fn id() -> impl Strategy<Value = Id> {
    prop_oneof![
        Just(Id::Anonymous),
        (0..3u8).prop_map(Id::Named),
        (0..4u32).prop_map(Id::LikeGenerated),
    ]
}

#[derive(Debug, Clone)]
enum Op {
    /// A peer arrives, its handshake done announcing an identity, or not
    /// yet.
    Connect(Option<Id>),
    /// The `n`th peer whose handshake is pending completes it.
    Establish(usize, Id),
    /// The `n`th peer's session goes away, leaving it closed in the list.
    Close(usize),
    /// Closed peers leave the list, as the socket reaps them.
    Reap,
    Sync,
    Send(Id),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => prop::option::weighted(0.8, id()).prop_map(Op::Connect),
        1 => (any::<usize>(), id()).prop_map(|(n, id)| Op::Establish(n, id)),
        1 => any::<usize>().prop_map(Op::Close),
        1 => Just(Op::Reap),
        3 => Just(Op::Sync),
        3 => id().prop_map(Op::Send),
    ]
}

/// What the table must know of a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    /// Not yet identified.
    Unknown,
    Routable(Vec<u8>),
    /// Announced an identity already in use, without handover.
    Ignored,
}

struct ModelPeer {
    key: PeerKey,
    session: Option<FakeSession>,
    /// The identity announced, once the handshake is done.
    announced: Option<Vec<u8>>,
    route: Route,
    /// Still in the peer list.
    listed: bool,
}

/// libzmq's `router_t`, simply: at each sync, established peers not yet
/// identified are, in the order they arrived. An anonymous peer gets the
/// next generated id. An id in use by a listed peer goes to the newcomer
/// with handover, disconnecting the holder; otherwise the newcomer is
/// ignored for good.
struct Model {
    handover: bool,
    first_generated: u32,
    generated: u32,
    peers: Vec<ModelPeer>,
}

impl Model {
    fn generated_id(counter: u32) -> Vec<u8> {
        let mut id = vec![0];
        id.extend_from_slice(&counter.to_be_bytes());
        id
    }

    fn resolve(&self, id: Id) -> Vec<u8> {
        match id {
            Id::Anonymous => Vec::new(),
            Id::Named(n) => vec![b'a' + n],
            Id::LikeGenerated(n) => Self::generated_id(self.first_generated.wrapping_add(n)),
        }
    }

    fn holder(&self, id: &[u8]) -> Option<usize> {
        self.peers
            .iter()
            .position(|peer| peer.listed && peer.route == Route::Routable(id.to_vec()))
    }

    fn sync(&mut self) {
        for i in 0..self.peers.len() {
            let peer = &self.peers[i];
            if !peer.listed || peer.route != Route::Unknown {
                continue;
            }
            let Some(announced) = peer.announced.clone() else {
                continue;
            };
            let id = if announced.is_empty() {
                let id = Self::generated_id(self.generated);
                self.generated = self.generated.wrapping_add(1);
                id
            } else {
                announced
            };
            if let Some(holder) = self.holder(&id) {
                if !self.handover {
                    self.peers[i].route = Route::Ignored;
                    continue;
                }
                self.peers[holder].route = Route::Unknown;
                self.peers[holder].listed = false;
            }
            self.peers[i].route = Route::Routable(id);
        }
    }

    /// Who a send to `id` reaches: a listed peer routable by it, whose
    /// session is still there.
    fn recipient(&self, id: &[u8]) -> Option<usize> {
        self.holder(id).filter(|&i| self.peers[i].session.is_some())
    }
}

fn established(identity: Vec<u8>) -> Established {
    Established {
        version: Version::V3_1,
        peer_socket_type: Some(SocketType::Dealer),
        peer_identity: Frame::from(identity),
        metadata: Metadata::new(),
        authenticated: None,
        implicit_subscription: false,
    }
}

fn establish(session: &FakeSession, identity: Vec<u8>) {
    *session.connection_peer.lock().unwrap() = Some(established(identity));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// The table routes as the model does, whatever order peers arrive,
    /// announce, leave and are sent to in, with and without handover.
    #[test]
    fn router_table_follows_the_model(
        handover in any::<bool>(),
        ops in prop::collection::vec(op(), 0..60),
    ) {
        let mut table = RouterTable::default();
        table.set_handover(handover);
        let mut peers = PeerList::default();
        let mut model = Model {
            handover,
            first_generated: table.next_generated,
            generated: table.next_generated,
            peers: Vec::new(),
        };
        for (sent, op) in ops.into_iter().enumerate() {
            match op {
                Op::Connect(announced) => {
                    let (connection, session) = connection(10_000);
                    let announced = announced.map(|id| model.resolve(id));
                    if let Some(identity) = &announced {
                        establish(&session, identity.clone());
                    }
                    let key = peers.push(connection);
                    model.peers.push(ModelPeer {
                        key,
                        session: Some(session),
                        announced,
                        route: Route::Unknown,
                        listed: true,
                    });
                }
                Op::Establish(n, id) => {
                    let identity = model.resolve(id);
                    let pending: Vec<usize> = (0..model.peers.len())
                        .filter(|&i| {
                            let peer = &model.peers[i];
                            peer.announced.is_none() && peer.session.is_some()
                        })
                        .collect();
                    if pending.is_empty() {
                        continue;
                    }
                    let peer = &mut model.peers[pending[n % pending.len()]];
                    establish(peer.session.as_ref().unwrap(), identity.clone());
                    peer.announced = Some(identity);
                }
                Op::Close(n) => {
                    let open: Vec<usize> = (0..model.peers.len())
                        .filter(|&i| model.peers[i].session.is_some())
                        .collect();
                    if open.is_empty() {
                        continue;
                    }
                    model.peers[open[n % open.len()]].session = None;
                }
                Op::Reap => {
                    peers.retain_mut(|peer| !peer.connection.tx.is_closed());
                    for peer in &mut model.peers {
                        if peer.session.is_none() {
                            peer.listed = false;
                        }
                    }
                }
                Op::Sync => {
                    table.sync(&mut peers);
                    model.sync();
                }
                Op::Send(id) => {
                    let id = model.resolve(id);
                    let message = Message::new(Frame::from(sent.to_string()));
                    let result = table.try_send(&mut peers, &id, message.clone());
                    match model.recipient(&id) {
                        Some(i) => {
                            prop_assert_eq!(result, Ok(()), "to {:?}", id);
                            let session = model.peers[i].session.as_mut().unwrap();
                            prop_assert_eq!(drain(session), [message]);
                        }
                        None => prop_assert_eq!(
                            result,
                            Err(RouterSendError::UnknownRoutingId(message)),
                            "to {:?}",
                            id
                        ),
                    }
                }
            }
            for peer in &mut model.peers {
                prop_assert_eq!(peers.get(peer.key).is_some(), peer.listed);
                // Asked only of listed peers: those reaped keep their id
                // until the next sync, but send nothing to be routed.
                if peer.listed {
                    let expected = match &peer.route {
                        Route::Routable(id) => Some(Frame::from(id.clone())),
                        Route::Unknown | Route::Ignored => None,
                    };
                    prop_assert_eq!(table.routing_id(peer.key).cloned(), expected);
                }
                if let Some(session) = &mut peer.session {
                    prop_assert!(drain(session).is_empty(), "a message went astray");
                }
            }
        }
    }
}
