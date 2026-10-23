//! The log against a model: a queue of committed octets and a buffer of
//! octets put but not committed.

#![cfg(not(loom))]

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use proptest::prelude::*;
use zmq_log::log;

#[derive(Debug, Clone)]
enum Op {
    Put(usize),
    EndMessage,
    Commit,
    /// Release this share, in percent, of what is committed.
    Release(u8),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (1usize..40).prop_map(Op::Put),
        1 => Just(Op::EndMessage),
        2 => Just(Op::Commit),
        2 => (0u8..=100).prop_map(Op::Release),
    ]
}

proptest! {
    #[test]
    fn committed_octets_match_the_model(
        capacity in prop::sample::select(vec![16usize, 32, 64]),
        hwm in prop::option::of(1usize..6),
        ops in prop::collection::vec(op(), 1..300),
    ) {
        let (mut writer, mut reader) = log(
            NonZeroUsize::new(capacity).unwrap(),
            hwm.and_then(NonZeroUsize::new),
        );
        let mut committed: VecDeque<u8> = VecDeque::new();
        let mut pending: Vec<u8> = Vec::new();
        let mut next = 0u8;
        let mut messages_ended = 0u64;
        let mut messages_committed = 0u64;
        let mut messages_released = 0u64;
        for op in ops {
            match op {
                Op::Put(len) => {
                    let used = committed.len() + pending.len();
                    let len = len.min(capacity - used);
                    prop_assert_eq!(writer.free(), capacity - used);
                    let data: Vec<u8> = (0..len)
                        .map(|_| {
                            next = next.wrapping_add(1);
                            next
                        })
                        .collect();
                    writer.put(&data);
                    pending.extend(data);
                }
                Op::EndMessage => {
                    writer.end_message();
                    messages_ended += 1;
                }
                Op::Commit => {
                    writer.commit();
                    committed.extend(pending.drain(..));
                    messages_committed = messages_ended;
                }
                Op::Release(percent) => {
                    let octets = committed.len() * usize::from(percent) / 100;
                    let messages = (messages_committed - messages_released) * u64::from(percent) / 100;
                    reader.release(octets, messages);
                    committed.drain(..octets);
                    messages_released += messages;
                }
            }
            let [a, b] = reader.committed();
            prop_assert_eq!([a, b].concat(), committed.iter().copied().collect::<Vec<_>>());
            let outstanding = messages_ended - messages_released;
            let used = committed.len() + pending.len();
            let expected_room = used < capacity && hwm.is_none_or(|hwm| outstanding < hwm as u64);
            prop_assert_eq!(writer.has_room(1), expected_room);
        }
    }
}
