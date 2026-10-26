//! When to try connecting again after a connection fails or ends.

use std::hash::{BuildHasher, RandomState};
use std::time::Duration;

/// How a connecting session retries after its connection fails or ends.
/// Protocol errors, such as an incompatible peer, never retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconnect {
    /// Give up: the connection's pipes close.
    Never,
    /// Wait `interval` plus a random fraction of it each time, like libzmq's
    /// `ZMQ_RECONNECT_IVL` (default 100 ms).
    Fixed { interval: Duration },
    /// Wait `initial`, then double up to `max`, each time plus a random
    /// fraction of `initial`, like libzmq's `ZMQ_RECONNECT_IVL` with
    /// `ZMQ_RECONNECT_IVL_MAX`. Resets once a connection is made, before
    /// its handshake, as in libzmq.
    Backoff { initial: Duration, max: Duration },
}

impl Default for Reconnect {
    fn default() -> Self {
        Self::Fixed {
            interval: Duration::from_millis(100),
        }
    }
}

/// The retry schedule of one session.
#[derive(Debug)]
pub(crate) struct Retry {
    policy: Reconnect,
    /// The last backoff delay, if retrying is under way.
    current: Option<Duration>,
}

impl Retry {
    pub(crate) fn new(policy: Reconnect) -> Self {
        Self {
            policy,
            current: None,
        }
    }

    /// The delay before the next attempt, or `None` to give up.
    pub(crate) fn next_delay(&mut self) -> Option<Duration> {
        match self.policy {
            Reconnect::Never => None,
            // libzmq: the base interval plus `random % interval`.
            Reconnect::Fixed { interval } => Some(interval.saturating_add(jitter(interval))),
            Reconnect::Backoff { initial, max } => {
                let next = match self.current {
                    None => initial,
                    Some(current) => current.saturating_mul(2).min(max),
                };
                self.current = Some(next);
                // libzmq bounds the jitter by the base interval, not the
                // current one.
                Some(next.saturating_add(jitter(initial)))
            }
        }
    }

    /// A connection succeeded: the next failure starts the schedule over.
    pub(crate) fn reset(&mut self) {
        self.current = None;
    }
}

/// A random duration below `bound`, so that many peers that lost the same
/// server do not all reconnect at the same instant.
fn jitter(bound: Duration) -> Duration {
    let nanos = bound.as_nanos();
    if nanos == 0 {
        return Duration::ZERO;
    }
    // Each RandomState gets a new key, which is random enough for jitter.
    let random = u128::from(RandomState::new().hash_one(()));
    Duration::from_nanos((random % nanos) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn default_matches_libzmq() {
        assert_eq!(Reconnect::default(), Reconnect::Fixed { interval: MS(100) });
    }

    #[test]
    fn never_gives_up_at_once() {
        assert_eq!(Retry::new(Reconnect::Never).next_delay(), None);
    }

    #[test]
    fn fixed_adds_up_to_one_interval_of_jitter() {
        let mut retry = Retry::new(Reconnect::Fixed { interval: MS(100) });
        let delays: Vec<Duration> = (0..200).map(|_| retry.next_delay().unwrap()).collect();
        assert!(
            delays.iter().all(|d| (MS(100)..MS(200)).contains(d)),
            "{delays:?}"
        );
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "jitter varies the delay"
        );
    }

    #[test]
    fn backoff_doubles_up_to_the_cap_and_resets() {
        let mut retry = Retry::new(Reconnect::Backoff {
            initial: MS(100),
            max: MS(500),
        });
        let delays: Vec<Duration> = (0..5).map(|_| retry.next_delay().unwrap()).collect();
        for (delay, base) in delays.iter().zip([100, 200, 400, 500, 500]) {
            assert!((MS(base)..MS(base + 100)).contains(delay), "{delays:?}");
        }
        retry.reset();
        let delay = retry.next_delay().unwrap();
        assert!((MS(100)..MS(200)).contains(&delay), "{delay:?}");
    }
}
