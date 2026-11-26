//! Why a bind or a connect is refused, compared with libzmq's `errno` for
//! the same endpoint.

use libc::{EINVAL, ENAMETOOLONG, ENODEV, EPROTONOSUPPORT};
use zmq::{Context, Error, Push};
use zmq_interop::libzmq::{self, Kind, Socket};

/// `ZMQ_HAUSNUMERO + 52`.
const ENOCOMPATPROTO: i32 = 156_384_764;

/// libzmq's `errno` for what we failed with.
fn errno(err: &Error) -> i32 {
    match err {
        Error::InvalidEndpoint(_) => EINVAL,
        Error::NoSuchDevice(_) => ENODEV,
        Error::NameTooLong(_) => ENAMETOOLONG,
        Error::UnsupportedTransport(_) => EPROTONOSUPPORT,
        Error::IncompatibleTransport(_) => ENOCOMPATPROTO,
        other => panic!("{other:?}"),
    }
}

/// A string of `length` octets that starts with `start` and names `test`
/// and this process, so that no two tests bind the same.
fn padded(start: &str, test: &str, length: usize) -> String {
    let start = format!("{start}{test}-{}-", std::process::id());
    format!("{start}{}", "x".repeat(length - start.len()))
}

/// Endpoints, with what binding each fails with and what connecting to it
/// does; `None` where it succeeds. `test` names the test, for the paths
/// that are bound.
fn cases(test: &str) -> Vec<(String, Option<i32>, Option<i32>)> {
    let directory = format!("{}/", std::env::temp_dir().display());
    let ipc = |length| format!("ipc://{}", padded(&directory, test, length));
    let mut cases = vec![
        // Neither an interface nor an address, and a host name that
        // resolvers know to be none without asking (RFC 6761).
        (
            "tcp://no-such-interface.invalid:5555".to_owned(),
            Some(ENODEV),
            None,
        ),
        ("tcp://127.0.0.1:foo".to_owned(), Some(EINVAL), Some(EINVAL)),
        ("tcp://127.0.0.1".to_owned(), Some(EINVAL), Some(EINVAL)),
        ("tcp://127.0.0.1:*".to_owned(), None, Some(EINVAL)),
        // `sun_path` holds 108 octets on Linux, 104 elsewhere, with the
        // zero that ends the path.
        (ipc(103), None, None),
        (ipc(108), Some(ENAMETOOLONG), Some(ENAMETOOLONG)),
        ("ipc://".to_owned(), Some(EINVAL), Some(EINVAL)),
        ("inproc://".to_owned(), Some(EINVAL), Some(EINVAL)),
        ("nothing".to_owned(), Some(EINVAL), Some(EINVAL)),
        ("://x".to_owned(), Some(EINVAL), Some(EINVAL)),
        (String::new(), Some(EINVAL), Some(EINVAL)),
        (
            "foo://bar".to_owned(),
            Some(EPROTONOSUPPORT),
            Some(EPROTONOSUPPORT),
        ),
        (
            "TCP://127.0.0.1:5561".to_owned(),
            Some(EPROTONOSUPPORT),
            Some(EPROTONOSUPPORT),
        ),
        // Only RADIO, DISH and DGRAM sockets use `udp`.
        (
            "udp://127.0.0.1:5555".to_owned(),
            Some(ENOCOMPATPROTO),
            Some(ENOCOMPATPROTO),
        ),
    ];
    if cfg!(any(target_os = "linux", target_os = "android")) {
        let name = |length| format!("ipc://@{}", padded("", test, length));
        cases.extend([
            (ipc(107), None, None),
            (name(106), None, None),
            (name(107), Some(ENAMETOOLONG), Some(ENAMETOOLONG)),
            ("ipc://@".to_owned(), Some(EINVAL), Some(EINVAL)),
        ]);
    }
    cases
}

#[test]
fn libzmq_says_why_it_refuses_an_endpoint() {
    assert_eq!(
        Socket::new(Kind::Push)
            .try_bind("tcp://:5555")
            .err()
            .and_then(|err| libzmq::errno(&err)),
        Some(ENODEV)
    );
    for (endpoint, bind, connect) in cases("libzmq") {
        let code =
            |result: std::io::Result<()>| result.err().map(|err| libzmq::errno(&err).unwrap());
        assert_eq!(
            code(Socket::new(Kind::Push).try_bind(&endpoint)),
            bind,
            "bind {endpoint}"
        );
        assert_eq!(
            code(Socket::new(Kind::Push).try_connect(&endpoint)),
            connect,
            "connect {endpoint}"
        );
    }
}

#[test]
fn we_say_why_we_refuse_an_endpoint() {
    let context = Context::new().unwrap();
    assert!(matches!(
        context.socket::<Push>().unwrap().bind("tcp://:5555"),
        Err(Error::NoSuchDevice(_))
    ));
    for (endpoint, bind, connect) in cases("ours") {
        let code = |result: Result<(), Error>| result.err().map(|err| errno(&err));
        assert_eq!(
            code(context.socket::<Push>().unwrap().bind(&endpoint).map(drop)),
            bind,
            "bind {endpoint}"
        );
        assert_eq!(
            code(
                context
                    .socket::<Push>()
                    .unwrap()
                    .connect(&endpoint)
                    .map(drop)
            ),
            connect,
            "connect {endpoint}"
        );
    }
}

/// A RADIO only connects over `udp` and a DISH only binds, as libzmq's
/// `test_radio_dish` has it.
#[cfg(feature = "draft")]
mod draft {
    use zmq::{Dish, Radio};

    use super::*;

    const UDP: &str = "udp://127.0.0.1:5556";

    #[test]
    fn libzmq_refuses_a_radio_that_binds_and_a_dish_that_connects_over_udp() {
        if !libzmq::has_drafts() {
            return;
        }
        let errno = |result: std::io::Result<()>| libzmq::errno(&result.unwrap_err());
        assert_eq!(
            errno(Socket::new(Kind::Radio).try_bind(UDP)),
            Some(ENOCOMPATPROTO)
        );
        assert_eq!(
            errno(Socket::new(Kind::Dish).try_connect(UDP)),
            Some(ENOCOMPATPROTO)
        );
    }

    #[test]
    fn we_refuse_a_radio_that_binds_and_a_dish_that_connects_over_udp() {
        let context = Context::new().unwrap();
        assert!(matches!(
            context.socket::<Radio>().unwrap().bind(UDP),
            Err(Error::IncompatibleTransport(_))
        ));
        assert!(matches!(
            context.socket::<Dish>().unwrap().connect(UDP),
            Err(Error::IncompatibleTransport(_))
        ));
    }
}
