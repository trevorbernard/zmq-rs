//! An endpoint a socket reports parses back to the same target, for binding
//! and for connecting.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

use proptest::prelude::*;

use super::*;

fn socket_addr() -> impl Strategy<Value = SocketAddr> {
    prop_oneof![
        (any::<[u8; 4]>(), any::<u16>())
            .prop_map(|(ip, port)| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip), port))),
        (any::<[u16; 8]>(), any::<u16>(), any::<u32>(), interface()).prop_map(
            |(ip, port, flowinfo, scope_id)| {
                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(ip),
                    port,
                    flowinfo,
                    scope_id,
                ))
            }
        ),
    ]
}

/// A scope id: an interface index, small in practice. The system resolver
/// handles scoped addresses, and macOS's drops ids from 65536 up.
fn interface() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0), 1u32..256]
}

/// A name or path: anything but empty, without NUL.
fn name() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-zA-Z0-9_./:@*-]{1,30}",
        prop::collection::vec(any::<char>().prop_filter("not NUL", |&c| c != '\0'), 1..10)
            .prop_map(|chars| chars.into_iter().collect()),
    ]
}

fn endpoint() -> impl Strategy<Value = Endpoint> {
    let ipc = name()
        .prop_filter("ipc://* binds anywhere", |path| path != "*")
        .prop_filter("ipc://@ is abstract on Linux", |path| {
            !(cfg!(any(target_os = "linux", target_os = "android")) && path.starts_with('@'))
        })
        .prop_map(|path| Endpoint::Ipc(PathBuf::from(path)));
    prop_oneof![
        socket_addr().prop_map(Endpoint::Tcp),
        ipc,
        name().prop_map(Endpoint::IpcAbstract),
        name().prop_map(Endpoint::Inproc),
    ]
}

/// What `target` is, as the endpoint a socket would report for it. The
/// flow label is not part of an endpoint string.
fn reported(target: Target) -> Option<Endpoint> {
    Some(match target {
        Target::Tcp(SocketAddr::V6(addr)) => Endpoint::Tcp(SocketAddr::V6(SocketAddrV6::new(
            *addr.ip(),
            addr.port(),
            0,
            addr.scope_id(),
        ))),
        Target::Tcp(addr) => Endpoint::Tcp(addr),
        Target::TcpHost { host, .. } => panic!("{host} is reported as an address"),
        Target::Ipc(path) => Endpoint::Ipc(path),
        Target::IpcAnywhere => return None,
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Target::IpcAbstract(name) => Endpoint::IpcAbstract(name),
        Target::Inproc(name) => Endpoint::Inproc(name),
    })
}

fn without_flowinfo(endpoint: Endpoint) -> Endpoint {
    match endpoint {
        Endpoint::Tcp(SocketAddr::V6(addr)) => Endpoint::Tcp(SocketAddr::V6(SocketAddrV6::new(
            *addr.ip(),
            addr.port(),
            0,
            addr.scope_id(),
        ))),
        other => other,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn endpoints_parse_back_to_themselves(endpoint in endpoint()) {
        let abstract_supported = cfg!(any(target_os = "linux", target_os = "android"));
        if let Endpoint::IpcAbstract(_) = endpoint
            && !abstract_supported
        {
            return Ok(());
        }
        let text = endpoint.to_string();
        let expected = Some(without_flowinfo(endpoint));
        for parse in [bind_target, connect_target] {
            match parse(&text) {
                Ok(target) => prop_assert_eq!(reported(target), expected.clone(), "{}", text),
                Err(err) => prop_assert!(false, "{} does not parse: {}", text, err),
            }
        }
    }
}
