//! libzmq's regression tests (`tests/test_*.cpp` at v4.3.5), ported to
//! zmq-rs's API: one module per libzmq test, one `#[test]` per test case,
//! named as libzmq names them. Where zmq-rs differs from libzmq on
//! purpose, a test asserts zmq-rs's behavior and says so, pointing to
//! `docs/rfc-compliance.md`. Tests for features zmq-rs lacks, and for
//! libzmq's C API rather than its behavior, are not ported; see
//! `README.md` beside this file.

mod support;

mod abstract_ipc;
mod base85;
mod bind_after_connect_tcp;
mod conflate;
mod connect_resolve;
mod disconnect_inproc;
mod heartbeats;
mod hwm;
mod hwm_pubsub;
mod immediate;
mod inproc_connect;
mod invalid_rep;
mod ipc_wildcard;
mod last_endpoint;
mod metadata;
mod mock_pub_sub;
mod monitor;
mod pair_inproc;
mod pair_ipc;
mod pair_tcp;
mod poller;
mod probe_router;
mod proxy;
mod proxy_hwm;
mod proxy_single_socket;
mod proxy_steerable;
mod proxy_terminate;
mod pub_invert_matching;
mod pubsub;
mod rebind_ipc;
mod reconnect_ivl;
mod req_correlate;
mod req_relaxed;
mod reqrep_device;
mod reqrep_inproc;
mod reqrep_ipc;
mod reqrep_tcp;
mod router_handover;
mod router_mandatory;
mod router_mandatory_hwm;
mod security_curve;
mod security_no_zap_handler;
mod security_null;
mod security_plain;
mod security_zap;
mod spec_dealer;
mod spec_pushpull;
mod spec_rep;
mod spec_req;
mod spec_router;
mod sub_forward;
mod term_endpoint;
mod timeo;
mod unbind_wildcard;
mod xpub_manual;
mod xpub_manual_last_value;
mod xpub_nodrop;
mod xpub_topic;
mod xpub_verbose;
mod xpub_welcome_msg;

#[cfg(feature = "draft")]
mod channel;
#[cfg(feature = "draft")]
mod client_server;
#[cfg(feature = "draft")]
mod peer;
#[cfg(feature = "draft")]
mod radio_dish;
#[cfg(feature = "draft")]
mod scatter_gather;
