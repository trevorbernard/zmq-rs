//! A minimal safe binding to the reference libzmq, just enough to drive it
//! from tests. All of the workspace's `unsafe` FFI lives in this module.
#![allow(unsafe_code)]

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::io;
use std::net::SocketAddr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const ZMQ_ROUTING_ID: c_int = 5;
const ZMQ_SUBSCRIBE: c_int = 6;
const ZMQ_UNSUBSCRIBE: c_int = 7;
const ZMQ_LINGER: c_int = 17;
const ZMQ_RCVTIMEO: c_int = 27;
const ZMQ_LAST_ENDPOINT: c_int = 32;
const ZMQ_PLAIN_SERVER: c_int = 44;
const ZMQ_PLAIN_USERNAME: c_int = 45;
const ZMQ_PLAIN_PASSWORD: c_int = 46;
const ZMQ_CURVE_SERVER: c_int = 47;
const ZMQ_CURVE_PUBLICKEY: c_int = 48;
const ZMQ_CURVE_SECRETKEY: c_int = 49;
const ZMQ_CURVE_SERVERKEY: c_int = 50;
const ZMQ_ZAP_DOMAIN: c_int = 55;
const ZMQ_HEARTBEAT_IVL: c_int = 75;
const ZMQ_HEARTBEAT_TTL: c_int = 76;
const ZMQ_HEARTBEAT_TIMEOUT: c_int = 77;
const ZMQ_SNDMORE: c_int = 2;

/// `zmq_msg_t`: 64 opaque octets, pointer-aligned.
#[repr(C, align(8))]
struct RawMsg([u8; 64]);

#[link(name = "zmq")]
unsafe extern "C" {
    fn zmq_ctx_new() -> *mut c_void;
    fn zmq_ctx_term(context: *mut c_void) -> c_int;
    fn zmq_socket(context: *mut c_void, socket_type: c_int) -> *mut c_void;
    fn zmq_close(socket: *mut c_void) -> c_int;
    fn zmq_setsockopt(
        socket: *mut c_void,
        option: c_int,
        value: *const c_void,
        len: usize,
    ) -> c_int;
    fn zmq_getsockopt(
        socket: *mut c_void,
        option: c_int,
        value: *mut c_void,
        len: *mut usize,
    ) -> c_int;
    fn zmq_bind(socket: *mut c_void, endpoint: *const c_char) -> c_int;
    fn zmq_connect(socket: *mut c_void, endpoint: *const c_char) -> c_int;
    fn zmq_send(socket: *mut c_void, buf: *const c_void, len: usize, flags: c_int) -> c_int;
    fn zmq_msg_init(msg: *mut RawMsg) -> c_int;
    fn zmq_msg_recv(msg: *mut RawMsg, socket: *mut c_void, flags: c_int) -> c_int;
    fn zmq_msg_data(msg: *mut RawMsg) -> *mut c_void;
    fn zmq_msg_size(msg: *const RawMsg) -> usize;
    fn zmq_msg_more(msg: *const RawMsg) -> c_int;
    fn zmq_msg_close(msg: *mut RawMsg) -> c_int;
    fn zmq_errno() -> c_int;
    fn zmq_has(capability: *const c_char) -> c_int;
    fn zmq_msg_init_size(msg: *mut RawMsg, size: usize) -> c_int;
    fn zmq_msg_send(msg: *mut RawMsg, socket: *mut c_void, flags: c_int) -> c_int;
    fn zmq_strerror(errnum: c_int) -> *const c_char;
}

// libzmq's draft API is exported under these names only by a build with
// drafts; a build without them has C++-mangled ones. Linking them would
// fail against such a libzmq, so they are looked up when first used.
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
}

/// `RTLD_DEFAULT`: search every loaded library.
#[cfg(target_os = "macos")]
const RTLD_DEFAULT: *mut c_void = -2isize as *mut c_void;
#[cfg(not(target_os = "macos"))]
const RTLD_DEFAULT: *mut c_void = std::ptr::null_mut();

/// The address of libzmq's draft function `name`, which must exist: check
/// [`has`]`("draft")` first.
fn draft_function(name: &str) -> *mut c_void {
    let symbol = CString::new(name).unwrap();
    // SAFETY: `symbol` is a valid C string; RTLD_DEFAULT searches every
    // loaded library.
    let address = unsafe { dlsym(RTLD_DEFAULT, symbol.as_ptr()) };
    assert!(
        !address.is_null(),
        "this libzmq has no {name}: built without drafts"
    );
    address
}

fn last_error() -> io::Error {
    // SAFETY: both functions are always safe to call; strerror returns a
    // static NUL-terminated string.
    let (code, message) = unsafe {
        let code = zmq_errno();
        (code, CStr::from_ptr(zmq_strerror(code)))
    };
    io::Error::other(format!(
        "libzmq error {code}: {}",
        message.to_string_lossy()
    ))
}

fn check(rc: c_int) -> io::Result<c_int> {
    if rc < 0 { Err(last_error()) } else { Ok(rc) }
}

/// A libzmq socket type, numbered as in `zmq.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Pair = 0,
    Pub = 1,
    Sub = 2,
    Req = 3,
    Rep = 4,
    Dealer = 5,
    Router = 6,
    Pull = 7,
    Push = 8,
    XPub = 9,
    XSub = 10,
    Server = 12,
    Client = 13,
    Radio = 14,
    Dish = 15,
    Gather = 16,
    Scatter = 17,
    Peer = 19,
    Channel = 20,
}

/// A libzmq context, shared by sockets that talk over `inproc`, such as a
/// server and its ZAP handler.
#[derive(Debug, Clone)]
pub struct Context(Arc<RawContext>);

#[derive(Debug)]
struct RawContext(NonNull<c_void>);

// SAFETY: a libzmq context is thread-safe.
unsafe impl Send for RawContext {}
unsafe impl Sync for RawContext {}

impl Context {
    /// A new context.
    pub fn new() -> Self {
        // SAFETY: plain constructor call; the result is checked for null.
        let raw = NonNull::new(unsafe { zmq_ctx_new() }).expect("zmq_ctx_new failed");
        Self(Arc::new(RawContext(raw)))
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RawContext {
    fn drop(&mut self) {
        // SAFETY: the pointer came from zmq_ctx_new, and every socket holds an
        // Arc to this context, so all of them are closed by now.
        unsafe { zmq_ctx_term(self.0.as_ptr()) };
    }
}

/// A libzmq socket with linger disabled. Like any non-thread-safe libzmq
/// socket it may move between threads but not be shared.
#[derive(Debug)]
pub struct Socket {
    raw: NonNull<c_void>,
    _context: Context,
}

// SAFETY: moving a socket to another thread is allowed; `Socket` is not Sync,
// so it is never used from two threads at once.
unsafe impl Send for Socket {}

impl Socket {
    /// A socket in its own context.
    pub fn new(kind: Kind) -> Self {
        Self::in_context(&Context::new(), kind)
    }

    /// A socket in `context`.
    pub fn in_context(context: &Context, kind: Kind) -> Self {
        // SAFETY: the context pointer is live; the result is checked for null.
        let raw = NonNull::new(unsafe { zmq_socket(context.0.0.as_ptr(), kind as c_int) })
            .unwrap_or_else(|| panic!("zmq_socket failed: {}", last_error()));
        let socket = Self {
            raw,
            _context: context.clone(),
        };
        socket.set_int(ZMQ_LINGER, 0);
        socket.set_receive_timeout(Duration::from_secs(5));
        socket
    }

    fn set_bytes(&self, option: c_int, value: &[u8]) {
        // SAFETY: `value` is valid for `value.len()` octets for the call.
        let rc = unsafe {
            zmq_setsockopt(
                self.raw.as_ptr(),
                option,
                value.as_ptr().cast(),
                value.len(),
            )
        };
        check(rc).unwrap_or_else(|err| panic!("zmq_setsockopt({option}): {err}"));
    }

    fn set_int(&self, option: c_int, value: c_int) {
        self.set_bytes(option, &value.to_ne_bytes());
    }

    /// How long `recv` waits before failing.
    pub fn set_receive_timeout(&self, timeout: Duration) {
        let millis = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
        self.set_int(ZMQ_RCVTIMEO, millis);
    }

    /// The identity announced to peers.
    pub fn set_routing_id(&self, id: &[u8]) {
        self.set_bytes(ZMQ_ROUTING_ID, id);
    }

    /// Makes this socket a PLAIN server.
    pub fn set_plain_server(&self) {
        self.set_int(ZMQ_PLAIN_SERVER, 1);
    }

    /// Makes this socket a PLAIN client with these credentials.
    pub fn set_plain_client(&self, username: &[u8], password: &[u8]) {
        self.set_bytes(ZMQ_PLAIN_USERNAME, username);
        self.set_bytes(ZMQ_PLAIN_PASSWORD, password);
    }

    /// Makes this socket a CURVE server with this long-term secret key.
    pub fn set_curve_server(&self, secret: &[u8; 32]) {
        self.set_int(ZMQ_CURVE_SERVER, 1);
        self.set_bytes(ZMQ_CURVE_SECRETKEY, secret);
    }

    /// Makes this socket a CURVE client with its long-term key pair, for
    /// the server whose public key is `server`.
    pub fn set_curve_client(&self, public: &[u8; 32], secret: &[u8; 32], server: &[u8; 32]) {
        self.set_bytes(ZMQ_CURVE_PUBLICKEY, public);
        self.set_bytes(ZMQ_CURVE_SECRETKEY, secret);
        self.set_bytes(ZMQ_CURVE_SERVERKEY, server);
    }

    /// Sends a PING every `interval`, gives up after `timeout` without
    /// traffic, and asks peers to give up after `ttl`.
    pub fn set_heartbeat(&self, interval: Duration, timeout: Duration, ttl: Duration) {
        let millis = |d: Duration| c_int::try_from(d.as_millis()).unwrap_or(c_int::MAX);
        self.set_int(ZMQ_HEARTBEAT_IVL, millis(interval));
        self.set_int(ZMQ_HEARTBEAT_TIMEOUT, millis(timeout));
        self.set_int(ZMQ_HEARTBEAT_TTL, millis(ttl));
    }

    /// The ZAP domain this server authenticates in.
    pub fn set_zap_domain(&self, domain: &[u8]) {
        self.set_bytes(ZMQ_ZAP_DOMAIN, domain);
    }

    /// Binds to `endpoint`, e.g. `inproc://name`.
    pub fn bind(&self, endpoint: &str) {
        let c_endpoint = CString::new(endpoint).unwrap();
        // SAFETY: `c_endpoint` is a valid C string for the call.
        check(unsafe { zmq_bind(self.raw.as_ptr(), c_endpoint.as_ptr()) })
            .unwrap_or_else(|err| panic!("zmq_bind({endpoint}): {err}"));
    }

    /// Connects to `endpoint`, such as a TCP address.
    pub fn connect(&self, endpoint: impl Into<zmq::Endpoint>) {
        let endpoint = endpoint.into();
        let c_endpoint = CString::new(endpoint.to_string()).unwrap();
        // SAFETY: `c_endpoint` is a valid C string for the call.
        check(unsafe { zmq_connect(self.raw.as_ptr(), c_endpoint.as_ptr()) })
            .unwrap_or_else(|err| panic!("zmq_connect({endpoint}): {err}"));
    }

    /// Joins a DISH socket to `group`. Needs a libzmq with drafts.
    pub fn join(&self, group: &str) {
        let group = CString::new(group).unwrap();
        // SAFETY: zmq_join has this signature in zmq.h's draft API.
        let join: unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int =
            unsafe { std::mem::transmute(draft_function("zmq_join")) };
        // SAFETY: the socket is open and `group` is a valid C string.
        check(unsafe { join(self.raw.as_ptr(), group.as_ptr()) }).expect("zmq_join");
    }

    /// Sends `body` to `group` from a RADIO socket. Needs a libzmq with
    /// drafts.
    pub fn send_to_group(&self, group: &str, body: &[u8]) -> io::Result<()> {
        let group = CString::new(group).unwrap();
        // SAFETY: zmq_msg_set_group has this signature in zmq.h's draft API.
        let set_group: unsafe extern "C" fn(*mut RawMsg, *const c_char) -> c_int =
            unsafe { std::mem::transmute(draft_function("zmq_msg_set_group")) };
        let mut msg = RawMsg([0; 64]);
        // SAFETY: `msg` is sized for `body`, filled, given its group and
        // sent, which takes it; it is closed if anything fails first.
        unsafe {
            check(zmq_msg_init_size(&mut msg, body.len()))?;
            let data = zmq_msg_data(&mut msg).cast::<u8>();
            std::ptr::copy_nonoverlapping(body.as_ptr(), data, body.len());
            let sent = check(set_group(&mut msg, group.as_ptr()))
                .and_then(|_| check(zmq_msg_send(&mut msg, self.raw.as_ptr(), 0)));
            if sent.is_err() {
                zmq_msg_close(&mut msg);
            }
            sent.map(drop)
        }
    }

    /// Connects a PEER socket to `endpoint`, returning the routing id of
    /// the new peer. Needs a libzmq with drafts.
    pub fn connect_peer(&self, endpoint: impl Into<zmq::Endpoint>) -> u32 {
        let endpoint = CString::new(endpoint.into().to_string()).unwrap();
        // SAFETY: zmq_connect_peer has this signature in zmq.h's draft API.
        let connect_peer: unsafe extern "C" fn(*mut c_void, *const c_char) -> u32 =
            unsafe { std::mem::transmute(draft_function("zmq_connect_peer")) };
        // SAFETY: the socket is open and `endpoint` is a valid C string.
        let id = unsafe { connect_peer(self.raw.as_ptr(), endpoint.as_ptr()) };
        assert_ne!(id, 0, "zmq_connect_peer: {}", last_error());
        id
    }

    /// Sends `body` to the peer with routing id `id`, from a SERVER or
    /// PEER socket. Needs a libzmq with drafts.
    pub fn send_routed(&self, id: u32, body: &[u8]) -> io::Result<()> {
        // SAFETY: zmq_msg_set_routing_id has this signature in zmq.h's draft
        // API.
        let set_routing_id: unsafe extern "C" fn(*mut RawMsg, u32) -> c_int =
            unsafe { std::mem::transmute(draft_function("zmq_msg_set_routing_id")) };
        let mut msg = RawMsg([0; 64]);
        // SAFETY: `msg` is sized for `body`, filled, addressed and sent,
        // which takes it; it is closed if anything fails first.
        unsafe {
            check(zmq_msg_init_size(&mut msg, body.len()))?;
            let data = zmq_msg_data(&mut msg).cast::<u8>();
            std::ptr::copy_nonoverlapping(body.as_ptr(), data, body.len());
            let sent = check(set_routing_id(&mut msg, id))
                .and_then(|_| check(zmq_msg_send(&mut msg, self.raw.as_ptr(), 0)));
            if sent.is_err() {
                zmq_msg_close(&mut msg);
            }
            sent.map(drop)
        }
    }

    /// Receives a message and the routing id of its sender, on a SERVER or
    /// PEER socket. Needs a libzmq with drafts.
    pub fn recv_routed(&self) -> io::Result<(u32, Vec<u8>)> {
        // SAFETY: zmq_msg_routing_id has this signature in zmq.h's draft API.
        let routing_id: unsafe extern "C" fn(*mut RawMsg) -> u32 =
            unsafe { std::mem::transmute(draft_function("zmq_msg_routing_id")) };
        let mut msg = RawMsg([0; 64]);
        // SAFETY: `msg` is initialized before use and closed afterwards;
        // its id and data are copied out while it is still open.
        unsafe {
            check(zmq_msg_init(&mut msg))?;
            if let Err(err) = check(zmq_msg_recv(&mut msg, self.raw.as_ptr(), 0)) {
                zmq_msg_close(&mut msg);
                return Err(err);
            }
            let id = routing_id(&mut msg);
            let size = zmq_msg_size(&msg);
            let body = if size == 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(zmq_msg_data(&mut msg).cast::<u8>(), size).to_vec()
            };
            zmq_msg_close(&mut msg);
            Ok((id, body))
        }
    }

    /// Receives a message and its group on a DISH socket. Needs a libzmq
    /// with drafts.
    pub fn recv_from_group(&self) -> io::Result<(String, Vec<u8>)> {
        // SAFETY: zmq_msg_group has this signature in zmq.h's draft API.
        let group_of: unsafe extern "C" fn(*mut RawMsg) -> *const c_char =
            unsafe { std::mem::transmute(draft_function("zmq_msg_group")) };
        let mut msg = RawMsg([0; 64]);
        // SAFETY: `msg` is initialized before use and closed afterwards;
        // its group and data are copied out while it is still open.
        unsafe {
            check(zmq_msg_init(&mut msg))?;
            if let Err(err) = check(zmq_msg_recv(&mut msg, self.raw.as_ptr(), 0)) {
                zmq_msg_close(&mut msg);
                return Err(err);
            }
            let group = CStr::from_ptr(group_of(&mut msg))
                .to_string_lossy()
                .into_owned();
            let size = zmq_msg_size(&msg);
            let body = if size == 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(zmq_msg_data(&mut msg).cast::<u8>(), size).to_vec()
            };
            zmq_msg_close(&mut msg);
            Ok((group, body))
        }
    }

    /// Subscribes a SUB socket to `topic`.
    pub fn subscribe(&self, topic: &[u8]) {
        self.set_bytes(ZMQ_SUBSCRIBE, topic);
    }

    /// Removes a SUB socket's subscription to `topic`.
    pub fn unsubscribe(&self, topic: &[u8]) {
        self.set_bytes(ZMQ_UNSUBSCRIBE, topic);
    }

    /// Binds to an ephemeral TCP port on loopback and returns its address.
    pub fn bind_loopback(&self) -> SocketAddr {
        let endpoint = CString::new("tcp://127.0.0.1:*").unwrap();
        // SAFETY: `endpoint` is a valid C string for the call.
        check(unsafe { zmq_bind(self.raw.as_ptr(), endpoint.as_ptr()) })
            .unwrap_or_else(|err| panic!("zmq_bind: {err}"));
        let mut buf = [0u8; 256];
        let mut len = buf.len();
        // SAFETY: `buf` and `len` describe a writable buffer.
        check(unsafe {
            zmq_getsockopt(
                self.raw.as_ptr(),
                ZMQ_LAST_ENDPOINT,
                buf.as_mut_ptr().cast(),
                &mut len,
            )
        })
        .unwrap_or_else(|err| panic!("zmq_getsockopt(LAST_ENDPOINT): {err}"));
        let endpoint = CStr::from_bytes_until_nul(&buf[..len])
            .expect("LAST_ENDPOINT is NUL-terminated")
            .to_str()
            .expect("LAST_ENDPOINT is ASCII");
        endpoint
            .strip_prefix("tcp://")
            .and_then(|addr| addr.parse().ok())
            .unwrap_or_else(|| panic!("unexpected endpoint {endpoint}"))
    }

    /// Sends a multipart message.
    pub fn send(&self, parts: &[&[u8]]) -> io::Result<()> {
        for (i, part) in parts.iter().enumerate() {
            let flags = if i + 1 < parts.len() { ZMQ_SNDMORE } else { 0 };
            // SAFETY: `part` is valid for `part.len()` octets for the call.
            check(unsafe { zmq_send(self.raw.as_ptr(), part.as_ptr().cast(), part.len(), flags) })?;
        }
        Ok(())
    }

    /// Receives a multipart message, failing after the receive timeout.
    pub fn recv(&self) -> io::Result<Vec<Vec<u8>>> {
        let mut parts = Vec::new();
        loop {
            let mut msg = RawMsg([0; 64]);
            // SAFETY: `msg` is initialized before use and closed afterwards;
            // its data is copied out while it is still open.
            unsafe {
                check(zmq_msg_init(&mut msg))?;
                if let Err(err) = check(zmq_msg_recv(&mut msg, self.raw.as_ptr(), 0)) {
                    zmq_msg_close(&mut msg);
                    return Err(err);
                }
                let data = zmq_msg_data(&mut msg).cast::<u8>();
                let size = zmq_msg_size(&msg);
                parts.push(if size == 0 {
                    Vec::new()
                } else {
                    std::slice::from_raw_parts(data, size).to_vec()
                });
                let more = zmq_msg_more(&msg) != 0;
                zmq_msg_close(&mut msg);
                if !more {
                    return Ok(parts);
                }
            }
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // SAFETY: the pointer came from zmq_socket and is closed exactly once.
        unsafe { zmq_close(self.raw.as_ptr()) };
    }
}

/// Whether this libzmq was built with `capability`, such as `"draft"` or
/// `"curve"`.
pub fn has(capability: &str) -> bool {
    let capability = CString::new(capability).unwrap();
    // SAFETY: `capability` is a valid C string for the call.
    unsafe { zmq_has(capability.as_ptr()) != 0 }
}

/// A ZAP handler (RFC 27) for `context` that answers one request with
/// `status` and `user_id`, then returns the request frames it saw.
pub fn zap_handler(
    context: &Context,
    status: &'static str,
    user_id: &'static str,
) -> JoinHandle<Vec<Vec<u8>>> {
    let handler = Socket::in_context(context, Kind::Rep);
    // Bound before returning, so the server finds it.
    handler.bind("inproc://zeromq.zap.01");
    thread::spawn(move || {
        let request = handler.recv().expect("ZAP request");
        let request_id = request[1].clone();
        handler
            .send(&[
                b"1.0",
                &request_id,
                status.as_bytes(),
                b"",
                user_id.as_bytes(),
                b"",
            ])
            .expect("ZAP reply");
        request
    })
}
