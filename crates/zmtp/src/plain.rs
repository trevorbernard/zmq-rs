//! The PLAIN security mechanism (RFC 24).
//!
//! ```text
//! client                       server
//! HELLO username password  ->
//!                          <-  WELCOME      (after ZAP accepts)
//! INITIATE metadata        ->
//!                          <-  READY metadata
//! ```
//!
//! PLAIN sends credentials in clear text, so it should only be used over a
//! secure transport.

use zeroize::Zeroizing;

use crate::command::{encode_metadata, parse_metadata};
use crate::handshake::{
    Rejection, ServerConfig, accept_peer, command_body, local_metadata, malformed, require_zmtp_3,
    unexpected, write_command, write_raw_command, zap_verdict,
};
use crate::{
    Authenticated, Command, Credential, ErrorReason, Established, HandshakeError, Identity,
    Incoming, PeerGreeting, PlainCredentials, SocketType, Version, ZapCredentials, ZapReply,
    ZapRequest,
};

/// The client side of a PLAIN handshake.
#[derive(Debug)]
#[must_use]
pub struct PlainClient {
    socket_type: SocketType,
    identity: Identity,
    version: Version,
    state: ClientState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientState {
    WaitingForWelcome,
    WaitingForReady,
}

/// Where a client handshake goes next.
#[derive(Debug)]
#[must_use]
pub enum PlainClientStep {
    Continue(PlainClient),
    Established(Established),
}

/// The server side of a PLAIN handshake.
#[derive(Debug)]
#[must_use]
pub struct PlainServer {
    config: ServerConfig,
    version: Version,
    state: ServerState,
}

#[derive(Debug)]
enum ServerState {
    WaitingForHello,
    WaitingForInitiate(Authenticated),
}

/// Where a server handshake goes next.
#[derive(Debug)]
#[must_use]
pub enum PlainServerStep {
    /// Ask the authenticator about `request`, then call
    /// [`PendingPlainAuthentication::reply`].
    Authenticate(PendingPlainAuthentication, ZapRequest),
    Established(Established),
}

/// A server handshake waiting for its ZAP reply.
#[derive(Debug)]
#[must_use]
pub struct PendingPlainAuthentication {
    config: ServerConfig,
    version: Version,
}

impl PlainClient {
    /// Starts the handshake, writing HELLO to `out`.
    pub fn start(
        socket_type: SocketType,
        identity: Identity,
        credentials: &PlainCredentials,
        peer: &PeerGreeting,
        out: &mut Vec<u8>,
    ) -> Result<Self, HandshakeError> {
        let version = require_zmtp_3(peer)?;
        let username = credentials.username.expose();
        let password = credentials.password.expose();
        let mut hello = Zeroizing::new(Vec::with_capacity(2 + username.len() + password.len()));
        hello.push(username.len() as u8);
        hello.extend_from_slice(username);
        hello.push(password.len() as u8);
        hello.extend_from_slice(password);
        write_raw_command(version, HELLO, &hello, out);
        Ok(Self {
            socket_type,
            identity,
            version,
            state: ClientState::WaitingForWelcome,
        })
    }

    /// Handles the next item from the server.
    pub fn receive(
        self,
        incoming: Incoming,
        out: &mut Vec<u8>,
    ) -> Result<PlainClientStep, HandshakeError> {
        match (self.state, parse_from_server(incoming)?) {
            (_, Command::Error(reason)) => Err(HandshakeError::PeerError(reason)),
            (ClientState::WaitingForWelcome, Command::Other { name, data })
                if name.as_slice() == WELCOME.as_bytes() =>
            {
                if !data.is_empty() {
                    return Err(malformed(WELCOME));
                }
                let mut initiate = Vec::new();
                encode_metadata(
                    &local_metadata(self.socket_type, &self.identity),
                    &mut initiate,
                );
                write_raw_command(self.version, INITIATE, &initiate, out);
                Ok(PlainClientStep::Continue(Self {
                    state: ClientState::WaitingForReady,
                    ..self
                }))
            }
            (ClientState::WaitingForReady, Command::Ready(metadata)) => {
                // In PLAIN only the server sends ERROR.
                accept_peer(
                    self.socket_type,
                    self.version,
                    metadata,
                    Rejection::Silent,
                    out,
                )
                .map(PlainClientStep::Established)
            }
            (_, other) => Err(unexpected(other.name())),
        }
    }
}

impl PlainServer {
    /// Starts the handshake. The client speaks first, so nothing is written.
    pub fn start(config: ServerConfig, peer: &PeerGreeting) -> Result<Self, HandshakeError> {
        let version = require_zmtp_3(peer)?;
        Ok(Self {
            config,
            version,
            state: ServerState::WaitingForHello,
        })
    }

    /// Handles the next item from the client.
    pub fn receive(
        self,
        incoming: Incoming,
        out: &mut Vec<u8>,
    ) -> Result<PlainServerStep, HandshakeError> {
        match (self.state, parse(incoming)?) {
            (_, Command::Error(reason)) => Err(HandshakeError::PeerError(reason)),
            (ServerState::WaitingForHello, Command::Other { name, data })
                if name.as_slice() == HELLO.as_bytes() =>
            {
                let credentials = parse_hello(&data)?;
                let request = self.config.zap_request(ZapCredentials::Plain(credentials));
                let pending = PendingPlainAuthentication {
                    config: self.config,
                    version: self.version,
                };
                Ok(PlainServerStep::Authenticate(pending, request))
            }
            (ServerState::WaitingForInitiate(authenticated), Command::Other { name, data })
                if name.as_slice() == INITIATE.as_bytes() =>
            {
                let metadata =
                    parse_metadata(&data, INITIATE).map_err(HandshakeError::MalformedCommand)?;
                let socket_type = self.config.socket_type;
                let mut established = accept_peer(
                    socket_type,
                    self.version,
                    metadata,
                    Rejection::SendError,
                    out,
                )?;
                let ready = Command::Ready(local_metadata(socket_type, &self.config.identity));
                write_command(self.version, &ready, out);
                established.authenticated = Some(authenticated);
                Ok(PlainServerStep::Established(established))
            }
            (_, other) => Err(unexpected(other.name())),
        }
    }
}

impl PendingPlainAuthentication {
    /// Applies the authenticator's verdict: WELCOME on success, otherwise
    /// ERROR or a silent close.
    pub fn reply(self, reply: ZapReply, out: &mut Vec<u8>) -> Result<PlainServer, HandshakeError> {
        let authenticated = zap_verdict(reply, self.version, out)?;
        write_raw_command(self.version, WELCOME, b"", out);
        Ok(PlainServer {
            config: self.config,
            version: self.version,
            state: ServerState::WaitingForInitiate(authenticated),
        })
    }
}

const HELLO: &str = "HELLO";
const WELCOME: &str = "WELCOME";
const INITIATE: &str = "INITIATE";

fn parse(incoming: Incoming) -> Result<Command, HandshakeError> {
    Command::parse(&command_body(incoming)?).map_err(HandshakeError::MalformedCommand)
}

/// libzmq declares its PLAIN error prefix as the C literal `"\x05ERROR"`, in
/// which `\x05E` is one escape (0x5E), so its PLAIN server sends ERROR as
/// `^RROR reason-length reason`. Accept that form as well as RFC 24's.
/// Reported as <https://github.com/zeromq/libzmq/issues/4943>.
const LIBZMQ_ERROR_PREFIX: &[u8] = b"^RROR";

fn parse_from_server(incoming: Incoming) -> Result<Command, HandshakeError> {
    if let Incoming::Command(body) = &incoming
        && let Some(data) = body.strip_prefix(LIBZMQ_ERROR_PREFIX)
    {
        let (&len, rest) = data.split_first().ok_or_else(|| malformed("ERROR"))?;
        let reason = rest
            .get(..usize::from(len))
            .ok_or_else(|| malformed("ERROR"))?;
        let reason = ErrorReason::new(reason.to_vec()).map_err(|_| malformed("ERROR"))?;
        return Ok(Command::Error(reason));
    }
    parse(incoming)
}

/// `username-length username password-length password`, ending exactly with
/// the password as libzmq requires.
fn parse_hello(data: &[u8]) -> Result<PlainCredentials, HandshakeError> {
    let malformed = || malformed(HELLO);
    let (&username_len, rest) = data.split_first().ok_or_else(malformed)?;
    let (username, rest) = rest
        .split_at_checked(usize::from(username_len))
        .ok_or_else(malformed)?;
    let (&password_len, password) = rest.split_first().ok_or_else(malformed)?;
    if password.len() != usize::from(password_len) {
        return Err(malformed());
    }
    // Both fit: their lengths came from single octets.
    Ok(PlainCredentials {
        username: Credential::new(username).map_err(|_| malformed())?,
        password: Credential::new(password).map_err(|_| malformed())?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandError, Encoder, Frame, Message, Metadata, PropertyName, ZapFailure};

    const V3_1: PeerGreeting = PeerGreeting::Zmtp3 {
        major: 3,
        minor: 1,
        as_server: true,
    };

    fn credentials() -> PlainCredentials {
        PlainCredentials {
            username: Credential::new("admin").unwrap(),
            password: Credential::new("hunter2").unwrap(),
        }
    }

    fn identity(id: &str) -> Identity {
        Identity::new(id.to_owned()).unwrap()
    }

    fn metadata(properties: &[(&str, &str)]) -> Metadata {
        let mut metadata = Metadata::new();
        for (name, value) in properties {
            let name = PropertyName::new(name.to_string()).unwrap();
            metadata.insert(name, value.to_string()).unwrap();
        }
        metadata
    }

    fn body(command: &Command) -> Vec<u8> {
        let mut body = Vec::new();
        command.encode(&mut body);
        body
    }

    fn wire(body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        Encoder::new(Version::V3_1)
            .encode_command(body, &mut out)
            .unwrap();
        out
    }

    fn raw(name: &str, data: &[u8]) -> Incoming {
        let mut body = vec![name.len() as u8];
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(data);
        Incoming::Command(Frame::from(body))
    }

    fn incoming(command: Command) -> Incoming {
        Incoming::Command(Frame::from(body(&command)))
    }

    fn metadata_command(name: &str, properties: &[(&str, &str)]) -> Incoming {
        let ready = body(&Command::Ready(metadata(properties)));
        raw(name, &ready[6..])
    }

    fn error(reason: &str) -> Vec<u8> {
        wire(&body(&Command::Error(
            ErrorReason::new(reason.to_owned()).unwrap(),
        )))
    }

    // ---- client ----

    fn client() -> (PlainClient, Vec<u8>) {
        let mut out = Vec::new();
        let client = PlainClient::start(
            SocketType::Dealer,
            identity("me"),
            &credentials(),
            &V3_1,
            &mut out,
        )
        .unwrap();
        (client, out)
    }

    fn welcomed_client() -> PlainClient {
        let (client, _) = client();
        match client
            .receive(raw("WELCOME", b""), &mut Vec::new())
            .unwrap()
        {
            PlainClientStep::Continue(client) => client,
            PlainClientStep::Established(_) => panic!("established before READY"),
        }
    }

    #[test]
    fn client_starts_with_hello() {
        let (_, out) = client();
        assert_eq!(out, wire(b"\x05HELLO\x05admin\x07hunter2"));
    }

    #[test]
    fn client_answers_welcome_with_initiate() {
        let (client, _) = client();
        let mut out = Vec::new();
        let step = client.receive(raw("WELCOME", b""), &mut out).unwrap();
        assert!(matches!(step, PlainClientStep::Continue(_)));
        let initiate = body(&Command::Ready(metadata(&[
            ("Socket-Type", "DEALER"),
            ("Identity", "me"),
        ])));
        let mut expected = b"\x08INITIATE".to_vec();
        expected.extend_from_slice(&initiate[6..]);
        assert_eq!(out, wire(&expected));
    }

    #[test]
    fn client_waiting_for_welcome_refuses_another_command() {
        let (client, _) = client();
        let mut out = Vec::new();
        assert_eq!(
            client.receive(raw("WELCOMX", b""), &mut out).unwrap_err(),
            HandshakeError::UnexpectedCommand(Frame::from("WELCOMX"))
        );
        assert!(out.is_empty(), "no INITIATE");
    }

    #[test]
    fn client_is_established_by_ready() {
        let client = welcomed_client();
        let ready = metadata_command("READY", &[("Socket-Type", "ROUTER"), ("Identity", "srv")]);
        let step = client.receive(ready, &mut Vec::new()).unwrap();
        let PlainClientStep::Established(established) = step else {
            panic!("expected established");
        };
        assert_eq!(established.version, Version::V3_1);
        assert_eq!(established.peer_socket_type, Some(SocketType::Router));
        assert_eq!(established.peer_identity, Frame::from("srv"));
        assert_eq!(established.authenticated, None);
    }

    #[test]
    fn client_accepts_error_while_waiting() {
        let reason = ErrorReason::new("400").unwrap();
        let (client, _) = client();
        assert_eq!(
            client
                .receive(incoming(Command::Error(reason.clone())), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::PeerError(reason.clone())
        );
        assert_eq!(
            welcomed_client()
                .receive(incoming(Command::Error(reason.clone())), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::PeerError(reason)
        );
    }

    #[test]
    fn client_understands_libzmqs_malformed_error() {
        // libzmq writes "\x05ERROR" in C, which parses as "\x5E" "RROR".
        let reason = ErrorReason::new("400").unwrap();
        let libzmq_error = Incoming::Command(Frame::from(&b"^RROR\x03400"[..]));
        let (client, _) = client();
        assert_eq!(
            client
                .receive(libzmq_error.clone(), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::PeerError(reason.clone())
        );
        assert_eq!(
            welcomed_client()
                .receive(libzmq_error, &mut Vec::new())
                .unwrap_err(),
            HandshakeError::PeerError(reason)
        );
    }

    #[test]
    fn welcome_must_be_empty() {
        let (client, _) = client();
        assert_eq!(
            client
                .receive(raw("WELCOME", b"x"), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::MalformedCommand(CommandError::Malformed { command: "WELCOME" })
        );
    }

    #[test]
    fn client_rejects_commands_out_of_order() {
        let (client, _) = client();
        let ready = metadata_command("READY", &[("Socket-Type", "ROUTER")]);
        assert_eq!(
            client.receive(ready, &mut Vec::new()).unwrap_err(),
            HandshakeError::UnexpectedCommand(Frame::from("READY"))
        );
        assert_eq!(
            welcomed_client()
                .receive(raw("WELCOME", b""), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::UnexpectedCommand(Frame::from("WELCOME"))
        );
    }

    #[test]
    fn client_never_sends_error() {
        let ready = metadata_command("READY", &[("Socket-Type", "PUB")]);
        let mut out = Vec::new();
        assert_eq!(
            welcomed_client().receive(ready, &mut out).unwrap_err(),
            HandshakeError::IncompatiblePeer {
                peer: SocketType::Pub
            }
        );
        assert!(out.is_empty());
    }

    #[test]
    fn client_rejects_messages_during_handshake() {
        let (client, _) = client();
        assert_eq!(
            client
                .receive(Incoming::Message(Message::new("x")), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::UnexpectedMessage
        );
    }

    #[test]
    fn plain_requires_zmtp_3() {
        let legacy = PeerGreeting::Zmtp2 { socket_type: 6 };
        let mut out = Vec::new();
        assert_eq!(
            PlainClient::start(
                SocketType::Dealer,
                identity("me"),
                &credentials(),
                &legacy,
                &mut out
            )
            .unwrap_err(),
            HandshakeError::LegacyPeer
        );
        assert!(out.is_empty());
        let config = server_config();
        assert_eq!(
            PlainServer::start(config, &legacy).unwrap_err(),
            HandshakeError::LegacyPeer
        );
    }

    // ---- server ----

    fn server_config() -> ServerConfig {
        ServerConfig {
            socket_type: SocketType::Router,
            identity: identity("srv"),
            domain: Frame::from("global"),
            peer_address: "127.0.0.1".to_owned(),
        }
    }

    fn hello() -> Incoming {
        raw("HELLO", b"\x05admin\x07hunter2")
    }

    fn pending() -> PendingPlainAuthentication {
        let server = PlainServer::start(server_config(), &V3_1).unwrap();
        match server.receive(hello(), &mut Vec::new()).unwrap() {
            PlainServerStep::Authenticate(pending, _) => pending,
            PlainServerStep::Established(_) => panic!("established before INITIATE"),
        }
    }

    fn accepted() -> Authenticated {
        Authenticated {
            user_id: Frame::from("user-1"),
            metadata: metadata(&[("X-Role", "admin")]),
        }
    }

    fn welcomed_server() -> PlainServer {
        let reply = ZapReply::Ok {
            user_id: accepted().user_id,
            metadata: accepted().metadata,
        };
        pending().reply(reply, &mut Vec::new()).unwrap()
    }

    #[test]
    fn server_asks_zap_about_hello() {
        let server = PlainServer::start(server_config(), &V3_1).unwrap();
        let mut out = Vec::new();
        let PlainServerStep::Authenticate(_, request) = server.receive(hello(), &mut out).unwrap()
        else {
            panic!("expected a ZAP request");
        };
        assert!(out.is_empty(), "nothing is sent until ZAP replies");
        assert_eq!(
            request,
            ZapRequest {
                domain: Frame::from("global"),
                address: "127.0.0.1".to_owned(),
                identity: Frame::from("srv"),
                credentials: ZapCredentials::Plain(credentials()),
            }
        );
    }

    #[test]
    fn accepted_client_is_welcomed_then_made_ready() {
        let mut out = Vec::new();
        let reply = ZapReply::Ok {
            user_id: accepted().user_id,
            metadata: accepted().metadata,
        };
        let server = pending().reply(reply, &mut out).unwrap();
        assert_eq!(out, wire(b"\x07WELCOME"));

        out.clear();
        let initiate = metadata_command(
            "INITIATE",
            &[("Socket-Type", "DEALER"), ("Identity", "cid")],
        );
        let PlainServerStep::Established(established) = server.receive(initiate, &mut out).unwrap()
        else {
            panic!("expected established");
        };
        assert_eq!(
            out,
            wire(&body(&Command::Ready(metadata(&[
                ("Socket-Type", "ROUTER"),
                ("Identity", "srv"),
            ]))))
        );
        assert_eq!(established.peer_socket_type, Some(SocketType::Dealer));
        assert_eq!(established.peer_identity, Frame::from("cid"));
        assert_eq!(established.authenticated, Some(accepted()));
    }

    #[test]
    fn rejected_client_gets_the_status_code() {
        let cases = [
            (ZapReply::AuthenticationFailure, "400", ZapFailure::Denied),
            (ZapReply::InternalError, "500", ZapFailure::Internal),
        ];
        for (reply, code, failure) in cases {
            let mut out = Vec::new();
            assert_eq!(
                pending().reply(reply, &mut out).unwrap_err(),
                HandshakeError::Authentication(failure)
            );
            assert_eq!(out, error(code));
        }
    }

    #[test]
    fn temporary_failure_closes_silently() {
        let mut out = Vec::new();
        assert_eq!(
            pending()
                .reply(ZapReply::TemporaryFailure, &mut out)
                .unwrap_err(),
            HandshakeError::Authentication(ZapFailure::Temporary)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn malformed_hello_is_rejected() {
        for data in [
            &b""[..],
            b"\x05adm",
            b"\x05admin",
            b"\x05admin\x07hunter",
            b"\x05admin\x07hunter2!",
        ] {
            let server = PlainServer::start(server_config(), &V3_1).unwrap();
            assert_eq!(
                server
                    .receive(raw("HELLO", data), &mut Vec::new())
                    .unwrap_err(),
                HandshakeError::MalformedCommand(CommandError::Malformed { command: "HELLO" }),
                "{data:?}"
            );
        }
    }

    #[test]
    fn server_expects_hello_first() {
        let server = PlainServer::start(server_config(), &V3_1).unwrap();
        let initiate = metadata_command("INITIATE", &[("Socket-Type", "DEALER")]);
        assert_eq!(
            server.receive(initiate, &mut Vec::new()).unwrap_err(),
            HandshakeError::UnexpectedCommand(Frame::from("INITIATE"))
        );
    }

    #[test]
    fn server_expects_initiate_after_welcome() {
        assert_eq!(
            welcomed_server()
                .receive(hello(), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::UnexpectedCommand(Frame::from("HELLO"))
        );
    }

    #[test]
    fn server_rejects_incompatible_initiate_with_error() {
        let mut out = Vec::new();
        let initiate = metadata_command("INITIATE", &[("Socket-Type", "SUB")]);
        assert_eq!(
            welcomed_server().receive(initiate, &mut out).unwrap_err(),
            HandshakeError::IncompatiblePeer {
                peer: SocketType::Sub
            }
        );
        assert_eq!(out, error("incompatible socket type"));
    }

    #[test]
    fn malformed_initiate_is_rejected() {
        assert_eq!(
            welcomed_server()
                .receive(raw("INITIATE", b"\x01"), &mut Vec::new())
                .unwrap_err(),
            HandshakeError::MalformedCommand(CommandError::Malformed {
                command: "INITIATE"
            })
        );
    }
}
