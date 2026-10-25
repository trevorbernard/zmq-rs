//! libzmq's `tests/test_proxy_single_socket.cpp`: not ported. It proxies
//! a REP socket to itself. zmq-rs's proxy cannot proxy REP, whose lockstep
//! does not fit a pass-through (see `zmq::ProxyEnd`), and a socket cannot
//! be both ends, as the proxy borrows each end mutably.
