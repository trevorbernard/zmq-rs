# ZeroMQ Guide examples

The [ZeroMQ Guide](https://zguide.zeromq.org)'s examples, ported from its C
versions to zmq-rs. Each is a binary:

```sh
cargo run -p zguide --bin hwserver
cargo run -p zguide --bin hwclient
```

Shared code lives in the library: `zhelpers.h` in `lib.rs`, plus `kvsimple`,
`kvmsg`, the Majordomo APIs (`mdp`, `mdcliapi`, `mdcliapi2`, `mdwrkapi`), the
Binary Star reactor (`bstar`), and the Freelance and Clone client APIs
(`flcliapi`, `clone`).

## Ported

| Chapter | Examples |
|---|---|
| 1 | hwserver, hwclient, version, wuserver, wuclient, taskvent, taskwork, tasksink |
| 2 | msreader, mspoller, taskwork2, tasksink2, rrclient, rrworker, rrbroker, msgqueue, wuproxy, mtserver, mtrelay, syncpub, syncsub, psenvpub, psenvsub |
| 3 | identity, rtreq, rtdealer, lbbroker, asyncsrv, peering1, peering2, peering3 |
| 4 | lpclient, lpserver, mdbroker, mdclient, mdclient2, mdworker, mmiecho, titanic, ticlient, flserver1, flserver2, flserver3, flclient1, flclient2, spqueue, spworker, ppqueue, ppworker, bstarsrv, bstarsrv2, bstarcli, flclient3 |
| 5 | espresso, pathopub, pathosub, lvcache, suisnail, clonesrv1 to clonesrv6, clonecli1 to clonecli6 |
| 7 | fileio1, fileio2, fileio3, tripping, eagain, dechat |

`zmq_poll` becomes `zmq::poll`. On a single socket it becomes `recv_timeout`
or `request_timeout`, and a socket polled only sometimes, as lbbroker's
frontend while no worker is free, is `readable().when(...)`.

## Not ported

- lbbroker2 and lbbroker3: lbbroker rewritten with CZMQ's message API and
  its zloop reactor. Ported, they would be lbbroker again.
- Signal handling: interrupt. Examples that loop forever are stopped with
  Ctrl-C, so the ones that print totals on interrupt (clonesrv1, clonecli1)
  never print them.
- UDP discovery: udpping1, udpping2, udpping3, interface.
- Hello World with other socket types, which aren't in the book: hwclient2
  to hwclient4, hwserver2 to hwserver4.
- mktestdata: fileio reads `testdata` from the current directory.

## Differences from the C

- `version` prints the crate's version; there is no libzmq to ask.
- wuproxy reaches the local weather server rather than the guide's
  192.168.55.210 and 10.1.1.0.
- dechat binds an address (or `*`), not an interface name.
- mdcliapi uses DEALER rather than REQ: REQ's `request_timeout` turns on
  request correlation, whose extra envelope frame an MDP broker doesn't
  expect.
- mdbroker's ROUTER uses `try_send`, dropping when a peer's queue is full,
  as libzmq's ROUTER does; `send` would block.
- titanic stores frames with a length prefix, not CZMQ's `zmsg_save` format.
- lbbroker waits for its clients to print before exiting, where the C's
  `zmq_ctx_destroy` waits for every socket to close.
- zloop reactors become one poll loop with timers, in clonesrv5,
  clonesrv6 and the clone client API. bstar has no reactor to register
  handlers with: the application polls `BinaryStar::readable` beside its
  own sockets and gets `Event::Request`, `Event::Active` and
  `Event::Passive` back from `handle`, and `start` runs that loop for a
  server with only a voter, as `bstar_start` does. The voter's endpoint is
  given to `BinaryStar::new`; it is always a ROUTER.
- spqueue, ppqueue, bstarsrv, flcliapi and the peering brokers send on
  ROUTERs with `try_send`, dropping when a peer is gone or full, as
  libzmq's ROUTER does.
- ppworker treats any message of more than one frame as a request, where
  the C wants exactly three: lpclient's `request_timeout` adds a request-id
  frame, which the C worker would reject.
- asyncsrv prints each reply as `<identity> <body>`, not in CZMQ's
  `zframe_print` format, and ends by returning from `main` after five
  seconds, without telling its tasks to stop.
- clonecli4 and clonecli5 subscribe only to their subtree; the C also
  subscribes to `""`, which makes its subtree filter do nothing.
- kvmsg's UUID is 16 random octets, not a libuuid UUID.
- The clone client API is `zguide::clone::CloneClient`, as a type named
  `Clone` would shadow the standard trait.
