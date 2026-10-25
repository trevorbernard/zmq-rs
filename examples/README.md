# ZeroMQ Guide examples

The [ZeroMQ Guide](https://zguide.zeromq.org)'s examples, ported from its C
versions to zmq-rs. Each is a binary:

```sh
cargo run -p zguide --bin hwserver
cargo run -p zguide --bin hwclient
```

Shared code lives in the library: `zhelpers.h` in `lib.rs`, plus `kvsimple`
and the Majordomo APIs (`mdp`, `mdcliapi`, `mdcliapi2`, `mdwrkapi`).

## Ported

| Chapter | Examples |
|---|---|
| 1 | hwserver, hwclient, version, wuserver, wuclient, taskvent, taskwork, tasksink |
| 2 | msreader, mspoller, taskwork2, tasksink2, rrclient, rrworker, rrbroker, msgqueue, wuproxy, mtserver, mtrelay, syncpub, syncsub, psenvpub, psenvsub |
| 3 | identity, rtreq, rtdealer, lbbroker |
| 4 | lpclient, lpserver, mdbroker, mdclient, mdclient2, mdworker, mmiecho, titanic, ticlient, flserver1, flserver2, flserver3, flclient1, flclient2 |
| 5 | espresso, pathopub, pathosub, suisnail, clonesrv1, clonecli1 |
| 7 | fileio1, fileio2, fileio3, tripping, eagain, dechat |

`zmq_poll` becomes `zmq::poll`. On a single socket it becomes `recv_timeout`
or `request_timeout`, and a socket polled only sometimes, as lbbroker's
frontend while no worker is free, is `readable().when(...)`.

## Not ported

- Polling several sockets, not yet ported: lbbroker2, lbbroker3, asyncsrv, peering1, peering2,
  peering3, spqueue, spworker, ppqueue, ppworker, bstarsrv, bstarsrv2,
  bstarcli, lvcache, flcliapi, flclient3, clonesrv2 to clonesrv6,
  clonecli2 to clonecli6, clone. kvmsg is used only by these, so it waits
  too.
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
