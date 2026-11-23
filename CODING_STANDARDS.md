# Coding standards

Read when reviewing a diff. What rustfmt, clippy and `just ci` enforce is
not repeated here.

## Tests

For each test a diff adds or changes, name a bug in the code under test
that would fail it. A test with no such bug is removed, not kept for
coverage. These shapes have none:

- **The type system or a derive guarantees it.** `assert_ne!(id.get(), 0)`
  on a `NonZeroU32`; `x.clone() == x` on a derived `Clone`; the value of a
  derived `Default`.
- **The expected value is the implementation.** A constant compared with
  the expression that defines it; `From::from(x)` compared with the
  constructor it forwards to; a `Debug` or `Display` of one fixed string
  compared with that string.
- **It reads back what it wrote.** A getter returning the field the test
  just set; a value built in the test and taken apart again.
- **The test double does the work.** A helper in `test_support.rs` or the
  test file performs what the test's name credits to the code under test.
- **The code under test does not run.** A default configuration that
  returns early, with the assertion on what the OS or `std` gives.
- **Nothing arrives where nothing was sent.** "Not received" needs a
  positive control: the same message getting through with the condition
  reversed, or a later message arriving on the same socket.
- **True by construction.** `collected.len() == n` after collecting `n`
  items; `x == a || x == b` over everything `x` could be.

These restate a literal and are kept: golden bytes from a ZeroMQ RFC, a
default that must match libzmq's, and the messages of an error with more
than one variant, where arms can be swapped.

A test whose assertions another makes on the same path is a duplicate:
the one that asserts more is kept. The same case through another entry
point, such as `bind_target` in a unit test and `Socket::bind` in a socket
test, is not a duplicate.

### Ported libzmq tests

`crates/zmq/tests/libzmq/README.md` says when a libzmq case is not ported.

### Mutation testing

A mutant that only a test of the shapes above could catch is not killed
with one. It is excluded in `.cargo/mutants.toml` with its reason, or, if
nothing calls the code, the code is removed.
