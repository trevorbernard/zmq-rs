# Contributing

zmq-rs follows [C4](https://rfc.zeromq.org/spec/42/), the ZeroMQ
community's Collective Code Construction Contract. Its text governs; this
page says how it is applied here.

## Problems

Change starts from a problem, logged in the
[issue tracker](https://github.com/trevorbernard/zmq-rs/issues):

- Describe what you face or observe, not what you would build. The title
  states the problem: "A SUB with a subscription queued hangs the
  context". C4's `Problem:` prefix is not needed.
- A problem is documented and provable: say how to reproduce it, what
  happens, and what should, by libzmq's behaviour or a ZeroMQ RFC.
- Feature requests, ideas and suggestions are not logged. If something is
  missing, the problem is what you cannot do, or can do only by a
  workaround.

Two labels are used: `Urgent`, and `Ready` for an issue with a pull request.

## Patches

- A patch is a minimal and accurate answer to exactly one problem.
- Fork the repository and send a pull request; a pull request needs no
  separate issue.
- The commit message states the problem in one short line, then the
  solution. C4's `Problem:` and `Solution:` prefixes are not needed:

  ```
  A SUB with a subscription queued hangs the context

  SUB, XSUB and DISH do not linger unless asked, as in libzmq.
  ```

  Commits before C4 was adopted are prefixed `fix:`, `feat:` and so on.
- A patch compiles cleanly and passes `just ci`, which is what CI runs:
  rustfmt, clippy with warnings denied, the tests and the fuzz targets'
  build. A fix comes with a test that fails without it.
- A patch is licensed as the project is, under the [MPL 2.0](LICENSE), and
  stays its author's: there is no copyright assignment. Add yourself to
  [AUTHORS](AUTHORS).
- Public contracts are documented: the API in its rustdoc, the wire
  behaviour in [docs/rfc-compliance.md](docs/rfc-compliance.md). What is
  not yet stable is behind the `draft` feature, as libzmq's draft API is.

## Merging

Correct patches are merged quickly and without value judgments; one that
could be better is improved by another patch. There are no topic branches
here, only `main`.

While the project has one maintainer, that maintainer merges their own
patches, which C4 allows where no other maintainer can.
