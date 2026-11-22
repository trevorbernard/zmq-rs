---
status: accepted
---

# libzmq's observable behaviour wins

zmq-rs exists to be used where libzmq is, by applications and peers that already work with it, so where the two could differ the Reference's observable behaviour wins: over a ZeroMQ RFC, and over what we would have preferred. A Divergence may stand as an Exception only if it fixes a crash, memory bug or data-loss hazard in the Reference, or if it is the typed API's shape for the same Capability. An RFC decides what we send only where the Reference handles the RFC's form exactly as it handles its own.

## Considered Options

- **The RFC wins on what we send, libzmq on what we accept** (the rule until now). It let Divergences in wherever libzmq departs from an RFC, such as refusing mismatched socket types over `inproc`, which breaks applications that work on libzmq.
- **Improve on libzmq where its behaviour is awkward.** Each improvement is a silent change for a ported application, and "awkward" has no boundary.

## Consequences

- Behaviour an RFC forbids is kept where libzmq has it: `inproc` pairs any two socket types, and an XPUB reports a cancel for a topic its peer never held.
- Every Exception is listed by name in `docs/libzmq-compatibility.md`. One not listed there is a bug.
- Sending PLAIN's ERROR in the RFC's form is an Exception by the safety rule's spirit only: libzmq's form is a bug that would break every other implementation. It is the one case of its kind.
