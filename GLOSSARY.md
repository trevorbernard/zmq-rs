# zmq-rs

A Rust implementation of ZeroMQ whose aim is to be as compatible with libzmq as possible. This language describes how the two are compared.

## Language

### Compatibility

**Reference**:
The libzmq release that zmq-rs is compared against: v4.3.5.
_Avoid_: upstream, libzmq master

**Capability**:
Something an application or a peer can do or observe with the Reference, whatever the shape of the call that reaches it.
_Avoid_: feature, API

**Gap**:
A Capability the Reference has and zmq-rs lacks.
_Avoid_: missing feature, to-do

**Divergence**:
A Capability both have whose observable behaviour differs, to a peer on the wire or to an application that worked on the Reference.
_Avoid_: deviation, difference, incompatibility

**Exception**:
A Divergence kept on purpose: it fixes a crash, memory bug or data-loss hazard in the Reference, or it is the typed API's shape for the same Capability.
_Avoid_: deliberate difference, improvement

**Exclusion**:
A Gap declared not planned unless someone asks for it.
_Avoid_: out of scope, later

**Verified**:
Shown to match the Reference by a test: against the Reference itself for what a peer sees, or by one of the Reference's own tests ported with its assertion unchanged for what only the application sees.
_Avoid_: tested, covered

**Unverified**:
Believed to match the Reference, with no test that makes it Verified.
_Avoid_: untested
