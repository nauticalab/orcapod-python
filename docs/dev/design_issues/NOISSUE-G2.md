## `src/orcapod/hashing/semantic_hashing/`

### G2 — Pod Group abstraction for other composite pod patterns
**Status:** open
**Severity:** low

Beyond AddResult (which warrants its own pod type — see G1), other composite patterns may
benefit from a general **PodGroup** abstraction that encapsulates a reusable sub-graph behind
a single pod-like interface.

A PodGroup:
- Accepts one or more input streams and produces one output stream (same interface as a pod)
- Internally contains a fixed sub-graph of pods, operators, and channels
- Hides the internal wiring from the user
- Participates in pipeline hashing as a single composite element

Potential patterns (to be designed as needs arise):
- **ConditionalPod** — route data to different pods based on a predicate, merge results
- **FanOutFanIn** — broadcast to N pods, collect and merge/concat results
- **FallbackPod** — try primary pod, fall back to secondary on error/None result
