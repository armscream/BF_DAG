## BF_DAG

**A Directed Acyclic Graph (DAG) for Bifrost Engine.**

**State*
- Successfully ported from older project, instrumented in current build with a handful of systems and everything appears to function well.
- Changed to an os sleep/wait that doesn't hold up any game threads.
- Fully lock-free and parallel, takes systems from the Core that can be registered using the SDK.
