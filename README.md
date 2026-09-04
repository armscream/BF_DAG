# BF_DAG v0.0.1

**A Directed Acyclic Graph (DAG) for Bifrost Engine.**

## Installation

- To install BF_DAG, simply clone the repository into your project's `modules` directory.
- In your project's directory, run ./rune manifest to generate a manifest file for this module, if you don't have one already.
- Add the following to your project's manifest file <project.toml> in the modules section:
[[modules]]
name = "BF_DAG"
enabled = true
required = true
version = { major = 0, minor = 0, patch = 1 }
- Run ./rune run <DEBUG/RELEASE/EDITOR>

## Current State

- Successfully ported from older project, instrumented in current build with a handful of systems and everything appears to function well.
- Changed to an os sleep/wait that doesn't hold up any game threads.
- Fully lock-free and parallel, takes systems from the Core that can be registered using the SDK.

## License

- Just as all Core Modules, this module inherets Bifrost Engine's licensing agreement.