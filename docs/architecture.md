# Architecture

The repository is organized as a Cargo workspace with separate CLI, core, and
cloud-client crates. The CLI depends on the core and cloud crates, while the
core crate remains independent of the control-plane client.
