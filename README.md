# 🧠 Memento Node (Sovereign Intelligence Index)

**Role:** The Persistent Long-Term Memory and Vector Store
**Stack:** Pure Rust
**Network Status:** Headless IPC Daemon (Portless)

## Characteristics
Memento acts as the long-term context bridge for all applications within Vilaros OS (Vetra, Latinos, Movilo). Like Hera, it has been stripped of web overhead to function purely as a high-speed data cruncher.

- **Headless Vector/Graph Engine**: Handles massive document embeddings, semantic search operations, and graph relationships instantly.
- **The "Shared Brain"**: Because it runs independently of the OS instances, it can simultaneously serve context to Vetra (contracts) and Movilo (catalogs).
- **Pure Speed Architecture**: Uses Unix Domain Sockets (IPC) for instantaneous responses to the OS, with zero HTTP overhead.

## Implementation Plan
1. **Headless Refactor**: Strip all REST API layers and HTTP listener logic from the `memento-node` crate.
2. **IPC Integration**: Bind the core database queries and vector-search modules directly to a `tokio` socket listener.
3. **File System Security**: Ensure Memento only accepts read/write IO commands strictly routed through the OS IPC connection, securing local disk memory state.
