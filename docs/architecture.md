# Memento Architecture

Memento is the Sovereign Zero-Knowledge Intelligence Index for the Vilaros Ecosystem.

## The Headless IPC Shift
Historically, Memento was bundled as an Axum Web Server that hosted a frontend Askama UI on port 3306 while also serving REST APIs for Qdrant uploads. 

To maximize local inference speed and embrace the pure UNIX philosophy:
1. **Axum HTTP Removed**: All REST endpoints (`/upload`, `/query`) were eliminated.
2. **UI Removed**: The administration dashboard was delegated to `Vilaros OS`.
3. **UDS Listener Implemented**: Memento now uses `tokio` to manage a `/tmp/memento.sock`. The native Vilaros OS components handle the Heavy HTTP bridging securely via Sentinel, parsing payloads and instantly dispatching lightweight IPC buffers to this socket.

## Testing Standards
- **Unit Logic**: Functions in `src/` can be unit tested natively.
- **Integration**: The `tests/ipc_integration.rs` executes a full mock UDS payload check.
- **Examples**: Run `cargo run --example mock_ipc_memory` to verify payload bridging natively from the CLI.
