use std::os::unix::net::UnixStream;
use std::io::{Write, Read};

/// Example demonstrating how to interact with the Memento headless Socket.
/// Run this with: `cargo run --example mock_ipc_memory`
fn main() {
    println!("🔌 Booting UDS Client for Memento Index...");
    
    let socket_path = "/tmp/memento.sock";
    
    match UnixStream::connect(socket_path) {
        Ok(mut stream) => {
            println!("✅ Successfully connected to Memento at {}", socket_path);
            
            // Craft a mock JSON-RPC vector search Payload
            let payload = r#"
            {
                "action": "query_memory",
                "payload": {
                    "semantic_vector": "how does vilaros work?"
                }
            }
            "#;

            println!("📤 Sending Memory Query: {}", payload);
            stream.write_all(payload.as_bytes()).expect("Failed to write to stream");

            // Read Response
            let mut response = String::new();
            if let Ok(bytes_read) = stream.read_to_string(&mut response) {
                if bytes_read > 0 {
                    println!("📥 Memento Responded: {}", response);
                } else {
                    println!("📥 Memento acknowledged but returned no nodes.");
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to connect. Is Memento running? Error: {}", e);
        }
    }
}
