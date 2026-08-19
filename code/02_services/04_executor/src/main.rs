//! Nautilus execution service entry point.
use nautilus_execution_service as lib;

fn main() {
    println!("nautilus-execution-service bootstrap ok");
    let _ = lib::bridge::PROTOCOL_VERSION;
}
