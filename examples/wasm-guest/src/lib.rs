//! Example / test-fixture cordis Wasm guest.
//!
//! Contract: `wit/deps/host.wit` + `wit/plugin.wit` (v0.1). Note both
//! exports are `[async]` in the world, so the generated `Guest` trait
//! requires futures — implemented here with `async fn`.
//!
//! Build (see the repo README "Guest build" for the full path):
//!
//! ```text
//! rustup target add wasm32-unknown-unknown
//! cargo build --release --target wasm32-unknown-unknown
//! wasm-tools component new target/wasm32-unknown-unknown/release/cordis_guest_example.wasm \
//!     -o cordis-wasm/tests/fixtures/hello.wasm
//! ```

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
    generate_all,
});

use cordis::host::host::{call_service, emit, register_effect};

struct MyGuest;

impl Guest for MyGuest {
    /// cordis `apply`: register one effect, call a host service, log both.
    async fn init(_config: String) -> Result<(), ()> {
        // sync WIT imports are plain calls
        let effect = register_effect("example-resource");
        emit(
            "guest/log",
            &format!("{{\"level\":\"info\",\"msg\":\"init effect={effect}\"}}"),
        );

        // async WIT import: exercises the host's Accessor/service bridge
        match call_service(String::from("calc"), String::from("add"), String::from("[1, 2]")).await {
            Ok(v) => emit(
                "guest/log",
                &format!("{{\"level\":\"info\",\"msg\":\"calc={v}\"}}"),
            ),
            Err(e) => emit(
                "guest/log",
                &format!("{{\"level\":\"error\",\"msg\":\"calc failed: {e}\"}}"),
            ),
        }
        Ok(())
    }

    /// Disposer for one effect id (host calls this at unload, LIFO).
    async fn dispose_effect(handle: u64) -> Result<(), ()> {
        emit(
            "guest/log",
            &format!("{{\"level\":\"info\",\"msg\":\"dispose effect={handle}\"}}"),
        );
        Ok(())
    }
}

export!(MyGuest);
