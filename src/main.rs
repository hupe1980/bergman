//! The `bergman` binary.
//!
//! A thin consumer of the library: everything here is process-level concern
//! (a runtime, an exit code, a message on stderr) and nothing here is
//! maintenance logic.

fn main() {
    // The library takes the caller's runtime; constructing one is the binary's
    // job. Maintenance is I/O-bound — metadata reads and catalog RPCs — so the
    // multi-threaded runtime earns its keep on the concurrent manifest walks.
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("bergman: could not start the async runtime: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = runtime.block_on(bergman::cli::main()) {
        eprintln!("bergman: {e}");
        std::process::exit(1);
    }
}
