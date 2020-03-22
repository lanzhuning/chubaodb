// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

pub mod coding;
pub mod config;
pub mod entity;
pub mod error;
pub mod http_client;
pub mod time;

#[macro_export]
macro_rules! sleep {
    ($x:expr) => {{
        std::thread::sleep(std::time::Duration::from_millis($x))
    }};
}

#[macro_export]
macro_rules! defer {
    {$($body:stmt;)+} => {
        let _guard = {
            pub struct Guard<F: FnOnce()>(Option<F>);
            impl<F: FnOnce()> Drop for Guard<F> {
                fn drop(&mut self) {
                    if let Some(f) = (self.0).take() {
                        f()
                    }
                }
            }
            Guard(Some(||{
                $($body)+
            }))
        };
    };
}

//println stack trace
pub fn stack_trace() {
    use log::Level::Debug;
    use log::{debug, log_enabled};

    if !log_enabled!(Debug) {
        return;
    }

    backtrace::trace(|frame| {
        // Resolve this instruction pointer to a symbol name
        backtrace::resolve_frame(frame, |symbol| {
            let line = format!(
                "ERROR========= name:{:?} line:{:?}",
                symbol.name(),
                symbol.lineno().unwrap_or(0)
            );

            if line.contains("chubaodb::")
                && !line.contains("chubaodb::util::stack_trace")
                && !line.contains("chubaodb::util::error::")
            {
                debug!("{}", line);
            }
        });

        true // keep going to the next frame
    });
}
