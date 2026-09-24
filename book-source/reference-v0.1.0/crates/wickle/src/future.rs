use std::{future::Future, pin::Pin};

/// Construct a large nested future in a separate frame before polling it.
/// Passing a factory avoids materializing the future in the caller's poll frame.
/// This does not spawn a task or change cancellation and persistence ownership.
#[inline(never)]
pub(crate) fn boxed<'a, T: 'a, F>(
    make: impl FnOnce() -> F,
) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
where
    F: Future<Output = T> + Send + 'a,
{
    Box::pin(make())
}
