use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::Pin;

use rayon::ThreadPool;
use std::task::{Context, Poll};
use std::thread;

use std::future::Future;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

/// Async handle for a blocking task running in a Rayon thread pool.
/// If the spawned task panics, `poll()` will propagate the panic.
pub struct AsyncRayonHandle<T> {
    pub(crate) rx: Receiver<thread::Result<T>>,
}

impl<T> Future for AsyncRayonHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rx = Pin::new(&mut self.rx);
        rx.poll(cx).map(|result| {
            result
                .expect("Unreachable error: Tokio channel closed")
                .unwrap_or_else(|err| resume_unwind(err))
        })
    }
}

/// Extension trait that integrates Rayon's [`ThreadPool`](rayon::ThreadPool)
/// with Tokio.
///
/// This trait is sealed and cannot be implemented by external crates.
pub trait AsyncThreadPool: private::Sealed {
    /// Asynchronous wrapper around Rayon's
    /// [`ThreadPool::spawn`](rayon::ThreadPool::spawn).
    ///
    /// Runs a function on the global Rayon thread pool with LIFO priority,
    /// produciing a future that resolves with the function's return value.
    ///
    /// # Panics
    /// If the task function panics, the panic will be propagated through the
    /// returned future. This will NOT trigger the Rayon thread pool's panic
    /// handler.
    ///
    /// If the returned handle is dropped, and the return value of `func` panics
    /// when dropped, that panic WILL trigger the thread pool's panic
    /// handler.
    fn spawn_async<F, R>(&self, func: F) -> AsyncRayonHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;

    /// Asynchronous wrapper around Rayon's
    /// [`ThreadPool::spawn_fifo`](rayon::ThreadPool::spawn_fifo).
    ///
    /// Runs a function on the global Rayon thread pool with FIFO priority,
    /// produciing a future that resolves with the function's return value.
    ///
    /// # Panics
    /// If the task function panics, the panic will be propagated through the
    /// returned future. This will NOT trigger the Rayon thread pool's panic
    /// handler.
    ///
    /// If the returned handle is dropped, and the return value of `func` panics
    /// when dropped, that panic WILL trigger the thread pool's panic
    /// handler.
    fn spawn_fifo_async<F, R>(&self, f: F) -> AsyncRayonHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}

impl AsyncThreadPool for ThreadPool {
    fn spawn_async<F, R>(&self, func: F) -> AsyncRayonHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.spawn(move || {
            let _result = tx.send(catch_unwind(AssertUnwindSafe(func)));
        });

        AsyncRayonHandle { rx }
    }

    fn spawn_fifo_async<F, R>(&self, func: F) -> AsyncRayonHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.spawn_fifo(move || {
            let _result = tx.send(catch_unwind(AssertUnwindSafe(func)));
        });

        AsyncRayonHandle { rx }
    }
}

mod private {
    use rayon::ThreadPool;

    pub trait Sealed {}

    impl Sealed for ThreadPool {}
}

/// Asynchronous wrapper around Rayon's [`spawn`](rayon::spawn).
///
/// Runs a function on the global Rayon thread pool with LIFO priority,
/// produciing a future that resolves with the function's return value.
///
/// # Panics
/// If the task function panics, the panic will be propagated through the
/// returned future. This will NOT trigger the Rayon thread pool's panic
/// handler.
///
/// If the returned handle is dropped, and the return value of `func` panics
/// when dropped, that panic WILL trigger the thread pool's panic
/// handler.
pub fn spawn<F, R>(func: F) -> AsyncRayonHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    rayon::spawn(move || {
        let _result = tx.send(catch_unwind(AssertUnwindSafe(func)));
    });

    AsyncRayonHandle { rx }
}

/// Asynchronous wrapper around Rayon's [`spawn_fifo`](rayon::spawn_fifo).
/// Runs a function on the global Rayon thread pool with FIFO priority.
pub fn spawn_fifo<F, R>(func: F) -> AsyncRayonHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    rayon::spawn_fifo(move || {
        let _result = tx.send(catch_unwind(AssertUnwindSafe(func)));
    });

    AsyncRayonHandle { rx }
}
