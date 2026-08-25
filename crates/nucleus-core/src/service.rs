//! Typed service identifiers for native plugins.

use std::marker::PhantomData;

/// A stable service name paired with its native Rust contract.
///
/// Nucleus still stores the textual name for diagnostics and dynamic/Wasm
/// bridges, while native callers use the type parameter to make accidental
/// cross-contract lookups impossible at the call site.
pub struct ServiceKey<T: ?Sized + 'static> {
    name: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: ?Sized + 'static> ServiceKey<T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    pub const fn name(self) -> &'static str {
        self.name
    }
}

impl<T: ?Sized + 'static> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized + 'static> Copy for ServiceKey<T> {}

impl<T: ?Sized + 'static> std::fmt::Debug for ServiceKey<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ServiceKey")
            .field(&self.name)
            .finish()
    }
}

impl<T: ?Sized + 'static> PartialEq for ServiceKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<T: ?Sized + 'static> Eq for ServiceKey<T> {}
