//! Host-side assembly and scoped lifetimes for approved Wickle adapters.
//!
//! Definitions and factories are registered by the embedding application.
//! Profile data never specifies executable paths or grants connection access.

mod lifecycle;
mod registry;
mod runtime;

pub use registry::{
    AdapterRegistration, AdapterRegistry, CatalogHookRegistration, CatalogSourceRegistration,
    CatalogToolRegistration, ConnectionRegistration,
};
pub use runtime::{AdapterRuntime, AdapterRuntimeSettings};
