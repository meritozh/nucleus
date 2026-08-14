//! Typed events with cordis dispatch semantics.
//!
//! | Mode       | Awaited | Order              | Return          |
//! |------------|---------|--------------------|-----------------|
//! | `emit`     | no      | registration order | ignored         |
//! | `parallel` | yes     | concurrent         | ignored         |
//! | `serial`   | yes     | registration order | first truthy wins, short-circuits |
//! | `waterfall`| yes     | around-middleware  | transformed / vetoed |
//!
//! `bail` (cordis' synchronous serial) is intentionally not provided: every
//! listener here is async, so `serial` covers it. A serial listener answers
//! `None` (JSON null) to pass and `Some(value)` to short-circuit.

use crate::context::Context;
use crate::error::{Error, Result};
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    Emit,
    Parallel,
    Serial,
    Waterfall,
}

/// A broadcast event. `NAME` replaces cordis' TS declaration merging;
/// `MODE` documents the dispatch contract and is asserted in debug builds.
pub trait Event: Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
    const MODE: DispatchMode = DispatchMode::Emit;
}

/// An around-middleware (waterfall) event.
pub trait WaterfallEvent: Send + Sync + 'static {
    type Args: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;
    const NAME: &'static str;
}

pub(crate) type PlainBox = Arc<
    dyn Fn(Context, serde_json::Value) -> BoxFuture<'static, serde_json::Value> + Send + Sync,
>;
pub(crate) type WaterfallBox = Arc<
    dyn Fn(
            Context,
            serde_json::Value,
            Next,
        ) -> BoxFuture<'static, Result<serde_json::Value, Error>>
        + Send
        + Sync,
>;
pub(crate) type DefaultBox = Arc<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, Error>> + Send + Sync,
>;

/// Continuation handed to a waterfall listener. Calling `run` delegates to
/// the rest of the chain (and finally the dispatcher's default); returning
/// without calling `run` is a veto. Observing listeners must call `run`.
pub struct Next {
    pub(crate) ctx: Context,
    pub(crate) rest: Vec<WaterfallBox>,
    pub(crate) default: DefaultBox,
    pub(crate) input: serde_json::Value,
}

impl Next {
    pub async fn run(self) -> Result<serde_json::Value> {
        let Next {
            ctx,
            rest,
            default,
            input,
        } = self;
        match rest.split_first() {
            Some((head, tail)) => {
                let tail_next = Next {
                    rest: tail.to_vec(),
                    default: default.clone(),
                    input: input.clone(),
                    ctx: ctx.clone(),
                };
                head(ctx, input, tail_next).await
            }
            None => default(input).await,
        }
    }
}

/// Typed view over [`Next`] for one waterfall event.
pub struct TypedNext<A, O> {
    inner: Next,
    _pd: PhantomData<fn() -> (A, O)>,
}

impl<A, O> TypedNext<A, O>
where
    A: Serialize + DeserializeOwned + Send + Sync + 'static,
    O: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(inner: Next) -> Self {
        Self {
            inner,
            _pd: PhantomData,
        }
    }

    pub async fn run(self) -> Result<O> {
        let out = self.inner.run().await?;
        Ok(serde_json::from_value(out)?)
    }
}
