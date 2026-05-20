//! # wasmtime-wasi-keyvalue-redis
//!
//! A [Redis]-backed host implementation of the [wasi:keyvalue] API for Wasmtime.
//!
//! This crate provides a persistent key-value store backend using Redis,
//! implementing the same interface contract as `wasmtime-wasi-keyvalue`.
//!
//! # Design
//!
//! All data is stored in Redis with keys encoded as `wasi_kv:{bucket}:{user_key}`.
//! Multiple buckets map to logical namespaces within a single Redis connection.
//! An empty identifier maps to the `"default"` bucket.
//!
//! The Redis URL is configured once on the [`WasiKeyValueRedisCtx`]; the `open(identifier)`
//! call just selects which bucket namespace to use.
//!
//! # Example
//!
//! ```no_run
//! use wasmtime::{Engine, Store, component::{Linker, ResourceTable}};
//! use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
//! use wasmtime_wasi_keyvalue_redis::{WasiKeyValueRedis, WasiKeyValueRedisCtx, WasiKeyValueRedisCtxBuilder};
//!
//! struct Ctx {
//!     table: ResourceTable,
//!     wasi: WasiCtx,
//!     kv: WasiKeyValueRedisCtx,
//! }
//!
//! impl WasiView for Ctx {
//!     fn ctx(&mut self) -> wasmtime_wasi::WasiCtxView<'_> {
//!         wasmtime_wasi::WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
//!     }
//! }
//!
//! # fn main() -> anyhow::Result<()> {
//! let kv_ctx = WasiKeyValueRedisCtxBuilder::new()
//!     .url("redis://127.0.0.1:6379")?
//!     .build()?;
//!
//! let mut linker = Linker::<Ctx>::new(&Engine::default());
//! wasmtime_wasi_keyvalue_redis::add_to_linker(&mut linker, |h: &mut Ctx| {
//!     WasiKeyValueRedis::new(&h.kv, &mut h.table)
//! })?;
//! # Ok(()) }
//! ```
//!
//! [Redis]: https://redis.io
//! [wasi:keyvalue]: https://github.com/WebAssembly/wasi-keyvalue

mod generated {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "wasi:keyvalue/imports",
        imports: { default: trappable },
        with: {
            "wasi:keyvalue/store.bucket": crate::Bucket,
        },
        trappable_error_type: {
            "wasi:keyvalue/store.error" => crate::Error,
        },
    });
}

use self::generated::wasi::keyvalue;
use redis::Commands;
use wasmtime::Result;
use wasmtime::component::{HasData, Resource, ResourceTable, ResourceTableError};

// ---------------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------------

/// Global prefix used for all wasi:keyvalue entries in Redis.
const KEY_PREFIX: &str = "wasi_kv";

fn encode_key(bucket: &str, key: &str) -> String {
    format!("{}:{}:{}", KEY_PREFIX, bucket, key)
}

fn bucket_pattern(bucket: &str) -> String {
    format!("{}:{}:*", KEY_PREFIX, bucket)
}

fn strip_prefix(bucket: &str, encoded: &str) -> String {
    let prefix = format!("{}:{}:", KEY_PREFIX, bucket);
    encoded.strip_prefix(&prefix).unwrap_or(encoded).to_string()
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[doc(hidden)]
pub enum Error {
    NoSuchStore,
    AccessDenied,
    Other(String),
}

impl From<ResourceTableError> for Error {
    fn from(err: ResourceTableError) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<redis::RedisError> for Error {
    fn from(e: redis::RedisError) -> Self {
        Self::Other(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Bucket
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub struct Bucket {
    name: String,
}

// ---------------------------------------------------------------------------
// Context + Builder
// ---------------------------------------------------------------------------

/// Holds the Redis client handle shared across all operations.
pub struct WasiKeyValueRedisCtx {
    client: redis::Client,
}

/// Builder for [`WasiKeyValueRedisCtx`].
#[derive(Default)]
pub struct WasiKeyValueRedisCtxBuilder {
    client: Option<redis::Client>,
}

impl WasiKeyValueRedisCtxBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the Redis URL (e.g. `"redis://127.0.0.1:6379"`).
    pub fn url(mut self, url: impl redis::IntoConnectionInfo) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        self.client = Some(client);
        Ok(self)
    }

    /// Use an already-constructed [`redis::Client`].
    pub fn client(mut self, client: redis::Client) -> Self {
        self.client = Some(client);
        self
    }

    pub fn build(self) -> anyhow::Result<WasiKeyValueRedisCtx> {
        let client = self
            .client
            .ok_or_else(|| anyhow::anyhow!("no Redis URL configured; call url() or client()"))?;
        Ok(WasiKeyValueRedisCtx { client })
    }
}

// ---------------------------------------------------------------------------
// View struct
// ---------------------------------------------------------------------------

/// Short-lived view implementing the wasi:keyvalue host traits.
pub struct WasiKeyValueRedis<'a> {
    ctx: &'a WasiKeyValueRedisCtx,
    table: &'a mut ResourceTable,
}

impl<'a> WasiKeyValueRedis<'a> {
    pub fn new(ctx: &'a WasiKeyValueRedisCtx, table: &'a mut ResourceTable) -> Self {
        Self { ctx, table }
    }

    fn conn(&self) -> Result<redis::Connection, Error> {
        self.ctx.client.get_connection().map_err(Error::from)
    }
}

// ---------------------------------------------------------------------------
// store::Host
// ---------------------------------------------------------------------------

impl keyvalue::store::Host for WasiKeyValueRedis<'_> {
    fn open(&mut self, identifier: String) -> Result<Resource<Bucket>, Error> {
        let name = if identifier.is_empty() {
            "default".to_string()
        } else {
            identifier
        };
        Ok(self.table.push(Bucket { name })?)
    }

    fn convert_error(&mut self, err: Error) -> Result<keyvalue::store::Error> {
        Ok(match err {
            Error::NoSuchStore => keyvalue::store::Error::NoSuchStore,
            Error::AccessDenied => keyvalue::store::Error::AccessDenied,
            Error::Other(s) => keyvalue::store::Error::Other(s),
        })
    }
}

// ---------------------------------------------------------------------------
// store::HostBucket
// ---------------------------------------------------------------------------

impl keyvalue::store::HostBucket for WasiKeyValueRedis<'_> {
    fn get(&mut self, bucket: Resource<Bucket>, key: String) -> Result<Option<Vec<u8>>, Error> {
        let bucket = self.table.get(&bucket)?;
        let k = encode_key(&bucket.name, &key);
        let mut conn = self.conn()?;
        let val: Option<Vec<u8>> = conn.get(&k)?;
        Ok(val)
    }

    fn set(&mut self, bucket: Resource<Bucket>, key: String, value: Vec<u8>) -> Result<(), Error> {
        let bucket = self.table.get(&bucket)?;
        let k = encode_key(&bucket.name, &key);
        let mut conn = self.conn()?;
        conn.set::<_, _, ()>(&k, value)?;
        Ok(())
    }

    fn delete(&mut self, bucket: Resource<Bucket>, key: String) -> Result<(), Error> {
        let bucket = self.table.get(&bucket)?;
        let k = encode_key(&bucket.name, &key);
        let mut conn = self.conn()?;
        conn.del::<_, ()>(&k)?;
        Ok(())
    }

    fn exists(&mut self, bucket: Resource<Bucket>, key: String) -> Result<bool, Error> {
        let bucket = self.table.get(&bucket)?;
        let k = encode_key(&bucket.name, &key);
        let mut conn = self.conn()?;
        let exists: bool = conn.exists(&k)?;
        Ok(exists)
    }

    fn list_keys(
        &mut self,
        bucket: Resource<Bucket>,
        cursor: Option<u64>,
    ) -> Result<keyvalue::store::KeyResponse, Error> {
        let bucket = self.table.get(&bucket)?;
        let pattern = bucket_pattern(&bucket.name);
        let mut conn = self.conn()?;

        // SCAN with cursor; WIT cursor=None means start (Redis cursor 0).
        let redis_cursor = cursor.unwrap_or(0);
        let (next_cursor, encoded_keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(redis_cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100u64)
            .query(&mut conn)?;

        let keys = encoded_keys
            .into_iter()
            .map(|k| strip_prefix(&bucket.name, &k))
            .collect();

        // Return next cursor only if Redis signals there are more results.
        let out_cursor = if next_cursor == 0 {
            None
        } else {
            Some(next_cursor)
        };

        Ok(keyvalue::store::KeyResponse {
            keys,
            cursor: out_cursor,
        })
    }

    fn drop(&mut self, bucket: Resource<Bucket>) -> Result<()> {
        self.table.delete(bucket)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// atomics::Host
// ---------------------------------------------------------------------------

impl keyvalue::atomics::Host for WasiKeyValueRedis<'_> {
    fn increment(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
        delta: u64,
    ) -> Result<u64, Error> {
        let bucket = self.table.get(&bucket)?;
        let k = encode_key(&bucket.name, &key);
        let mut conn = self.conn()?;
        // INCRBY returns i64; delta is u64 so cast is safe for reasonable values.
        let new_val: i64 = conn.incr(&k, delta as i64)?;
        Ok(new_val as u64)
    }
}

// ---------------------------------------------------------------------------
// batch::Host
// ---------------------------------------------------------------------------

impl keyvalue::batch::Host for WasiKeyValueRedis<'_> {
    fn get_many(
        &mut self,
        bucket: Resource<Bucket>,
        keys: Vec<String>,
    ) -> Result<Vec<Option<(String, Vec<u8>)>>, Error> {
        let bucket = self.table.get(&bucket)?;
        let mut conn = self.conn()?;

        if keys.is_empty() {
            return Ok(vec![]);
        }

        let encoded: Vec<String> = keys.iter().map(|k| encode_key(&bucket.name, k)).collect();
        let vals: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&encoded)
            .query(&mut conn)?;

        let results = keys
            .into_iter()
            .zip(vals)
            .map(|(k, v)| v.map(|bytes| (k, bytes)))
            .collect();

        Ok(results)
    }

    fn set_many(
        &mut self,
        bucket: Resource<Bucket>,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> Result<(), Error> {
        let bucket = self.table.get(&bucket)?;
        let mut conn = self.conn()?;

        if key_values.is_empty() {
            return Ok(());
        }

        let pairs: Vec<(String, Vec<u8>)> = key_values
            .into_iter()
            .map(|(k, v)| (encode_key(&bucket.name, &k), v))
            .collect();

        redis::cmd("MSET").arg(pairs).query::<()>(&mut conn)?;
        Ok(())
    }

    fn delete_many(&mut self, bucket: Resource<Bucket>, keys: Vec<String>) -> Result<(), Error> {
        let bucket = self.table.get(&bucket)?;
        let mut conn = self.conn()?;

        if keys.is_empty() {
            return Ok(());
        }

        let encoded: Vec<String> = keys.iter().map(|k| encode_key(&bucket.name, k)).collect();
        conn.del::<_, ()>(&encoded)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Linker registration
// ---------------------------------------------------------------------------

/// Register all `wasi:keyvalue` interfaces into a [`wasmtime::component::Linker`].
pub fn add_to_linker<T: Send + 'static>(
    l: &mut wasmtime::component::Linker<T>,
    f: fn(&mut T) -> WasiKeyValueRedis<'_>,
) -> Result<()> {
    keyvalue::store::add_to_linker::<_, HasWasiKeyValueRedis>(l, f)?;
    keyvalue::atomics::add_to_linker::<_, HasWasiKeyValueRedis>(l, f)?;
    keyvalue::batch::add_to_linker::<_, HasWasiKeyValueRedis>(l, f)?;
    Ok(())
}

struct HasWasiKeyValueRedis;

impl HasData for HasWasiKeyValueRedis {
    type Data<'a> = WasiKeyValueRedis<'a>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::wasi::keyvalue::{
        atomics::Host as AtomicsHost,
        batch::Host as BatchHost,
        store::{Host, HostBucket},
    };
    use wasmtime::component::ResourceTable;

    // Tests require a running Redis at 127.0.0.1:6379.
    // Run with: docker run --rm -p 6379:6379 redis:7-alpine

    fn make_ctx() -> WasiKeyValueRedisCtx {
        WasiKeyValueRedisCtxBuilder::new()
            .url("redis://127.0.0.1:6379")
            .unwrap()
            .build()
            .unwrap()
    }

    macro_rules! bucket {
        ($kv:expr, $name:expr) => {
            Host::open(&mut $kv, $name.to_string()).unwrap()
        };
    }

    /// Flush all wasi_kv keys for a bucket to ensure test isolation.
    fn flush_bucket(ctx: &WasiKeyValueRedisCtx, bucket: &str) {
        let mut conn = ctx.client.get_connection().unwrap();
        let pattern = bucket_pattern(bucket);
        let keys: Vec<String> = redis::cmd("KEYS").arg(&pattern).query(&mut conn).unwrap();
        if !keys.is_empty() {
            let _: () = redis::cmd("DEL").arg(&keys).query(&mut conn).unwrap();
        }
    }

    #[test]
    fn set_get_delete() {
        let ctx = make_ctx();
        flush_bucket(&ctx, "default");
        let mut table = ResourceTable::new();
        let mut kv = WasiKeyValueRedis::new(&ctx, &mut table);

        let b = bucket!(kv, "");
        HostBucket::set(&mut kv, b, "hello".to_string(), b"world".to_vec()).unwrap();
        let b = bucket!(kv, "");
        assert_eq!(
            HostBucket::get(&mut kv, b, "hello".to_string()).unwrap(),
            Some(b"world".to_vec())
        );
        let b = bucket!(kv, "");
        HostBucket::delete(&mut kv, b, "hello".to_string()).unwrap();
        let b = bucket!(kv, "");
        assert_eq!(HostBucket::get(&mut kv, b, "hello".to_string()).unwrap(), None);
    }

    #[test]
    fn exists() {
        let ctx = make_ctx();
        flush_bucket(&ctx, "exists_test");
        let mut table = ResourceTable::new();
        let mut kv = WasiKeyValueRedis::new(&ctx, &mut table);

        let b = bucket!(kv, "exists_test");
        assert!(!HostBucket::exists(&mut kv, b, "k".to_string()).unwrap());
        let b = bucket!(kv, "exists_test");
        HostBucket::set(&mut kv, b, "k".to_string(), b"v".to_vec()).unwrap();
        let b = bucket!(kv, "exists_test");
        assert!(HostBucket::exists(&mut kv, b, "k".to_string()).unwrap());
    }

    #[test]
    fn list_keys() {
        let ctx = make_ctx();
        flush_bucket(&ctx, "listing");
        let mut table = ResourceTable::new();
        let mut kv = WasiKeyValueRedis::new(&ctx, &mut table);

        let b = bucket!(kv, "listing");
        HostBucket::set(&mut kv, b, "a".to_string(), b"1".to_vec()).unwrap();
        let b = bucket!(kv, "listing");
        HostBucket::set(&mut kv, b, "b".to_string(), b"2".to_vec()).unwrap();
        let b = bucket!(kv, "listing");
        HostBucket::set(&mut kv, b, "c".to_string(), b"3".to_vec()).unwrap();

        let b = bucket!(kv, "listing");
        let resp = HostBucket::list_keys(&mut kv, b, None).unwrap();
        let mut keys = resp.keys;
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn buckets_are_isolated() {
        let ctx = make_ctx();
        flush_bucket(&ctx, "alpha");
        flush_bucket(&ctx, "beta");
        let mut table = ResourceTable::new();
        let mut kv = WasiKeyValueRedis::new(&ctx, &mut table);

        let b = bucket!(kv, "alpha");
        HostBucket::set(&mut kv, b, "key".to_string(), b"from-alpha".to_vec()).unwrap();

        let b = bucket!(kv, "beta");
        assert_eq!(HostBucket::get(&mut kv, b, "key".to_string()).unwrap(), None);
        let b = bucket!(kv, "alpha");
        assert_eq!(
            HostBucket::get(&mut kv, b, "key".to_string()).unwrap(),
            Some(b"from-alpha".to_vec())
        );
    }

    #[test]
    fn increment() {
        let ctx = make_ctx();
        flush_bucket(&ctx, "counters");
        let mut table = ResourceTable::new();
        let mut kv = WasiKeyValueRedis::new(&ctx, &mut table);

        let b = bucket!(kv, "counters");
        assert_eq!(AtomicsHost::increment(&mut kv, b, "hits".to_string(), 1).unwrap(), 1);
        let b = bucket!(kv, "counters");
        assert_eq!(AtomicsHost::increment(&mut kv, b, "hits".to_string(), 4).unwrap(), 5);
        let b = bucket!(kv, "counters");
        assert_eq!(
            AtomicsHost::increment(&mut kv, b, "hits".to_string(), 10).unwrap(),
            15
        );
    }

    #[test]
    fn batch_ops() {
        let ctx = make_ctx();
        flush_bucket(&ctx, "batch");
        let mut table = ResourceTable::new();
        let mut kv = WasiKeyValueRedis::new(&ctx, &mut table);

        let b = bucket!(kv, "batch");
        BatchHost::set_many(
            &mut kv,
            b,
            vec![
                ("x".to_string(), b"1".to_vec()),
                ("y".to_string(), b"2".to_vec()),
                ("z".to_string(), b"3".to_vec()),
            ],
        )
        .unwrap();

        let b = bucket!(kv, "batch");
        let results =
            BatchHost::get_many(&mut kv, b, vec!["x".to_string(), "missing".to_string(), "z".to_string()])
                .unwrap();
        assert_eq!(results[0], Some(("x".to_string(), b"1".to_vec())));
        assert_eq!(results[1], None);
        assert_eq!(results[2], Some(("z".to_string(), b"3".to_vec())));

        let b = bucket!(kv, "batch");
        BatchHost::delete_many(&mut kv, b, vec!["x".to_string(), "y".to_string()]).unwrap();
        let b = bucket!(kv, "batch");
        let resp = HostBucket::list_keys(&mut kv, b, None).unwrap();
        assert_eq!(resp.keys, vec!["z"]);
    }
}
