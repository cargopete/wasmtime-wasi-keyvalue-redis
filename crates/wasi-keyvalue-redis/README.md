# wasmtime-wasi-keyvalue-redis

A [Redis]-backed host implementation of the [wasi:keyvalue] API for Wasmtime.

Drop-in alternative to the official `wasmtime-wasi-keyvalue` crate (in-memory only) for use cases
that need persistent storage backed by Redis. Redis is explicitly listed in the
[wasi:keyvalue Phase 2 portability criteria].

## Design

All keys are stored in Redis with the encoding `wasi_kv:{bucket}:{user_key}`. Multiple buckets
map to logical namespaces within a single Redis connection. An empty identifier maps to the
`"default"` bucket.

```
open("logs")   → Redis keys "wasi_kv:logs:{key}"
open("cache")  → Redis keys "wasi_kv:cache:{key}"
open("")       → normalised to "wasi_kv:default:{key}"
```

Batch operations use `MGET`/`MSET`. Atomic increment uses `INCRBY`. Key listing uses `SCAN`
with cursor-based pagination matching the WIT `cursor` field.

## Usage

```rust
use wasmtime::{Engine, Store, component::{Linker, ResourceTable}};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_keyvalue_redis::{WasiKeyValueRedis, WasiKeyValueRedisCtx, WasiKeyValueRedisCtxBuilder};

struct Ctx {
    table: ResourceTable,
    wasi: WasiCtx,
    kv: WasiKeyValueRedisCtx,
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

let kv_ctx = WasiKeyValueRedisCtxBuilder::new()
    .url("redis://127.0.0.1:6379")?
    .build()?;

let mut linker = Linker::<Ctx>::new(&Engine::default());
wasmtime_wasi_keyvalue_redis::add_to_linker(&mut linker, |h: &mut Ctx| {
    WasiKeyValueRedis::new(&h.kv, &mut h.table)
})?;
```

## WIT interface coverage

| Interface | Status |
|---|---|
| `wasi:keyvalue/store` — `open`, `get`, `set`, `delete`, `exists`, `list-keys` | done |
| `wasi:keyvalue/atomics` — `increment` | done |
| `wasi:keyvalue/batch` — `get-many`, `set-many`, `delete-many` | done |

## Running tests

Tests require a running Redis instance at `127.0.0.1:6379`:

```
docker run --rm -p 6379:6379 redis:7-alpine
cargo test
```

## Related

- [wasmtime-wasi-keyvalue-redb](https://github.com/cargopete/wasmtime-wasi-keyvalue-redb) — embedded redb backend (no external services)
- [WebAssembly/wasi-keyvalue](https://github.com/WebAssembly/wasi-keyvalue) — the proposal
- [bytecodealliance/wasmtime](https://github.com/bytecodealliance/wasmtime) — runtime
- [Redis](https://redis.io) — the database

[Redis]: https://redis.io
[wasi:keyvalue]: https://github.com/WebAssembly/wasi-keyvalue
[wasi:keyvalue Phase 2 portability criteria]: https://github.com/WebAssembly/wasi-keyvalue
