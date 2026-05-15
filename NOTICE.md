# NOTICE — konserve sync-only fork

This branch (`sync-only`) of [replikativ/konserve](https://github.com/replikativ/konserve) is a fork maintained by Sean Tempesta (<sean.tempesta@gmail.com>).

## Upstream

- **Upstream project:** replikativ/konserve
- **Forked from tag:** `0.9.340`
- **License:** Eclipse Public License v1.0 (preserved unchanged in `LICENSE`)

The original copyright and EPL-1.0 license terms apply to all files inherited from upstream. Modifications introduced on this branch are also released under EPL-1.0.

## Purpose

Provide a sync-only konserve build suitable for GraalVM Native Image WebAssembly compilation (`--tool:svm-wasm`, GraalVM 25). Upstream konserve already exposes `:sync? true` in its public API, but the `async+sync` macro indirection keeps `core.async` reachable in the analyzer's closed-world view of the resulting WASM module, even on code paths that never execute async at runtime.

This fork strips the async branch entirely. Read-only datahike (`seantempesta/datahike` branch `read-only-wasm`) calls into this fork's sync entry points directly. Result: no `core.async` on the classpath, no thread-in-image-heap errors, no `@Delete`-method validation failures.

The intent is **not** to fragment konserve. If replikativ wants the sync-only build path upstreamed — perhaps via a Maven classifier or a separate library — these changes are offered freely under EPL-1.0.

## Scope of modifications

See `CHANGES.md` for the per-commit log. At a high level:

- **Removed:** the async branch of `async+sync` macro indirection across `konserve.core` and storage namespaces. Sync-only entry points retained.
- **Dropped from `deps.edn`:** `clojure.core.async`, `superv.async`.
- **Preserved:** the storage protocol shape (`PEDNKeyValueStore`, `PBinaryKeyValueStore`, etc.) so existing backends can be ported to sync-only by removing their async branches.

## EPL-1.0 compliance

- `LICENSE` (EPL-1.0) preserved verbatim from upstream.
- Modifications are tracked per commit on this branch (`git log 0.9.340..sync-only`).
- No upstream copyright headers in retained files have been altered.
- This fork is published as source. No binary redistribution.

Per EPL-1.0 §3.4, recipients of this fork are entitled to the source of the modifications, available via the GitHub repository hosting this branch.
