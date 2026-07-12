# CHANGES — Seon compatibility fork

## 0.9.356-seon.1

- Merge upstream Konserve 0.9.356, including ordered `multi-assoc` batches and
  per-write metadata.
- Share metadata-size header decoding across Clojure and ClojureScript so both
  runtimes can read legacy one-byte ClojureScript headers.
- Publish the fork's version from its own classpath resources when consumed as
  a Git dependency.
