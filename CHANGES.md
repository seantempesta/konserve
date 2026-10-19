# CHANGES — Seon compatibility fork

## 0.9.359-seon.1

- Base the maintained fork on exact upstream Konserve 0.9.359, preserving its
  synchronous, asynchronous, Node filestore, and tiered delete-store fixes.
- Share legacy one-byte ClojureScript metadata-size decoding across Clojure and
  ClojureScript readers. New writes remain upstream four-byte big-endian.
- Publish the fork version from its classpath resources when consumed as a Git
  dependency.
