# NOTICE — Seon compatibility fork

This branch is maintained from
[replikativ/konserve](https://github.com/replikativ/konserve) under the
project's existing Eclipse Public License 1.0.

It tracks upstream Konserve 0.9.356 and retains one compatibility extension:
both Clojure and ClojureScript readers recognize the legacy one-byte metadata
size header written by older ClojureScript releases. New writes use the
upstream four-byte big-endian header format.

The fork identifies itself as `0.9.356-seon.1` when consumed directly as a
Git dependency. See `CHANGES.md` for the maintained delta.
