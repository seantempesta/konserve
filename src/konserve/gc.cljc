(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.impl.defaults :as defaults]
            #?(:clj [konserve.utils :as utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :as utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn- delete-batch!
  "Delete one batch of keys, concurrently where the platform allows it.

   `:ignore-existence?` because GC never uses `dissoc`'s `existed?` return, and
   it lets miss-safe backings skip the per-key HEAD probe — a delete here is
   idempotent by construction.

   The concurrency is worth keeping rather than simplifying away: the stores
   that most need it are exactly the ones that take this path. A backing with
   multi-key support batches into one round trip above; everything else — the
   FILESTORE included, which is what scriptum and proximum run on — deletes key
   by key, and doing that serially against a remote store costs one round trip
   per key.

   Async mode merges the per-key channels. Sync mode has no channels to merge,
   so the JVM uses `pmap` for the same effect; ClojureScript runs serially,
   which costs it nothing it could have had — sync mode there is single-threaded
   by definition.

   `pmap`'s thread cost here is bounded at `cpus + 2`, measured at 11 on an
   8-core machine, drawn from the cached pool `future` uses. THAT BOUND DEPENDS
   ON `batch` NOT BEING CHUNKED: `pmap` realizes a chunk at a time, so a chunked
   seq jumps to 32 + lookahead (35, measured, for a `range`). `sweep!` passes
   `partition-all` output, which is not chunked — keep it that way, or bound the
   parallelism explicitly.

   A bounded virtual-thread executor would be the better instrument and a
   simpler one, but Loom is new enough that konserve should not require it of
   its users yet."
  [store batch sync?]
  (if sync?
    #?(:clj (dorun (pmap (fn [{:keys [key]}]
                           (k/dissoc store key {:sync? true :ignore-existence? true}))
                         batch))
       :cljs (doseq [{:keys [key]} batch]
               (k/dissoc store key {:sync? true :ignore-existence? true})))
    (let [pending (mapv (fn [{:keys [key]}]
                          (k/dissoc store key {:ignore-existence? true}))
                        batch)]
      (async/into [] (async/merge pending)))))

(defn sweep!
  "Delete every key that is neither in `whitelist` nor written at/after `cutoff`.

   PRECONDITION, and the whole reason `konserve.gc-guard` exists: on a store
   with guarded writers, `cutoff` must come from `konserve.gc-guard/cutoff`,
   and it must be read BEFORE the caller computes `whitelist`:

       (let [cutoff    (guard/cutoff store-id (utils/now))   ; 1. guard
             whitelist (mark-reachable! store)]              ; 2. mark
         (sweep! store whitelist cutoff))                    ; 3. sweep

   That order is not a style preference. A values-then-pointer sequence that is
   open at step 1 pins the cutoff at or before its own writes, so they survive
   however step 2 turns out. Mark first and the same sequence can land its
   pointer in between: step 2 walked roots that did not name its values yet, and
   by step 3 the guard is closed again, so nothing spares them — they are older
   than the cutoff and unreachable from the whitelist, and the sweep deletes
   objects a live pointer names.

   `sweep!` DELIBERATELY DOES NOT CONSULT THE GUARD ITSELF. It cannot: it
   receives `whitelist` already computed, so any reading it does is necessarily
   after the caller's mark — the broken order above, wearing the appearance of
   safety. Only the caller is in a position to interleave the two correctly.

   Passing a raw `(utils/now)` is correct only on a store no writer guards.

   With `:sync? true` the sweep runs synchronously and returns its result.

   With `:konserve.gc/sync-io? true`, the key listing and multi-key deletions use
   the store's synchronous operations (still returning a channel): a sweep run by
   one blocking owner wants that, where the async path costs a channel hand-off
   per key read (Jolt: 990 ms against 75 ms for 6,200 keys on LMDB).

   With `:konserve.gc/dry-run? true`, returns the selected logical keys without
   deleting them or invoking deletion callbacks. Selection and batching are identical.

   With `:konserve.gc/report? true`, returns a map containing
   `:konserve.gc/candidates` (the same selected key set) and
   `:konserve.gc/observed-keys` (all logical keys observed before deletion).
   Both come from the same metadata enumeration. Without this option the
   result remains the selected key set.

   The optional `:konserve.gc/batch-issued` callback receives each realized
   vector of encoded physical store-key strings after the batch is fixed and
   before the backing store performs its first existence check or deletion, for
   a store without multi-key deletes. The callback runs synchronously; returning
   normally admits that batch, while throwing prevents the backing store from
   seeing it."
  ([store whitelist cutoff]
   (sweep! store whitelist cutoff 1000 {}))
  ([store whitelist cutoff batch-size]
   (sweep! store whitelist cutoff batch-size {}))
  ([store whitelist cutoff batch-size opts]
   (async+sync
    (:sync? opts)
    *default-sync-translation*
    (go-try-
     (let [sync? (:sync? opts)
           sync-io? (or sync? (:konserve.gc/sync-io? opts))
           metadata (if sync-io? (k/keys store {:sync? true}) (<?- (k/keys store)))
           observed (when (:konserve.gc/report? opts)
                      (into #{} (map :key) metadata))
           to-delete (->> metadata
                          (filter (fn [{:keys [key last-write] :as meta}]
                                    (not
                                     (or (contains? whitelist key)
                                         (<= (.getTime ^Date cutoff)
                                             (.getTime (if last-write
                                                         ^Date last-write
                                                         ;; old name
                                                         ^Date (:konserve.core/timestamp meta))))))))
                          (partition-all batch-size))
           candidates
           (<?-
            (reduce<?-
             (fn [deleted-files batch]
               (go-try-
                (cond
                  (:konserve.gc/dry-run? opts)
                  (into deleted-files (map :key batch))

                  ;; one round trip for the whole batch where the backing allows it
                  (utils/multi-key-capable? store)
                  (let [keys-to-delete (mapv :key batch)]
                    (if sync-io?
                      (k/multi-dissoc store keys-to-delete (assoc opts :sync? true))
                      (<?- (k/multi-dissoc store keys-to-delete (assoc opts :sync? false))))
                    (into deleted-files keys-to-delete))

                  :else
                  (do (when-let [batch-issued (:konserve.gc/batch-issued opts)]
                        (batch-issued (mapv (comp defaults/key->store-key :key) batch)))
                      (if sync?
                        (delete-batch! store batch true)
                        (<?- (delete-batch! store batch false)))
                      (into deleted-files (map :key batch))))))
             #{}
             to-delete))]
       (if (:konserve.gc/report? opts)
         {:konserve.gc/candidates candidates :konserve.gc/observed-keys observed}
         candidates))))))
