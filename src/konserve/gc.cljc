(ns konserve.gc
  (:require [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.impl.defaults :as defaults]
            [konserve.utils :as utils]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn sweep!
  "Deletes keys outside the whitelist that precede the retention instant.

  With `:konserve.gc/sync-io? true`, the key listing and the deletions use the store's
  synchronous operations (still returning a channel).

  With `:konserve.gc/dry-run? true`, returns the selected logical keys without
  deleting them or invoking deletion callbacks. Selection and batching are identical.

  With `:konserve.gc/report? true`, returns a map containing
  `:konserve.gc/candidates` (the same selected key set) and
  `:konserve.gc/observed-keys` (all logical keys observed before deletion).
  Both come from the same metadata enumeration. Without this option the
  result remains the selected key set.

  The optional `:konserve.gc/batch-issued` callback receives each realized
  vector of encoded physical store-key strings after the batch is fixed and
  before the backing store performs its first existence check or deletion.
  The callback runs synchronously; returning normally admits that batch, while
  throwing prevents the backing store from seeing it."
  ([store whitelist ts]
   (sweep! store whitelist ts 1000))
  ([store whitelist ts batch-size]
   (sweep! store whitelist ts batch-size {}))
  ([store whitelist ts batch-size opts]
   (go-try-
    (let [sync-io? (:konserve.gc/sync-io? opts)
          ;; :konserve.gc/sync-io? true lists and deletes through the store's synchronous
          ;; operations, which a sweep run by one blocking owner (a writer-side maintenance
          ;; proc) wants: the async path costs a channel hand-off per key read on hosts
          ;; where that is not cheap (Jolt: 990 ms against 75 ms for 6,200 keys on LMDB).
          metadata (if sync-io? (k/keys store {:sync? true}) (<?- (k/keys store)))
          observed (when (:konserve.gc/report? opts)
                     (into #{} (map :key) metadata))
          to-delete (->> metadata
                         (filter (fn [{:keys [key last-write] :as meta}]
                                   (not
                                    (or (contains? whitelist key)
                                        (<= (.getTime ^Date ts)
                                            (.getTime (if last-write
                                                        ^Date last-write
                                                        ;; old name
                                                        ^Date (:konserve.core/timestamp meta))))))))
                         (partition-all batch-size))
          candidates (<?-
       (reduce<?-
        (fn [deleted-files batch]
          (go-try-
           (if (:konserve.gc/dry-run? opts)
             (into deleted-files (map :key batch))
             (if (utils/multi-key-capable? store)
             ;; Use multi-dissoc for batch deletion if supported
             (let [keys-to-delete (mapv :key batch)]
               (if sync-io?
                 (k/multi-dissoc store keys-to-delete (assoc opts :sync? true))
                 (<?- (k/multi-dissoc store keys-to-delete
                                      (assoc opts :sync? false))))
               (into deleted-files keys-to-delete))
             ;; Fallback to single operations for stores without multi-key support.
             ;; GC does not use dissoc's existed? return, so :ignore-existence? lets
             ;; miss-safe backings skip the per-key HEAD probe (idempotent delete).
             (let [_ (when-let [batch-issued (:konserve.gc/batch-issued opts)]
                       (batch-issued (mapv (comp defaults/key->store-key :key) batch)))
                   pending-deletes (mapv (fn [{:keys [key]}]
                                           (k/dissoc store key {:ignore-existence? true}))
                                         batch)]
               (<?- (async/into [] (async/merge pending-deletes)))
               (into deleted-files (map :key batch)))))))
        #{}
        to-delete))]
      (if (:konserve.gc/report? opts)
        {:konserve.gc/candidates candidates :konserve.gc/observed-keys observed}
        candidates)))))
