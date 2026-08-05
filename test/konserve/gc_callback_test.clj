(ns konserve.gc-callback-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.string :as string]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.gc :as gc]
            [konserve.impl.defaults :as defaults])
  (:import [java.nio.file Files Path Paths]
           [java.util Date]))

(defn- store-path
  [root store-key]
  (.resolve ^Path (Paths/get root (make-array String 0)) ^String store-key))

(defn- future-date
  []
  (Date. (+ (System/currentTimeMillis) 60000)))

(deftest sweep-reports-each-fixed-physical-batch-before-deletion
  (let [root (str "target/konserve-gc-callback-test/" (random-uuid))
        keys [:first :second :third]
        expected-store-keys (set (map defaults/key->store-key keys))
        issued-batches (atom [])]
    (try
      (let [store (<!! (connect-fs-store root))]
        (doseq [key keys]
          (<!! (k/assoc store key {:value key})))
        (is (= (set keys)
               (<!! (gc/sweep!
                     store
                     #{}
                     (future-date)
                     2
                     {:konserve.gc/batch-issued
                      (fn [store-keys]
                        (testing "the complete encoded batch exists before deletion starts"
                          (is (vector? store-keys))
                          (is (every? #(string/ends-with? % ".ksv") store-keys))
                          (is (every? #(Files/exists (store-path root %)
                                                     (make-array java.nio.file.LinkOption 0))
                                      store-keys)))
                        (swap! issued-batches conj store-keys))}))))
        (is (= [1 2] (sort (mapv count @issued-batches))))
        (is (= expected-store-keys (set (mapcat identity @issued-batches)))))
      (finally
        (delete-store root)))))

(deftest callback-failure-precedes-any-delete-from-its-batch
  (let [root (str "target/konserve-gc-callback-failure-test/" (random-uuid))
        keys [:left :right]
        expected-store-keys (mapv defaults/key->store-key keys)]
    (try
      (let [store (<!! (connect-fs-store root))
            refusal (ex-info "test callback refusal" {:test/refused? true})]
        (doseq [key keys]
          (<!! (k/assoc store key {:value key})))
        (let [result (<!! (gc/sweep!
                           store
                           #{}
                           (future-date)
                           2
                           {:konserve.gc/batch-issued
                            (fn [_store-keys]
                              (throw refusal))}))]
          (is (instance? Throwable result))
          (is (= {:test/refused? true} (ex-data result)))
          (is (every? #(Files/exists (store-path root %)
                                     (make-array java.nio.file.LinkOption 0))
                      expected-store-keys))))
      (finally
        (delete-store root)))))
