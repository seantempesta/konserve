(ns konserve.filestore-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!! go chan put! close! <!] :as async]
            [konserve.core :as k :refer [bassoc bget keys]]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :refer [PDataSyncBackingStore]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.tests.cache :as ct]
            [konserve.tests.encryptor :as et]
            [konserve.tests.gc :as gct]
            [konserve.tests.serializers :as st]
            [konserve.tests.tiered :as tiered-tests]
            [konserve.memory :as memory]
            [konserve.tiered :as tiered]))

(deftest binary-file-input-streams-without-materializing-the-source
  (let [root (str "target/konserve-binary-file-test/" (random-uuid))
        store-path (str root "/store")
        payload-file (io/file root "payload.bin")
        payload (byte-array [0 -1 -128 127 42])]
    (try
      (.mkdirs (.getParentFile payload-file))
      (with-open [output (io/output-stream payload-file)]
        (.write output payload))
      (let [store (connect-fs-store store-path :opts {:sync? true})]
        (is (true? (bassoc store :file payload-file {:sync? true})))
        (is (java.util.Arrays/equals
             payload
             (bget store :file
                   (fn [{:keys [input-stream]}]
                     (.readAllBytes ^java.io.InputStream input-stream))
                   {:sync? true}))))
      (finally
        (delete-store store-path)
        (io/delete-file payload-file true)
        (io/delete-file (io/file root) true)))))

(deftest file-binary-range-work-is-proportional-to-the-request
  (let [root (str "target/konserve-binary-range-test/" (random-uuid))
        store-path (str root "/store")
        payload-file (io/file root "payload.bin")
        payload-size (* 32 1024 1024)
        range-offset (+ 1024 1024 3)
        expected (byte-array (map #(unchecked-byte (mod % 256))
                                  (range 4096)))
        bean ^com.sun.management.ThreadMXBean
        (java.lang.management.ManagementFactory/getThreadMXBean)
        thread-id (.getId (Thread/currentThread))]
    (try
      (.mkdirs (.getParentFile payload-file))
      (with-open [payload (java.io.RandomAccessFile. payload-file "rw")]
        (.setLength payload payload-size)
        (.seek payload range-offset)
        (.write payload expected))
      (.setThreadAllocatedMemoryEnabled bean true)
      (let [store (connect-fs-store store-path :opts {:sync? true})
            write-before (.getThreadAllocatedBytes bean thread-id)
            _ (bassoc store :large-file payload-file {:sync? true})
            write-allocation (- (.getThreadAllocatedBytes bean thread-id)
                                write-before)
            read-before (.getThreadAllocatedBytes bean thread-id)
            actual (konserve.core/bget-range
                    store :large-file range-offset (alength expected))
            read-allocation (- (.getThreadAllocatedBytes bean thread-id)
                               read-before)]
        (is (java.util.Arrays/equals expected actual))
        (is (< write-allocation (* 8 1024 1024))
            (str "32 MiB File bassoc allocated " write-allocation " bytes"))
        (is (< read-allocation (* 1024 1024))
            (str "4 KiB range read allocated " read-allocation " bytes")))
      (finally
        (delete-store store-path)
        (io/delete-file payload-file true)
        (io/delete-file (io/file root) true)))))

;; A binary written with `:compress? true` passes through the store compressor
;; and reads back whole and by range; metadata given to `bassoc` is stored.
(deftest binary-values-compress-through-the-store-compressor
  (let [store-path (str "target/konserve-binary-compress-test/" (random-uuid))
        text (.getBytes (apply str (repeat 2000 "{:seon/key :value :n 12345}\n")) "UTF-8")
        noise (let [bs (byte-array 20000)] (.nextBytes (java.util.Random. 7) bs) bs)
        read-all (fn [store key]
                   (bget store key
                         (fn [{:keys [input-stream]}]
                           (.readAllBytes ^java.io.InputStream input-stream))
                         {:sync? true}))
        range-of (fn [^bytes bs offset length]
                   (java.util.Arrays/copyOfRange
                    bs (int (min offset (alength bs))) (int (min (+ offset length) (alength bs)))))
        file-size (fn [key] (.length (io/file store-path (defaults/key->store-key key))))]
    (try
      (let [store (connect-fs-store store-path :opts {:sync? true}
                                    :config {:compressor {:type :lz4}})]
        (testing "compressible bytes are stored compressed and read back unchanged"
          (is (true? (bassoc store :text text {:immutable? true} {:sync? true :compress? true})))
          (is (= {:immutable? true :compressed? true :size (alength text) :type :binary}
                 (select-keys (k/get-meta store :text nil {:sync? true})
                              [:immutable? :compressed? :size :type])))
          (is (< (file-size :text) (quot (alength text) 10)))
          (is (java.util.Arrays/equals text ^bytes (read-all store :text)))
          (doseq [[offset length] [[0 10] [1000 100] [(- (alength text) 5) 100] [(+ (alength text) 1) 10]]]
            (is (java.util.Arrays/equals ^bytes (range-of text offset length)
                                         ^bytes (k/bget-range store :text offset length))
                (str "range " offset " " length))))
        (testing "bytes compression does not shrink are stored as they are"
          (is (true? (bassoc store :noise noise nil {:sync? true :compress? true})))
          (is (nil? (:compressed? (k/get-meta store :noise nil {:sync? true}))))
          (is (java.util.Arrays/equals noise ^bytes (read-all store :noise)))
          (is (java.util.Arrays/equals ^bytes (range-of noise 100 50)
                                       ^bytes (k/bget-range store :noise 100 50))))
        (testing "an uncompressed write on a compressing store stays raw, and a rewrite drops the marker"
          (is (true? (bassoc store :text text {:sync? true})))
          (is (nil? (:compressed? (k/get-meta store :text nil {:sync? true}))))
          (is (> (file-size :text) (alength text)))
          (is (java.util.Arrays/equals text ^bytes (read-all store :text)))
          (is (java.util.Arrays/equals ^bytes (range-of text 1000 100)
                                       ^bytes (k/bget-range store :text 1000 100)))))
      (let [store (connect-fs-store (str store-path "-null") :opts {:sync? true})]
        (testing "a store without a compressor ignores :compress?"
          (is (true? (bassoc store :text text nil {:sync? true :compress? true})))
          (is (nil? (:compressed? (k/get-meta store :text nil {:sync? true}))))
          (is (java.util.Arrays/equals text ^bytes (read-all store :text)))))
      (finally
        (delete-store store-path)
        (delete-store (str store-path "-null"))))))

(deftest filestore-compliance-test
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder))]
    (testing "Compliance test with default config."
      (compliance-test store))))

(deftest sync-plan-defers-only-new-immutable-keys-of-an-opted-in-store
  ;; The only write allowed to skip the store-wide barrier is an immutable key
  ;; that was absent before its write, in a store that opted in, on a backing
  ;; that can defer. An existing immutable key (a replacement of something a
  ;; durable head may already name) and every mutable key pay the barrier
  ;; before and after the move; without the opt-in nothing changes.
  (let [folder "/tmp/konserve-fs-sync-plan-test"
        _      (delete-store folder)
        store  (connect-fs-store folder :opts {:sync? true})
        backing (:backing store)
        opted (clojure.core/assoc (:config store) :defer-immutable-sync? true)
        mac? (.startsWith ^String (System/getProperty "os.name" "") "Mac")]
    (try
      (is (satisfies? PDataSyncBackingStore backing))
      (is (= :per-write (defaults/sync-plan backing (:config store) true false))
          "no opt-in: the parent's per-write sequence")
      (is (= :per-write (defaults/sync-plan backing (clojure.core/assoc opted :sync-blob? false) true false)))
      (is (= :per-write (defaults/sync-plan backing (clojure.core/assoc opted :in-place? true) true false)))
      (if mac?
        (do (is (= :deferred (defaults/sync-plan backing opted true false)))
            (is (= :barriered (defaults/sync-plan backing opted true true)) "existing immutable key")
            (is (= :barriered (defaults/sync-plan backing opted true nil)) "existence unknown")
            (is (= :barriered (defaults/sync-plan backing opted false false)) "mutable key"))
        (is (= :per-write (defaults/sync-plan backing opted true false)) "only macOS defers"))
      (finally (delete-store folder)))))

(deftest an-opted-in-store-publishes-in-datahike-order-and-replaces-immutable-keys
  ;; Nodes, then the commit record, then the head that names them; then the
  ;; same immutable node written again with new bytes (the barriered path).
  ;; Every value reads back, the marker is kept, no staged `.new` blob is left.
  (let [folder "/tmp/konserve-fs-deferred-barrier-test"
        _      (delete-store folder)
        store  (connect-fs-store folder :opts {:sync? true} :config {:defer-immutable-sync? true})
        nodes  (mapv (fn [i] (vec (range (* 100 i)))) (range 1 6))]
    (try
      (<!! (async/go
             (let [ops (mapv (fn [i v] (k/assoc store [:node i] v {:immutable? true} {:sync? false}))
                             (range) nodes)]
               (doseq [op ops] (<! op)))
             (<! (k/assoc store :commit {:nodes (count nodes)} {:immutable? true} {:sync? false}))
             (<! (k/assoc store :head {:commit :commit} {:sync? false}))))
      (is (= nodes (mapv #(k/get store [:node %] nil {:sync? true}) (range (count nodes)))))
      (is (= {:commit :commit} (k/get store :head nil {:sync? true})))
      (is (true? (:immutable? (k/get-meta store :commit nil {:sync? true}))))
      (is (nil? (:immutable? (k/get-meta store :head nil {:sync? true}))))
      (k/assoc store [:node 0] [:replaced] {:immutable? true} {:sync? true})
      (is (= [:replaced] (k/get store [:node 0] nil {:sync? true})))
      (is (empty? (filter #(.endsWith ^String % ".new") (.list (io/file folder)))))
      (finally (delete-store folder)))))

(deftest filestore-compliance-test-no-fsync
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (connect-fs-store folder :opts {:sync? true} :config {:sync-blob? false})]
    (testing "Compliance test without syncing."
      (compliance-test store))))

(deftest filestore-compliance-test-no-file-lock
  (let [folder "/tmp/konserve-fs-comp-test"
        _      (delete-store folder)
        store  (<!! (connect-fs-store folder :config {:lock-blob? false}))]
    (testing "Compliance test without file locking."
      (compliance-test store))))

(defn create-tiered-stores [folder]
  (delete-store folder)
  {:frontend (<!! (memory/new-mem-store))
   :backend (<!! (connect-fs-store folder))})

(deftest tiered-store-filestore-backend-test
  (testing "Tiered Store with Filestore Backend"
    (let [folder "/tmp/konserve-tiered-fs-test"]

      (testing "Compliance (Async)"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-tiered-compliance-async frontend backend))
          (delete-store folder)))

      (testing "Compliance (Sync)"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (tiered-tests/test-tiered-compliance-sync frontend backend)
          (delete-store folder)))

      (testing "Write Policies"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-write-policies-async frontend backend))
          (delete-store folder)))

      (testing "Read Policies"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-read-policies-async frontend backend))
          (delete-store folder)))

      (testing "Key Operations"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-key-operations-async frontend backend))
          (delete-store folder)))

      (testing "Binary Operations"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-binary-operations-async frontend backend))
          (delete-store folder)))

      (testing "Sync on Connect"
        (let [{:keys [frontend backend]} (create-tiered-stores folder)]
          (<!! (tiered-tests/test-sync-on-connect-async frontend backend))
          (delete-store folder)))

      (testing "Error Handling"
        (tiered-tests/test-error-handling nil nil)))))

(deftest binary-polymorhism-test
  (testing "Test storage of different binary input formats."
    (let [folder "/tmp/konserve-fs-test"
          _      (spit "/tmp/foo" (range 1 10))
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder))]
      (testing "Binary"
        (testing "ByteArray"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :byte-array (byte-array (range 10))))))
            (is (= true (<!! (bget store :byte-array
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (mapv byte (slurp input-stream)))))))))
            (is (=  (mapv byte (byte-array (range 10))) (<!! res-ch)))
            (close! res-ch)))
        (testing "CharArray"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :char-array (char-array "foo")))))
            (is (= true (<!! (bget store :char-array
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo" (<!! res-ch)))))
        (testing "File Inputstream"
          (let [res-ch (chan)]
            (spit "/tmp/foo" (range 1 10))
            (is (= true (<!! (bassoc store :file-input-stream (java.io.FileInputStream. "/tmp/foo")))))
            (is (= true (<!! (bget store :file-input-stream
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  (str (range 1 10)) (<!! res-ch)))))
        (testing "Byte Array Inputstream"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :input-stream (java.io.ByteArrayInputStream. (byte-array (range 10)))))))
            (is (= true (<!! (bget store :input-stream
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (map byte (slurp input-stream)))))))))
            (is (=  (map byte (byte-array (range 10))) (<!! res-ch)))
            (close! res-ch)))
        (testing "String"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :string "foo bar"))))
            (is (= true (<!! (bget store :string
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (= "foo bar" (<!! res-ch)))))
        (testing "Reader"
          (let [res-ch (chan)]
            (is (= true (<!! (bassoc store :reader (java.io.StringReader. "foo bar")))))
            (is (= true (<!! (bget store :reader
                                   (fn [{:keys [input-stream]}]
                                     (go
                                       (put! res-ch (slurp input-stream))))))))
            (is (=  "foo bar" (<!! res-ch))))))
      (delete-store folder)
      (let [store (<!! (connect-fs-store folder))]
        (is (= (<!! (keys store))
               #{}))))))

#!============
#! Cache tests

(deftest cache-PEDNKeyValueStore-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})]
    (<!! (ct/test-cached-PEDNKeyValueStore-async store))))

(deftest cache-PKeyIterable-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})]
    (<!! (ct/test-cached-PKeyIterable-async store))))

(deftest cache-PBin-test
  (delete-store "/tmp/cache-store")
  (let [store (connect-fs-store "/tmp/cache-store" :opts {:sync? true})
        f (fn [{:keys [input-stream]}]
            (async/to-chan! [input-stream]))]
    (<!! (ct/test-cached-PBin-async store f))))

#!============
#! GC tests

(deftest async-gc-test
  (delete-store "/tmp/gc-store")
  (let [store (connect-fs-store "/tmp/gc-store" :opts {:sync? true})]
    (<!! (gct/test-gc-async store))))

#!==================
#! Serializers tests

(deftest fressian-serializer-test
  (<!! (st/test-fressian-serializers-async "/tmp/serializers-test"
                                           connect-fs-store
                                           (fn [p] (go (delete-store p)))
                                           (fn [{:keys [input-stream]}]
                                             (async/to-chan! [input-stream])))))

(deftest CBOR-serializer-test
  (st/cbor-serializer-test "/tmp/konserve-fs-cbor-test"
                           connect-fs-store
                           (fn [p] (go (delete-store p)))))

#!==================
#! Encryptor tests

(deftest encryptor-sync-test
  (et/sync-encryptor-test "/tmp/encryptor-test"
                          connect-fs-store
                          delete-store))

(deftest encryptor-async-test
  (<!! (et/async-encryptor-test "/tmp/encryptor-test"
                                connect-fs-store
                                (fn [p] (go (delete-store p))))))
