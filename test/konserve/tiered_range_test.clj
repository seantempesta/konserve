(ns konserve.tiered-range-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [konserve.core :as k]
            [konserve.filestore :as fs]
            [konserve.tiered :as tiered]))

(deftest binary-ranges-follow-the-read-policy
  (let [root (str "target/tiered-range/" (random-uuid))
        front-path (str root "/front")
        back-path (str root "/back")
        inherited (byte-array [0 -1 -128 127 42])
        local (byte-array [10 20 30 40 50])
        opts {:sync? true}]
    (try
      (let [front (fs/connect-fs-store front-path :opts opts)
            back (fs/connect-fs-store back-path :opts opts)
            store (tiered/connect-tiered-store front back :write-policy :frontend-only :opts opts)]
        (k/bassoc back :inherited inherited opts)
        (k/bassoc back :shadowed inherited opts)
        (k/bassoc store :local local opts)
        (k/bassoc store :shadowed local opts)
        (doseq [[key bytes] [[:inherited inherited] [:local local] [:shadowed local]]
                [offset length] [[0 5] [1 3] [4 10] [5 2] [8 2] [2 0]]]
          (is (java.util.Arrays/equals
               (java.util.Arrays/copyOfRange ^bytes bytes (min offset 5) (min (+ offset length) 5))
               ^bytes (k/bget-range store key offset length))))
        (is (nil? (k/bget-range store :absent 0 4)))
        (is (false? (k/exists? front :inherited opts)))
        (let [front-only (tiered/connect-tiered-store front back :read-policy :frontend-only :opts opts)]
          (is (nil? (k/bget-range front-only :inherited 0 4)))
          (is (java.util.Arrays/equals local ^bytes (k/bget-range front-only :shadowed 0 5)))))
      (finally
        (fs/delete-store front-path)
        (fs/delete-store back-path)
        (io/delete-file root true)))))

(deftest binary-range-allocation-follows-the-request
  (let [root (str "target/tiered-range-allocation/" (random-uuid))
        front-path (str root "/front")
        back-path (str root "/back")
        file (io/file root "payload")
        offset 1048579
        expected (byte-array (map unchecked-byte (range 4096)))
        bean ^com.sun.management.ThreadMXBean (java.lang.management.ManagementFactory/getThreadMXBean)
        thread-id (.getId (Thread/currentThread))
        opts {:sync? true}]
    (try
      (.mkdirs (.getParentFile file))
      (with-open [out (java.io.RandomAccessFile. file "rw")]
        (.setLength out (* 32 1024 1024))
        (.seek out offset)
        (.write out expected))
      (let [front (fs/connect-fs-store front-path :opts opts)
            back (fs/connect-fs-store back-path :opts opts)
            store (tiered/connect-tiered-store front back :write-policy :frontend-only :opts opts)]
        (k/bassoc back :inherited file opts)
        (k/bassoc store :local file opts)
        (.setThreadAllocatedMemoryEnabled bean true)
        (doseq [key [:inherited :local]]
          (let [before (.getThreadAllocatedBytes bean thread-id)
                start (System/nanoTime)
                actual (k/bget-range store key offset 4096)
                elapsed (/ (- (System/nanoTime) start) 1e6)
                allocated (- (.getThreadAllocatedBytes bean thread-id) before)]
            (is (java.util.Arrays/equals expected ^bytes actual))
            (is (< allocated (* 1024 1024)))
            (println {:key key :ms elapsed :allocated-bytes allocated}))))
      (finally
        (fs/delete-store front-path)
        (fs/delete-store back-path)
        (io/delete-file file true)
        (io/delete-file root true)))))
