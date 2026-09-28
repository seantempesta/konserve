(ns konserve.compressor-test
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.compressor :refer [lz4-compressor]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.protocols :refer [-serialize -deserialize]]
            [konserve.serializers :refer [fressian-serializer]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [net.jpountz.lz4 LZ4FrameOutputStream]))

(defn- lz4-bytes [value]
  (let [bos (ByteArrayOutputStream.)]
    (-serialize (lz4-compressor (fressian-serializer)) bos (atom {}) value)
    (.toByteArray bos)))

(deftest lz4-frames-use-64k-blocks
  ;; Frame descriptor: magic (4 bytes), FLG, BD. BD bits 4-6 carry the block
  ;; maximum size: 4 = 64 KiB, 7 = 4 MiB (the stream's default).
  (is (= 4 (bit-and 7 (bit-shift-right (aget ^bytes (lz4-bytes {:a 1}) 5) 4)))))

(deftest lz4-reads-4mib-block-frames
  (testing "values written before the block-size change stay readable"
    (let [value {:a (vec (range 1000))}
          raw (let [bos (ByteArrayOutputStream.)]
                (-serialize (fressian-serializer) bos (atom {}) value)
                (.toByteArray bos))
          framed (let [bos (ByteArrayOutputStream.)
                       out (LZ4FrameOutputStream. bos)]
                   (.write out ^bytes raw)
                   (.flush out)
                   (.toByteArray bos))]
      (is (= value (-deserialize (lz4-compressor (fressian-serializer)) (atom {})
                                 (ByteArrayInputStream. framed)))))))

(deftest lz4-filestore-round-trip
  (let [path "/tmp/konserve-lz4-compressor-test"
        _ (delete-store path)
        store (connect-fs-store path :opts {:sync? true} :config {:compressor {:type :lz4}})
        small {:a 1}
        large {:xs (vec (range 100000))}]
    (try
      (k/assoc store :small small {:sync? true})
      (k/assoc store :large large {:sync? true})
      (is (= small (k/get store :small nil {:sync? true})))
      (is (= large (k/get store :large nil {:sync? true})))
      (finally (delete-store path)))))
