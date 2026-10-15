(ns konserve.compressor-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [konserve.compressor :refer [lz4-compressor decompressing-stream]]
            [konserve.lz4-fixture-values :as fixtures]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store delete-store]]
            [konserve.protocols :refer [-serialize -deserialize]]
            [konserve.serializers :refer [fressian-serializer]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.lang.management ManagementFactory]
           [java.util Arrays Random]
           [net.jpountz.lz4 LZ4FrameInputStream LZ4FrameOutputStream LZ4FrameOutputStream$BLOCKSIZE
            LZ4FrameOutputStream$FLG$Bits]))

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

;; ---------------------------------------------------------------------------
;; The frame reader: `Lz4Compressor` decodes a value's frames from its bytes.

(defn- fixture-bytes ^bytes [file]
  (with-open [in (io/input-stream (io/resource (str "konserve/lz4_frames/" file)))]
    (.readAllBytes in)))

(defn- read-frame [^bytes frame]
  (-deserialize (lz4-compressor (fressian-serializer)) (atom {}) (ByteArrayInputStream. frame)))

(deftest lz4-frames-written-before-the-reader-changed-read-back
  (testing "frames captured from the 64 KiB writer and from the 4 MiB default it replaced"
    (doseq [[k v] fixtures/values
            writer ["64k" "4m"]]
      (is (= v (read-frame (fixture-bytes (str (name k) "." writer ".lz4"))))
          (str k " " writer))))
  (testing "a binary value's frame, read as a stream"
    (with-open [in (decompressing-stream lz4-compressor
                                         (ByteArrayInputStream. (fixture-bytes "octets.64k.lz4")))]
      (is (Arrays/equals ^bytes fixtures/octets (.readAllBytes in))))))

(defn- random-value
  "A value of about `n` bytes from `rnd`: compressible strings and keywords, or
  incompressible longs."
  [^Random rnd n compressible?]
  (if compressible?
    {:words (vec (repeatedly (max 1 (quot n 12)) #(str "w" (.nextInt rnd 40))))
     :kw (keyword "k" (str (.nextInt rnd 1000)))}
    {:longs (vec (repeatedly (max 1 (quot n 9)) #(.nextLong rnd)))}))

(deftest lz4-round-trips-values-of-every-size
  (doseq [seed (range 40)
          n [1 50 4000 70000 300000]
          compressible? [true false]]
    (let [v (random-value (Random. seed) n compressible?)]
      (is (= v (read-frame (lz4-bytes v))) (str seed " " n " " compressible?)))))

(defn- frame-of
  "`raw` as LZ4 frames written by LZ4FrameOutputStream with `flags` and
  `block-size`, closed (with an end mark) or only flushed."
  ^bytes [^bytes raw block-size flags close?]
  (let [bos (ByteArrayOutputStream.)
        out (LZ4FrameOutputStream. bos block-size (alength raw) (into-array LZ4FrameOutputStream$FLG$Bits flags))]
    (.write out raw)
    (if close? (.close out) (.flush out))
    (.toByteArray bos)))

(def ^:private octets
  "A serializer whose value is the stream it is given."
  (reify konserve.protocols/PStoreSerializer
    (-deserialize [_ _ in] in)
    (-serialize [_ _ _ _])))

(defn- frame-content-bytes [^bytes frames]
  (.readAllBytes ^java.io.InputStream (-deserialize (lz4-compressor octets) nil (ByteArrayInputStream. frames))))


(deftest lz4-frame-reader-agrees-with-lz4-frame-input-stream
  (let [rnd (Random. 7)
        flag-sets [[LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE]
                   [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE LZ4FrameOutputStream$FLG$Bits/BLOCK_CHECKSUM]
                   [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE LZ4FrameOutputStream$FLG$Bits/CONTENT_CHECKSUM]
                   [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE LZ4FrameOutputStream$FLG$Bits/CONTENT_SIZE
                    LZ4FrameOutputStream$FLG$Bits/CONTENT_CHECKSUM LZ4FrameOutputStream$FLG$Bits/BLOCK_CHECKSUM]]]
    (doseq [n [0 1 100 65536 65537 200000]
            compressible? [true false]
            flags flag-sets
            block-size [LZ4FrameOutputStream$BLOCKSIZE/SIZE_64KB LZ4FrameOutputStream$BLOCKSIZE/SIZE_4MB]
            close? [true false]
            :let [raw (if compressible?
                        (.getBytes ^String (apply str (repeatedly (quot n 4) #(char (+ 97 (.nextInt rnd 3)))))
                                   "UTF-8")
                        (let [b (byte-array n)] (.nextBytes rnd b) b))
                  frames (frame-of raw block-size flags close?)]]
      (is (Arrays/equals raw (frame-content-bytes frames))
          (str n " " compressible? " " (vec flags) " " block-size " " close?))
      (when close?
        (is (Arrays/equals raw (with-open [in (LZ4FrameInputStream. (ByteArrayInputStream. frames))]
                                 (.readAllBytes in))))))
    (testing "frames in sequence, with a skippable frame between them"
      (let [a (.getBytes "first frame " "UTF-8")
            b (.getBytes (apply str (repeat 3000 "second ")) "UTF-8")
            skippable (byte-array [0x50 0x2A 0x4D 0x18 3 0 0 0 9 9 9])
            frames (byte-array (concat (frame-of a LZ4FrameOutputStream$BLOCKSIZE/SIZE_64KB
                                                 [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE] true)
                                       skippable
                                       (frame-of b LZ4FrameOutputStream$BLOCKSIZE/SIZE_64KB
                                                 [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE] true)))]
        (is (Arrays/equals (byte-array (concat a b)) (frame-content-bytes frames)))))
    (testing "a corrupted frame is refused, not decoded"
      (let [frames (frame-of (.getBytes (apply str (repeat 100 "abc")) "UTF-8") LZ4FrameOutputStream$BLOCKSIZE/SIZE_64KB
                             [LZ4FrameOutputStream$FLG$Bits/BLOCK_INDEPENDENCE LZ4FrameOutputStream$FLG$Bits/BLOCK_CHECKSUM]
                             true)
            _ (aset-byte frames 12 (unchecked-byte (inc (aget frames 12))))]
        (is (thrown? clojure.lang.ExceptionInfo (frame-content-bytes frames)))))))

(defn- allocated-bytes-per-call
  "Mean bytes the calling thread allocates per call of `f`, after warming it."
  ^double [n f]
  (let [mx (ManagementFactory/getThreadMXBean)
        id (.getId (Thread/currentThread))
        allocated #(.getThreadAllocatedBytes ^com.sun.management.ThreadMXBean mx id)]
    (dotimes [_ 50] (f))
    (let [a0 (allocated)]
      (dotimes [_ n] (f))
      (/ (- (allocated) a0) (double n)))))

(deftest lz4-store-reads-allocate-in-proportion-to-the-value
  ;; Before the frame reader every read allocated two 64 KiB frame buffers
  ;; (131 KB), an 8 KiB BufferedInputStream and a merged handler table: a
  ;; 61-byte value cost ~190 KB. What remains is the store's own per-read work
  ;; (key hashing, lock, header, env maps) and the value itself. The node-sized
  ;; value is 6,000 compressible bytes, so decoding it allocates about its size.
  (let [path "/tmp/konserve-lz4-allocation-test"
        _ (delete-store path)
        store (connect-fs-store path :opts {:sync? true} :config {:compressor {:type :lz4}})
        node (.getBytes (apply str (repeat 500 "datom-bytes ")) "UTF-8")]
    (try
      (k/assoc store :tiny (:tiny fixtures/values) {:sync? true})
      (k/assoc store :node node {:sync? true})
      (let [tiny (allocated-bytes-per-call 200 #(k/get store :tiny nil {:sync? true}))
            node-read (allocated-bytes-per-call 200 #(k/get store :node nil {:sync? true}))]
        (is (< tiny 48000) (str "tiny value: " tiny " bytes per read"))
        (is (< node-read (+ (alength node) 48000)) (str "node-sized value: " node-read " bytes per read")))
      (finally (delete-store path)))))

(deftest lz4-store-reads-from-many-threads
  (let [path "/tmp/konserve-lz4-concurrent-test"
        _ (delete-store path)
        store (connect-fs-store path :opts {:sync? true} :config {:compressor {:type :lz4}})
        values (into {} (map (fn [[k v]] [k v])) fixtures/values)]
    (try
      (doseq [[k v] values] (k/assoc store k v {:sync? true}))
      (let [results (->> (range 16)
                         (mapv (fn [t] (future (vec (for [i (range 40)
                                                          :let [k (nth (keys values) (mod (+ t i) (count values)))]]
                                                      (= (get values k) (k/get store k nil {:sync? true})))))))
                         (mapcat deref))]
        (is (= 640 (count results)))
        (is (every? true? results)))
      (finally (delete-store path)))))

(deftest lz4-store-binary-values-round-trip
  (let [path "/tmp/konserve-lz4-binary-test"
        _ (delete-store path)
        store (connect-fs-store path :opts {:sync? true} :config {:compressor {:type :lz4}})]
    (try
      (doseq [[k ^bytes octets] {:compressible fixtures/octets
                                 :noise (let [b (byte-array 150000)] (.nextBytes (Random. 3) b) b)}]
        (is (true? (k/bassoc store k octets nil {:sync? true :compress? true})))
        (is (Arrays/equals octets ^bytes (k/bget store k (fn [{:keys [input-stream]}] (.readAllBytes ^java.io.InputStream input-stream))
                                                 {:sync? true}))
            (str k)))
      (finally (delete-store path)))))

;; ---------------------------------------------------------------------------
;; The frame writer: `Lz4Compressor` encodes a value's frame from its bytes.

(def ^:private raw-serializer
  "Writes a byte-array value as it is, closing the stream when `close?`."
  (fn [close?]
    (reify konserve.protocols/PStoreSerializer
      (-serialize [_ out _ octets]
        (.write ^java.io.OutputStream out ^bytes octets)
        (when close? (.close ^java.io.OutputStream out)))
      (-deserialize [_ _ in] in))))

(defn- stream-frame
  "The frame LZ4FrameOutputStream writes for `octets`."
  ^bytes [^bytes octets close?]
  (let [bos (ByteArrayOutputStream.)
        out (LZ4FrameOutputStream. bos LZ4FrameOutputStream$BLOCKSIZE/SIZE_64KB)]
    (.write out octets)
    (if close? (.close out) (.flush out))
    (.toByteArray bos)))

(defn- writer-frame
  ^bytes [^bytes octets close?]
  (let [bos (ByteArrayOutputStream.)]
    (-serialize (lz4-compressor (raw-serializer close?)) bos nil octets)
    (.toByteArray bos)))

(defn- test-octets
  "`n` bytes: compressible when `compressible?`, else random; `mixed` makes
  alternating 64 KiB blocks of each."
  ^bytes [seed n kind]
  (let [r (Random. seed) b (byte-array n)]
    (case kind
      :random (.nextBytes r b)
      :compressible (dotimes [i n] (aset b i (unchecked-byte (mod (quot i 7) 13))))
      :mixed (dotimes [i n] (aset b i (unchecked-byte (if (even? (quot i 65536)) (mod (quot i 7) 13) (.nextInt r 256))))))
    b))

(deftest lz4-writer-is-byte-identical-to-the-stream
  (doseq [n [0 1 2 15 16 61 100 1000 4095 65535 65536 65537 131071 131072 131073 200000 300000]
          kind [:random :compressible :mixed]
          close? [false true]]
    (let [octets (test-octets n n kind)]
      (is (Arrays/equals (stream-frame octets close?) (writer-frame octets close?))
          (str n " " kind " closed=" close?)))))

(deftest lz4-writer-round-trips-through-every-reader
  (doseq [n [0 1 61 1000 65536 65537 300000]
          kind [:random :compressible :mixed]]
    (let [octets (test-octets 7 n kind)
          frame (writer-frame octets true)]
      (is (Arrays/equals octets (.readAllBytes (LZ4FrameInputStream. (ByteArrayInputStream. frame)))))
      (is (Arrays/equals octets (.readAllBytes (decompressing-stream nil (ByteArrayInputStream. frame)))))
      (is (Arrays/equals octets (.readAllBytes ^java.io.InputStream
                                               (-deserialize (lz4-compressor (raw-serializer false)) nil
                                                             (ByteArrayInputStream. (writer-frame octets false))))))))
  (let [value {:a (vec (range 5000)) :b "x"}
        bos (ByteArrayOutputStream.)]
    (-serialize (lz4-compressor (fressian-serializer)) bos (atom {}) value)
    (is (= value (-deserialize (lz4-compressor (fressian-serializer)) (atom {})
                               (ByteArrayInputStream. (.toByteArray bos)))))))

(defn- allocated ^long [f]
  (let [^com.sun.management.ThreadMXBean mx (ManagementFactory/getThreadMXBean)
        id (.getId (Thread/currentThread))
        before (.getThreadAllocatedBytes mx id)]
    (f)
    (- (.getThreadAllocatedBytes mx id) before)))

(deftest lz4-write-allocates-in-proportion-to-the-value
  ;; The stream allocated 2 x 64 KiB per value (~137 KB for a 30-byte value).
  (let [c (lz4-compressor (fressian-serializer))]
    (doseq [[value bound] [[{:a 1 :b "hello"} 20000]
                           [(vec (range 200)) 30000]
                           [(vec (range 60000)) 3600000]]]
      (let [w #(-serialize c (ByteArrayOutputStream.) (atom {}) value)]
        (dotimes [_ 300] (w))
        (is (< (apply min (repeatedly 20 #(allocated w))) bound))))))
