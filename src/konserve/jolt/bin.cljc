(ns konserve.jolt.bin
  "A small tagged binary codec for EDN data, pure Clojure (runs on the JVM and
  Jolt), measured as an alternative to the EDN string serializer.

  One tag byte per value; integers are zigzag varints; strings, keywords and
  symbols are UTF-8 with a varint length; collections carry a varint count.
  Any value outside the core set (records, datoms, index handles) is written as
  its printed EDN text under one tag and read back with the store's EDN read
  handlers, so whatever round-trips through the string serializer round-trips
  here too."
  (:require [konserve.protocols :refer [PStoreSerializer]]
            [incognito.edn :refer [read-string-safe]])
  #?(:clj (:import [java.io ByteArrayOutputStream InputStream OutputStream])))

(def ^:private T-NIL 0) (def ^:private T-TRUE 1) (def ^:private T-FALSE 2)
(def ^:private T-LONG 3) (def ^:private T-DOUBLE 4) (def ^:private T-STR 5)
(def ^:private T-KW 6) (def ^:private T-SYM 7) (def ^:private T-VEC 8)
(def ^:private T-LIST 9) (def ^:private T-MAP 10) (def ^:private T-SET 11)
(def ^:private T-UUID 12) (def ^:private T-INST 13) (def ^:private T-EDN 14)

(defn- w-byte [^ByteArrayOutputStream out b] (.write out (int b)))

(defn- w-varint [out n]
  (loop [n n]
    (if (zero? (bit-and n (bit-not 0x7f)))
      (w-byte out n)
      (do (w-byte out (bit-or 0x80 (bit-and n 0x7f)))
          (recur (unsigned-bit-shift-right n 7))))))

(defn- w-long8 [out n]
  (loop [i 56] (when (>= i 0) (w-byte out (bit-and (unsigned-bit-shift-right n i) 0xff)) (recur (- i 8)))))

(defn- w-utf8 [^ByteArrayOutputStream out ^String s]
  (let [bs (.getBytes s "UTF-8")]
    (w-varint out (alength bs))
    (.write out bs 0 (alength bs))))

(defn- w-name [out x]
  (let [ns (namespace x)]
    (if ns (do (w-byte out 1) (w-utf8 out ns)) (w-byte out 0))
    (w-utf8 out (name x))))

(declare encode!)

(defn- w-items [out xs] (w-varint out (count xs)) (doseq [x xs] (encode! out x)))

(defn encode! [out x]
  (cond
    (nil? x) (w-byte out T-NIL)
    (true? x) (w-byte out T-TRUE)
    (false? x) (w-byte out T-FALSE)
    (and (int? x) (<= Long/MIN_VALUE x Long/MAX_VALUE))
    (do (w-byte out T-LONG) (w-varint out (bit-xor (bit-shift-left x 1) (bit-shift-right x 63))))
    (double? x) (do (w-byte out T-DOUBLE) (w-long8 out (Double/doubleToRawLongBits x)))
    (string? x) (do (w-byte out T-STR) (w-utf8 out x))
    (keyword? x) (do (w-byte out T-KW) (w-name out x))
    (symbol? x) (do (w-byte out T-SYM) (w-name out x))
    (map? x) (if (record? x)
               (do (w-byte out T-EDN) (w-utf8 out (pr-str x)))
               (do (w-byte out T-MAP) (w-varint out (count x))
                   (reduce-kv (fn [_ k v] (encode! out k) (encode! out v)) nil x)))
    (vector? x) (do (w-byte out T-VEC) (w-items out x))
    (set? x) (do (w-byte out T-SET) (w-items out x))
    (or (list? x) (seq? x)) (do (w-byte out T-LIST) (w-items out x))
    (uuid? x) (do (w-byte out T-UUID)
                  (w-long8 out (.getMostSignificantBits ^java.util.UUID x))
                  (w-long8 out (.getLeastSignificantBits ^java.util.UUID x)))
    (inst? x) (do (w-byte out T-INST) (w-long8 out (inst-ms x)))
    :else (do (w-byte out T-EDN) (w-utf8 out (pr-str x)))))

;; decoding works over the whole byte-array with a position cell
(deftype Reader [^bytes bs ^:unsynchronized-mutable pos]
  clojure.lang.IDeref
  (deref [_] pos)
  clojure.lang.IFn
  (invoke [_] (let [b (bit-and (aget bs pos) 0xff)] (set! pos (inc pos)) b))
  (invoke [_ n] (let [p pos] (set! pos (+ pos n)) p)))

(defn- r-varint [^Reader r]
  (loop [shift 0 acc 0]
    (let [b (r)]
      (if (zero? (bit-and b 0x80))
        (bit-or acc (bit-shift-left b shift))
        (recur (+ shift 7) (bit-or acc (bit-shift-left (bit-and b 0x7f) shift)))))))

(defn- r-long8 [^Reader r]
  (loop [i 0 acc 0] (if (< i 8) (recur (inc i) (bit-or (bit-shift-left acc 8) (r))) acc)))

(defn- r-utf8 [^Reader r ^bytes bs]
  (let [n (r-varint r) p (r n)] (String. bs (int p) (int n) "UTF-8")))

(defn- r-name [r bs] (let [ns (when (= 1 (r)) (r-utf8 r bs))] [ns (r-utf8 r bs)]))

(defn decode [^Reader r ^bytes bs handlers]
  (let [t (r)]
    (case (int t)
      0 nil 1 true 2 false
      3 (let [z (r-varint r)] (bit-xor (unsigned-bit-shift-right z 1) (- (bit-and z 1))))
      4 (Double/longBitsToDouble (r-long8 r))
      5 (r-utf8 r bs)
      6 (let [[ns n] (r-name r bs)] (keyword ns n))
      7 (let [[ns n] (r-name r bs)] (symbol ns n))
      8 (let [n (r-varint r)] (loop [i 0 acc (transient [])] (if (< i n) (recur (inc i) (conj! acc (decode r bs handlers))) (persistent! acc))))
      9 (let [n (r-varint r)] (apply list (loop [i 0 acc (transient [])] (if (< i n) (recur (inc i) (conj! acc (decode r bs handlers))) (persistent! acc)))))
      10 (let [n (r-varint r)] (loop [i 0 acc (transient {})] (if (< i n) (recur (inc i) (let [k (decode r bs handlers)] (assoc! acc k (decode r bs handlers)))) (persistent! acc))))
      11 (let [n (r-varint r)] (loop [i 0 acc (transient #{})] (if (< i n) (recur (inc i) (conj! acc (decode r bs handlers))) (persistent! acc))))
      12 (java.util.UUID. (r-long8 r) (r-long8 r))
      13 (java.util.Date. (long (r-long8 r)))
      14 (read-string-safe handlers (r-utf8 r bs)))))

(defn to-bytes ^bytes [x]
  (let [out (ByteArrayOutputStream.)] (encode! out x) (.toByteArray out)))

(defn from-bytes [^bytes bs handlers] (decode (->Reader bs 0) bs handlers))

(defrecord BinSerializer []
  PStoreSerializer
  (-deserialize [_ read-handlers in]
    (from-bytes (.readAllBytes ^InputStream in) @read-handlers))
  (-serialize [_ out _ val]
    (.write ^OutputStream out ^bytes (to-bytes val))))
