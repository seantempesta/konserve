(ns konserve.storage-layout-test
  "Byte-level tests for the blob header, focused on the meta-size field.

  History: CLJ always wrote meta-size as a 4-byte big-endian int at header
  bytes 4-7; CLJS used to write a SINGLE byte at offset 4 (wrapping silently
  for meta >= 256). Since the BE32 fix both platforms write 4-byte BE, and
  parse-header carries a legacy sniff (byte4 != 0 while bytes 5-7 = 0 =>
  legacy 1-byte encoding) so old CLJS-written stores remain readable on
  both platforms."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.compressor :refer [null-compressor]]
            [konserve.encryptor :refer [null-encryptor]]
            [konserve.serializers :refer [fressian-serializer key->serializer]]
            [konserve.impl.storage-layout :refer [create-header parse-header
                                                  read-meta-size header-size
                                                  default-version]]))

(def serializer (fressian-serializer))

(defn header-byte
  "Unsigned value of byte i in a platform header array."
  [header i]
  #?(:clj (bit-and (aget ^bytes header (int i)) 0xff)
     :cljs (aget header i)))

(defn make-header-bytes
  "Build a 20-byte header array from a seq of leading byte values
  (rest zero-padded) — platform-appropriate array type."
  [byte-vals]
  #?(:clj (byte-array header-size (map unchecked-byte byte-vals))
     :cljs (let [arr (js/Uint8Array. header-size)]
             (doseq [[i v] (map-indexed vector byte-vals)]
               (aset arr i v))
             arr)))

(defn ser-id []
  ;; whatever id create-header assigns the fressian serializer on this platform
  (header-byte (create-header default-version serializer null-compressor null-encryptor 1) 1))

(deftest new-format-write-is-be32
  (testing "create-header writes meta-size as 4-byte big-endian at bytes 4-7"
    (doseq [meta [0 1 32 255 256 300 65535 70000 1234567]]
      (let [h (create-header default-version serializer null-compressor null-encryptor meta)]
        (is (= [(bit-and (unsigned-bit-shift-right meta 24) 0xff)
                (bit-and (unsigned-bit-shift-right meta 16) 0xff)
                (bit-and (unsigned-bit-shift-right meta 8) 0xff)
                (bit-and meta 0xff)]
               [(header-byte h 4) (header-byte h 5) (header-byte h 6) (header-byte h 7)])
            (str "meta " meta " encodes BE32"))))))

(deftest new-format-roundtrip
  (testing "create-header -> parse-header roundtrips meta-size, incl. >= 256"
    (doseq [meta [0 1 32 255 256 300 65535 70000 1234567]]
      (let [h (create-header default-version serializer null-compressor null-encryptor meta)
            [version _ser _comp _enc meta-size actual-header-size]
            (parse-header h key->serializer)]
        (is (= default-version version))
        (is (= meta meta-size) (str "meta " meta " roundtrips"))
        (is (= header-size actual-header-size))))))

(deftest legacy-one-byte-blobs-parse
  (testing "legacy CLJS 1-byte meta-size encoding is sniffed and read correctly"
    (doseq [meta [1 32 100 255]]
      ;; legacy layout: [version ser comp enc META 0 0 0 ...zeros]
      (let [h (make-header-bytes [default-version (ser-id) 0 0 meta 0 0 0])
            [_ _ _ _ meta-size _] (parse-header h key->serializer)]
        (is (= meta meta-size) (str "legacy 1-byte meta " meta " parses"))))))

(deftest cross-platform-be32-headers-parse
  (testing "a BE32 header (as the OTHER platform writes it) parses on this platform"
    ;; byte-identical to what CLJ .putInt / fixed CLJS write for these metas
    (doseq [[meta bytes47] [[32 [0 0 0 32]]
                            [256 [0 0 1 0]]
                            [300 [0 0 1 44]]
                            [70000 [0 1 17 112]]]]
      (let [h (make-header-bytes (into [default-version (ser-id) 0 0] bytes47))
            [_ _ _ _ meta-size _] (parse-header h key->serializer)]
        (is (= meta meta-size) (str "BE32 meta " meta " parses"))))))

(deftest read-meta-size-sniff-boundaries
  (testing "sniff only fires for byte4!=0 with bytes5-7 all zero"
    ;; pure BE32 with low byte set — byte4 = 0 => no sniff
    (is (= 32 (read-meta-size (make-header-bytes [1 0 0 0 0 0 0 32]))))
    ;; legacy pattern => sniff returns byte4
    (is (= 7 (read-meta-size (make-header-bytes [1 0 0 0 7 0 0 0]))))
    ;; byte4 set AND a lower byte set => genuine BE32 (>= 16MiB), no sniff
    (is (= (+ (* 2 16777216) 5) (read-meta-size (make-header-bytes [1 0 0 0 2 0 0 5]))))
    ;; zero meta
    (is (= 0 (read-meta-size (make-header-bytes [1 0 0 0 0 0 0 0]))))))
