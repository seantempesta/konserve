(ns konserve.compressor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]])
  #?(:jolt (:require [konserve.jolt.lz4 :as jlz4] [konserve.jolt.zstd :as jzstd])
     :clj (:import [java.io ByteArrayInputStream InputStream]
                    [net.jpountz.lz4 LZ4Factory LZ4SafeDecompressor
                     LZ4FrameOutputStream LZ4FrameOutputStream$BLOCKSIZE
                     LZ4FrameOutputStream$FLG LZ4FrameOutputStream$FLG$Bits LZ4FrameOutputStream$BD
                     LZ4FrameInputStream]
                    [net.jpountz.xxhash XXHash32 XXHashFactory])))

(defrecord NullCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

(defrecord UnsupportedLZ4Compressor [serializer]
  PStoreSerializer
  (-deserialize [_ _read-handlers bytes]
    (throw (ex-info "Unsupported LZ4 compressor." {:bytes bytes})))
  (-serialize [_ bytes _write-handlers _val]
    (throw (ex-info "Unsupported LZ4 compressor." {:bytes bytes}))))

;; Reading a value decodes its LZ4 frame from the value's bytes, into an array
;; of exactly the content's length. LZ4FrameInputStream would allocate two
;; buffers of the frame's maximum block size (2 x 64 KiB, 2 x 4 MiB for frames
;; written before the block-size change) on its first read of every value,
;; however small: 70% of what reading a 61-byte value allocated. The stream
;; stays for binary values, which are read as streams (`decompressing-stream`).
;; Frame format: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md

#?(:clj
   (def ^:private lz4-codecs
     (delay {:decompressor (.safeDecompressor (LZ4Factory/fastestInstance))
             :xxhash (.hash32 (XXHashFactory/fastestInstance))})))

#?(:clj
   (defn- le-int
     "The little-endian int at `i` in `src`."
     ^long [^bytes src ^long i]
     (unchecked-int
      (bit-or (bit-and (aget src i) 0xff)
              (bit-shift-left (bit-and (aget src (+ i 1)) 0xff) 8)
              (bit-shift-left (bit-and (aget src (+ i 2)) 0xff) 16)
              (bit-shift-left (bit-and (aget src (+ i 3)) 0xff) 24)))))

#?(:clj
   (defn- extra-length
     "The sum of an LZ4 length's extra bytes starting at `i`: bytes of 255
     continue it, the first byte below 255 ends it. It spans
     `(inc (quot sum 255))` bytes."
     ^long [^bytes src ^long i]
     (loop [i i sum 0]
       (let [b (bit-and (aget src i) 0xff)]
         (if (== b 255) (recur (inc i) (+ sum b)) (+ sum b))))))

#?(:clj
   (defn- block-length
     "The decompressed length of the compressed LZ4 block of `size` bytes at
     `off`: the sum of its sequences' literal and match lengths. Every sequence
     is a token, its literals and a 2-byte match offset; the last has literals
     only and ends the block."
     ^long [^bytes src ^long off ^long size]
     (let [end (+ off size)]
       (loop [i off n 0]
         (let [token (bit-and (aget src i) 0xff)
               lit (unsigned-bit-shift-right token 4)
               lit-extra (if (== lit 15) (extra-length src (inc i)) 0)
               i (+ i 1 (if (== lit 15) (inc (quot lit-extra 255)) 0) lit lit-extra)
               n (+ n lit lit-extra)]
           (if (>= i end)
             n
             (let [match (bit-and token 15)
                   match-extra (if (== match 15) (extra-length src (+ i 2)) 0)]
               (recur (+ i 2 (if (== match 15) (inc (quot match-extra 255)) 0))
                      (+ n match match-extra 4)))))))))

#?(:clj
   (defn- frame-blocks!
     "[index after the frame, content length so far] for the blocks of the
     frame whose first block is at `i`, its content starting at `pos`. With
     `dest` the blocks are decoded into it and their checksums verified. A frame
     ends at its end mark or, from a writer that only flushed, at the end of
     `src`."
     [^bytes src dest i pos ^LZ4FrameOutputStream$FLG flg]
     (let [{:keys [^LZ4SafeDecompressor decompressor ^XXHash32 xxhash]} @lz4-codecs
           n (alength src)
           block-checksum? (.isEnabled flg LZ4FrameOutputStream$FLG$Bits/BLOCK_CHECKSUM)
           start (long pos)]
       (loop [i (long i) pos start]
         (if (>= i n)
           [i pos]
           (let [word (le-int src i)
                 size (bit-and word 0x7fffffff)
                 i (+ i 4)]
             (if (zero? size)
               (do (when dest
                     (when (and (.isEnabled flg LZ4FrameOutputStream$FLG$Bits/CONTENT_CHECKSUM)
                                (not= (le-int src i) (.hash xxhash ^bytes dest (int start) (int (- pos start)) 0)))
                       (throw (ex-info "LZ4 content checksum mismatch." {:type :lz4-content-checksum}))))
                   [(+ i (if (.isEnabled flg LZ4FrameOutputStream$FLG$Bits/CONTENT_CHECKSUM) 4 0)) pos])
               (let [compressed? (>= word 0)
                     length (if compressed? (block-length src i size) size)]
                 (when dest
                   (when (and block-checksum?
                              (not= (le-int src (+ i size)) (.hash xxhash src (int i) (int size) 0)))
                     (throw (ex-info "LZ4 block checksum mismatch." {:type :lz4-block-checksum})))
                   (if compressed?
                     (.decompress decompressor src (int i) (int size) ^bytes dest (int pos) (int length))
                     (System/arraycopy src i dest pos size)))
                 (recur (+ i size (if block-checksum? 4 0)) (+ pos length))))))))))

#?(:clj
   (defn- frames-content!
     "The content length of the LZ4 frames in `src`, as LZ4FrameInputStream
     reads them (frames in sequence, skippable frames skipped). With `dest` the
     content is decoded into it and the frames' checksums verified."
     ^long [^bytes src dest]
     (let [n (alength src)
           ^XXHash32 xxhash (:xxhash @lz4-codecs)]
       (loop [i 0 pos 0]
         (if (>= i n)
           pos
           (let [magic (le-int src i)]
             (cond
               (== magic 0x184D2204)
               (let [flg (LZ4FrameOutputStream$FLG/fromByte (aget src (+ i 4)))
                     _ (LZ4FrameOutputStream$BD/fromByte (aget src (+ i 5)))
                     sized? (.isEnabled flg LZ4FrameOutputStream$FLG$Bits/CONTENT_SIZE)
                     checksum-at (+ i 6 (if sized? 8 0))]
                 (when (and dest
                            (not= (bit-and (aget src checksum-at) 0xff)
                                  (bit-and (bit-shift-right (.hash xxhash src (int (+ i 4)) (int (- checksum-at i 4)) 0) 8)
                                           0xff)))
                   (throw (ex-info "LZ4 frame descriptor checksum mismatch." {:type :lz4-descriptor-checksum})))
                 (let [[i' pos'] (frame-blocks! src dest (inc checksum-at) pos flg)]
                   (when (and dest sized?
                              (not= (- (long pos') pos)
                                    (.getLong (.order (java.nio.ByteBuffer/wrap src (+ i 6) 8)
                                                      java.nio.ByteOrder/LITTLE_ENDIAN))))
                     (throw (ex-info "LZ4 frame content size mismatch." {:type :lz4-content-size})))
                   (recur (long i') (long pos'))))

               (== (unsigned-bit-shift-right (bit-and magic 0xffffffff) 4) 0x184D2A5)
               (recur (+ i 8 (bit-and (le-int src (+ i 4)) 0xffffffff)) pos)

               :else
               (throw (ex-info "Not an LZ4 frame." {:type :lz4-not-a-frame :magic magic})))))))))

#?(:clj
   (defn- frame-content
     "An input stream of the content of the LZ4 frames `in` holds."
     ^InputStream [^InputStream in]
     (let [src (.readAllBytes in)
           dest (byte-array (frames-content! src nil))]
       (frames-content! src dest)
       (ByteArrayInputStream. dest))))

;; Jolt: the same frames through liblz4 (konserve.jolt.lz4): the serializer's
;; bytes, compressed whole, written as one frame; read back whole.
#?(:jolt
   (defrecord Lz4Compressor [serializer]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [^bytes arr (if (bytes? bytes) bytes (.readAllBytes ^java.io.InputStream bytes))]
         (-deserialize serializer read-handlers (jlz4/decompress arr))))
     (-serialize [_ out write-handlers val]
       (let [bos (java.io.ByteArrayOutputStream.)]
         (-serialize serializer bos write-handlers val)
         (.write ^java.io.OutputStream out ^bytes (jlz4/compress (.toByteArray bos))))))
   :clj
   (defrecord Lz4Compressor [serializer]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (-deserialize serializer read-handlers (frame-content bytes)))
     (-serialize [_ bytes write-handlers val]
       ;; 64 KiB frame blocks. The stream's default is 4 MiB, and both
       ;; LZ4FrameOutputStream and the LZ4FrameInputStream that reads the frame
       ;; back allocate buffers of the frame's block size per value: a few-KB
       ;; value paid ~70 us to write and ~95 us to read in allocation alone
       ;; (13x/7x the 64 KiB cost), for the same compressed size. The input
       ;; stream sizes its buffers from each frame's own descriptor, so frames
       ;; written with the old block size stay readable.
       (let [lz4-byte (LZ4FrameOutputStream. bytes LZ4FrameOutputStream$BLOCKSIZE/SIZE_64KB)]
         (-serialize serializer lz4-byte write-handlers val)
         (.flush lz4-byte)))))

;; Jolt: zstd frames through libzstd (konserve.jolt.zstd), the same shape: the serializer's
;; bytes compressed whole as one frame, read back whole. Header byte 2.
#?(:jolt
   (defrecord ZstdCompressor [serializer]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [^bytes arr (if (bytes? bytes) bytes (.readAllBytes ^java.io.InputStream bytes))]
         (-deserialize serializer read-handlers (jzstd/decompress arr))))
     (-serialize [_ out write-handlers val]
       (let [bos (java.io.ByteArrayOutputStream.)]
         (-serialize serializer bos write-handlers val)
         (.write ^java.io.OutputStream out ^bytes (jzstd/compress (.toByteArray bos)))))))

;; A binary value has no serializer: its bytes are the value. This identity
;; serializer lets a binary payload pass through a store compressor. It closes
;; the stream it writes to, so the compressor's frame is complete (Lz4Compressor
;; only flushes, which leaves an LZ4 frame without its end mark: Fressian stops
;; reading before it, a raw read of the whole stream does not).
#?(:clj
   (def ^:private octets-serializer
     (reify PStoreSerializer
       (-serialize [_ out _write-handlers octets]
         (.write ^java.io.OutputStream out ^bytes octets)
         (.close ^java.io.OutputStream out))
       (-deserialize [_ _read-handlers in]
         in))))

#?(:clj
   (defn compress-octets
     "`octets` passed through `compressor`, or nil when that does not make them
     smaller."
     [compressor ^bytes octets]
     (let [out (java.io.ByteArrayOutputStream.)]
       (-serialize (compressor octets-serializer) out nil octets)
       (let [compressed (.toByteArray out)]
         (when (< (alength compressed) (alength octets))
           compressed)))))

#?(:jolt (declare zstd-compressor))

#?(:jolt
   (defn decompressing-stream
     "An input stream of the octets `compress-octets` passed through
     `compressor`: the frame read whole and decompressed by its codec."
     ^java.io.InputStream [compressor in]
     (let [frame (.readAllBytes ^java.io.InputStream in)]
       (java.io.ByteArrayInputStream. (if (= compressor zstd-compressor) (jzstd/decompress frame) (jlz4/decompress frame)))))
   :clj
   (defn decompressing-stream
     "An input stream of the octets `compress-octets` passed through
     `compressor`, read from `in` as it is consumed, so a range read
     decompresses only up to its end. Only an LZ4 compressor makes octets
     smaller, so its frames are what `in` holds."
     ^java.io.InputStream [_compressor in]
     (LZ4FrameInputStream. in)))

(defn null-compressor [serializer]
  (NullCompressor. serializer))

(defn unsupported-lz4-compressor [serializer]
  (UnsupportedLZ4Compressor. serializer))

#?(:jolt
   (defn zstd-compressor [serializer]
     (ZstdCompressor. serializer)))

#?(:jolt
   (defn lz4-compressor [serializer]
     (Lz4Compressor. serializer))
   :clj
   (defn lz4-compressor [serializer]
     (Lz4Compressor. serializer)))

#?(:clj
   (defmacro native-image-build? []
     (try
       ;(and 
       (Class/forName "org.graalvm.nativeimage.ImageInfo")
        ;(eval '(org.graalvm.nativeimage.ImageInfo/inImageBuildtimeCode))) ;; TODO: when will this be uncommented?
       (catch Exception _
         false))))

(def byte->compressor
  {0 null-compressor
   1 #?(:jolt lz4-compressor
        :clj (if (native-image-build?)
               unsupported-lz4-compressor
               lz4-compressor)
        :cljs unsupported-lz4-compressor)
   #?@(:jolt [2 zstd-compressor])})

(def compressor->byte
  (invert-map byte->compressor))

(defn get-compressor [type]
  (case type
    #?@(:jolt [:zstd zstd-compressor])
    :lz4 #?(:jolt lz4-compressor
            :clj lz4-compressor
            :cljs unsupported-lz4-compressor)
    null-compressor))
