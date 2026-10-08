(ns konserve.compressor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]])
  #?(:clj (:import [net.jpountz.lz4 LZ4FrameOutputStream LZ4FrameOutputStream$BLOCKSIZE
                    LZ4FrameInputStream])))

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

#?(:clj
   (defrecord Lz4Compressor [serializer]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [lz4-byte (LZ4FrameInputStream. bytes)]
         (-deserialize serializer read-handlers lz4-byte)))
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

#?(:clj
   (defn decompressing-stream
     "An input stream of the octets `compress-octets` passed through
     `compressor`, read from `in`."
     ^java.io.InputStream [compressor in]
     (-deserialize (compressor octets-serializer) nil in)))

(defn null-compressor [serializer]
  (NullCompressor. serializer))

(defn unsupported-lz4-compressor [serializer]
  (UnsupportedLZ4Compressor. serializer))

#?(:clj
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
   1 #?(:clj (if (native-image-build?)
               unsupported-lz4-compressor
               lz4-compressor)
        :cljs unsupported-lz4-compressor)})

(def compressor->byte
  (invert-map byte->compressor))

(defn get-compressor [type]
  (case type
    :lz4 #?(:clj lz4-compressor
            :cljs unsupported-lz4-compressor)
    null-compressor))
