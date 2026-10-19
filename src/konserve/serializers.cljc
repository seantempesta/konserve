(ns konserve.serializers
  (:require #?(:jolt [clojure.core] :clj [clj-cbor.core :as cbor])
            #?(:jolt [clojure.core] :clj [clojure.data.fressian :as fress] :cljs [fress.api :as fress])
            [konserve.protocols :refer [PStoreSerializer]]
            [incognito.fressian :refer [incognito-read-handlers incognito-write-handlers]]
            [incognito.edn :refer [read-string-safe]])
  #?(:jolt (:import) :clj (:import [java.io InputStream]
                   [org.fressian FressianReader]
                   [org.fressian.handlers ILookup])))

#?(:jolt nil :clj
   (defrecord CBORSerializer [codec]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (when-not (empty? @read-handlers)
         (throw (ex-info "Read handlers not supported yet." {:type :handlers-not-supported-yet})))
       (cbor/decode codec bytes))
     (-serialize [_ bytes write-handlers val]
       (when-not (empty? @write-handlers)
         (throw (ex-info "Write handlers not supported yet." {:type :handlers-not-supported-yet})))
       (cbor/encode codec bytes val))))

#?(:jolt nil :clj
   (defn cbor-serializer
     ([] (cbor-serializer {} {}))
     ([read-handlers write-handlers]
      (let [codec (cbor/cbor-codec
                   :write-handlers (merge cbor/default-write-handlers write-handlers)
                   :read-handlers (merge cbor/default-read-handlers read-handlers))]
        (map->CBORSerializer {:codec codec})))))

(defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
  #?@(:cljs (INamed ;clojure.lang.Named
             (-name [_] "FressianSerializer")
             (-namespace [_] "konserve.serializers")))
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    #?(:jolt (throw (ex-info "jolt: no fressian reader" {}))
       :cljs (fress/read bytes :handlers (merge custom-read-handlers (incognito-read-handlers read-handlers)))
       ;; The handler a tag names is the one `(merge clojure custom incognito)`
       ;; would hold, looked up in each map instead of merged per read. The
       ;; reader takes the stream as it is: `fress/read` would wrap it in a
       ;; BufferedInputStream with an 8 KiB buffer.
       :clj (let [incognito (incognito-read-handlers read-handlers)
                  lookup (reify ILookup
                           (valAt [_ tag]
                             (or (get incognito tag)
                                 (get custom-read-handlers tag)
                                 (get fress/clojure-read-handlers tag))))]
              (.readObject (FressianReader. ^InputStream bytes lookup false)))))
  (-serialize [_ #?(:clj bytes :cljs _) write-handlers val]
    (let [handlers #?(:jolt nil :clj (-> (merge
                                fress/clojure-write-handlers
                                custom-write-handlers
                                (incognito-write-handlers write-handlers))
                               fress/associative-lookup
                               fress/inheritance-lookup)
                      ;; CLJS fress expects flat {Type fn} format.
                      ;; Custom handlers must provide this format directly.
                      :cljs (merge custom-write-handlers
                                   (incognito-write-handlers write-handlers)))]
      #?(:jolt (throw (ex-info "jolt: no fressian writer" {}))
         :clj (let [writer (fress/create-writer bytes :handlers handlers)]
                (fress/write-object writer val))
         :cljs (fress/write val :handlers handlers)))))

(defn fressian-serializer
  ([] (fressian-serializer {} {}))
  ([read-handlers write-handlers] (map->FressianSerializer {:custom-read-handlers read-handlers
                                                            :custom-write-handlers write-handlers})))

(defrecord StringSerializer []
  #?@(:cljs (INamed
             (-name [_] "StringSerializer")
             (-namespace [_] "konserve.serializers")))
  PStoreSerializer
  ;; DefaultStore hands a serializer an InputStream / OutputStream on the JVM
  ;; (and on Jolt). The previous body bound *out* to the OutputStream, which
  ;; throws ClassCastException (a ByteArrayOutputStream is not a Writer), and
  ;; passed the InputStream to edn/read-string. EDN text is stored as UTF-8.
  (-deserialize [_ read-handlers s]
    (read-string-safe @read-handlers
                      #?(:cljs s
                         :clj (if (string? s)
                                s
                                (String. ^bytes (.readAllBytes ^java.io.InputStream s) "UTF-8")))))
  (-serialize [_ #?(:clj output-stream :cljs _) _ val]
    #?(:cljs (pr-str val)
       :clj (.write ^java.io.OutputStream output-stream
                    ^bytes (.getBytes ^String (binding [*print-length* nil *print-level* nil]
                                                (pr-str val))
                                      "UTF-8")))))

(defn string-serializer []
  (map->StringSerializer {}))

(defn construct->class [m]
  (->> (map (fn [[k v]] [#?(:clj (class v)
                            :cljs (type v)) k]) m)
       (into {})))

(def byte->serializer
  {0 (string-serializer)
   1 (fressian-serializer)
   #?@(:jolt [] :clj [2 (cbor-serializer)])})

(def serializer-class->byte
  (construct->class byte->serializer))

(defn construct->keys [m]
  (->> (map (fn [[_ v]]
              [#?(:clj (-> v class .getSimpleName keyword)
                  :cljs (-> v name keyword)) v]) m)
       (into {})))

(def key->serializer
  (construct->keys byte->serializer))

(defn construct->byte [m n]
  (->> (map (fn [[k0 _v0] [k1 _v1]] [k0 k1]) m n)
       (into {})))

(def byte->key
  (construct->byte byte->serializer key->serializer))
