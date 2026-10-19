(ns konserve.lz4-fixture-values
  "Values whose LZ4 frames, written by the writers in use before the frame
  reader changed, are fixtures under konserve/lz4_frames: `<name>.64k.lz4` from
  the 64 KiB block writer, `<name>.4m.lz4` from the 4 MiB default it replaced.")

(defn- seeded-longs [seed n]
  (let [r (java.util.Random. seed)] (vec (repeatedly n #(.nextLong r)))))

(def values
  {;; one uncompressed block: LZ4 cannot shrink a value this small
   :tiny {:datahike/parents #{#uuid "6ad06955-68e9-553d-8734-dd02acf670d9"}
          :datahike/generation 1727}
   ;; one compressed block of a few KB, the size of an index node
   :node {:keys (vec (for [i (range 200)] [i :seon.fn/name (str "my.ns/fn-" (mod i 17)) 536870912]))
          :level 0}
   ;; several compressed blocks
   :multi {:xs (vec (for [i (range 30000)] (str "datom-" (mod i 50))))}
   ;; several blocks stored uncompressed (random bytes do not compress)
   :incompressible {:longs (seeded-longs 42 20000)}})

(def octets
  "A binary value of several blocks, compressible."
  (.getBytes ^String (apply str (repeat 20000 "konserve binary value ")) "UTF-8"))
