(ns konserve.filestore-multi-key-crash-child
  (:require [konserve.core :as k]
            [konserve.filestore :as filestore]))

(defn -main
  "Run one filestore batch and pause at the requested crash stage."
  [store-path pause-stage]
  (let [pause-stage (keyword pause-stage)
        stage-hook (fn [{::filestore/keys [stage]}]
                     (when (= pause-stage stage)
                       (println (str "READY " (name stage)))
                       (flush)
                       (read-line)))
        store (filestore/connect-fs-store store-path :opts {:sync? true})]
    (with-bindings {(requiring-resolve 'konserve.filestore/*multi-write-stage-hook*)
                    stage-hook}
      (k/multi-assoc store
                     [[:new-node-a {:node/value :new-a}]
                      [:new-node-b {:node/value :new-b}]
                      [:branch {:branch/refs [:new-node-a :new-node-b]}]]
                     {:sync? true}))))
