(ns konserve.tiered-populate-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.memory :as mem]
            [konserve.tiered :as tiered]))

(defn- settled-keys
  "The frontend's keys once every asynchronous warm a read started has landed."
  [front]
  (async/<!! (async/timeout 100))
  (set (map :key (k/keys front {:sync? true}))))

(deftest reads-populate-the-frontend-only-when-asked
  (doseq [[populate? expected] [[true #{:inherited :other :local}] [false #{:local}]]]
    (testing (str ":populate-frontend? " populate?)
      (let [opts {:sync? true}
            front (mem/new-mem-store (atom {}) opts)
            back (mem/new-mem-store (atom {}) opts)
            store (tiered/connect-tiered-store front back :write-policy :frontend-only
                                               :populate-frontend? populate? :opts opts)]
        (k/assoc back :inherited 1 opts)
        (k/assoc back :other 2 opts)
        (k/assoc store :local 3 opts)
        (is (= 1 (k/get store :inherited nil opts)))
        (is (= {:other 2} (k/multi-get store [:other] opts)))
        (is (= 3 (k/get store :local nil opts)))
        (is (= expected (settled-keys front)))
        (is (= #{:inherited :other} (set (map :key (k/keys back opts)))))))))
