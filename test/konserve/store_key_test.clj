(ns konserve.store-key-test
  (:require [clojure.test :refer [deftest is]]
            [hasch.core :as hasch]
            [konserve.impl.defaults :refer [key->store-key]])
  (:import [java.util Random]))

(defrecord Key [a b])

(deftest store-keys-are-hasch-uuids
  (let [rnd (Random. 11)
        keys (concat [:a "a" 1 1.5 nil true [] {} #{} '() 'sym
                      [:datahike/commit-graph #uuid "6ad06955-68e9-553d-8734-dd02acf670d9"]
                      #uuid "6ad06955-68e9-553d-8734-dd02acf670d9"
                      {:nested [1 #{2 3} {:x "y"}]} (->Key 1 2) (byte-array [1 2 3])]
                     (repeatedly 500 #(vector (keyword (str "k" (.nextInt rnd 100))) (.nextLong rnd)
                                              (str (.nextInt rnd)) (java.util.UUID. (.nextLong rnd) (.nextLong rnd)))))]
    (doseq [k keys]
      (is (= (str (hasch/uuid k) ".ksv") (key->store-key k)) (pr-str k)))))
