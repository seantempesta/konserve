(ns konserve.filestore-multi-key-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.filestore :as filestore]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as storage-layout]
            [konserve.utils :as utils])
  (:import [java.util UUID]
           [java.util.concurrent TimeUnit]))

(defn- test-store-path
  []
  (str "target/filestore-multi-key-test/" (UUID/randomUUID)))

(defn- java-command
  [store-path stage]
  [(str (System/getProperty "java.home") "/bin/java")
   "-cp" (System/getProperty "java.class.path")
   "clojure.main" "-m" "konserve.filestore-multi-key-crash-child"
   store-path (name stage)])

(defn- kill-at-stage!
  [store-path stage]
  (let [process (.start (ProcessBuilder. ^java.util.List (java-command store-path stage)))
        ready-line (deref (future (.readLine (io/reader (.getInputStream process))))
                          30000
                          ::startup-timeout)]
    (when (= ::startup-timeout ready-line)
      (.destroyForcibly process)
      (throw (ex-info "Crash child did not publish its stage within 30 seconds."
                      {:stage stage})))
    (when-not (= (str "READY " (name stage)) ready-line)
      (.destroyForcibly process)
      (throw (ex-info "Crash child exited before reaching the requested stage."
                      {:stage stage :child-output ready-line})))
    (.destroyForcibly process)
    (when-not (.waitFor process 30 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (throw (ex-info "SIGKILL did not terminate the crash child within 30 seconds."
                      {:stage stage})))
    (is (not (zero? (.exitValue process)))
        "The crash child must be killed, not exit normally.")))

(defn- reachable-state
  [store]
  (let [branch (k/get store :branch nil {:sync? true})
        refs (:branch/refs branch)]
    {:branch branch
     :facts (mapv #(k/get store % ::missing {:sync? true}) refs)}))

(deftest filestore-advertises-multi-key-support
  (let [path (test-store-path)]
    (try
      (is (true? (utils/multi-key-capable?
                  (filestore/connect-fs-store path :opts {:sync? true}))))
      (finally
        (filestore/delete-store path)))))

(deftest durable-batch-orders-forces-before-publication
  (let [path (test-store-path)
        events (atom [])]
    (try
      (let [store (filestore/connect-fs-store path :opts {:sync? true})]
        (with-bindings {#'filestore/*multi-write-stage-hook* #(swap! events conj %)}
          (k/multi-assoc store [[:a 1] [:b 2] [:root {:refs [:a :b]}]]
                         {:sync? true})))
      (let [stages (into []
                         (comp (map ::filestore/stage)
                               (filter #{:blob-forced :blob-moved :directory-forced}))
                         @events)]
        (is (= [:blob-forced :blob-moved :directory-forced
                :blob-forced :blob-moved :directory-forced
                :blob-forced :blob-moved :directory-forced]
               stages)
            "Each batch member retains the fallback's force/move/directory barrier."))
      (finally
        (filestore/delete-store path)))))

(deftest duplicate-keys-apply-in-sequence-order
  (let [path (test-store-path)]
    (try
      (let [store (filestore/connect-fs-store path :opts {:sync? true})]
        (is (= {:same true}
               (k/multi-assoc store [[:same 1] [:same 2]] {:sync? true})))
        (is (= 2 (k/get store :same ::missing {:sync? true}))))
      (finally
        (filestore/delete-store path)))))

(deftest multi-get-closes-every-acquired-blob-on-read-failure
  (let [path (test-store-path)
        close-count (atom 0)]
    (try
      (let [store (filestore/connect-fs-store path :opts {:sync? true})
            original-close storage-layout/-close]
        (k/multi-assoc store [[:a 1] [:b 2]] {:sync? true})
        (with-redefs [defaults/read-blob (fn [& _]
                                           (throw (ex-info "injected read failure" {})))
                      storage-layout/-close (fn [blob env]
                                              (swap! close-count inc)
                                              (original-close blob env))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"injected read failure"
                                (k/multi-get store [:a :b] {:sync? true}))))
        (is (= 2 @close-count)
            "The outer multi-read owner closes unread blobs after a failure."))
      (finally
        (filestore/delete-store path)))))

(deftest duplicate-multi-get-keys-open-one-blob
  (let [path (test-store-path)
        close-count (atom 0)]
    (try
      (let [store (filestore/connect-fs-store path :opts {:sync? true})
            original-close storage-layout/-close]
        (k/assoc store :a 1 {:sync? true})
        (with-redefs [storage-layout/-close (fn [blob env]
                                              (swap! close-count inc)
                                              (original-close blob env))]
          (is (= {:a 1} (k/multi-get store [:a :a] {:sync? true}))))
        (is (= 1 @close-count)))
      (finally
        (filestore/delete-store path)))))

(deftest killed-batch-never-publishes-a-torn-branch
  (doseq [stage [:staged :after-first-move :before-last-move :after-last-move]]
    (testing (str "SIGKILL at " stage)
      (let [path (test-store-path)]
        (try
          (let [store (filestore/connect-fs-store path :opts {:sync? true})]
            (k/assoc store :old-node {:node/value :old} {:sync? true})
            (k/assoc store :branch {:branch/refs [:old-node]} {:sync? true}))
          (kill-at-stage! path stage)
          (let [reopened (filestore/connect-fs-store path :opts {:sync? true})
                state (reachable-state reopened)]
            (is (contains?
                 #{{:branch {:branch/refs [:old-node]}
                    :facts [{:node/value :old}]}
                   {:branch {:branch/refs [:new-node-a :new-node-b]}
                    :facts [{:node/value :new-a} {:node/value :new-b}]}}
                 state)
                (str "Reopened branch and reachable facts must be wholly old or wholly new: "
                     (pr-str state))))
          (finally
            (filestore/delete-store path)))))))
