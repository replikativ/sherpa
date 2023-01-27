(ns sherpa.utils
  (:require [datahike.api :as d]))

(defn load-test-db
  "Simple helper function to create test databases."
  [{:keys [config tx-count] :or {tx-count 100}}]
  (when-not (d/database-exists?)
    (println "Creating test database...")
    (d/create-database config)
    (println "Done."))
  (let [conn (d/connect config)
        schema [{:db/ident :name
                 :db/cardinality :db.cardinality/one
                 :db/index true
                 :db/unique :db.unique/identity
                 :db/valueType :db.type/string}
                {:db/ident :sibling
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/ref}
                {:db/ident :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/long}]]
    (println "Creating test schema...")
    (d/transact conn schema)
    (println "Done.")
    (println "Creating test data...")
    (let [start-time (System/currentTimeMillis)
          counter (atom 0)]
      (doall
       (repeatedly tx-count
                   (fn []
                     (d/transact conn (vec
                                       (repeatedly 1000
                                                   (fn [] {:age  (long (rand-int (* 100 tx-count)))
                                                           :name (str (rand-int (* 100 tx-count)))
                                                           :sibling [{:age  (long (rand-int (* 100 tx-count)))
                                                                      :name (str (rand-int (* 100 tx-count)))}]}))))
                     (when (= 0 (mod @counter 10))
                       (println (format "%.2f %%" (float (* 100.0 (/ @counter tx-count))))))
                     (swap! counter inc))))
      (println (format "%s entities in %s transactions generated." (d/q '[:find (count ?e) . :where [?e _ _ _]] @conn)  tx-count))
      (println (format "Total time: %s secs" (/ (- (System/currentTimeMillis) start-time) 1000.0))))
    (println "Done.")
    true))
