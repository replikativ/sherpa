(ns io.datahike.migration
  (:require [datahike.api :as d]
            [clj-cbor.core :as cbor]
            [clojure.data :as data]
            [clojure.java.io :as io]
            [taoensso.timbre :as log])
  (:import [java.io BufferedWriter OutputStream]
           (java.text DateFormat)))

(log/set-level! :warn)

(def schema [{:db/ident :name
              :db/cardinality :db.cardinality/one
              :db/index true
              :db/unique :db.unique/identity
              :db/valueType :db.type/string}
             {:db/ident :sibling
              :db/cardinality :db.cardinality/many
              :db/valueType :db.type/ref}
             {:db/ident :age
              :db/cardinality :db.cardinality/one
              :db/valueType :db.type/long}])

(def cfg {:store {:backend :mem
                  :id "export"}
          :keep-history? true
          :schema-flexibility :write
          :index :datahike.index/persistent-set
          :attribute-refs? true})

(def conn (do
            (d/delete-database cfg)
            (d/create-database cfg)
            (d/connect cfg)))

(d/transact conn schema)

(d/q '[:find (count ?e)
       :where
       [?e :name _]]
     @conn)

(defn load-test-db []
  (time (doall (repeatedly 1000 (fn [] (d/transact conn (vec (repeatedly 1000 (fn [] {:age  (long (rand-int 10000))
                                                                                     :name (str (rand-int 10000))}))))))))
  true)

(defn get-txs [conn]
  (->> (d/q '[:find [(pull ?t [*]) ...]
              :where
              [?e _ _ ?t]]
            @conn)
       (remove #(#{#inst"1970-01-01T00:00:00.000-00:00"} (:db/txInstant %)))
       (sort-by :db/id)))

(defn find-tx-datoms [conn tx]
  (let [query (cond->
                '{:find  [?e ?a ?v ?t ?s]
                  :in    [$ ?t]
                  :where [[?e ?a ?v ?t ?s]
                          [(not= ?e ?t)]]}
                (-> @conn :config :attribute-refs?) (assoc :where '[[?e ?aid ?v ?t ?s]
                                                                    [?aid :db/ident ?a]
                                                                    [(not= ?e ?t)]]))]
    (into []
          (sort-by first (d/q query
                              (d/history @conn) tx)))))

(->> (get-txs conn)
     last
     :db/id
     (find-tx-datoms conn)
     )

(into [[123 45234]]
      (find-tx-datoms conn 536870913))

(sort-by first (d/q '[:find (count ?e)
                      :in $
                      :where
                      [?e ?a ?v ?t false]]
                    (d/history @conn)
                    ))

(->> (d/q '[:find ?a
            :where
            [?e :age ?a]] (d/history @conn))
     (map first)
     (into #{})
     count)

(load-test-db)

(defn export-db [conn filename]
  (let [txs (get-txs conn)]
    (with-open [^OutputStream out (io/output-stream (io/file filename))]
      (.write out (cbor/encode (dissoc (:config @conn) :store)))
      (.write out (cbor/encode (:meta @conn)))
      (doseq [{:keys [db/id db/txInstant]} txs]
        (.write out (cbor/encode (into [[id txInstant]]
                                       (find-tx-datoms conn id))))))))

(def cfg2 {:store {:backend :mem
                   :id "imported"}
           :keep-history? true
           :schema-flexibility :write
           :index :datahike.index/persistent-set
           :attribute-refs? true})

(def conn2 (do
             (d/delete-database cfg2)
             (d/create-database cfg2)
             (d/connect cfg2)))

(export-db conn "db_export.cbor")

(defn compatible-cfg? [old-cfg new-cfg]
  (let [[in-old in-new _] (data/diff (dissoc old-cfg :store :name)
                                     (dissoc new-cfg :store :name))]
    (and (nil? in-old)
         (nil? in-new))))

(let [[old-cfg old-meta & txs] (cbor/slurp-all "db_export.cbor")
      new-cfg (:config @conn2)]
  (if (compatible-cfg? old-cfg new-cfg)
    (doseq [[[tid txInstant] & tx-data]  txs]
      (let [ident-ref-map (:ident-ref-map @conn2)]
        (d/transact conn2 {:tx-data (map (fn [datom] (into [:db/add]
                                                           (update datom 1 (fn [a] (ident-ref-map a))))) tx-data)
                           :tx-meta {:db/txInstant (java.util.Date/from txInstant)}})))
    (throw (ex-info "Incompatible configuration!" {:type :incompatible-config
                                                   :imported-config old-cfg
                                                   :config (:config @conn2)
                                                   :cause (data/diff (dissoc old-cfg :store)
                                                                     (dissoc new-cfg :store))}))))

(with-out-str (clojure.pprint/pprint @conn))

(d/q '[:find (count ?e)
       :where [?e _ _ _]]
     @conn2)


(def imported (cbor/slurp-all "db_export.cbor"))

(def imported-txs (-> imported rest rest))


(nth imported 3)