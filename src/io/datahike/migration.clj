(ns io.datahike.migration
  (:require
   [clj-cbor.core :as cbor]
   [clojure.data :as data]
   [clojure.java.io :as io]
   [datahike.api :as d]
   [datahike.schema :as ds]
   [taoensso.timbre :as log]
   [clojure.string :as string])
  (:import
   [java.io OutputStream]))

(defn is-system-keyword? [value]
  (and (or (keyword? value) (string? value))
       (if-let [ns (namespace (keyword value))]
         (= "db" (first (string/split ns #"\.")))
         false)))

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

(defn load-test-db [config]
  (when-not (d/database-exists?)
    (println "Creating test database...")
    (d/create-database config)
    (println "Done."))
  (let [conn (d/connect config)]
    (println "Creating test schema...")
    (d/transact conn schema)
    (println "Done.")
    (println "Creating test data...")
    (time
     (doall
      (repeatedly 10
                  (fn []
                    (d/transact conn (vec
                                      (repeatedly 100
                                                  (fn [] {:age  (long (rand-int 10000))
                                                          :name (str (rand-int 10000))}))))))))
    (println "Done.")
    true))

(defn ping [x]
  (println (format "PING: %s" (str x))))

(defn get-txs [conn]
  (->> (d/q '[:find [(pull ?t [*]) ...]
              :where
              [?e _ _ ?t]]
            @conn)
       (remove #(#{#inst"1970-01-01T00:00:00.000-00:00"} (:db/txInstant %)))
       (sort-by :db/id)))

(defn find-tx-datoms [conn tx]
  (let [db @conn
        attribute-refs?  (-> @conn :config :attribute-refs?)
        query (cond->
               '{:find  [?e ?a ?v ?t ?s]
                 :in    [$ ?t]
                 :where [[?e ?a ?v ?t ?s]
                         [(not= ?e ?t)]]}
                attribute-refs? (assoc :where '[[?e ?aid ?v ?t ?s]
                                                                    [?aid :db/ident ?a]
                                                                    [(not= ?e ?t)]]))]
    (->> tx
         (d/q query (d/history db))
         (map (fn [[_ a v _ _ :as datom]]
                (if (and attribute-refs?
                         (is-system-keyword? a)
                         (number? v))
                  (assoc datom 2 (get (:ref-ident-map db) v))
                  datom)))
         (sort-by first)
         (into []))))

(defn extract-db [conn filename]
  (let [txs (get-txs conn)]
    (with-open [^OutputStream out (io/output-stream (io/file filename))]
      (.write out (cbor/encode (dissoc (:config @conn) :store)))
      (.write out (cbor/encode (:meta @conn)))
      (doseq [{:keys [db/id db/txInstant]} txs]
        (.write out (cbor/encode (into [[id txInstant]]
                                       (find-tx-datoms conn id))))))))

(defn export-db [{:keys [config filename]}]
  (if (d/database-exists? config)
    (let [conn (d/connect config)]
      (extract-db conn filename))
    (throw (ex-info "Database does not exist." {:config config}))))

(defn compatible-cfg? [old-cfg new-cfg]
  (let [[in-old in-new _] (data/diff (dissoc old-cfg :store :name :attribute-refs?)
                                     (dissoc new-cfg :store :name :attribute-refs?))]
    (and (nil? in-old)
         (nil? in-new))))

(defn prepare-tx-data [conn txs ident-map]
  (let [db @conn
        ident-ref-map (:ident-ref-map db)
        attribute-refs? (-> db :config :attribute-refs?)
        max-eid (atom (inc (:max-eid db)))]
    (reduce (fn [coll [e a v _ added]]
              (let [new-eid (or (get @ident-map e) @max-eid (d/tempid :db/sys))
                    new-aid (if attribute-refs?
                              (get ident-ref-map a)
                              a)]
                (swap! ident-map assoc e new-eid)
                (when (= new-eid @max-eid)
                  (swap! max-eid inc))
                (conj coll (if (true? added)
                             [:db/add new-eid new-aid v]
                             [:db/retract new-eid new-aid v]))))
            []
            txs)))

(defn load-db [conn filename]
  (time (let [[old-cfg _old-meta & txs] (cbor/slurp-all filename)
              cfg (:config @conn)
              ident-map (atom {})]
          (if (compatible-cfg? old-cfg cfg)
            (doseq [[[_tid txInstant] & datoms] txs]
              (let [tx-data (prepare-tx-data conn datoms ident-map)
                    tx-meta {:db/txInstant (java.util.Date/from txInstant)}]
                (d/transact conn {:tx-data tx-data
                                  :tx-meta tx-meta})))
            (throw (ex-info "Incompatible configuration!" {:type            :incompatible-config
                                                           :imported-config old-cfg
                                                           :config          (:config @conn)
                                                           :cause           (data/diff (dissoc old-cfg :store)
                                                                                       (dissoc cfg :store))}))))))

(defn import-db [config exported-db-path]
  (if (d/database-exists? config)
    (throw (ex-info "Target database already exists." {:config config}))
    (let [conn (do (d/create-database config)
                   (d/connect config))]
      (load-db conn exported-db-path))))

(comment

  (log/set-level! :warn)

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

  (load-test-db conn)

  (export-db conn "db_export.cbor")

  (def cfg2 {:store {:backend :file
                     ;;:id "import"
                     :path "/tmp/import.dh"}
             :keep-history? true
             :schema-flexibility :write
             :index :datahike.index/persistent-set
             :attribute-refs? true})

  (d/database-exists? cfg2)
  (d/delete-database cfg2)

  (def import-path "1674757273892-datahike-export.cbor")

  (import-db cfg2 "db_export.cbor")

  (def conn2 (d/connect cfg2))

  ;
  )
