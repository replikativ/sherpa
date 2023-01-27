(ns sherpa.core
  (:require
   [clj-cbor.core :as cbor]
   [clojure.data :as data]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [datahike.api :as d]
   [taoensso.timbre :as log])
  (:import
   [java.io OutputStream]))

(defn is-system-keyword? [value]
  (and (or (keyword? value) (string? value))
       (if-let [nmsp (namespace (keyword value))]
         (= "db" (first (string/split nmsp #"\.")))
         false)))

(defn get-txs
  "Find all transactions without system transactions, sorted by db/id."
  [conn]
  (->> (d/q '[:find [(pull ?t [*]) ...]
              :where
              [?e _ _ ?t]]
            @conn)
       (remove #(#{#inst"1970-01-01T00:00:00.000-00:00"} (:db/txInstant %)))
       (sort-by :db/id)))

(defn find-tx-datoms
  "Find all datoms given a database connection `conn` and a transaction id `tx`.
    "
  [conn tx]
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
  (let [_ (println "Extracting transactions...")
        start-time (System/currentTimeMillis)
        txs (get-txs conn)
        _ (println "Done.")
        counter (atom 0)
        tx-count (count txs)]
    (println (format "Writing %s transactions to %s..." tx-count filename))
    (with-open [^OutputStream out (io/output-stream (io/file filename))]
      (.write out (cbor/encode (dissoc (:config @conn) :store)))
      (.write out (cbor/encode (:meta @conn)))
      (doseq [{:keys [db/id db/txInstant]} txs]
        (.write out (cbor/encode (into [[id txInstant]]
                                       (find-tx-datoms conn id))))
        (when (= 0 (mod @counter 10))
          (println (format "%.2f %%" (float (* 100.0 (/ @counter tx-count))))))
        (swap! counter inc)))
    (println (format "Total time: %s secs" (/ (- (System/currentTimeMillis) start-time) 1000.0)))
    (println "Done.")))

(defn export-db [{:keys [config filename]}]
  (if (d/database-exists? config)
    (let [conn (d/connect config)]
      (extract-db conn filename))
    (throw (ex-info "Database does not exist." {:config config}))))

(defn compatible-cfg? [old-cfg new-cfg]
  (let [[in-old in-new _] (data/diff (select-keys old-cfg [:keep-history? :schema-flexibility])
                                     (select-keys new-cfg [:keep-history? :schema-flexibility]))]
    (and (nil? in-old)
         (nil? in-new))))

(defn prepare-tx-data [conn txs ident-map]
  (let [db @conn
        ident-ref-map (:ident-ref-map db)
        attribute-refs? (-> db :config :attribute-refs?)
        max-eid (atom (inc (:max-eid db)))
        ref-attribute? (or (:db.type/ref (:rschema db)) #{})]
    (reduce (fn [coll [e a v _ added]]
              (let [new-eid (or (get @ident-map e) @max-eid)
                    _ (when (= new-eid @max-eid)
                        (swap! ident-map assoc e new-eid)
                        (swap! max-eid inc))
                    new-aid (if attribute-refs?
                              (get ident-ref-map a)
                              a)
                    new-v (if (ref-attribute? a)
                            (or (get @ident-map v) @max-eid)
                            v)
                    _ (when (and (ref-attribute? a)
                                 (= new-v @max-eid))
                        (swap! ident-map assoc v new-v)
                        (swap! max-eid inc))]
                (conj coll (if (true? added)
                             [:db/add new-eid new-aid new-v]
                             [:db/retract new-eid new-aid new-v]))))
            []
            txs)))

(defn load-db [conn filename]
  (log/set-level! :warn)
  (let [start-time (System/currentTimeMillis)
        _ (println (format "Reading transactions from %s..." filename))
        [old-cfg _old-meta & txs] (cbor/slurp-all filename)
        _ (println "Done.")
        cfg (:config @conn)
        ident-map (atom {})
        counter (atom 0)]
    (if (compatible-cfg? old-cfg cfg)
      (do
        (println "Importing transactions ...")
        (doseq [[[_tid txInstant] & datoms] txs]

          (let [tx-data (prepare-tx-data conn datoms ident-map)
                tx-meta {:db/txInstant (java.util.Date/from txInstant)}]
            (d/transact conn {:tx-data tx-data
                              :tx-meta tx-meta}))
          (when (= 0 (mod @counter 10))
            (println (format "%s transactions imported." @counter)))
          (swap! counter inc))
        (println (format "Total transactions imported: %s" @counter))
        (println (format "Total time: %s secs" (/ (- (System/currentTimeMillis) start-time) 1000.0)))
        (println "Done."))
      (throw (ex-info "Incompatible configuration!" {:type            :incompatible-config
                                                     :imported-config old-cfg
                                                     :config          (:config @conn)
                                                     :cause           (data/diff (dissoc old-cfg :store)
                                                                                 (dissoc cfg :store))})))))

(defn import-db [{:keys [config export-file]}]
  (if (d/database-exists? config)
    (throw (ex-info "Target database already exists." {:config config}))
    (let [conn (do (println "Creating database...")
                   (d/create-database config)
                   (println "Done.")
                   (d/connect config))]
      (load-db conn export-file))))

