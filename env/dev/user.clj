(ns user
  (:require
   [datahike.api :as d]
   [hashp.core]
   [sherpa.core :refer [get-txs find-tx-datoms export-db import-db]]
   [sherpa.utils :refer [load-test-db]]
   [taoensso.timbre :as log]))

;; eval this file using the dev profile


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

  (d/delete-database cfg)

  (def cfg (slurp "bb/resources/import-test-config.edn"))

  (def conn (d/connect cfg))

  (load-test-db {:config cfg :tx-count 100})

  (get-txs conn)

  (find-tx-datoms conn 536870913)

  (d/q '{:find  [?e ?a ?v ?t ?s]
         :in    [$ ?t]
         :where [[?e ?a ?v ?t ?s]
                 [(not= ?e ?t)]]}
       (d/history @conn) 536870914)

  (find-tx-datoms conn 536870914)

  (export-db {:config cfg
              :filename "db_export2.cbor"})

  (def cfg2 {:store {:backend :mem
                     :id "import"
                     ;; :path "/tmp/import.dh"
                     }
             :keep-history? true
             :schema-flexibility :write
             :index :datahike.index/persistent-set
             :attribute-refs? false})

  (d/database-exists? cfg2)
  (d/delete-database cfg2)

  (import-db {:config cfg2
              :export-file "db_export2.cbor"})

  (def conn2 (d/connect cfg2))

  (:config @conn2)
  [(take 20 (find-tx-datoms conn 536870914))
   (take 20 (find-tx-datoms conn2 536870914))]

  ;
  )
