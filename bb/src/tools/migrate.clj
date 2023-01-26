(ns tools.migrate
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [babashka.process :as p]))

(defn clj [opts & args] (apply p/shell opts "clojure" args))

(defn create-test-db [version config-path & _args]
  (let [config (edn/read-string (slurp config-path))]
    (clj {:dir "."}
         "-Sdeps" (str (format
                        "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}"
                        (or version "0.6.1531")))
         "-X" "io.datahike.migration/load-test-db" config)))

(defn -main [version config-path & _args]
  (let [export-file (format "/tmp/%s-datahike-%s.cbor" (str (System/currentTimeMillis)) "export")
        config (edn/read-string (slurp config-path))
        export-cfg {:config config
                    :filename export-file}]
    (clj {:dir "."}
         "-Sdeps" (str (format
                        "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}"
                        (or version "0.6.1531")))
         "-X" "io.datahike.migration/export-db" export-cfg)))

#_(comment
  (def version "0.3.0")
  (def config "bb/resources/test-config.edn")

  (clj {:dir "."}
       "-Sdeps" (str (format "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}" (or version "0.6.1531"))
                     " :paths [\"src/io/datahike\"]}") 
       "-X" "io.datahike.migration/ping" config)



  )



