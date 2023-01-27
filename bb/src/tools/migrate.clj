(ns tools.migrate
  (:require
   [babashka.fs :as fs]
   [babashka.process :as p]
   [clojure.edn :as edn]))

(defn clj [opts & args] (apply p/shell opts "clojure" args))

(defn create-test-db [version config-path tx-count & _args]
  (let [config (edn/read-string (slurp config-path))
       db-args {:config   config
                :tx-count (Integer/parseInt (or tx-count "10"))}]
    (clj {:dir "."}
         "-Sdeps" (str (format
                        "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}"
                        (or version "0.6.1531")))
         "-X" "sherpa.utils/load-test-db" db-args)))

(defn -main [version import-path export-path & _args]
  (let [export-file (format "/tmp/%s-datahike-%s.cbor" (str (System/currentTimeMillis)) "export")
        import-config (edn/read-string (slurp import-path))
        import-args {:config import-config
                    :filename export-file}
        export-config (edn/read-string (slurp export-path))
        export-args {:config export-config
                    :export-file export-file}]
    (clj {:dir "."}
         "-Sdeps" (str (format
                        "{:deps {io.replikativ/datahike {:mvn/version \"%s\"}}}"
                        (or version "0.6.1531")))
         "-X" "sherpa.core/export-db" import-args)
    (clj {:dir "."}
         "-Sdeps" (str (format
                        "{:deps {io.replikativ/datahike {:mvn/version \"RELEASE\"}}}"))
         "-X" "sherpa.core/import-db" export-args)
    (println "Cleaning up...")
    (fs/delete export-file)
    (println "Done.")))

