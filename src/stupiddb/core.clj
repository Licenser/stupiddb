(ns stupiddb.core
  (:import [java.io File PrintWriter PushbackReader FileInputStream FileOutputStream InputStreamReader]
           [java.util.zip GZIPInputStream GZIPOutputStream]))


(try
 (require '[clojure.contrib.io :as io])
 (catch Exception e (require '[clojure.contrib.duck-streams :as io])))

(defn- dependant-writer [db arg]
  (if (:gzip db)
    (PrintWriter. (GZIPOutputStream. (FileOutputStream. arg)))
    (io/writer arg)))

(defn- dependant-reader [db arg] 
  (if (:gzip db)
    (PushbackReader.
     (InputStreamReader.
      (GZIPInputStream. (FileInputStream. arg))))
    (PushbackReader. (io/reader arg))))

(defn- write-log [out action key value]
  (binding [*out* out]
    (prn [action key value])
    (flush))
  out)

(defn- new-log [out db]
  (.close out)
  (io/writer (:log-file db)))

(defn db-get
  "Returns the value mapped to key in the db, nil or default if key is not present."
  ([db key default] 
    (or (get-in @db [:data key]) default))
  ([db key]
     (db-get db key nil)))

(defn db-get-in [db ks]
  "Returns the value that is behind the vecotr ks in a nested map"
  (get-in @db (vec (concat [:data] ks))))

(defn db-assoc [db key value]
  "Associates key with value in the db."
  (dosync 
   (send (:log @db) write-log  :assoc key value)
   (alter db update-in [:data] assoc key value)))

(defn db-dissoc [db key]
  "Dissociates key in db."
  (dosync
      (send (:log @db) write-log :dissoc key nil)
      (alter db update-in [:data] dissoc key)))

(defn db-assoc-in [db ks v]
  "Associates a value in a nested map in the db. (same as update-in with constatly)."
  (dosync
   (send (:log @db) write-log :assoc-in ks v)
   (alter db update-in (vec (concat [:data] ks)) (constantly v))))

(defn db-dissoc-in [db ks]
  "Dissociates a key in a nested map in the db."
  (dosync
   (send (:log @db) write-log :dissoc-in ks)
   (alter db update-in (into [:data] (butlast ks)) (last ks))))

(defn db-update-in [db ks f & args]
  "Gets the value from a nested vector in db that is behind the kys ks
then applys f with the value as first and args as following arguments and sets the new value"
  (dosync
   (let [v (apply f (db-get-in db ks) args)]
     (db-assoc-in db ks v))))

(defn- handle-log [db [action key value]]
  (cond
   (= action :assoc) (update-in db [:data] assoc key value)
   (= action :dissoc) (update-in db [:data] dissoc key)
   (= action :assoc-in) (update-in db (vec (concat [:data] key)) (constantly value))
   :else db))

(defn- load-log [db]
  (let [log (File. (:log-file db))]
     (let [db (if (.exists log)
                (binding [*read-eval* false]
                  (with-open [r (PushbackReader. (io/reader log))]
                    (loop [db db log (read r false false)]
                      (if log
                        (recur (handle-log db log)
                               (read r false false))
                        db))))
		db)] 
       (assoc db :log (agent (io/writer log))))))

(defn- load-db [db]
  (let [f (File. (:file db))]
    (if (.exists f)
      (binding [*read-eval* false]
        (with-open [r (dependant-reader db f)]
          (assoc db :data (read r))))
      db)))


(defn- flush-db [db]
  (dosync
   (send (:log @db) (fn flush-db-write-db-fn [out db]
		      (with-open [w (dependant-writer db (File. (:file db)))]
			(binding [*out* w]
			  (prn (:data db))))
		      out) @db)
      (send (:log @db) new-log @db)))

(defn db-init [file time & {gzip :gzip}]
  "Initializes a db ref. If the db file (or file.log) already exists loads the data.
The db is saved every time seconds, so a log is keeped for any manipulation to reconstruct
data in the case of a crash."
  (let [gzip (boolean gzip)
        r {:data {}
           :file (if gzip (str file ".gz") file)
           :log-file (str file ".log")
           :gzip gzip
           :time time}
	r (ref (load-log (load-db r)))]
    (dosync 
     (alter r assoc :thread
            (doto
                (Thread.
                 (fn [] (while true
                               (dosync  (flush-db r))
                               (Thread/sleep (* 1000 (:time @r))))))
              (.start))))
    r))

(defn db-close [db]
  "Closes a db, stops the auto saving and writes the entire log into the db file for faster startup."
  (.stop (:thread @db))
  (flush-db db)
  (send (:log @db) (fn [o] (.close o)))
  (:data @db))

