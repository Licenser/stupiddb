(ns stupiddb.core
  (:import (java.io File PushbackReader)))

(try
 (require '[clojure.contrib.io :as io])
 (catch Exception e (require '[clojure.contrib.duck-streams :as io])))

(defn- write-log [db action key value]
  (binding [*out* (:log db)]
    (prn [action key value])
    (flush)))

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
   (write-log @db :assoc key value)
   (alter db update-in [:data] assoc key value)))

(defn db-dissoc [db key]
  "Dissociates key in db."
  (dosync
      (write-log @db :dissoc key nil)
      (alter db update-in [:data] dissoc key)))

(defn db-assoc-in [db ks v]
  "Associates a value in a nested map in the db. (same as update-in with constatly)."
  (dosync
   (write-log @db :assoc-in ks v)
   (alter db update-in (vec (concat [:data] ks)) (constantly v))))

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
  (let [log (File. (str (:file db) ".log"))]
     (let [db (if (.exists log)
                (binding [*read-eval* false]
                  (with-open [r (PushbackReader. (io/reader log))] 
                    (loop [db db log (read r false false)]
                      (if log
                        (recur (handle-log db log)
                               (read r false false))
                        db))))
		db)] 
       (assoc db :log (io/writer log)))))

(defn- load-db [db]
  (let [f (File. (:file db))]
    (if (.exists f)
      (binding [*read-eval* false]
        (with-open [r (PushbackReader. (io/reader f))]
          (assoc db :data (read r))))
      db)))


(defn- flush-db [db]
  (dosync
   (println (:file @db))
   (with-open [w (io/writer (:file @db))]
     (binding [*out* w]
              (prn (:data @db))))
      (.close (:log @db))
         (alter db assoc :log (io/writer (str (:file @db)
                                               ".log")))))

(defn db-init [file time]
  "Initializes a db ref. If the db file (or file.log) already exists loads the data.
The db is saved every time seconds, so a log is keeped for any manipulation to reconstruct
data in the case of a crash."
  (let [r {:data {}
           :file file
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
  (.close (:log @db)))
