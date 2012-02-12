;; ## StupidDB - A very simple DB in clojure.
;;
;; StupidDB is a simple thread safe database handler serializing a
;; clojure ref to a file. Keep in mind that concurrent access is only
;; possible within one thread! Different processes are _not_ supported.
;;
;; A database is generated using the *db-init* function and closed
;; using *db-close* (see [this section](#db-handling) for further
;; information). Data can be retrieved using *db-get* and *db-get-in*
;; for flat or nested structures respectively. The data can be
;; manipulated using the following functions:
;;
;;  - *db-assoc* or *db-assoc-in* to assoc a given key with a given value,
;;  - *db-update-in* to update the value at the given location using a
;;    given function,
;;  - *db-dissoc* or *db-dissoc-in* to remove a key value pair at a
;;    given location.
;;
;; All functions for data retrieval and manipulation are described in
;; [this section](#db-manip).
(ns stupiddb.core
  "Core namespace."
  (:import [java.io File PrintWriter PushbackReader FileInputStream FileOutputStream InputStreamReader]
           [java.util.zip GZIPInputStream GZIPOutputStream])
  (:require [clojure.java.io :as io]))

;; ## File IO Helpers

;; Internal functions used for writing and reading the required files.
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

;; <a name="db-manip" />
;; ## Database Manipulators

(defn db-get
  "Returns the value mapped to *key* in *db*; nil or *default* is
   returned if the *key* is not present."
  ([db key default] 
    (or (get-in @db [:data key]) default))
  ([db key]
     (db-get db key nil)))

(defn db-get-in
  "Returns the value that is behind the vector *ks* in a nested *db*."
  [db ks]
  (get-in @db (vec (concat [:data] ks))))

(defn db-assoc
  "Associates *key* with *value* in the given *db*."
  [db key value]
  (dosync 
   (send (:log @db) write-log  :assoc key value)
   (alter db update-in [:data] assoc key value)))

(defn db-dissoc
  "Removes the *key* and its value from the *db*."
  [db key]
  (dosync
      (send (:log @db) write-log :dissoc key nil)
      (alter db update-in [:data] dissoc key)))

(defn db-assoc-in
  "Associates *value* in a nested *db* at the location given in *ks*."
  [db ks value]
  (dosync
   (send (:log @db) write-log :assoc-in ks value)
   (alter db update-in (vec (concat [:data] ks)) (constantly value))))

(defn db-dissoc-in
  "Remove the *key* at *ks* in a nested *db*."
  [db ks key]
  (dosync
   (send (:log @db) write-log :dissoc-in [ks key] nil)
   (alter db update-in (concat [:data] ks) dissoc key)))

(defn db-update-in
  "Update the value at the location given in *ks* in *db* by applying
   the function *f* with the current value and the given arguments
   *args*."
  [db ks f & args]
  (dosync
   (let [v (apply f (db-get-in db ks) args)]
     (db-assoc-in db ks v))))

;; ## File Handling

;; Some internal functions for handling the log and actual db file.
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

;; <a name="db-handling" />
;; ## Database Handling

(defn db-init
  "Initializes a db ref. If the file containing the actual db or logs
   given by *file* already exists loads the contained data. The db is
   saved every *time* seconds using a dedicated thread. For crash
   handling a journal is kept in a log file, so that data can be
   reconstructed in case of a crash. The database is compressed if
   *gzip* is set to true."
  [file time & {gzip :gzip}]
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

(defn db-close
  "Closes the given *db*; this includes stopping the thread used for
   auto saving and flushes all remaining actions for a faster startup."
  [db]
  (.stop (:thread @db))
  (flush-db db)
  (send (:log @db) (fn [o] (.close o)))
  (:data @db))

