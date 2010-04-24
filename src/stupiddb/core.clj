(ns stupiddb.core
  (:import (java.io File PushbackReader)))

(try
 (require '[clojure.contrib.duck-streams :as io])
 (catch Exception e (require '[clojure.contrib.io :as io])))

(defn- handle-log [db [action key value]]
  (cond
   (= action :assoc) (update-in db [:data] assoc key value)
   (= action :dissoc) (update-in db [:data] dissoc key)
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

(defn load-db [db]
  (let [f (File. (:file db))]
    (if (.exists f)
      (binding [*read-eval* false]
        (with-open [r (PushbackReader. (io/reader f))]
          (assoc db :data (read r))))
      db)))


(defn flush-db [db]
  (dosync
   (println (:file @db))
   (with-open [w (io/writer (:file @db))]
     (binding [*out* w]
              (prn (:data @db))))
      (.close (:log @db))
         (alter db assoc :log (io/writer (str (:file @db)
                                               ".log")))))

(defn init-db [file time]
  (let [r {:data {}
           :file file
           :time time}
	r (ref (load-log (load-db r)))]
    (dosync 
     (alter r assoc :thread
            (doto
                (Thread.
                 (fn [] (while true
                               (println "Saving!")
                               (dosync  (flush-db r))
                               (Thread/sleep (* 1000 time)))))
              (.start))))
    r))


(defn write-log [db action key value]
  (binding [*out* (:log db)]
    (prn [action key value])
    (flush)))

(defn db-assoc [db key value]
  (dosync 
   (write-log @db :assoc key value)
   (alter db update-in [:data] assoc key value)))

(defn db-dissoc [db key]
  (dosync
      (write-log @db :dissoc key nil)
      (alter db update-in [:data] dissoc key)))

(defn db-get
  ([db key default] 
     (or (get-in @db [:data key]) default))
  ([db key]
     (db-get db key nil)))

(defn close-db [db]
  (.stop (:thread @db))
  (flush-db db)
  (.close (:log @db)))
