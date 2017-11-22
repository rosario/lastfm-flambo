(ns lastfm.core
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [clj-time.format :as tf]
            [clj-time.core :as tc]
            [clj-time.local :as tl])
  (:gen-class))


(defn sort-by-ts [list]
  (sort-by #(tf/parse (:ts %)) #(tc/after? %1 %2) list))


(defn pairs [list]
  (partition 2 (interleave list (rest list))))

(defn time-interval [[a b]]
  (let [time-a (tf/parse  (tf/formatters :date-time-no-ms) a)
        time-b (tf/parse  (tf/formatters :date-time-no-ms) b)]
        (tc/in-minutes (tc/interval time-b time-a))))

(defn partition-by-time [as]
  (partition-by (fn [[a b]] (< (time-interval [(:ts a) (:ts b)]) 20)) as))

(defn gap [list]
  (let [[[a b] & _] list]
    (> (time-interval [(:ts a) (:ts b)]  ) 20)))

(defn session-from-partitions [ps]
  (let [tracks (distinct (map :track-name (flatten ps )))]
    {:tracks tracks
     :count (count tracks)}))


(defn longest-sessions [rdd-data]
  (-> rdd-data
      ; group by :user-id
      (f/group-by (f/fn [e] (:user-id e)))

      ; create a flat list of sessions
      (f/flat-map (ft/key-val-fn (f/iterator-fn [k v]
        (map session-from-partitions (remove gap (partition-by-time (pairs (sort-by-ts v))))))))

      ;top 50 longest sessions by tracks count
      (f/map-to-pair (f/fn [w] (ft/tuple (:count w) (:tracks w))))
      (f/sort-by-key false)
      (f/take 50)))

(defn top10 [rdd-data]
  (-> rdd-data
      ; all track names in the top 50 sessions
      (f/flat-map (ft/key-val-fn (f/iterator-fn [k v] v)))
      ; top 10 tracks
      (f/group-by (f/fn [e] e))
      (f/map-to-pair (ft/key-val-fn (f/fn [k v] (ft/tuple (count v) k))))
      (f/sort-by-key false)
      (f/take 10)))


(defn read-data [sc filename]
  (-> (f/text-file sc filename)
      (f/map (fn [l] (clojure.string/split l #"\t")))
      ; create a list of :user-id, :ts (timestamp), and :track-name
      (f/map (fn [l] (let [[user-id ts _ _ _ track-name] l]
        {:user-id user-id :ts ts :track-name track-name})))))


(defn write-data [out-file rdd-data]
  (-> rdd-data
      (f/map (ft/key-val-fn (f/fn [k v] (str v "\t" k))))
      (f/coalesce 1)
      (f/save-as-text-file out-file)))


(defn -main [& args]
  (let [c (-> (conf/spark-conf)
              (conf/master "local[8]")
              (conf/app-name "top10"))
        sc (f/spark-context c)
        filename (first args)
        out-filename (last args)
        input-rdd (read-data sc filename)
        ls (longest-sessions (read-data sc filename))
        ts (top10 (f/parallelize sc ls))]
    (print (str "Reading inputfile: " filename))
    (write-data out-filename (f/parallelize sc ts))))


