(ns redisranktest.core
  (:require (clj-redis [client :as redis]))
  (:gen-class))

(def num-users (int 1e6))
(def max-score (dec num-users))
(def scores-key "scores")
(def num-ranks 10)
(def rank-print-delay 100)

(def db (redis/init))
(def updater (agent nil))

(defn timed-call [f]
  (let [start-time (System/nanoTime)
        val (f)
        end-time (System/nanoTime)
        ]
    [val (/ (- end-time start-time) 1e6)]))
  
(defmacro time [body]
  `(timed-call #(~@body)))

(defn set-user-score [uid score]
  (redis/zadd db scores-key score (str uid)))

(defn get-random-score []
  (let [sign ([1, -1] (rand-int 2))]
    (* sign (rand-int max-score))))

(defn set-init-scores []
  (dotimes [u num-users]
    (set-user-score u (get-random-score))))

(defn get-top-users []
  (redis/zrevrange db scores-key 0 (dec num-ranks)))

(defn set-random-user-score [_]
  (let [top-ranked-user (first (shuffle (get-top-users)))]
    (set-user-score top-ranked-user (get-random-score))))

(defn set-random-user-scores []
  (loop []
    (send-off updater set-random-user-score)
    (recur)))

(defn print-top-ranks []
  (let [[top-ranked-users query-time] (time (get-top-users))
        ;[top-ranks _] (time (map #(- num-users (redis/zrank db scores-key %)) top-ranked-users))
        ;top-scores (map #(redis/zscore db scores-key %)  top-ranked-users)
        ;expected-ranks (range 1 (inc num-ranks))
        ]

    (println "top" num-ranks "users => " top-ranked-users ", time taken(msecs) => " query-time)
    
    ;(println "their scores => " top-scores)
    ;(println "their ranks => " top-ranks)

    (comment (when-not (= top-ranks expected-ranks)
       (println "\nANOMALY, POSSIBLE DUE TO NON-TRANSACTIONAL QUERIES >>>>> " expected-ranks "!=" top-ranks "<<<<<\n")
       (Thread/sleep 5000)))))


(defn delayed-repeat [f delay]
  (fn []
   (loop []
     (f)
     (Thread/sleep delay)
     (recur))))


(defn -main [& args]
  (println "Settings default scores for" num-users "users ... (< 1 min)")
  (set-init-scores)
  (println "Done setting default scores")
  (.start (Thread. (delayed-repeat print-top-ranks rank-print-delay)))
  (set-random-user-scores))




