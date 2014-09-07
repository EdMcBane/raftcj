(ns raftcj.core
  (:require clojure.string))

(defn initial-state [id] {
                        :statename :follower 
                        :id id
                        :current-term 0 :voted-for nil :log [{:term 0}]
                        :commit-index 0 :last-applied 0
                        :votes #{}
                        })

(defn has-vote [state candidate-id]
  (or
   (nil? (:voted-for state))
   (= candidate-id (:voted-for state))))

(defn last-log [state]
  (assert (> (count (:log state)) 0))
  (let [index (dec (count (:log state)))
        entry (get (:log state) index)]
    [entry, index]))

(defn up-to-date [state last-log-term last-log-index]
  (let [[entry, index] (last-log state)
        term (:term entry)]
    (or
     (> term last-log-term)
     (and (= term last-log-term) (>= index last-log-index) ))))

(defn state-of [state & _] (:statename state))

(def timer :todo)
(defn reset [candidate-id delay] :todo)
