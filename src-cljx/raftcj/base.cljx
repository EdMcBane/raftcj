(ns raftcj.base
  (:require clojure.string))

(defn initial-state [id config] {
  :statename :follower 
  :id id
  :current-term 0 :voted-for nil :log [{:term 0}]
  :commit-index 0 :last-applied 0
  :votes #{}
  :config config
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

(defn state-of [state & _] (:statename state)) ;TODO: variable arity

(defn msg [target type & args]
    (concat [target type] args))

(defn bad-arg [& args]
    #+clj (throw (new IllegalArgumentException (apply str args)))
    #+cljs (throw (apply str args)))
