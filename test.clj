(use '[clojure.string :only (join split)])


(def config [
             {:id 1 :addr "127.0.0.1"}
             {:id 2 :addr "127.0.0.2"}
             {:id 3 :addr "127.0.0.3"}])

(def timer :todo)
(defn reset [] :todo)

(defn initial-state [id] {
                        :statename :follower
                        :id id
                        :currentTerm 0 :votedFor nil, :log [{:term 0}]
                        :commitIndex 0, :lastApplied: 0
                        })

(defn has-vote [state candidate-id]
  (or
   (nil? (:votedFor state))
   (= candidate-id (:votedFor state))))

(defn last-log [state]
  (let [index (dec (count (:log state)))
        entry ((:log state) index)]
    [entry, index]))


(defn up-to-date [state lastLogTerm lastLogIndex]
  (let [[entry, index] (last-log state)
        term (:term entry)]
    (or
     (> term lastLogTerm)
     (and (= term lastLogTerm) (>= index lastLogIndex) ))))


(= true (up-to-date (initial-state 0) 0 0))
(= false (up-to-date (initial-state 0) 0 1))
(= true
   (up-to-date
    (update-in (initial-state 0) [:log] (fn [old] (conj old {:term 0})))
    0 1))
(= false (up-to-date (initial-state 0) 1 1))


(defn state-of [state & args] (:statename state))

(defmulti become-follower state-of)
(defmethod become-follower :default [state term]
  (assoc (assoc (assoc state :currentTerm term) :votedFor nil) :statename :follower))

(defmulti request-vote state-of)
(defmethod request-vote :follower [state] (throw (new java.lang.RuntimeException "Not implemented")))

(defmethod request-vote :candidate [state, term, candidate-id, last-log-index last-log-term]
    (if (< term (:currentTerm state))
      '(state (:currentTerm state) false)
      (if (> term (:currentTerm state))
        (request-vote (become-follower state term) term candidate-id last-log-index last-log-term)
        (if (and
             (has-vote state candidate-id)
             (up-to-date state last-log-term last-log-index))
          [(assoc state :votedFor candidate-id) [(send candidate-id voted (:currentTerm state) true)]]
          [state [(send candidate-id voted (:currentTerm state) false)]]))))


(defn become-candidate [state]
  (assoc state :statename :candidate))

(defmulti timeout state-of)
(defmethod timeout :follower [state]
  (timeout (become-candidate state)))

(defn vote [state candidate]
  (do
    (assert (nil? (:votedFor state)))
    (assoc state :votedFor candidate)))

(defn send [target msg & args]
  (clojure.string/join " " [msg "with args" args "to" (:id target) ]))

(defmethod timeout :candidate [state]
  (let [state (assoc (update-in state [:currentTerm] inc) :votedFor nil)]
    (let
      [state (vote state (:id state))]
      [state,
       (concat
        (send timer reset)
        (map
         #(send % request-vote (map state [:currentTerm :id last-log-index last-log-term]))
         (filter #(not (= % (:id state))) config))
        )]
      )))

(defmulti append-entries state-of)

(defmethod append-entries :candidate [state term & todo]
  (if (> term (:currentTerm state))
    [(become-follower state term) true]
    [state false])
  )

(let [
      [state1 & msgs] (timeout (initial-state 1))
      [state2 & msgs] (append-entries state1 2)]
  [state1 state2]
  )
