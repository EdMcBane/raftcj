(ns raftcj.fsm
  (:require clojure.string)
  (:require  [raftcj.core :refer :all]))
(defn fsm [config]

  (defmulti become-follower state-of)
  (defmethod become-follower :default [state term]
    (assoc (assoc (assoc state :current-term term) :voted-for nil) :statename :follower))

  (defn msg [target type & args] 
    (concat [target type] args))

  (defn vote [state candidate]
    (do 
      (let [vote (:voted-for state)]
        (assert (or 
          (nil? vote) 
          (= candidate vote))))
      (assoc state :voted-for candidate)))

  (defmulti voted state-of)
  (defmethod voted :default [state term voter granted] 
    (if (> term (:current-term state))
      [(become-follower state term) []]
      (if granted
        [(update-in state [:votes] #(conj % voter)) []]
        [state []])))

  (defmulti request-vote state-of)
  (defmethod request-vote :default [state term candidate-id last-log-index last-log-term]
    (if (< term (:current-term state))
      [state [(msg candidate-id voted (:current-term state) false)]]
      (if (> term (:current-term state))
        (request-vote (become-follower state term) term candidate-id last-log-index last-log-term)
        (if (and
         (has-vote state candidate-id)
         (up-to-date state last-log-term last-log-index))
        [(vote state candidate-id) [(msg candidate-id voted (:current-term state) true)]]
        [state [(msg candidate-id voted (:current-term state) false)]]))))


  (defn become-candidate [state]
    (assoc (assoc state :statename :candidate) :votes #{}))

  (defmulti timeout state-of)
  (defmethod timeout :follower [state]
    (timeout (become-candidate state)))

  (defmethod timeout :candidate [state]
    (let [
      state (update-in state [:current-term] inc)
      state (vote state (:id state))]
      [state, (concat
        [(msg timer reset)]
        (map
         #(apply msg (concat [% request-vote] (map state [:current-term :id :last-log-index :last-log-term])))
         (filter #(not (= % (:id state))) config))
        )]
      ))

  )

  ; (defmulti append-entries state-of)

  ; (defmethod append-entries :candidate [state term & todo]
  ;   (if (> term (:currentTerm state))
  ;     [(become-follower state term) true]
  ;     [state false])
  ;   ))