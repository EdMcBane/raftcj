(ns raftcj.fsm
  (:require clojure.string)
  (:require  [raftcj.core :refer :all]))
(defn fsm [config]

  (defmulti become-follower state-of)
  (defmethod become-follower :default [state term]
    (assoc (assoc (assoc state :current-term term) :voted-for nil) :statename :follower))

  (defn msg [target type & args] 
    (concat [target type] args))

  (defn majority [cluster votes]
    (do 
      (assert (every? #(contains? cluster %) votes))
      (> (count votes) (/ (count cluster) 2))))

  (defn vote [state candidate]
    (do 
      (let [vote (:voted-for state)]
        (assert (or 
          (nil? vote) 
          (= candidate vote))))
      (assoc state :voted-for candidate)))

  (defn become-leader [state]
    (let [
    [_ last-log-index] (last-log state)
    to-entry #(vector % (inc last-log-index))
    next-index (into {} (map to-entry (keys config)))
    next-match (into {} (map #(vector % 0) (keys config)))]
    (assoc (assoc (assoc state :next-index next-index) :next-match next-match) :statename :leader)))

  (defmulti voted state-of)
  (defmethod voted :default [state term voter granted]
    (if (> term (:current-term state))
      [(become-follower state term) []]
      (if (and granted (contains? config voter)) ; TODO: move part-of-cluster check logic outside ?
        (let
          [state (update-in state [:votes] #(conj % voter))]
          (if (majority config (:votes state))
            [(become-leader state), []]
            [state []]))
        [state []])))

  (defmulti request-vote state-of)
  (defmethod request-vote :default [state term candidate-id last-log-index last-log-term]
    (if (< term (:current-term state))
      [state [(msg candidate-id voted (:current-term state) (:id state) false)]]
      (if (> term (:current-term state))
        (request-vote (become-follower state term) term candidate-id last-log-index last-log-term)
        (if (and
         (has-vote state candidate-id)
         (up-to-date state last-log-term last-log-index))
        [(vote state candidate-id) [(msg candidate-id voted (:current-term state) (:id state) true)]]
        [state [(msg candidate-id voted (:current-term state) (:id state) false)]]))))


  (defn become-candidate [state]
    (assoc (assoc state :statename :candidate) :votes #{}))

  (defmulti timeout state-of)
  (defmethod timeout :follower [state]
    (timeout (become-candidate state)))

  (defmethod timeout :candidate [state]
    (let [
      state (update-in state [:current-term] inc)
      state (vote state (:id state))
      [state msgs] (voted state (:current-term state) (:id state) true)]
      [state, (concat
        [(msg timer reset)]
        (map
         #(apply msg (concat [% request-vote] (map state [:current-term :id :last-log-index :last-log-term])))
         (filter #(not (= (:id state) %)) (keys config)))
        msgs)]
      ))
)
  ; (defmulti append-entries state-of)

  ; (defmethod append-entries :candidate [state term & todo]
  ;   (if (> term (:currentTerm state))
  ;     [(become-follower state term) true]
  ;     [state false])
  ;   ))