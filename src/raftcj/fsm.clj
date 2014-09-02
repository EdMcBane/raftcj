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

  (declare elected)

  (defmulti voted state-of)
  (defmethod voted :default [state term voter granted]
    (if (> term (:current-term state))
      [(become-follower state term) []]
      (if (and granted (contains? config voter)) ; TODO: move part-of-cluster check logic outside ?
        (let
          [state (update-in state [:votes] #(conj % voter))]
          (if (majority config (:votes state))
            (elected (become-leader state))
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
        [(vote state candidate-id) [
          (msg candidate-id voted (:current-term state) (:id state) true)
          (msg timer reset (:id state))]]
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


  (defn update-or-append ([[x & xs :as orig][y & ys] acc]
    (if (nil? y)
      (concat acc orig)
      (if (= x y)
        (recur xs ys (conj acc x))
        (recur nil ys (conj acc y)))))
    ([orig newer] (update-or-append orig newer [])))


  (defn update-log [log prev-log-index entries] 
    (let [
      prefix (subvec log 0 (inc prev-log-index))
      updating (subvec log (inc prev-log-index))
      suffix (update-or-append updating entries)
      ]
      (vec (concat prefix suffix))))

  (defn append-log [state prev-log-index entries leader-commit] 
    (let [
      state (update-in state [:log]  #(update-log % prev-log-index entries))
      commit-index (:commit-index state)
      [_, last-log-index] (last-log state)]
      (assoc state :commit-index (max 
        commit-index 
        (min leader-commit last-log-index)))))

  (defmulti appended state-of)
  (defmethod appended :default [state term appender success]
    (if (> term (:current-term state))
      [(become-follower state term) []]))

  (defmulti append-entries state-of)
  (defmethod append-entries :follower [state term leader-id prev-log-index prev-log-term entries leader-commit]
    (if (> term (:current-term state))
      (append-entries 
        (become-follower state term) 
        term leader-id prev-log-index prev-log-term entries leader-commit)
      (if (< term (:current-term state))
        [state [(msg leader-id appended (:current-term state) (:id state) false)]]
        (let [
          local-prev-log (get (:log state) prev-log-index)]
          (if (or (nil? local-prev-log) (not (= prev-log-term (:term local-prev-log))))
            [state [
              (msg leader-id appended (:current-term state) (:id state) false)
              (msg timer reset (:id state))]]
            [(append-log state prev-log-index entries leader-commit) [
              (msg leader-id appended (:current-term state) (:id state) true)
              (msg timer reset (:id state))]])))))

  (defmethod append-entries :candidate [state term leader-id prev-log-index prev-log-term entries leader-commit]
    (if (> term (:current-term state))
      (append-entries (become-follower state term) term leader-id prev-log-index prev-log-term entries leader-commit)
      (if (< term (:current-term state))
        [state [(msg leader-id appended (:current-term state) (:id state) false)]]
        (append-entries (become-follower state term) term leader-id prev-log-index prev-log-term entries leader-commit))))

  (defn elected [state]
    (let [
    [prev-log-entry prev-log-index] (last-log state)
    appends (vec (map 
      (fn [[k v]] (msg k append-entries (:current-term state) (:id state) prev-log-index (:term prev-log-entry) [] (:commit-index state)))
      config))
    reset (msg beat reset)]
    [state (concat appends reset)]))


; TODO: RPC, not messages
)
