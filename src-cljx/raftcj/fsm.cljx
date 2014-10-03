(ns raftcj.fsm
  (:require 
    clojure.string 
    [raftcj.base :refer [msg has-vote last-log up-to-date state-of]]
    #+clj [raftcj.macros :refer [defev]])
    #+cljs (:require-macros [raftcj.macros :refer [defev]]))
  
  (defn redispatch [[state, msgs] event & args]
    (let [
      [state, moremsgs] (apply (partial event state) args)]
      [state (concat msgs moremsgs)]))

  (defn election-delay [state]
    (let [
      base (get-in state [:config :election-delay])]
      (+ base (rand-int base))))

  (defn become-follower [state term]
    (let [
      outstanding-reqs (:client-reqs state)
      state (-> state 
        (assoc :current-term term)
        (assoc :voted-for nil)
        (assoc :statename :follower)
        (dissoc :client-reqs))
      reset-msg (msg :timer 'reset (election-delay state))
      reply-msgs  (map (fn [[idx client]] (msg client 'executed false)) outstanding-reqs)]
      [state (conj reply-msgs reset-msg)]))

  (defn majority [cluster votes]
    (assert (every? #(contains? cluster %) votes))
    (> (count votes) (/ (count cluster) 2)))

  (defn vote [state candidate]
    (let [
      vote (:voted-for state)]
      (assert (or (nil? vote) (= candidate vote)))
      (assoc state :voted-for candidate)))

  (defn member-sets [state]
    (map (comp set keys) (get-in state [:config :members])))

  (defn merge-set [& args]
    (set (flatten (map vec args))))

  (defn become-leader [state] ; TODO: change interface to return [state, []]
    (let [
      [_ last-log-index] (last-log state)
      to-entry #(vector % (inc last-log-index))
      members (apply merge-set (member-sets state))
      others (filter #(not (= (:id state) %)) members)]
      (-> state 
        (assoc :next-index (into {} (map to-entry others)))
        (assoc :next-match (into {} (map #(vector % 0) others)))
        (assoc :statename :leader))))

  (defn init [state]
    [state [(msg :timer 'reset (election-delay state))]])

  (declare elected advertise-leader)
  (defmulti voted state-of)
  (defev voted :default [voter granted]
    []
    (if (not granted)
      [state []]
      (let [
        state (update-in state [:votes] #(conj % voter))
        member-sets (member-sets state)]
        (if (every? true? (map #(majority % (:votes state)) member-sets))
          (elected (become-leader state))
          [state []]))))
  
  (defmulti request-vote state-of)
  (defev request-vote :default [candidate-id last-log-index last-log-term]
    [(msg candidate-id 'voted (:current-term state) (:id state) false)]
    (if (not (and
       (has-vote state candidate-id)
       (up-to-date state last-log-term last-log-index)))
      [state [(msg candidate-id 'voted (:current-term state) (:id state) false)]]

      (let [
          reply-msg (msg candidate-id 'voted (:current-term state) (:id state) true)
          reset-msg (msg :timer 'reset (election-delay state))]
          [(vote state candidate-id) [reply-msg reset-msg]])))

  (defn become-candidate [state]
    [(-> state
      (assoc :statename :candidate)
      (assoc :voted-for nil)
      (assoc :votes #{})) []])

  (defmulti timeout state-of)
  (defmethod timeout :follower [state]
    (redispatch 
      (become-candidate state)
      timeout))

  (defmethod timeout :candidate [state]
    (let [
      state (update-in state [:current-term] inc)
      reset-msg (msg :timer 'reset (election-delay state))
      [last-log-entry last-log-index] (last-log state)
      members (merge-set (member-sets state))
      vote-reqs (map
         #(msg % 'request-vote (:current-term state) (:id state) last-log-index (:term last-log-entry))
         members)]
      [state, (cons reset-msg vote-reqs)]))

  (defmethod timeout :leader [state] ; TODO: test
    [state (advertise-leader state)])

  (defn update-or-append 
    ([orig newer] 
      (update-or-append orig newer []))
    ([[x & xs :as orig][y & ys] acc]
      (cond 
        (nil? y)
        (concat acc orig)
        (= x y)
        (recur xs ys (conj acc x))
        :else
        (recur nil ys (conj acc y)))))

  (defn update-log [log prev-log-index entries] 
    (let [
      prefix (subvec log 0 (inc prev-log-index))
      updating (subvec log (inc prev-log-index))
      suffix (update-or-append updating entries)]
      (vec (concat prefix suffix))))

  (defn update-state-on-log [state prev-log-index entries]
    (-> state 
      (update-in [:log] #(update-log % prev-log-index entries))
      (assoc :members 
        (if-let [
          new-members (last (filter :members entries))] ; TODO: what if we have 3 configs running over the cluster?
          (cons new-members (:members state)) 
          (:members state))))) ; TODO: members config should be lazily loaded from log so that it works when starting up after crash

  (def fsm-fn conj)

  (defn apply-to-fsm [state cmd]
    (let [
      state (update-in state [:fsm] #(fsm-fn % cmd))
      state (update-in state [:last-applied] inc)]
      state))

  (defn append-log [state prev-log-index entries leader-commit] 
    (let [
      state (update-state-on-log state prev-log-index entries)
      commit-index (:commit-index state)
      [_, last-log-index] (last-log state)
      new-commit-index (max commit-index (min leader-commit last-log-index))
      state (assoc state :commit-index new-commit-index)
      newly-committed (map :cmd (subvec (:log state) (inc (:last-applied state)) (inc new-commit-index)))]
      (reduce apply-to-fsm state newly-committed)))


  (defn submap [src ks]
    (into {} (map (fn [k] [k (src k)]) ks)))

  (defn replicated-on-majority [indexes i]
    (let [
      nodes-ge (count (filter (partial <= i) (vals indexes)))]
      (> nodes-ge (/ (count (keys indexes)) 2))))

  ; TODO: refactor so that we have a majority? fn which takes a list of ids and a member-sets
  ; TODO: refactor so that we have a single "others" function which takes state or config
  (defn highest-majority [member-sets indexes fallback]
    (let [
      indexes-per-set (map #(submap indexes %) member-sets)
      is-majority-in-all-configs (apply every-pred (map #(partial replicated-on-majority %) indexes-per-set))]
      (reduce max fallback (filter is-majority-in-all-configs (set (vals indexes))))))

  ; TODO simplify by handling match-index for self like everybody else ?
  (defn new-commit-index [state] 
    (let [
      member-sets (member-sets state)
      current-term (:current-term state) 
      log (:log state) 
      old-commit-index (:commit-index state)
      local-match-index (dec (count log))
      match-indexes (assoc (:next-match state) (:id state) local-match-index)
      highest-uncommited-majority (highest-majority member-sets match-indexes old-commit-index)
      uncommitted-replicated-indexes (range highest-uncommited-majority old-commit-index -1)
      is-from-current-term (fn [idx] (= current-term (:term (log idx))))]
      (first (filter is-from-current-term uncommitted-replicated-indexes))))

  (declare append-entries)
  (defn update-msg [state peer idx]
    (let [
      prev-log-index (dec idx)
      prev-log-entry (get (:log state) prev-log-index)
      entries (subvec (:log state) idx)]
      (msg peer 'append-entries 
        (:current-term state) (:id state) 
        prev-log-index (:term prev-log-entry) 
        entries (:commit-index state))))

  (defmulti appended state-of)
  (defev appended :leader [appender next-index success]
    []
    (if success
      (let [
          state (-> state 
            (assoc-in [:next-index appender] next-index)
            (assoc-in [:next-match appender] (dec next-index)))
          old-commit-index (:commit-index state)
          commit-index (if-let 
            [updated (new-commit-index state)]
          updated old-commit-index)
          state (assoc state :commit-index commit-index)
          newly-committed (reverse (range commit-index old-commit-index -1))
          outstanding-reqs (filter (complement nil?) (map (partial get (:client-reqs state)) newly-committed))
          msgs (map #(msg % 'executed true) outstanding-reqs)
          state (reduce apply-to-fsm state (map #(:cmd (get (:log state) %)) newly-committed))] ;TODO test
          [state msgs])
      [(assoc-in state [:next-index appender] (dec next-index))
        [(update-msg state appender (dec next-index))]]))

  (defev appended :default [appender next-index success]
    []
    [state []])

  (defmulti append-entries state-of)
  (defev append-entries :follower [leader-id prev-log-index prev-log-term entries leader-commit]
    [(msg leader-id 'appended (:current-term state) (:id state) (inc prev-log-index) false)]
    (let [
      local-prev-log (get (:log state) prev-log-index)
      reset-msg (msg :timer 'reset (election-delay state))]
      (if (or (nil? local-prev-log) (not (= prev-log-term (:term local-prev-log)))) ; TODO: test when prev-log-index is beyond end of log
        [state [
          (msg leader-id 'appended (:current-term state) (:id state) (inc prev-log-index) false)
          reset-msg]]
        [(append-log state prev-log-index entries leader-commit) [
          (msg leader-id 'appended (:current-term state) (:id state) (inc (+ (count entries) prev-log-index)) true)
          reset-msg]])))

  (defev append-entries :candidate [leader-id prev-log-index prev-log-term entries leader-commit]
    [(msg leader-id 'appended (:current-term state) (:id state) (int prev-log-index) false)]
    (redispatch
        (become-follower state term)
        append-entries
        term leader-id prev-log-index prev-log-term entries leader-commit))

  (defn advertise-leader [state] ;TODO: test
    (let [
      heartbeats (map #(apply (partial update-msg state) %)(:next-index state))
      reset (msg :timer 'reset (get-in state [:config :heartbeat-delay]))]
      (conj heartbeats reset)))

  (defn elected [state]
    [state (advertise-leader state)])

  (defn append-and-replicate [state entry] 
    (let [
      [_ last-log-index] (last-log state)
      entry (merge entry {:term (:current-term state)})
      state (update-in state [:log] #(update-log % last-log-index [entry]))
      needing-update (filter (fn [[peer idx]] (>= (inc last-log-index) idx)) (:next-index state))
      updates (map #(apply (partial update-msg state) %) needing-update)] ; TODO: test
      [state updates]))

  (defmulti execute state-of)
  (defmethod execute :leader [state client cmd]
    (let [
      [_ last-log-index] (last-log state)
      next-index (inc last-log-index)
      state (assoc-in state [:client-reqs next-index] client)]
      (append-and-replicate state {:cmd cmd})))


  (defmulti change-config state-of)
  (defmethod change-config :leader [state members]
    (append-and-replicate state {:members members}))

; TODO: logic is duplicated between leader and followers for log and fsm updates
; TODO: extract logic for state persistance so that it is pluggable
; TODO: create part-of-cluster check logic outside ?