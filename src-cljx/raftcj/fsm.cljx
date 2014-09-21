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
      (assert (has-vote state candidate))
      (assoc state :voted-for candidate))

  (defn become-leader [state] ; TODO: change interface to return [state, []]
    (let [
      [_ last-log-index] (last-log state)
      members (keys (get-in state [:config :members]))
      others (filter #(not (= (:id state) %)) members)]
      (-> state 
        (assoc :next-index (into {} (map vector others (repeat (inc last-log-index)))))
        (assoc :next-match (into {} (map vector others (repeat 0))))
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
        members (get-in state [:config :members])]
        (if (majority members (:votes state))
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
      members (keys (get-in state [:config :members]))
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

  (def fsm-fn conj)

  (defn apply-to-fsm [state, cmd] 
    (-> state
      (update-in [:fsm] #(fsm-fn % cmd))
      (update-in [:last-applied] inc)))

  (defn append-log [state prev-log-index entries leader-commit] 
    (let [
      state (update-in state [:log] #(update-log % prev-log-index entries))
      [_, last-log-index] (last-log state)
      new-commit-index (max (:commit-index state) (min leader-commit last-log-index))
      state (assoc state :commit-index new-commit-index)
      newly-committed (map :cmd (subvec (:log state) (inc (:last-applied state)) (inc new-commit-index)))]
      (reduce apply-to-fsm state newly-committed)))

  (defn highest-majority [members indexes fallback]
    (let [
      count-ge #(count (filter (partial <= %) indexes))
      is-majority #(> % (/ (count members) 2))]
      (reduce max fallback (filter (comp is-majority count-ge) (set indexes)))))

  ; TODO simplify by handling match-index for self like everybody else ?
  (defn new-commit-index [members current-term log indexes commit-index]
    (let [
      local-match-index (dec (count log))
      highest-uncommited-majority (highest-majority members (conj indexes local-match-index) commit-index)
      uncommitted-replicated-indexes (range highest-uncommited-majority commit-index -1)
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
            [updated (new-commit-index (get-in state [:config :members]) (:current-term state) (:log state) (vals (:next-match state)) old-commit-index)]
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

  (defmulti execute state-of)
  (defmethod execute :leader [state client cmd]
    (let [
      state (update-in state [:log] #(conj % {:term (:current-term state) :cmd cmd}))
      [_ last-log-index] (last-log state)
      state (assoc-in state [:client-reqs last-log-index] client)
      _ (println (:next-index state))
      needing-update (filter (fn [[peer idx]] (>= last-log-index idx)) (:next-index state))
      _ (println "upd" needing-update)
      updates (map #(apply (partial update-msg state) %) needing-update)] ; TODO: test
      [state updates]))

; TODO: create part-of-cluster check logic outside ?