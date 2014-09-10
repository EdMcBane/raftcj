(ns raftcj.fsm
  (:require clojure.string)
  (:require  [raftcj.core :refer :all]))

  (defn msg [target type & args]
    (concat [target type] args))

  (defn redispatch [[state, msgs] event & args]
    (let [
      [state, moremsgs] (apply (partial event state) args)]
      [state (concat msgs moremsgs)]))

  (defn executed [client success] 
    :todo)

  (defn become-follower [state term]
    (let [
      outstanding-reqs (:client-reqs state)
      state (-> state 
        (assoc :current-term term)
        (assoc :voted-for nil)
        (assoc :statename :follower)
        (dissoc :client-reqs))
      reset-msg (msg timer reset (:id state) (get-in state [:config :election-delay]))
      reply-msgs  (vec (map (fn [[idx client]] (msg client executed false)) outstanding-reqs))]
      [state (vec (conj reply-msgs reset-msg))]))

  (defn majority [cluster votes]
    (assert (every? #(contains? cluster %) votes))
    (> (count votes) (/ (count cluster) 2)))

  (defn vote [state candidate]
    (let [
      vote (:voted-for state)]
      (assert (or (nil? vote) (= candidate vote)))
      (assoc state :voted-for candidate)))

  (defn become-leader [state] ; TODO: change interface to return [state, []]
    (let [
      [_ last-log-index] (last-log state)
      to-entry #(vector % (inc last-log-index))
      others (filter #(not (= (:id state) %)) (keys (get-in state [:config :members])))]
      (-> state 
        (assoc :next-index (into {} (map to-entry others)))
        (assoc :next-match (into {} (map #(vector % 0) others)))
        (assoc :statename :leader))))

  (declare elected advertise-leader)

  
  
  (defmacro defev [evname selector rest failmsgs body] 
    `(defmethod ~evname ~selector ~(vec (concat ['state 'term] rest))
      (cond 
        (> ~'term (:current-term ~'state))
        (redispatch
            (become-follower ~'state ~'term)
            ~evname
            ~'term ~@rest)
        
        (< ~'term (:current-term ~'state))
        [~'state ~failmsgs]
        
        :else
        ~body)))

  (defmulti voted state-of)
  (defev voted :default [voter granted]
    []
    (if (not granted)
      [state []]
      (let
        [state (update-in state [:votes] #(conj % voter))]
        (if (majority (get-in state [:config :members]) (:votes state))
          (elected (become-leader state))
          [state []]))))
  
  (defmulti request-vote state-of)
  (defev request-vote :default [candidate-id last-log-index last-log-term]
    [(msg candidate-id voted (:current-term state) (:id state) false)]
    (cond 
      (not (and
       (has-vote state candidate-id)
       (up-to-date state last-log-term last-log-index)))
      [state [(msg candidate-id voted (:current-term state) (:id state) false)]]

      :else
      (let [
          reply-msg (msg candidate-id voted (:current-term state) (:id state) true)
          reset-msg (msg timer reset (:id state) (get-in state [:config :election-delay]))]
          [(vote state candidate-id) [reply-msg reset-msg]])))

  (defn become-candidate [state]
    [(-> state 
      (assoc :statename :candidate) 
      (assoc :votes #{})) []])

  (defmulti timeout state-of)
  (defmethod timeout :follower [state]
    (redispatch 
      (become-candidate state)
      timeout))

  (defmethod timeout :candidate [state]
    (let [
      state (-> state 
        (update-in [:current-term] inc)
        (vote (:id state)))
      [state msgs] (voted state (:current-term state) (:id state) true)
      reset-msg (msg timer reset (:id state) (get-in state [:config :election-delay]))
      vote-reqs (map
         #(apply msg (concat [% request-vote] (map state [:current-term :id :last-log-index :last-log-term])))
         (filter #(not (= (:id state) %)) (keys (get-in state [:config :members]))))]
      [state, (concat [reset-msg] vote-reqs msgs)]))

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
    (let [
      state (update-in state [:fsm] #(fsm-fn % cmd))
      state (update-in state [:last-applied] inc)]
      state))

  (defn append-log [state prev-log-index entries leader-commit] 
    (let [
      state (update-in state [:log] #(update-log % prev-log-index entries))
      commit-index (:commit-index state)
      [_, last-log-index] (last-log state)
      new-commit-index (max commit-index (min leader-commit last-log-index))
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
      (msg peer append-entries (:current-term state) (:id state) prev-log-index (:term prev-log-entry) entries (:commit-index state))))

  (defmulti appended state-of)
  (defev appended :default [appender next-index success]
    []
    (if success
      (let [
          state (-> state 
            (assoc-in [:next-index appender] next-index)
            (assoc-in [:next-match appender] next-index))
          old-commit-index (:commit-index state)
          commit-index (if-let 
            [updated (new-commit-index (get-in state [:config :members]) (:current-term state) (:log state) (vals (:next-match state)) old-commit-index)]
          updated old-commit-index)
          state (assoc state :commit-index commit-index)
          newly-committed (range commit-index old-commit-index -1)
          outstanding-reqs (filter (complement nil?) (map (partial get (:client-reqs state)) newly-committed))
          msgs (vec (map #(msg % executed true) outstanding-reqs))]
          [state msgs])
      [(assoc-in state [:next-index appender] (dec next-index))
        (update-msg state appender (dec next-index))]))

  (defmulti append-entries state-of)
  (defev append-entries :follower [leader-id prev-log-index prev-log-term entries leader-commit]
    [(msg leader-id appended (:current-term state) (:id state) false)]
    (let [
      local-prev-log (get (:log state) prev-log-index)
      reset-msg (msg timer reset (:id state) (get-in state [:config :election-delay]))]
      (if (or (nil? local-prev-log) (not (= prev-log-term (:term local-prev-log)))) ; TODO: test when prev-log-index is beyond end of log
        [state [
          (msg leader-id appended (:current-term state) (:id state) false)
          reset-msg]]
        [(append-log state prev-log-index entries leader-commit) [
          (msg leader-id appended (:current-term state) (:id state) true)
          reset-msg]])))

  (defev append-entries :candidate [leader-id prev-log-index prev-log-term entries leader-commit]
    [(msg leader-id appended (:current-term state) (:id state) false)]
    (redispatch
        (become-follower state term)
        append-entries
        term leader-id prev-log-index prev-log-term entries leader-commit))

  (defn advertise-leader [state] ;TODO: test
    (let [
      heartbeats (vec (map #(apply (partial update-msg state) %)(:next-index state)))
      reset (msg timer reset (:id state) (get-in state [:config :heartbeat-delay]))]
      (conj heartbeats reset)))

  (defn elected [state]
    [state (advertise-leader state)])

  (defmulti execute state-of)
  (defmethod execute :leader [state client cmd]
    (let [
      state (update-in state [:log] #(conj % {:term (:current-term state) :cmd cmd}))
      [_ last-log-index] (last-log state)
      state (update-in state [:client-reqs last-log-index] client)
      needing-update (filter (fn [peer idx] (>= last-log-index idx)) (:next-index state))
      updates (vec (map #(apply (partial update-msg state) %) needing-update))] ; TODO: test
      [state updates]))

; TODO: RPC, not messages
; TODO: create part-of-cluster check logic outside ?