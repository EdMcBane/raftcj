(ns raftcj.fsm
  (:require clojure.string)
  (:require  [raftcj.core :refer :all]))

(defn fsm [config]
  (let [
    members (:members config)]
  (defn msg [target type & args]
    (concat [target type] args))
  (declare executed)

  (defmulti become-follower state-of)
  (defmethod become-follower :default [state term]
    (let [
      outstanding-reqs (:client-reqs state)
      state (dissoc (assoc (assoc (assoc state :current-term term) :voted-for nil) :statename :follower) :client-reqs)
      reset-msg (msg timer reset (:id state) (:election-delay config))
      reply-msgs  (vec (map (fn [[idx client]] (msg client executed false)) outstanding-reqs))]
      [state (vec (conj reply-msgs reset-msg))]))

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
      others (filter #(not (= (:id state) %)) (keys members))
      next-index (into {} (map to-entry others))
      next-match (into {} (map #(vector % 0) others))]
      (assoc (assoc (assoc state :next-index next-index) :next-match next-match) :statename :leader)))

  (declare elected)

  (defmulti voted state-of)
  (defmethod voted :default [state term voter granted]
    (if (> term (:current-term state))
      (become-follower state term)
      (if (and granted (contains? members voter)) ; TODO: move part-of-cluster check logic outside ?
        (let
          [state (update-in state [:votes] #(conj % voter))]
          (if (majority members (:votes state))
            (elected (become-leader state))
            [state []]))
        [state []])))

  (defn redispatch [[state, msgs] event & args]
    (let [
      [state, moremsgs] (apply (partial event state) args)]
      [state (concat msgs moremsgs)]))

  (defmulti request-vote state-of)
  (defmethod request-vote :default [state term candidate-id last-log-index last-log-term]
    (if (< term (:current-term state))
      [state [(msg candidate-id voted (:current-term state) (:id state) false)]]
      (if (> term (:current-term state))
        (redispatch
          (become-follower state term)
          request-vote
          term candidate-id last-log-index last-log-term)
        (if (and
         (has-vote state candidate-id)
         (up-to-date state last-log-term last-log-index))
        [(vote state candidate-id) [
        (msg candidate-id voted (:current-term state) (:id state) true)
        (msg timer reset (:id state) (:election-delay config))]]
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
        [(msg timer reset (:id state) (:election-delay config))]
        (map
         #(apply msg (concat [% request-vote] (map state [:current-term :id :last-log-index :last-log-term])))
         (filter #(not (= (:id state) %)) (keys members)))
        msgs)]
      ))

  (defmethod timeout :leader [state] ; TODO: test
    (elected state))

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
      [_, last-log-index] (last-log state)
      new-commit-index (max commit-index (min leader-commit last-log-index))
      state (assoc state :commit-index new-commit-index)
      fsm-fn (fn [current cmd] (conj current cmd))
      apply-to-fsm (fn [state, cmd] (update-in (update-in state [:fsm] #(fsm-fn % cmd)) [:last-applied] inc))]
      (reduce 
        apply-to-fsm state 
        (map :cmd (subvec (:log state) (inc (:last-applied state)) (inc new-commit-index))))))


  (defn highest-majority [indexes fallback]
    (let [
      count-ge #(count (filter (partial <= %) indexes))
      is-majority #(> % (/ (count members) 2))
      ]
      (reduce max fallback (filter (comp is-majority count-ge) (set indexes)))))

  ; TODO simplify by handling match-index for self like everybody else ?
  (defn new-commit-index [current-term log indexes commit-index]
    (let [
      local-match-index (dec (count log))
      highest-uncommited-majority (highest-majority (conj indexes local-match-index) commit-index)
      uncommitted-replicated-indexes (range highest-uncommited-majority commit-index -1)
      is-from-current-term (fn [idx] (= current-term (:term (log idx))))]
      (first (filter is-from-current-term uncommitted-replicated-indexes))))

;TODO: check all redispatches after state change
  (declare update-msg)
  (declare executed)
  (defmulti appended state-of)
  (defmethod appended :default [state term appender next-index success]
    (cond
      (> term (:current-term state))
      (become-follower state term)
      
      success
      (let [
          state (assoc-in (assoc-in state [:next-index appender] next-index) [:next-match appender] next-index)
          old-commit-index (:commit-index state)
          commit-index (if-let 
            [updated (new-commit-index (:current-term state) (:log state) (vals (:next-match state)) old-commit-index)]
          updated old-commit-index)
          state (assoc state :commit-index commit-index)
          msgs (vec (map ; TODO: refactor out
            #(msg % executed true) 
            (filter (complement nil?) (map (partial get (:client-reqs state)) (range commit-index old-commit-index -1)))))]
          [state msgs])

      :else
      [(assoc-in state [:next-index appender] (dec next-index))
      (update-msg state appender (dec next-index))]))

  (defmulti append-entries state-of)
  (defmethod append-entries :follower [state term leader-id prev-log-index prev-log-term entries leader-commit]
    (cond 
      (> term (:current-term state))
      (redispatch
        (become-follower state term)
        append-entries
        term leader-id prev-log-index prev-log-term entries leader-commit)
      
      (< term (:current-term state))
      [state [(msg leader-id appended (:current-term state) (:id state) false)]]
      
      :else
      (let [
        local-prev-log (get (:log state) prev-log-index)]
        (if (or (nil? local-prev-log) (not (= prev-log-term (:term local-prev-log))))
          [state [
            (msg leader-id appended (:current-term state) (:id state) false)
            (msg timer reset (:id state) (:election-delay config))]]
          [(append-log state prev-log-index entries leader-commit) [
            (msg leader-id appended (:current-term state) (:id state) true)
            (msg timer reset (:id state)(:election-delay config))]]))))

  (defmethod append-entries :candidate [state term leader-id prev-log-index prev-log-term entries leader-commit]
    (cond 
      (> term (:current-term state))
      (redispatch
        (become-follower state term)
        append-entries
        term leader-id prev-log-index prev-log-term entries leader-commit)

      (< term (:current-term state))
      [state [(msg leader-id appended (:current-term state) (:id state) false)]]
      
      :else
      (redispatch
        (become-follower state term)
        append-entries
        term leader-id prev-log-index prev-log-term entries leader-commit)))

  (defn elected [state]
    (let [
      [prev-log-entry prev-log-index] (last-log state)
      heartbeats (vec (map 
        (fn [[peer _]] (msg peer append-entries (:current-term state) (:id state) prev-log-index (:term prev-log-entry) [] (:commit-index state)))
        (:next-index state)))
      reset (msg timer reset (:id state) (:heartbeat-delay members))]
      [state (conj heartbeats reset)]))

  (defn update-msg [state peer idx]
    (let [
      prev-log-index (dec idx)
      prev-log-entry (get (:log state) prev-log-index)
      entries (subvec (:log state) idx)]
      (msg peer append-entries (:current-term state) (:id state) prev-log-index (:term prev-log-entry) entries (:commit-index state))))

  (defn executed [client success] 
    :todo)

  (defmulti execute state-of)
  (defmethod execute :leader [state client cmd]
    (let [
      state (update-in state [:log] #(conj % {:term (:current-term state) :cmd cmd}))
      [_ last-log-index] (last-log state)
      state (update-in state [:client-reqs last-log-index] client)
      needing-update (filter (fn [peer idx] (>= last-log-index idx)) (:next-index state))
      updates (vec (map (partial update-msg state) needing-update))]
      [state updates]))
)) ; TODO: RPC, not messages
