(ns raftcj.fsm
  (:require clojure.string)
  (:require  [raftcj.core :refer :all]))

(defn fsm [config]
  (let [
    members (:members config)]
  (defn msg [target type & args]
    (concat [target type] args))
  (declare executed)

  (defn become-follower [state term]
    (let [
      outstanding-reqs (:client-reqs state)
      state (assoc state :current-term term)
      state (assoc state :voted-for nil)
      state (assoc state :statename :follower)
      state (dissoc state :client-reqs)
      reset-msg (msg timer reset (:id state) (:election-delay config))
      reply-msgs  (vec (map (fn [[idx client]] (msg client executed false)) outstanding-reqs))]
      [state (vec (conj reply-msgs reset-msg))]))

  (defn majority [cluster votes]
    (do 
      (assert (every? #(contains? cluster %) votes))
      (> (count votes) (/ (count cluster) 2))))

  (defn vote [state candidate]
    (let [
      vote (:voted-for state)]
      (do
        (assert (or (nil? vote) (= candidate vote)))
        (assoc state :voted-for candidate))))

  (defn become-leader [state]
    (let [
      [_ last-log-index] (last-log state)
      to-entry #(vector % (inc last-log-index))
      others (filter #(not (= (:id state) %)) (keys members))
      state (assoc state :next-index (into {} (map to-entry others)))
      state (assoc state :next-match (into {} (map #(vector % 0) others)))
      state (assoc state :statename :leader)]
      state))

  (declare elected advertise-leader)

  (defn redispatch [[state, msgs] event & args]
    (let [
      [state, moremsgs] (apply (partial event state) args)]
      [state (concat msgs moremsgs)]))
  
  ; TODO: macro to redispatch on newer term?

  (defmulti voted state-of)
  (defmethod voted :default [state term voter granted]
    (cond 
      (> term (:current-term state))
      (redispatch
          (become-follower state term)
          voted
          term voter granted)
      
      (< term (:current-term state))
      [state []]
      
      (not granted)
      [state []]
      
      :else
      (let
        [state (update-in state [:votes] #(conj % voter))]
        (if (majority members (:votes state))
          (elected (become-leader state))
          [state []]))))

  ; TODO: macro to reply false on old term?

  (defmulti request-vote state-of)
  (defmethod request-vote :default [state term candidate-id last-log-index last-log-term]
    (cond 
      (< term (:current-term state))
      [state [(msg candidate-id voted (:current-term state) (:id state) false)]]
      
      (> term (:current-term state))
      (redispatch
        (become-follower state term)
        request-vote
        term candidate-id last-log-index last-log-term)
      
      (not (and
       (has-vote state candidate-id)
       (up-to-date state last-log-term last-log-index)))
      [state [(msg candidate-id voted (:current-term state) (:id state) false)]]

      :else
      (let [
          reply-msg (msg candidate-id voted (:current-term state) (:id state) true)
          reset-msg (msg timer reset (:id state) (:election-delay config))]
          [(vote state candidate-id) [reply-msg reset-msg]])))

  (defn become-candidate [state]
    (let [
      state (assoc state :statename :candidate) 
      state (assoc state :votes #{})]
      [state []]))

  (defmulti timeout state-of)
  (defmethod timeout :follower [state]
    (redispatch 
      (become-candidate state)
      timeout))

  (defmethod timeout :candidate [state]
    (let [
      state (update-in state [:current-term] inc)
      state (vote state (:id state))
      [state msgs] (voted state (:current-term state) (:id state) true)
      reset-msg (msg timer reset (:id state) (:election-delay config))
      vote-reqs (map
         #(apply msg (concat [% request-vote] (map state [:current-term :id :last-log-index :last-log-term])))
         (filter #(not (= (:id state) %)) (keys members)))]
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


  (defn highest-majority [indexes fallback]
    (let [
      count-ge #(count (filter (partial <= %) indexes))
      is-majority #(> % (/ (count members) 2))]
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
      (redispatch
          (become-follower state term)
          appended
          term appender next-index success)

      success
      (let [
          state (assoc-in (assoc-in state [:next-index appender] next-index) [:next-match appender] next-index)
          old-commit-index (:commit-index state)
          commit-index (if-let 
            [updated (new-commit-index (:current-term state) (:log state) (vals (:next-match state)) old-commit-index)]
          updated old-commit-index)
          state (assoc state :commit-index commit-index)
          newly-committed (range commit-index old-commit-index -1)
          outstanding-reqs (filter (complement nil?) (map (partial get (:client-reqs state)) newly-committed))
          msgs (vec (map #(msg % executed true) outstanding-reqs))]
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


  (defn advertise-leader [state]
    (let [
      [prev-log-entry prev-log-index] (last-log state)
      heartbeats (vec (map 
        (fn [[peer _]] (msg peer append-entries (:current-term state) (:id state) prev-log-index (:term prev-log-entry) [] (:commit-index state)))
        (:next-index state)))
      reset (msg timer reset (:id state) (:heartbeat-delay members))]
      (conj heartbeats reset)))

  (defn elected [state]
    [state (advertise-leader state)])

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
)) 
; TODO: RPC, not messages
; TODO: create part-of-cluster check logic outside ?

