(ns raftcj.fsm-test
  (:require [clojure.test :refer :all]
            [raftcj.core :refer :all]
            [raftcj.fsm :refer :all]))

(def a-term 42)
(def a-candidate-id 12)
(def another-candidate-id 23)

(def config {
    :heartbeat-delay 1
    :election-delay  3
    :members {
        0  ["127.0.0.1" 10000]
        12 ["127.0.0.2" 10012]
        23 ["127.0.0.3" 10023]
        34 ["127.0.0.4" 10034]
        45 ["127.0.0.5" 10045]}})

(deftest become-follower-test
    (testing ":statename is :follower on becoming follower"
        (let [
            [state msgs] (become-follower (initial-state 0 config) a-term)]
            (is (= :follower (:statename state)))))
    (testing ":voted-for is nil on becoming follower"
        (let [
            [state msgs] (become-follower (assoc (initial-state 0 config) :voted-for a-candidate-id) a-term)]
            (is (nil? (:voted-for state)))))
    (testing ":current-term is new term on becoming follower"
        (let [
            [state msgs] (become-follower (initial-state 0 config) a-term)]
            (is (= a-term (:current-term state)))))
    (testing "notifies clients of failure of pending requests"
        (let [
            state (assoc-in (initial-state 0 config) [:client-reqs 0] :a-client)
            [_ msgs] (become-follower state a-term)]
            (is (some #(= :a-client (first %)) msgs))))
    (testing "cleans up pending requests"
        (let [
            state (assoc-in (initial-state 0 config) [:client-reqs 0] :a-client)
            [state _] (become-follower state a-term)]
            (is (nil? (:client-reqs state))))))

(deftest msg-test :todo :really?)
(deftest vote-test 
    (testing "throws if already voted for other candidate"
        (is (thrown? AssertionError 
            (vote 
                (vote (initial-state 0 config) a-candidate-id) 
                another-candidate-id))))
    (testing "yields input if already voted for same candidate"
        (let [ 
            before (vote (initial-state 0 config) a-candidate-id)]
            (is (=
                before
                (vote before a-candidate-id)))))
    (testing "sets voted-for to candidate-id"
        (is (= 
            a-candidate-id 
            (:voted-for (vote (initial-state 0 config) a-candidate-id))))))

(deftest become-candidate-test
    (testing "statename becomes :candidate"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after _] (become-candidate before)]
            (is (= :candidate (:statename after)))))
    (testing "votes are reset"
        (let [
            before (assoc (initial-state 0 config) :votes #{:a :b})
            after (become-candidate before)]
            (is (= 0 (count (:votes after)))))))

(deftest timeout-test
    (testing "follower becomes candidate on timeout"
        (let [
            before (initial-state 0 config)
            [after msgs] (timeout before)]
            (is (= :candidate (:statename after)))))
    (testing "candidate stays candidate on timeout"
        (let [
            [before, _] (become-candidate (initial-state 0 config))
            [after msgs] (timeout before)]
            (is (= :candidate (:statename after)))))
    (testing "candidate increments term on timeout"
        (let [
            [after msgs] (timeout (initial-state 0 config))]
            (is (= 1 (:current-term after))))))

(deftest request-vote-follower-test
    (testing "state unchanged if term is old"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after, msgs] (request-vote before 41 a-candidate-id 0 0)]
            (is (= before after))))
    (testing "vote denied if term is old"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after, [msg]] (request-vote before 41 a-candidate-id 0 0)]
            (is (false? (last msg)))))
    (testing "grants vote if up to date"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after, [msg]] (request-vote before 42 a-candidate-id 0 0)]
            (is (true? (last msg)))))
    (testing "resets timer when granting vote"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after msgs] (request-vote before 42 a-candidate-id 0 0)
            [reply [target type args]] msgs]
                (is (= 'reset type))))
    (testing "denies vote if already voted"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [during, msgs] (request-vote before 42 a-candidate-id 0 0)
            [after, [msg]] (request-vote during 42 another-candidate-id 0 0)]
            (is (false? (last msg)))))
    (testing "updates current-term on higher term"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after, [msg]] (request-vote before 43 a-candidate-id 0 0)]
            (is (= 43 (:current-term after))))))

(deftest request-vote-candidate-test
    (testing "becomes follower if higher term"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after, msgs] (request-vote before 43 a-candidate-id 0 0)]
            (is (= :follower (:statename after)))))
    (testing "grants vote if higher term"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after msgs] (request-vote before 43 a-candidate-id 0 0)]
            (is (some (fn [[target type & args]] (and (= 'voted type) (true? (last args)))) msgs))))
     (testing "denies vote otherwise"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after, [msg]] (request-vote before 0 a-candidate-id 0 0)]
            (is (false? (last msg)))))
     )

(deftest become-leader-test
    (testing "sets statename to :leader"
        (is (= :leader (:statename (become-leader (initial-state 0 config))))))
    (testing "sets next-match to zero for cluster members"
        (is (= 0 (get (:next-match (become-leader (initial-state 0 config))) 12))))
    (testing "sets next-index to last-log-index+1 for cluster members"
        (is (= 1 (get (:next-index (become-leader (initial-state 0 config))) 12)))))

(deftest voted-test
    (testing "updates current-term on higher term"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after, [msg]] (voted before 43 a-candidate-id false)]
            (is (= 43 (:current-term after)))))
    (testing "follower remains follower on vote"
        (let [
            before (assoc (initial-state 0 config) :current-term 42)
            [after, [msg]] (voted before 43 a-candidate-id true)]
            (is (= :follower (:statename after)))))
    (testing "candidate accumulates granted votes"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after, [msg]] (voted before 1 a-candidate-id true)]
            (is (contains? (:votes after) a-candidate-id))))
    (testing "candidate does not accumulate denied votes"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after, [msg]] (voted before 1 a-candidate-id false)]
            (is (not (contains? (:votes after) a-candidate-id)))))
    (testing "candidate does not accumulate votes for older terms"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after, [msg]] (voted before 0 a-candidate-id true)]
            (is (not (contains? (:votes after) a-candidate-id)))))
    ; (testing "candidate ignores votes from servers not part of cluster"
    ;     (let [
    ;         [before, msgs] (timeout (initial-state 0 config))
    ;         [after, [msg]] (voted before 1 :nonmember true)]
    ;         (is (not (contains? (:votes after) :nonmember))))
    ;     )
    (testing "candidate ignores votes from older terms"
        (let [
            [before, msgs] (timeout (initial-state 0 config))
            [after, [msg]] (voted before 0 a-candidate-id true)]
            (is (not (contains? (:votes after) :nonmember))))
        )
    (testing "candidate becomes leader on majority"
        (let [
            [state, msgs] (timeout (initial-state 0 config))
            [state, [msg]] (voted state 1 12 true)
            [state, [msg]] (voted state 1 23 true)]
            (is (= :leader (:statename state))))
        ))

(deftest update-or-append-test
    (testing "appends missing entries"
        (is (= [1 2 3 4 5] (update-or-append [1 2 3] [1 2 3 4 5] []))))
    (testing "does not truncate on missing entries"
        (is (= [1 2 3] (update-or-append [1 2 3] [] []))))
    (testing "truncates on mismatch"
        (is (= [1 4] (update-or-append [1 2 3] [1 4] [])))))

(deftest append-log-test 
    (testing "appends to the end if non-overlapping"
        (let [
            before (initial-state 0 config)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 0)]
            (is (= 
                [{:term 0} {:term 1} {:term 2}] 
                (:log after)))))
    (testing "overwrites if mismatched"
        (let [
            before (initial-state 0 config)
            entries [{:term 1} {:term 2}]
            during (append-log before 0 entries 0)
            after (append-log during 1 [{:term 3}] 0)]
            (is (= 
                [{:term 0} {:term 1} {:term 3}] 
                (:log after)))))
    (testing "overwrites and truncates if mismatched"
        (let [
            before (initial-state 0 config)
            entries [{:term 1} {:term 2} {:term 3}]
            during (append-log before 0 entries 0)
            after (append-log during 0 [{:term 4}] 0)]
            (is (= 
                [{:term 0} {:term 4}] 
                (:log after)))))
    (testing "does not overwrite with empty entries"
        (let [
            before (initial-state 0 config)
            entries [{:term 1} {:term 2}]
            during (append-log before 0 entries 0)
            after (append-log during 0 [] 0)]
            (is (= 
                [{:term 0} {:term 1} {:term 2}] 
                (:log after)))))
    (testing "updates commit-index to leader-commit < last-entry-index"
        (let [
            before (initial-state 0 config)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 1)]
            (is (= 1 (:commit-index after)))))
    (testing "updates commit-index to leader-commit == last-entry-index"
        (let [
            before (initial-state 0 config)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 2)]
            (is (= 2 (:commit-index after)))))
    (testing "does not update commit-index to leader-commit < commit-index"
        (let [
            before (assoc (assoc (initial-state 0 config) :commit-index 3) :log [{:term 0} {:term 0} {:term 0} {:term 0}])
            entries []
            after (append-log before 0 entries 2)]
            (is (= 3 (:commit-index after))))))

(deftest highest-majority-test 
    (testing "yields default value when no majority is present"
        (is (= 0 (highest-majority (:members config) [1 2] 0))))
    (testing "yields default on empty vector"
        (is (= 0 (highest-majority (:members config) [] 0))))
    (testing "yields majority if present"
        (is (= 1 (highest-majority (:members config) [1 1 2] 0))))
    (testing "yields highest majority if present"
        (is (= 2 (highest-majority (:members config) [1 1 2 2 2] 0)))))

(deftest new-commit-index-test 
    (testing "yields nil on no majority"
        (is (nil? (new-commit-index (:members config) 0 [{:term 0}] [] 0))))
    (testing "yields replicated uncommited index"
        (is (= 1 (new-commit-index (:members config) 0 [{:term 0} {:term 0} {:term 0}] [1 1 1] 0))))
    (testing "yields highest replicated uncommited index"
        (is (= 2 (new-commit-index (:members config) 0 [{:term 0} {:term 0} {:term 0}] [2 2 2] 0))))
    (testing "yields highest replicated uncommited index from current term"
        (is (= 1 (new-commit-index (:members config) 1 [{:term 0} {:term 1} {:term 2}] [2 2 2] 0))))
    (testing "self participates in voting with highest log index"
        (is (= 1 (new-commit-index (:members config) 0 [{:term 0} {:term 0}] [2 2 1] 0)))))

(deftest appended-test
    (testing "leader becomes follower if higher term"
        (let [
            before (become-leader (initial-state 0 config))
            [after, msgs] (appended before 43 a-candidate-id 1 false)]
            (is (= :follower (:statename after)))))
    (testing "updates peer next-index and match-index on success"
        (let [
            before (assoc (become-leader (initial-state 0 config)) :log [{:term 0} {:term 0}])
            [after, msgs] (appended before 0 a-candidate-id 3 true)]
            (do
                (is (= 3 (get-in after [:next-index a-candidate-id])))
                (is (= 2 (get-in after [:next-match a-candidate-id])))
                )))
    (testing "does not update peer next-index and match-index on older term"
        (let [
            [before _] (timeout (initial-state 0 config))
            before (assoc (become-leader before) :log [{:term 0} {:term 0}])
            [after, msgs] (appended before 0 a-candidate-id 2 true)]
            (do
                (is (= 1 (get-in after [:next-index a-candidate-id])))
                (is (= 0 (get-in after [:next-match a-candidate-id]))))))
    (testing "updates commit-index on successful replication on majority"
        (let [
            state (assoc (become-leader (initial-state 0 config)) :log [{:term 0} {:term 0} {:term 0}])
            [state, msgs] (appended state 0 a-candidate-id 3 true)
            [state, msgs] (appended state 0 another-candidate-id 3 true)]
            (is (= 2 (:commit-index state)))))
    (testing "notifies success to clients on commit-index update"
        (let [
            state (assoc-in (assoc (become-leader (initial-state 0 config)) :log [{:term 0} {:term 0} {:term 0}]) [:client-reqs 1] :a-client)
            [state, msgs] (appended state 0 a-candidate-id 2 true)
            [state, msgs] (appended state 0 another-candidate-id 2 true)]
            (is (some #(= :a-client (first %)) msgs)))))

(deftest append-entries-test
    (testing "candidate becomes follower if higher term"
        (let [
            [before msgs] (timeout (initial-state 0 config))
            [after, msgs] (append-entries before 43 a-candidate-id 0 0 [] 0)]
            (is (= :follower (:statename after)))))
    (testing "replies with failure if lower term"
        (let [
            [before msgs] (timeout (initial-state 0 config))
            [after, [msg]] (append-entries before 0 a-candidate-id 0 0 [] 0)]
            (is (= false (last msg)))))
    (testing "replies with failure if referencing non-existent prev-log-entry"
        (let [
            before (initial-state 0 config)
            [after, msgs] (append-entries before 0 a-candidate-id 1 1 [] 0)]
            (is (some #(= false (last %)) msgs))))
    (testing "resets timer if referencing non-existent prev-log-entry"
        (let [
            before (initial-state 0 config)
            [after, msgs] (append-entries before 1 a-candidate-id 1 1 [] 0)]
            (is (some (fn [[target type args]] (= 'reset type)) msgs))))
     (testing "replies with success if referencing existing prev-log-entry"
        (let [
            before (initial-state 0 config)
            [after, msgs] (append-entries before 1 a-candidate-id 0 0 [] 0)]
            (is (some #(= true (last %)) msgs))))
     (testing "resets timer on successful append-entries"
        (let [
            before (initial-state 0 config)
            [after, msgs] (append-entries before 1 a-candidate-id 0 0 [] 0)]
            (is (some (fn [[target type args]] (= 'reset type)) msgs))))
     (testing "applies committed commands to fsm"
        (let [
            before (initial-state 0 config)
            [after, msgs] (append-entries before 0 a-candidate-id 0 0 [{:term 0 :cmd "mario"}, {:term 0 :cmd "luigi"}] 1)]
            (is (= ["mario"] (:fsm after)))))
     (testing "applies committed commands to fsm"
        (let [
            before (initial-state 0 config)
            [after, msgs] (append-entries before 0 a-candidate-id 0 0 [{:term 0 :cmd "mario"}] 1)]
            (is (= 1 (:last-applied after))))))

; TODO: what does it mean to "retry" in case of timeout?


