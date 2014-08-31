(ns raftcj.fsm-test
  (:require [clojure.test :refer :all]
            [raftcj.core :refer :all]
            [raftcj.fsm :refer :all]))

(def config {0 "127.0.0.1"
             12 "127.0.0.2"
             23 "127.0.0.3"
             34 "127.0.0.4"
             45 "127.0.0.5"})

(fsm config)

(def a-term 42)
(def a-candidate-id 12)
(def another-candidate-id 23)


(deftest become-follower-test
    (testing ":statename is :follower on becoming follower"
        (is (= :follower (:statename (become-follower (initial-state 0) a-term)))))
    (testing ":voted-for is nil on becoming follower"
        (is (nil? (:voted-for (become-follower (assoc (initial-state 0) :voted-for a-candidate-id) a-term)))))
    (testing ":current-term is new term on becoming follower"
        (is (= a-term (:current-term (become-follower (initial-state 0) a-term))))))

(deftest msg-test :todo :really?)
(deftest vote-test 
    (testing "throws if already voted for other candidate"
        (is (thrown? AssertionError 
            (vote 
                (vote (initial-state 0) a-candidate-id) 
                another-candidate-id))))
    (testing "yields input if already voted for same candidate"
        (let [ 
            before (vote (initial-state 0) a-candidate-id)]
            (is (=
                before
                (vote before a-candidate-id)))))
    (testing "sets voted-for to candidate-id"
        (is (= 
            a-candidate-id 
            (:voted-for (vote (initial-state 0) a-candidate-id))))))

(deftest become-candidate-test
    (testing "statename becomes :candidate"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            after (become-candidate before)]
            (is (= :candidate (:statename after)))))
    (testing "votes are reset"
        (let [
            before (assoc (initial-state 0) :votes #{:a :b})
            after (become-candidate before)]
            (is (= 0 (count (:votes after)))))))

(deftest timeout-test
    (testing "follower becomes candidate on timeout"
        (let [
            before (initial-state 0)
            [after msgs] (timeout before)]
            (is (= :candidate (:statename after)))))
    (testing "candidate stays candidate on timeout"
        (let [
            before (become-candidate (initial-state 0))
            [after msgs] (timeout before)]
            (is (= :candidate (:statename after)))))
    (testing "candidate increments term on timeout"
        (let [
            [after msgs] (timeout (initial-state 0))]
            (is (= 1 (:current-term after))))))

(deftest request-vote-follower-test
    (testing "state unchanged if term is old"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [after, msgs] (request-vote before 41 a-candidate-id 0 0)]
            (is (= before after))))
    (testing "vote denied if term is old"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [after, [msg]] (request-vote before 41 a-candidate-id 0 0)]
            (is (false? (last msg)))))
    (testing "grants vote if up to date"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [after, [msg]] (request-vote before 42 a-candidate-id 0 0)]
            (is (true? (last msg)))))
    (testing "denies vote if already voted"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [during, msgs] (request-vote before 42 a-candidate-id 0 0)
            [after, [msg]] (request-vote during 42 another-candidate-id 0 0)]
            (is (false? (last msg)))))
    (testing "updates current-term on higher term"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [after, [msg]] (request-vote before 43 a-candidate-id 0 0)]
            (is (= 43 (:current-term after))))))

(deftest request-vote-candidate-test
    (testing "becomes follower if higher term"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, msgs] (request-vote before 43 a-candidate-id 0 0)]
            (is (= :follower (:statename after)))))
    (testing "grants vote if higher term"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, [[target type & args]]] (request-vote before 43 a-candidate-id 0 0)]
            (is (true? (last args)))))
     (testing "denies vote otherwise"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, [msg]] (request-vote before 0 a-candidate-id 0 0)]
            (is (false? (last msg)))))
     )

(deftest become-leader-test
    (testing "sets statename to :leader"
        (is (= :leader (:statename (become-leader (initial-state 0))))))
    (testing "sets next-match to zero for cluster members"
        (is (= 0 (get (:next-match (become-leader (initial-state 0))) 12))))
    (testing "sets next-index to last-log-index+1 for cluster members"
        (is (= 1 (get (:next-index (become-leader (initial-state 0))) 12)))))

(deftest voted-test
    (testing "updates current-term on higher term"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [after, [msg]] (voted before 43 a-candidate-id false)]
            (is (= 43 (:current-term after)))))
    (testing "follower remains follower on vote"
        (let [
            before (assoc (initial-state 0) :current-term 42)
            [after, [msg]] (voted before 43 a-candidate-id true)]
            (is (= :follower (:statename after)))))
    (testing "candidate accumulates granted votes"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, [msg]] (voted before 1 a-candidate-id true)]
            (is (contains? (:votes after) a-candidate-id))))
    (testing "candidate does not accumulate denied votes"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, [msg]] (voted before 1 a-candidate-id false)]
            (is (not (contains? (:votes after) a-candidate-id)))))
    (testing "candidate ignores votes from servers not part of cluster"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, [msg]] (voted before 1 :nonmember true)]
            (is (not (contains? (:votes after) :nonmember))))
        )
    (testing "candidate ignores votes from older terms"
        (let [
            [before, msgs] (timeout (initial-state 0))
            [after, [msg]] (voted before 0 a-candidate-id true)]
            (is (not (contains? (:votes after) :nonmember))))
        )
    (testing "candidate becomes leader on majority"
        (let [
            [state, msgs] (timeout (initial-state 0))
            [state, [msg]] (voted state 0 12 true)
            [state, [msg]] (voted state 0 23 true)]
            (is (= :leader (:statename state))))
        ))

(deftest append-log-test 
    (testing "appends to the end if non-overlapping"
        (let [
            before (initial-state 0)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 0)]
            (is (= 
                [{:term 0} {:term 1} {:term 2}] 
                (:log after)))))
    (testing "overwrites if overlapping"
        (let [
            before (initial-state 0)
            entries [{:term 1} {:term 2}]
            during (append-log before 0 entries 0)
            after (append-log during 1 [{:term 3}] 0)]
            (is (= 
                [{:term 0} {:term 1} {:term 3}] 
                (:log after)))))
    (testing "updates commit-index to leader-commit < last-entry-index"
        (let [
            before (initial-state 0)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 1)]
            (is (= 1 (:commit-index after)))))
    (testing "updates commit-index to leader-commit == last-entry-index"
        (let [
            before (initial-state 0)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 2)]
            (is (= 2 (:commit-index after)))))
    (testing "does not update commit-index to leader-commit < commit-index"
        (let [
            before (assoc (initial-state 0) :commit-index 3)
            entries [{:term 1} {:term 2}]
            after (append-log before 0 entries 2)]
            (is (= 3 (:commit-index after))))))

(deftest appended-test
    (testing "leader becomes follower if higher term"
        (let [
            before (become-leader (initial-state 0))
            [after, msgs] (appended before 43 a-candidate-id false)]
            (is (= :follower (:statename after))))))

(deftest append-entries-test
    (testing "candidate becomes follower if higher term"
        (let [
            [before msgs] (timeout (initial-state 0))
            [after, msgs] (append-entries before 43 a-candidate-id 0 0 [] 0)]
            (is (= :follower (:statename after)))))
    (testing "replies with failure if lower term"
        (let [
            [before msgs] (timeout (initial-state 0))
            [after, [msg]] (append-entries before 0 a-candidate-id 0 0 [] 0)]
            (is (= false (last msg)))))
    (testing "replies with failure if referencing non-existent prev-log-entry"
        (let [
            [before msgs] (timeout (initial-state 0))
            [after, [msg]] (append-entries before 1 a-candidate-id 1 1 [] 0)]
            (is (= false (last msg)))))
     (testing "replies with success if referencing existing prev-log-entry"
        (let [
            [before msgs] (timeout (initial-state 0))
            [after, [msg]] (append-entries before 1 a-candidate-id 0 0 [] 0)]
            (is (= true (last msg))))))




















