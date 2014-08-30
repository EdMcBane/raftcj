(ns raftcj.fsm-test
  (:require [clojure.test :refer :all]
            [raftcj.core :refer :all]
            [raftcj.fsm :refer :all]))

(def config [
             {:id 1 :addr "127.0.0.1"}
             {:id 2 :addr "127.0.0.2"}
             {:id 3 :addr "127.0.0.3"}])

(fsm config)

(def a-term 42)
(def a-candidate-id 12)
(def another-candidate-id 23)


(deftest become-follower-test
    (testing ":statename is :follower on becoming follower"
        (is (= :follower (:statename (become-follower (initial-state 0) a-term)))))
    (testing ":voted-for is nil on becoming follower"
        (is (nil? (:voted-for (become-follower (initial-state 0) a-term)))))
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
     (testing "grants vote if up to date")
     (testing "denies vote if already voted"))


; (let [
;       [state1 & msgs] (timeout (initial-state 1))
;       [state2 & msgs] (append-entries state1 2)]
;   [state1 state2]
;   )