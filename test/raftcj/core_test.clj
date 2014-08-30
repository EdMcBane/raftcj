(ns raftcj.core-test
  (:require [clojure.test :refer :all]
            [raftcj.core :refer :all]))

(def any-candidate-id 42)

(deftest has-vote-test
    (testing "yields true if :voted-for is nil"
        (is (= true (has-vote {:voted-for nil} any-candidate-id))))
    (testing "yields true if :voted-for is candidate-id"
        (is (= true (has-vote {:voted-for 883} 883))))
    (testing "yields false if :voted-for is another candidate id"
        (is (= false (has-vote {:voted-for 1234} 883)))))

(deftest last-log-test
    (testing "throws if empty"
        (is (thrown? AssertionError (last-log {:log []}))))
    (testing "yields only element"
        (is (= 
            {:x :y} 
            ((last-log 
                {:log [{:x :y}]}) 0))))
    (testing "yields last element"
        (is (= 
            {:z :z} 
            ((last-log 
                {:log [{:x :x}, {:z :z}]}) 0))))
    (testing "yields last index"
        (is (= 
            1 
            ((last-log 
                {:log [{:x :x}, {:z :z}]}) 1)))))

(deftest up-to-date-test
    (testing "yields true if both empty"
        (is (true? (up-to-date (initial-state 0) 0 0))))
    (testing "yields false if higher term"
        (is (false? (up-to-date (initial-state 0) 1 0))))
    (testing "yields false if same term and higher index"
        (is (false? (up-to-date (initial-state 0) 0 1))))
    (testing "yields true if same term and same index"
        (is (true? (up-to-date 
            (update-in (initial-state 0) [:log] (fn [old] (conj old {:term 0})))
            0 1)))))