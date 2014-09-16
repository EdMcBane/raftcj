(ns raftcj.base-test
  (:require [clojure.test :refer :all]
            [raftcj.base :refer :all]))

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
        (is (true? (up-to-date (initial-state 0 {}) 0 0))))
    (testing "yields true if higher term"
        (let [
            local-state (assoc (initial-state 0 {}) :log [{:term 0} {:term 1}])]
            (is (true? (up-to-date local-state 2 1)))))
    (testing "yields true if same term and higher index"
        (let [
            local-state (assoc (initial-state 0 {}) :log [{:term 0} {:term 1}])]
            (is (true? (up-to-date local-state 1 2)))))
    (testing "yields false if same term and lower index"
        (let [
            local-state (assoc (initial-state 0 {}) :log [{:term 0} {:term 1} {:term 1}])]
            (is (false? (up-to-date local-state 1 1)))))
    (testing "yields false if lower term"
        (let [
            local-state (assoc (initial-state 0 {}) :log [{:term 0} {:term 2}])]
            (is (false? (up-to-date local-state 1 1)))))
    (testing "yields true if same term and same index"
        (let [
            local-state (assoc (initial-state 0 {}) :log [{:term 0} {:term 1}])]
            (is (true? (up-to-date local-state 1 1))))))