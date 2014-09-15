(ns raftcj.core
	(:require 
		[raftcj.loop :refer [eventloop]]
		[raftcj.base :refer [initial-state msg bad-arg]]
		[raftcj.udp :refer [make-socket send-data recv-data]]
		[clojure.edn :as edn])
    (:gen-class))

(defn now [] (System/currentTimeMillis))

(def alarm-time (atom nil))

(defn clear-timer []
    (swap! alarm-time (constantly nil)))

(defn calc-timeout []
    (if (nil? @alarm-time) 
        nil
        (- @alarm-time (now))))

(defn reset [delay] 
  (swap! alarm-time (constantly (+ delay (now)))))

(defn recv-event-or-timeout [net-recv-event id]
    (let [
        timeout (calc-timeout)]
        (if (or (nil? timeout) (> timeout 0))
            (let 
                [data (net-recv-event (or timeout 0))]
                (if (nil? data)
                    (recur net-recv-event id)
                    (cons id data)))

            (do (clear-timer) (msg id 'timeout)))))

(defn send-event [send-fn serialize-fn dest data]
    (send-fn (serialize-fn data)))

(defn dispatch [net-send-event state [target evname & args]]
    (println "dispatching event" target evname args)
    (cond 
        (= :timer target)
        (do
            (apply reset args)
            [state []])

        (number? target)
        (do 
            (net-send-event target (cons evname args))
            [state []])
    
        :else
        (bad-arg "invalid target" target)))

(defn sync-timeout [config id net-send net-recv]
    (let [
        state (initial-state id config)
        net-recv-event (fn [timeout] (if-let [result (net-recv timeout)] (edn/read-string result) nil))
        receiver (repeatedly #(recv-event-or-timeout net-recv-event id))
        eventsrc (cons (msg id 'init) receiver)
        net-send-event (fn [dest data] (net-send dest (pr-str data)))
        dispatch-fn (partial dispatch net-send-event)]
            (eventloop dispatch-fn state eventsrc)))

(defn udp [config id]
    (let [
        bind-addr (get-in config [:members id])
        async-socket (make-socket bind-addr)
        net-send (fn [dest data] (send-data async-socket ((:members config) dest) data))
        net-recv (partial recv-data async-socket)]
        (sync-timeout config id net-send net-recv)))

(defn -main [id]
    (let [
        id  (read-string id)
        config {
            :heartbeat-delay 1000
            :election-delay  2000
            :members {
                0  ["127.0.0.1" 10000]
                12 ["127.0.0.1" 10012]
                23 ["127.0.0.1" 10023]
                34 ["127.0.0.1" 10034]
                45 ["127.0.0.1" 10045]}}]
        (udp config id)))

