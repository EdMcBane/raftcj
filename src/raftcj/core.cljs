(ns raftcj.core
	(:require 
        cljs.reader
		[raftcj.loop :refer [eventloop recv-event]]
		[raftcj.base :refer [initial-state msg]]))

(enable-console-print!)

(defn run [config id]
    (let [
        state (initial-state id config)
        [ip port] (get-in config [:members id])
        sender { :send-fn (fn [dest data] (println "sending to " dest data)) :serialize-fn pr-str }
        eventsrc nil
        events (cons (msg id 'init) eventsrc)]
            (eventloop config sender state events)))

(defn -main [id]
    (run {
        :heartbeat-delay 1000
        :election-delay  2000
        :members {
            0  ["127.0.0.1" 10000]
            12 ["127.0.0.1" 10012]
            23 ["127.0.0.1" 10023]
            34 ["127.0.0.1" 10034]
            45 ["127.0.0.1" 10045]}}
        (cljs.reader/read-string id)))
