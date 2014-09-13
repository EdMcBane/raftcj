(ns raftcj.core
	(:require 
		[raftcj.loop :refer [eventloop recv-event]]
		[raftcj.base :refer [initial-state msg]]
		[raftcj.udp :refer [make-socket send-data recv-data]]
		[clojure.edn :as edn]))

(defn run [config id]
    (let [
        state (initial-state id config)
        [ip port] (get-in config [:members id])
        dest [ip port]
        selector (make-socket dest)
        sender {:send-fn (partial send-data selector) :serialize-fn pr-str}
        recv-fn (partial recv-data selector)
        deserialize-fn edn/read-string
        receiver (repeatedly #(recv-event recv-fn deserialize-fn (:id state)))
        eventsrc (cons (msg id 'init) receiver)]
            (eventloop config sender state eventsrc)))

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
        (read-string id)))