(ns raftcj.loop
  (:require clojure.string
  [clojure.edn :as edn]
  [raftcj.udp :refer :all]
  [raftcj.core :refer :all]
  [raftcj.fsm :refer :all]))

(defn recv-event [sock] 
    (edn/read-string (recv-data sock)))
(defn send-event [sock dest event] 
    (send-data sock dest (pr-str event)))

(defn invoke-handler [state evname args]
    (apply (ns-resolve 'raftcj.fsm evname) (cons state args)))

(defn eventloop [sendfn state events]
    (let [
        event (first events)
        otherevents (rest events)]
        (if (nil? event)
            :exit
            (let [
                [target evname & args] event]
                (if (= (:id state) target)
                    (let [
                        [state newevents] (invoke-handler state evname args)]
                        (recur sendfn state (concat newevents otherevents)))
                    (do 
                        (sendfn target (cons evname args)) 
                        (recur sendfn state otherevents)))))))

(defn run [config id]
    (let [
        state (initial-state id config)
        [ip port] (get-in config [:members id])
        sock (make-socket port)
        network (repeatedly #(recv-event sock))
        eventsrc (cons (msg id 'init) network)
        sendfn (fn [target event] 
            (let [
                [ip port] (get-in config [:members target] ["127.0.0.1" 19999])]
                (send-event sock [(make-address ip) port] event)))]
        (eventloop sendfn state eventsrc)))


(def config {
    :heartbeat-delay 1
    :election-delay  3
    :members {
        0  ["127.0.0.1" 10000]
        12 ["127.0.0.1" 10012]
        23 ["127.0.0.1" 10023]
        34 ["127.0.0.1" 10034]
        45 ["127.0.0.1" 10045]}})

(defn -main []
    (run config 0))