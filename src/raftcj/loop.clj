(ns raftcj.loop
  (:require clojure.string
  [clojure.edn :as edn]
  [clojure.pprint]
  [raftcj.udp :refer :all]
  [raftcj.core :refer :all]
  [raftcj.fsm :refer :all]))

(defn now [] (System/currentTimeMillis))

(def alarm-time (atom nil))

(defn clear-timer []
  (swap! alarm-time (constantly nil)))

(defn reset [delay] 
  (swap! alarm-time (constantly (+ delay (now)))))

(defn calc-timeout []
    (if (nil? @alarm-time) 
        nil
        (- @alarm-time (now))))

(defn recv-event [id selector] 
    (let [
        timeout (calc-timeout)]
        (if (or (nil? timeout) (> timeout 0))
            (let 
                [data (recv-data selector (or timeout 0))]
                (if (nil? data)
                    (recur id selector)
                    (cons id (edn/read-string data))))

            (do (clear-timer) (msg id 'timeout)))))

(defn send-event [selector dest event] 
    (send-data selector dest (pr-str event)))

(defn invoke-handler [state evname args]
    (apply (ns-resolve 'raftcj.fsm evname) (cons state args)))

(defn eventloop [dispatchfn state events]
    (let [
        event (first events)]
        (if (nil? event)
            :exit
            (let [
                [state newevents] (dispatchfn state event)]
                (recur dispatchfn state (concat newevents (rest events)))))))

(defn dispatch [config selector state [target evname & args]]
    (println "dispatching event" target evname args)
    (cond 
        (= :timer target)
        (do
            (apply  (ns-resolve 'raftcj.loop evname) args)
            [state []])

        (vector? target)
        (do
            (send-event selector (apply make-address target) (cons evname args))
            [state []])

        (number? target)
        (if (= (:id state) target)
            (invoke-handler state evname args)
            (let [
                [ip port] (get-in config [:members target])
                dest (make-address ip port)]
                (send-event selector dest (cons evname args))
                [state []]))
        
        :else
        (throw (new IllegalArgumentException (str "invalid target " target)))))

(defn run [config id]
    (let [
        state (initial-state id config)
        [ip port] (get-in config [:members id])
        addr (make-address ip port)
        chan (make-chan addr)
        selector (make-selector chan)
        network (repeatedly #(recv-event (:id state) selector))
        eventsrc (cons (msg id 'init) network)
        dispatchfn (partial dispatch config selector)]
            (eventloop dispatchfn state eventsrc)))

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