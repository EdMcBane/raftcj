(ns raftcj.loop
  (:require clojure.string
  [clojure.edn :as edn]
  [clojure.pprint]
  [raftcj.udp :refer :all]
  [raftcj.core :refer :all]
  [raftcj.fsm :refer :all]))

(def next-timeout (atom nil))
(defn clear-timer []
  (swap! next-timeout (constantly nil)))
(defn reset [delay] 
  (swap! next-timeout (constantly (+ delay (System/currentTimeMillis)))))

(defn calc-offset []
    (if (nil? (deref next-timeout))
        nil
        (let [
            now (System/currentTimeMillis)]
            (- (deref next-timeout) now))))


        
(defn recv-event [id selector] 
    (let [
        delay (calc-offset)]
        (cond
            (nil? delay)
            (let 
                [data (recv-data selector 0)] ;TODO: duplication, see below
                (if (nil? data)
                    (recur id selector)
                    (cons id (edn/read-string data)) ))

            (<= delay 0)
            (do (clear-timer) (msg id 'timeout))
            
            :else
            (let 
                [data (recv-data selector delay)]
                (if (nil? data)
                    (recur id selector)
                    (cons id (edn/read-string data)))))))

(defn send-event [selector dest event] 
    (send-data selector dest (pr-str event)))

(defn invoke-handler [state evname args]
    (apply (ns-resolve 'raftcj.fsm evname) (cons state args)))

(defn eventloop [dispatchfn state events]
    (let [
        event (first events)
        otherevents (rest events)]
        (if (nil? event)
            :exit
            (let [
                [state newevents] (dispatchfn state event)]
                (recur dispatchfn state (concat newevents otherevents))))))

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
        _ (clojure.pprint/pprint state)
        [ip port] (get-in config [:members id])
        addr (make-address ip port)
        chan (make-chan addr)
        selector (make-selector chan)
        network (repeatedly #(recv-event (:id state) selector))
        eventsrc (cons (msg id 'init) network)
        dispatchfn (partial dispatch config selector)]
            (eventloop dispatchfn state eventsrc)))


(def config {
    :heartbeat-delay 1000
    :election-delay  2000
    :members {
        0  ["127.0.0.1" 10000]
        12 ["127.0.0.1" 10012]
        23 ["127.0.0.1" 10023]
        34 ["127.0.0.1" 10034]
        45 ["127.0.0.1" 10045]}})

(defn -main [id]
    (run config (read-string id)))