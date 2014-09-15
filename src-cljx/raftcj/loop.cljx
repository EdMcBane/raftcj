(ns raftcj.loop
  (:require clojure.string
  [raftcj.base :refer [msg]]
  [raftcj.fsm]))

(defn now [] 
    #+clj (System/currentTimeMillis)
    #+cljs (.now js/Date))
(defn bad-arg [& args] 
    #+clj (throw (new IllegalArgumentException (apply str args)))
    #+cljs (throw (apply str args)))


(def alarm-time (atom nil))

(defn clear-timer []
    (swap! alarm-time (constantly nil)))

(defn calc-timeout []
    (if (nil? @alarm-time) 
        nil
        (- @alarm-time (now))))

(defn recv-event [recv-fn deserialize-fn id]
    (let [
        timeout (calc-timeout)]
        (if (or (nil? timeout) (> timeout 0))
            (let 
                [data (recv-fn (or timeout 0))]
                (if (nil? data)
                    (recur recv-fn deserialize-fn id)
                    (cons id (deserialize-fn data))))

            (do (clear-timer) (msg id 'timeout)))))

(defn send-event [send-fn serialize-fn dest event] 
    (send-fn dest (serialize-fn event)))

#+cljs 
(defn ns-resolve [_ns sym] 
    ({  'execute        raftcj.fsm/execute
        'request-vote   raftcj.fsm/request-vote
        'voted          raftcj.fsm/voted
        'append-entries raftcj.fsm/append-entries
        'appended       raftcj.fsm/appended
        'timeout        raftcj.fsm/timeout
        'init           raftcj.fsm/init}
    sym))

(defn invoke-handler [state evname args]
    (apply (ns-resolve 'raftcj.fsm evname) (cons state args)))

(defn dispatch [config sender state [target evname & args]]
    (println "dispatching event" target evname args)
    (let [
        do-send (partial send-event (:send-fn sender) (:serialize-fn sender))]
        (cond 
            (= :timer target)
            (do
                (apply  (:reset sender) sender args)
                [state []])

            (number? target)
            (if (= (:id state) target)
                (invoke-handler state evname args)
                (let [
                    dest (get-in config [:members target])]
                    (do-send dest (cons evname args))
                    [state []]))
            
            :else
            (do
                (do-send target (cons evname args))
                [state []]))))

(defn eventloop [config sender state events]
    (let [
        event (first events)]
        (if (nil? event)
            state
            (let [
                [state newevents] (dispatch config sender state event)]
                (recur config sender state (concat newevents (rest events)))))))
