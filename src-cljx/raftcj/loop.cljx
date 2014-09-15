(ns raftcj.loop
  (:require clojure.string
  [raftcj.base :refer [msg]]
  [raftcj.fsm]))

#+cljs 
(defn resolve-handler [sym] 
    ({  'execute        raftcj.fsm/execute
        'request-vote   raftcj.fsm/request-vote
        'voted          raftcj.fsm/voted
        'append-entries raftcj.fsm/append-entries
        'appended       raftcj.fsm/appended
        'timeout        raftcj.fsm/timeout
        'init           raftcj.fsm/init}
    sym))

#+clj
(defn resolve-handler [sym]
    (ns-resolve 'raftcj.fsm sym))

(defn invoke-handler [state evname args]
    (apply (resolve-handler evname) (cons state args)))

(defn eventloop [dispatch-fn state events]
    (if (empty? events)
        state
        (let [
            [target evname & args :as event] (first events)
            [state newevents] 
            (if (= (:id state) target)
                (invoke-handler state evname args)
                (dispatch-fn state event))]
            (recur dispatch-fn state (concat newevents (rest events))))))