(ns raftcj.ui
    (:require 
        [raftcj.core :refer [-main world exec]]
        [dommy.utils :as utils]
        [dommy.core :as dommy])
    (:use-macros
        [dommy.macros :only [node sel sel1]]))

(defn display-state []
    (dommy/set-text! (sel1 :#id) (:id @world))
    (dommy/set-text! (sel1 :#state) (:statename @world))
    (dommy/set-text! (sel1 :#log) (:log @world))
    (dommy/set-text! (sel1 :#commit) (:commit-index @world))
    (dommy/set-text! (sel1 :#fsm) (:fsm @world)))

(defn start-polling []
    (js/setTimeout (fn [] (start-polling) (display-state)) 100))

(defn start-button [id]
    (let [
        btn (node [:button#start-btn {} (str "Start " id)])]
        (dommy/listen! btn :click 
            #(do (-main id) (start-polling)))
        btn))

(defn setup-ui []
    (apply (partial dommy/append! (sel1 :#cluster))
        (map start-button [0 12 23]))

    (dommy/listen! 
        (sel1 :#execute-btn) :click 
        #(let [
            cmd (dommy/value (sel1 :#cmd-field))]
            (exec cmd))))