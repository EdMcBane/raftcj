(ns raftcj.core
	(:require 
        cljs.reader
		[raftcj.loop :refer [eventloop]]
		[raftcj.base :refer [initial-state msg bad-arg]]
        [raftcj.peerjskey :refer [peerjs-key]]))

(enable-console-print!)

;;; webrtc stuff

(def peers (atom {}))
(defn webrtc-setup [id peerjs-key update-fn]
    (let [
        local (new js/Peer (str "id" id) #js {"key" peerjs-key})]
        (.on local "connection" #(.on % "data" update-fn))
        (swap! peers assoc :local local)))

(defn connect! [local peer-id]
    (let [
        conn (.connect local (str "id" peer-id))]
        (.on conn "error" (partial println "error in connection to" peer-id))
        (.on conn "open" (partial println "opened connection to" peer-id))
        conn))

(defn webrtc-send [dest data]
    (let [
        conn (get @peers dest)]
        (if conn
            (if (aget conn "open")
                (.send conn data)
                (do (.close conn) (swap! peers dissoc dest)))
            (let [
                conn (connect! (:local @peers) dest)]
                (swap! peers assoc dest conn)))))

;;; event-driven stuff

(def world (atom nil))
(def dispatch-fn (atom nil))
(defn update-world [event]
    (let [
        state (eventloop @dispatch-fn @world [event])]
        (swap! world (constantly state))))

(def timer (atom nil))
(defn reset [delay] 
    (when @timer
        (js/clearTimeout @timer))
    (let [
        timeout-event (msg (:id @world) 'timeout)
        timer-id (js/setTimeout #(update-world timeout-event) delay)]
        (swap! timer (constantly timer-id))))

(defn exec [cmd]
    (update-world (msg (:id @world) 'execute :client cmd))) ; todo accept a callback

(defn dispatch [net-send-event state [target evname & args]]
    (cond 
        (= :timer target)
        (do
            (apply reset args)
            [state []])

        (= :client target)
        (do
            (println "command executed")
            [state []])

        (number? target)
        (do
            (println "-->" target (cons evname args))
            (net-send-event target (cons evname args))
            [state []])
    
        :else
        (bad-arg "invalid target" target)))

(defn event-driven [config id net-send net-event-src]
    (let [
        state (initial-state id config)
        on-event-fn (fn [data]
            (update-world 
            (cons id (cljs.reader/read-string data))))
        net-send-event (fn [dest data] (net-send dest (pr-str data)))]
        (swap! world (constantly state))
        (swap! dispatch-fn (constantly (partial dispatch net-send-event))) ; TODO: make update-world an atom that updates itself
        (net-event-src on-event-fn)
        (update-world (msg id 'init))))

(defn webrtc [config id peerjs-key]
    (let [
        net-send webrtc-send
        net-event-src (partial webrtc-setup id peerjs-key)]
        (event-driven config id net-send net-event-src)))

(defn -main [id]
    (let [
        config {
            :heartbeat-delay 1000
            :election-delay  2000
            :members {
                0  0
                12 12
                23 23
            ;   34 34
            ;   45 45
            }}]
        (webrtc config id peerjs-key)))
