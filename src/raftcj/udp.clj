(ns raftcj.udp
  (:import 
    (java.net InetSocketAddress) 
    (java.nio.channels DatagramChannel Selector SelectionKey)
    (java.nio CharBuffer ByteBuffer)
    (java.nio.charset Charset)))

(defn make-chan [addr] 
  (println "binding to" addr)
  (-> (java.nio.channels.DatagramChannel/open) 
    (.bind addr)
    (.configureBlocking false)))

(defn make-selector [chan]
  (let [
    selector (Selector/open)]
    (.register chan selector (SelectionKey/OP_READ))
    selector))

(defn make-address [ip port] (new InetSocketAddress ip port))
    
(defn send-data [selector dest data]
  (let [
    chan (.channel (first (.keys selector)))
    charset (Charset/defaultCharset)
    buffer (.encode charset (CharBuffer/wrap data))]
  (.send chan buffer dest)))

(defn recv-data [selector timeout]
  (let [
    buffer (ByteBuffer/allocate 1024)
    charset (Charset/defaultCharset)
    n-selectables (.select selector timeout)]
  (if (> n-selectables 0)
    (let [
      selkey (first (.selectedKeys selector))]
      (.remove (.selectedKeys selector) selkey) 
      (.receive (.channel selkey) buffer )
      (.flip buffer)
      (.toString (.decode charset buffer)))
    nil)))
    