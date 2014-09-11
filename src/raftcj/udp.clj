(ns raftcj.udp
  (:import (java.net DatagramPacket DatagramSocket InetAddress)))

(defn make-socket [port] (new DatagramSocket port))
(defn make-address [ip] (InetAddress/getByName ip))
    
(defn send-data [socket [ipaddress port] data]
  (let [
    bytes (.getBytes data)
    packet (new DatagramPacket bytes (count bytes) ipaddress port)]
  (.send socket packet)))

(defn recv-data [socket]
  (let [
    buffer (byte-array 1024)
    packet (new DatagramPacket buffer 1024)]
  (.receive socket packet)
  (new String (.getData packet) 0 (.getLength packet))))