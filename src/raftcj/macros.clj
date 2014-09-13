(ns raftcj.macros)

(defmacro defev [evname selector rest failmsgs body] 
    `(defmethod ~evname ~selector ~(vec (concat ['state 'term] rest))
      (cond 
        (> ~'term (:current-term ~'state))
        (~'redispatch
            (~'become-follower ~'state ~'term)
            ~evname
            ~'term ~@rest)
        
        (< ~'term (:current-term ~'state))
        [~'state ~failmsgs]
        
        :else
        ~body)))