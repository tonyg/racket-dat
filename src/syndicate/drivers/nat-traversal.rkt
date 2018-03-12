#lang syndicate

(provide (struct-out port-assignment) ;; defined in nat-traversal
         (struct-out nat-mapping))

(require (prefix-in core: syndicate/core))
(require nat-traversal)

(assertion-struct nat-mapping (requested-protocol requested-interface requested-port assignment))

(spawn #:name 'nat-traversal
       (during/spawn (observe (nat-mapping $protocol $interface $port _))
         #:name (list 'nat-mapping protocol port)

         (define (handle-mapping-change assignments)
           (send-ground-patch
            (for/fold [(acc (core:retract (nat-mapping protocol interface port ?)))]
                      [(a assignments)]
              (patch-seq acc (core:assert (nat-mapping protocol interface port a))))))

         (define m (mapping-change-listener protocol (or interface "0.0.0.0") port handle-mapping-change))

         (on-stop (mapping-change-listener-stop! m))

         (during (inbound (nat-mapping protocol interface port $a))
           (assert (nat-mapping protocol interface port a)))))
