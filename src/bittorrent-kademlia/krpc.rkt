#lang syndicate

(require (only-in racket/random crypto-random-bytes))
(require (only-in file/sha1 bytes->hex-string))
(require racket/set)
(require bitsyntax)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/timestate)
(require syndicate/protocol/advertise)

(require "wire.rkt")
(require "protocol.rkt")

(spawn #:name 'kademlia-krpc-udp-transport
       (stop-when-reloaded)

       (define PORT 42769)
       (define endpoint (udp-listener PORT))

       (on (message (udp-packet $peer endpoint $body))
           (define p (decode-packet body peer))
           (when p
             (log-info "RECEIVING ~a ~v" peer p)
             (define-values (id type body) (analyze-krpc-packet p))
             (send! (krpc-packet 'inbound peer id type body))))

       (on (message (krpc-packet 'outbound $peer $id $type $body))
           (define p (synthesize-krpc-packet id type body))
           ;; (log-info "SENDING ~a ~v" peer p)
           (send! (udp-packet endpoint peer (encode-packet p))))

       (during (advertise (udp-packet _ endpoint _))
         (on-start (log-info "Socket is ready."))
         (assert (krpc-ready))))

(spawn #:name 'kademlia-krpc-transaction-manager
       (stop-when-reloaded) ;; TODO: this really shouldn't start from scratch at each reload!
       (field [counter 1]) ;; TODO: start at a randomized location to avoid martian packets
       (during (observe (fresh-transaction $name _))
         (define number (counter))
         (counter (+ number 1))
         (define byte-count (max 2 (quotient (+ 7 (integer-length number)) 8)))
         (define id (bit-string (number :: little-endian bytes byte-count)))
         (during (krpc-ready)
           (assert (fresh-transaction name id)))))

(spawn #:name 'kademlia-krpc-transaction-manager
       (stop-when-reloaded)

       (field [active-queries (set)])

       (during (local-node $local-id)
         (during/spawn (observe (krpc-transaction $src $tgt $pr $txn-name $method $args _))
           (field [results #f])
           (assert #:when (results) (krpc-transaction src tgt pr txn-name method args (results)))
           (during (fresh-transaction txn-name $txn)
             (if pr
                 (on-start (send! (krpc-packet 'outbound pr txn 'request (list method args))))
                 (during (node-coordinates tgt $peer _)
                   (on-start (send! (krpc-packet 'outbound peer txn 'request (list method args))))))
             (stop-when-timeout 5000
               (log-info "KRPC request timeout contacting ~a" (bytes->hex-string tgt))
               (send! (node-activity tgt 'timeout))
               (results 'timeout))
             (stop-when (message (krpc-packet 'inbound $peer1 txn 'response $details))
               (log-info "KRPC reply from ~a (~a): ~v" (bytes->hex-string tgt) peer1 details)
               (send! (node-activity tgt 'activity))
               (suggest-node! 'received-response tgt peer1)
               (results details))
             (stop-when (message (krpc-packet 'inbound $peer1 txn 'error $details))
               (log-info "KRPC error from ~a (~a): ~v" (bytes->hex-string tgt) peer1 details)
               ;; Error replies probably... shouldn't? count as activity??
               ;; (send! (node-activity tgt 'activity))
               ;; (suggest-node! 'received-error tgt peer1`)
               (results 'error))))))

(define (spawn-node [local-id (crypto-random-bytes 20)])
  (spawn #:name (list 'kademlia-node local-id)
         (stop-when-reloaded)

         (assert (local-node local-id))
         (assert (known-node local-id))
         (on-start (log-info "Starting node ~v" (bytes->hex-string local-id)))
         (on-stop (log-info "Stopping node ~v" (bytes->hex-string local-id)))

         (on-start
          (react (assert (locate-node local-id #t))
                 (stop-when (asserted (closest-nodes-to local-id _ #t))
                   (log-info "Initial discovery of nodes close to self complete."))
                 (on (asserted (closest-nodes-to local-id $ns _))
                     (log-info "Closest to self: ~a" (format-nodes/peers ns)))))

         (on (message (krpc-packet 'inbound $peer $txn 'request (list $method $details)))
             (spawn* #:name (list 'kademlia-request-handler peer txn method details)
                     (define peer-id (hash-ref details #"id")) ;; required in all BEP 0005 requests
                     (send! (node-activity peer-id 'activity))
                     (suggest-node! 'incoming-request peer-id peer)
                     (match method
                       [#"ping"
                        (log-info "Pinged by ~a, id ~a" peer (bytes->hex-string peer-id))
                        (send! (krpc-packet 'outbound peer txn 'response (hash #"id" local-id)))]
                       [#"find_node"
                        (define target (hash-ref details #"target"))
                        (log-info "Asked for nodes near ~a by ~a, id ~a"
                                  (bytes->hex-string target)
                                  peer
                                  (bytes->hex-string peer-id))
                        (define bucket (node-id->bucket target local-id))
                        (define peers (K-closest (query-all-nodes) target))
                        (log-info "Best IDs near ~a: ~a"
                                  (bytes->hex-string target)
                                  (map bytes->hex-string (map car peers)))
                        (send! (krpc-packet 'outbound peer txn 'response
                                            (hash #"nodes" (format-peers peers))))]
                       [method
                        (log-warning "Unimplemented request: ~a ~v" method details)
                        (send! (krpc-packet 'outbound peer txn 'error
                                            (list 202 #"Not yet implemented Ler<OrId0")))])))))

(spawn-node #"\330r\22\237z\365\247E\30LqZ\337\301F\23\341<\314G")
