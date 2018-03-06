#lang syndicate

(require (only-in racket/random crypto-random-bytes))
(require (only-in file/sha1 bytes->hex-string))
(require racket/set)

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
             ;; (log-info "RECEIVING ~a ~v" peer p)
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
       (stop-when-reloaded)

       (field [active-queries (set)])

       (during (observe (fresh-transaction $name _))
         (define number
           (let loop ((len 1))
             (define candidate (crypto-random-bytes len))
             (if (set-member? (active-queries) candidate)
                 (loop (+ len 1))
                 (begin (active-queries (set-add (active-queries) candidate))
                        candidate))))
         (on-stop (active-queries (set-remove (active-queries) number)))
         (during (krpc-ready)
           (assert (fresh-transaction name number)))))

(spawn #:name 'kademlia-krpc-transaction-manager
       (stop-when-reloaded)

       (field [active-queries (set)])

       (during (local-node $local-id)
         (during/spawn (observe (krpc-transaction $src $tgt $txn-name $method $args _))
           (field [results #f])
           (assert #:when (results) (krpc-transaction src tgt txn-name method args (results)))
           (during (fresh-transaction txn-name $txn)
             (during (node-coordinates tgt $peer _)
               (on-start (send! (krpc-packet 'outbound peer txn 'request (list method args)))))
             (stop-when-timeout 2000
               (log-info "KRPC request timeout contacting ~a" (bytes->hex-string tgt))
               (send! (transaction-resolution tgt 'timeout))
               (results 'timeout))
             (stop-when (message (krpc-packet 'inbound $peer1 txn 'response $details))
               (log-info "KRPC reply from ~a (~a): ~v" (bytes->hex-string tgt) peer1 details)
               (send! (transaction-resolution tgt 'reply))
               (results details))
             (stop-when (message (krpc-packet 'inbound $peer1 txn 'error $details))
               (log-info "KRPC error from ~a (~a): ~v" (bytes->hex-string tgt) peer1 details)
               (send! (transaction-resolution tgt 'reply))
               (results 'error))))))

(define (spawn-node [local-id (crypto-random-bytes 20)])
  (spawn #:name (list 'kademlia-node local-id)
         (stop-when-reloaded)

         (assert (local-node local-id))
         (assert (known-node local-id))
         (on-start (log-info "Starting node ~v" (bytes->hex-string local-id)))
         (on-stop (log-info "Stopping node ~v" (bytes->hex-string local-id)))

         (on (message (krpc-packet 'inbound $peer $txn 'request $req))
             (spawn* #:name (list 'kademlia-request-handler peer txn req)
                     (match req
                       [(list #"ping" details)
                        (log-info "Pinged by ~a, id ~a" peer (bytes->hex-string (hash-ref details #"id")))
                        (send! (krpc-packet 'outbound peer txn 'response (hash #"id" local-id)))]
                       [(list #"find_node" details)
                        (define target (hash-ref details #"target"))
                        (log-info "Asked for nodes near ~a by ~a, id ~a"
                                  (bytes->hex-string target)
                                  peer
                                  (bytes->hex-string (hash-ref details #"id")))
                        (define bucket (node-id->bucket target local-id))
                        (define all-peers
                          (set->list (immediate-query
                                      [query-set (node-coordinates $i $p _) (list i p)])))
                        (define peers
                          (take-at-most K (sort all-peers #:key car (distance-to-<? target))))
                        (log-info "Best IDs near ~a: ~a"
                                  (bytes->hex-string target)
                                  (map bytes->hex-string (map car peers)))
                        (send! (krpc-packet 'outbound peer txn 'response
                                            (hash #"nodes" (format-peers peers))))]
                       [(list method details)
                        (log-warning "Unimplemented request: ~a ~v" method details)
                        (send! (krpc-packet 'outbound peer txn 'error
                                            (list 202 #"Not yet implemented Ler<OrId0")))])))))

(spawn-node #"\330r\22\237z\365\247E\30LqZ\337\301F\23\341<\314G")

(spawn #:name 'node-coordinate-monitor
       (stop-when-reloaded)
       (during (node-coordinates $id _ _)
         (on (asserted (node-coordinates id $loc $timestamp))
             (on-start (log-info "N: ~a = ~a (~a)" (bytes->hex-string id) loc timestamp)))
         (on-stop (log-info "N? ~a" (bytes->hex-string id)))))
