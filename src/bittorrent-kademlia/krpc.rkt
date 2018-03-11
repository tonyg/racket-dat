#lang syndicate

(require (only-in racket/random crypto-random-bytes))
(require racket/set)
(require bitsyntax)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/timestate)
(require syndicate/protocol/advertise)

(require "wire.rkt")
(require "protocol.rkt")

(define-logger dht/transport)
(define-logger dht/krpc)
(define-logger dht/server)

(spawn #:name 'kademlia-krpc-udp-transport
       (stop-when-reloaded)

       (define PORT 42769)
       (define endpoint (udp-listener PORT))

       (on (message (udp-packet $peer endpoint $body))
           (define p (decode-packet body peer))
           (when p
             (log-dht/transport-debug "RECEIVING ~a ~v" peer p)
             (define-values (id type body) (analyze-krpc-packet p))
             (send! (krpc-packet 'inbound peer id type body))))

       (on (message (krpc-packet 'outbound $peer $id $type $body))
           (define p (synthesize-krpc-packet id type body))
           (log-dht/transport-debug "SENDING ~a ~v" peer p)
           (send! (udp-packet endpoint peer (encode-packet p))))

       (during (advertise (udp-packet _ endpoint _))
         (on-start (log-dht/transport-info "Socket is ready."))
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
         (during/spawn (observe (krpc-transaction $src $tgt $txn-name $method $args _))
           (field [results #f])
           (assert #:when (results) (krpc-transaction src tgt txn-name method args (results)))
           (define debug-name (if (udp-remote-address? tgt) tgt (~id tgt)))
           (during (fresh-transaction txn-name $txn)
             (on-start (log-dht/krpc-debug "KRPC request: ~a <- ~a ~v" debug-name method args))
             (if (udp-remote-address? tgt)
                 (on-start (send! (krpc-packet 'outbound tgt txn 'request (list method args))))
                 (during (node-coordinates tgt $peer _)
                   (on-start (send! (krpc-packet 'outbound peer txn 'request (list method args))))))
             (stop-when-timeout 3000
               (log-dht/krpc-debug "KRPC request timeout contacting ~a" debug-name)
               (when (bytes? tgt) (send! (node-timeout tgt)))
               (results 'timeout))
             (stop-when (message (krpc-packet 'inbound $peer1 txn 'response $details))
               (log-dht/krpc-debug "KRPC reply from ~a (~a): ~v" debug-name peer1 details)
               (define id (hash-ref details #"id" #f)) ;; required in all BEP 0005 responses
               (when id (suggest-node! 'received-response id peer1 #t))
               (results details))
             (stop-when (message (krpc-packet 'inbound $peer1 txn 'error $details))
               (log-dht/krpc-debug "KRPC error from ~a (~a): ~v" debug-name peer1 details)
               ;; Error replies probably... shouldn't?... count as activity??
               (results 'error))))))

(define (spawn-server [local-id (crypto-random-bytes 20)])
  (spawn #:name (list 'kademlia-node local-id)
         (stop-when-reloaded)

         (assert (local-node local-id))
         (assert (known-node local-id))
         (on-start (log-dht/server-info "Starting node ~v" (~id local-id)))
         (on-stop (log-dht/server-info "Stopping node ~v" (~id local-id)))

         (on-start
          (react (assert (locate-node
                          local-id
                          (list (list #f (udp-remote-address "router.bittorrent.com" 6881))
                                (list #f (udp-remote-address "router.utorrent.com" 6881))
                                (list #f (udp-remote-address "dht.transmissionbt.com" 6881)))))
                 (stop-when (asserted (closest-nodes-to local-id _ #t))
                   (log-dht/server-info "Initial discovery of nodes close to self complete."))))

         (on (message (krpc-packet 'inbound $peer $txn 'request (list $method $details)))
             (spawn* #:name (list 'kademlia-request-handler peer txn method details)
                     (define peer-id (hash-ref details #"id")) ;; required in all BEP 0005 requests
                     (suggest-node! 'incoming-request peer-id peer #f)
                     (match method

                       [#"ping"
                        (log-dht/server-debug "Pinged by ~a, id ~a" peer (~id peer-id))
                        (send! (krpc-packet 'outbound peer txn 'response (hash #"id" local-id)))]

                       [#"find_node"
                        (define target (hash-ref details #"target"))
                        (log-dht/server-debug "Asked for nodes near ~a by ~a, id ~a"
                                              (~id target) peer (~id peer-id))
                        (define peers (K-closest (query-all-nodes) target))
                        (log-dht/server-debug "Best IDs near ~a: ~a"
                                              (~id target) (map ~id (map car peers)))
                        (send! (krpc-packet 'outbound peer txn 'response
                                            (hash #"id" local-id
                                                  #"nodes" (format-peers peers))))]

                       [#"get_peers"
                        (define info_hash (hash-ref details #"info_hash"))
                        (log-dht/server-debug "Asked for participants in ~a by ~a, id ~a"
                                              (~id info_hash) peer (~id peer-id))
                        (define token
                          (car (immediate-query [query-value #f (valid-tokens $ts) ts])))
                        (match (set->list
                                (immediate-query
                                 [query-set (participant-record info_hash $h $p)
                                            (udp-remote-address h p)]))
                          ['()
                           (log-dht/server-debug "No known participants in ~a." (~id info_hash))
                           (define peers (K-closest (query-all-nodes) info_hash))
                           (send! (krpc-packet 'outbound peer txn 'response
                                               (hash #"id" local-id
                                                     #"token" token
                                                     #"nodes" (format-peers peers))))]
                          [rs
                           (log-dht/server-debug "Known participants in ~a: ~v" (~id info_hash) rs)
                           (define formatted-rs (filter values (map format-ip/port rs)))
                           (send! (krpc-packet 'outbound peer txn 'response
                                               (hash #"id" local-id
                                                     #"token" token
                                                     #"values" formatted-rs)))])]

                       [#"announce_peer"
                        (define info_hash (hash-ref details #"info_hash"))
                        (define implied_port (positive? (hash-ref details #"implied_port" 0)))
                        (define host (udp-remote-address-host peer))
                        (define port (if implied_port
                                         (udp-remote-address-port peer)
                                         (hash-ref details #"port")))
                        (define token (hash-ref details #"token"))
                        (define tokens (immediate-query [query-value #f (valid-tokens $ts) ts]))
                        (cond
                          [(member token tokens)
                           (log-dht/server-debug "Announce ~a peer ~a (req port ~a) id ~a"
                                                 (~id info_hash) peer port (~id peer-id))
                           (send! (received-announcement (participant-record info_hash host port)))
                           (send! (krpc-packet 'outbound peer txn 'response
                                               (hash #"id" local-id)))]
                          [else
                           (log-dht/server-debug "Bad token ~a (valid = ~a)" token tokens)
                           (send! (krpc-packet 'outbound peer txn 'error
                                               (list 203 "Bad token")))])]

                       [method
                        (log-dht/server-warning "Unimplemented request: ~a ~v" method details)
                        (send! (krpc-packet 'outbound peer txn 'error
                                            (list 204 #"Method unknown")))])))))

(spawn-server #"\330r\22\237z\365\247E\30LqZ\337\301F\23\341<\314G")

(spawn #:name 'dht-token-manager
       (stop-when-reloaded)

       (define (make-token) (crypto-random-bytes 8))
       (define (next-rollover-time) (+ (current-inexact-milliseconds) (* 5 60 1000)))

       (field [tokens (list (make-token))]
              [rollover-time (next-rollover-time)])

       (assert (valid-tokens (tokens)))

       (on (asserted (later-than (rollover-time)))
           (rollover-time (next-rollover-time))
           (tokens (take-at-most 2 (cons (make-token) (tokens))))))
