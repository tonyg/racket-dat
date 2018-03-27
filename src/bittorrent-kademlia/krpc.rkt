#lang syndicate

(require (only-in racket/random crypto-random-bytes))
(require racket/set)
(require bitsyntax)
(require (only-in nat-traversal private-ip-address?))

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/timestate)

(require "protocol.rkt")

(define-logger dht/krpc)
(define-logger dht/server)

(spawn #:name 'kademlia-krpc-transaction-manager
       (stop-when-reloaded) ;; TODO: this really shouldn't start from scratch at each reload!
       (field [counter 1]) ;; TODO: start at a randomized location to avoid martian packets
       (during (observe (fresh-transaction $name _))
         (define number (counter))
         (counter (+ number 1))
         (define byte-count (max 2 (quotient (+ 7 (integer-length number)) 8)))
         (define id (bit-string (number :: little-endian bytes byte-count)))
         (assert (fresh-transaction name id))))

(spawn #:name 'kademlia-krpc-transaction-manager
       (stop-when-reloaded)

       (during (local-node $local-name)
         (during/spawn (observe (krpc-transaction $src $tgt $txn-name $method $args _))
           (field [results #f])
           (assert #:when (results) (krpc-transaction src tgt txn-name method args (results)))
           (define debug-name (if (udp-remote-address? tgt) tgt (~name tgt)))
           (during (fresh-transaction txn-name $txn)
             (on-start (log-dht/krpc-debug "KRPC request: ~a <- ~a ~v" debug-name method args))
             (if (udp-remote-address? tgt)
                 (on-start (send! (krpc-packet 'outbound tgt txn 'request (list method args))))
                 (during (node-coordinates tgt $addr _)
                   (on-start (send! (krpc-packet 'outbound addr txn 'request (list method args))))))
             (stop-when-timeout 3000
               (log-dht/krpc-debug "KRPC request timeout contacting ~a" debug-name)
               (when (bytes? tgt) (send! (node-timeout tgt)))
               (results 'timeout))
             (stop-when (message (krpc-packet 'inbound $addr1 txn 'response $details))
               (log-dht/krpc-debug "KRPC reply from ~a (~a): ~v" debug-name addr1 details)
               (define name (hash-ref details #"id" #f)) ;; required in all BEP 0005 responses
               (when name (suggest-node! 'received-response name addr1 #t))
               (results details))
             (stop-when (message (krpc-packet 'inbound $addr1 txn 'error $details))
               (log-dht/krpc-debug "KRPC error from ~a (~a): ~v" debug-name addr1 details)
               ;; Error replies probably... shouldn't?... count as activity??
               (results 'error))))))

(define (spawn-server [local-name (crypto-random-bytes 20)])
  (spawn #:name (list 'kademlia-node local-name)
         (stop-when-reloaded)

         (assert (local-node local-name))
         (assert (known-node local-name))
         (on-start (log-dht/server-info "Starting node ~v" (~name local-name)))
         (on-stop (log-dht/server-info "Stopping node ~v" (~name local-name)))

         (on-start
          (react (assert (locate-node
                          local-name
                          (list (list #f (udp-remote-address "router.bittorrent.com" 6881))
                                (list #f (udp-remote-address "router.utorrent.com" 6881))
                                (list #f (udp-remote-address "dht.transmissionbt.com" 6881)))))
                 (stop-when (asserted (closest-nodes-to local-name _ #t))
                   (log-dht/server-info "Initial discovery of nodes close to self complete."))))

         (on (message (krpc-packet 'inbound $addr $txn 'request (list $method $details)))
             (spawn* #:name (list 'kademlia-request-handler addr txn method details)
                     (define peer-name (hash-ref details #"id")) ;; required in all BEP 0005 reqs
                     (suggest-node! 'incoming-request peer-name addr #f)
                     (log-dht/server-debug "Req: method ~v, peer ~a ~a, details ~v"
                                           method (~name peer-name) addr details)
                     (handle-rpc local-name peer-name addr method details
                                 (lambda (results)
                                   (send! (krpc-packet 'outbound addr txn 'response results)))
                                 (lambda (code error-message)
                                   (send! (krpc-packet 'outbound addr txn 'error
                                                       (list code error-message)))))))))

(define (handle-rpc local-name peer-name addr method details k-reply k-error)
  (match method
    [#"ping" (k-reply (hash #"id" local-name))]

    [#"find_node"
     (define target (hash-ref details #"target"))
     (define addrs (K-closest (query-all-nodes) target))
     (log-dht/server-debug "Best names near ~a: ~a" (~name target) (map ~name (map car addrs)))
     (k-reply (hash #"id" local-name #"nodes" (format-addrs addrs)))]

    [#"get_peers"
     (define info_hash (hash-ref details #"info_hash"))
     (define token (car (immediate-query [query-value #f (valid-tokens $ts) ts])))
     (match (set->list (immediate-query [query-set (participant-record info_hash $h $p)
                                                   (udp-remote-address h p)]))
       ['()
        (log-dht/server-debug "No known participants in ~a." (~name info_hash))
        (define addrs (K-closest (query-all-nodes) info_hash))
        (k-reply (hash #"id" local-name #"token" token #"nodes" (format-addrs addrs)))]
       [rs
        (log-dht/server-debug "Known participants in ~a: ~v" (~name info_hash) rs)
        (define formatted-rs (filter values (map format-ip/port rs)))
        (k-reply (hash #"id" local-name #"token" token #"values" formatted-rs))])]

    [#"announce_peer"
     (define info_hash (hash-ref details #"info_hash"))
     (define implied_port (positive? (hash-ref details #"implied_port" 0)))
     (define host (udp-remote-address-host addr))
     (define port (if implied_port (udp-remote-address-port addr) (hash-ref details #"port")))
     (define token (hash-ref details #"token"))
     (define tokens (immediate-query [query-value #f (valid-tokens $ts) ts]))
     (cond
       [(private-ip-address? host)
        (log-dht/server-debug "Ignoring RFC 1918 addr: ~a ~a ~a" (~name info_hash) addr port)
        (k-error 203 #"Bad IP address (RFC 1918 network)")]
       [(member token tokens)
        (log-dht/server-debug "Announce ~a addr ~a (req port ~a) name ~a"
                              (~name info_hash) addr port (~name peer-name))
        (send! (received-announcement (participant-record info_hash host port)))
        (k-reply (hash #"id" local-name))]
       [else
        (log-dht/server-debug "Bad token ~v (valid = ~v)" token tokens)
        (k-error 203 #"Bad token")])]

    [method
     (log-dht/server-warning "Unimplemented request: ~a ~v" method details)
     (k-error 204 #"Method unknown")]))

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
