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

(define (spawn-bootstrap-node host port)
  (define peer (udp-remote-address host port))
  (spawn #:name (list 'kademlia-bootstrap-node peer)
         (stop-when-reloaded)
         (define root-facet-id (current-facet-id))
         (during (local-node $local-id)
           (during (fresh-transaction (list 'bootstrap-ping peer) $txn)
             (on-start
              (let retry ((attempt-count 0))
                (react (on-start (send! (krpc-packet 'outbound
                                                     peer
                                                     txn
                                                     'request
                                                     (list #"ping"
                                                           (hash #"id" local-id)))))
                       (stop-when-timeout 2000
                         (log-info "Timeout #~a contacting bootstrap node ~a:~a"
                                   (+ attempt-count 1) host port)
                         (if (< attempt-count 10)
                             (retry (+ attempt-count 1))
                             (log-info "Too many retries contacting bootstrap node ~a:~a"
                                       host port)))
                       (stop-when (message (krpc-packet 'inbound _ txn 'error $details))
                         (log-info "Bootstrap ping error: ~v" details))
                       (stop-when (message (krpc-packet 'inbound _ txn 'response $details))
                         (suggest-node! 'krpc-reply (hash-ref details #"id") peer)
                         (stop-facet root-facet-id)))))))))

(spawn #:name 'kademlia-bootstrap-monitor
       (stop-when-reloaded)
       (define/query-set all-nodes (node-coordinates $id _ _) id)
       (on-start (sleep 1)
                 (react
                  (begin/dataflow
                    (when (set-empty? (all-nodes))
                      (spawn-bootstrap-node "router.bittorrent.com" 6881)
                      (spawn-bootstrap-node "router.utorrent.com" 6881)
                      (spawn-bootstrap-node "dht.transmissionbt.com" 6881))))))
