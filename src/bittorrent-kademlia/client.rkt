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

(define-logger dht/client)

(define (recursive-resolver local-id
                            target-id
                            roots-list
                            method
                            args
                            handle-response!
                            build-status)
  (field [bad-nodes (set)]
         [possible-nodes (set)]
         [asked-nodes (set)]
         [best-nodes (list)]
         [query-count 0]
         [state 'starting])

  (assert (build-status (best-nodes) (eq? (state) 'final)))

  (define (ask-node node/peer)
    (match-define (list maybe-id peer) node/peer)
    (asked-nodes (set-add (asked-nodes) node/peer))
    (query-count (+ (query-count) 1))
    (react (on-stop (query-count (- (query-count) 1)))
           (stop-when (asserted (krpc-transaction local-id
                                                  peer
                                                  (list method peer target-id)
                                                  method
                                                  args
                                                  $results))
             (match results
               [(or 'timeout 'error)
                (possible-nodes (set-remove (possible-nodes) node/peer))
                (bad-nodes (set-add (bad-nodes) node/peer))]
               [(? hash?)
                (handle-response! maybe-id peer results)
                (define peers (extract-peers (hash-ref results #"nodes" #"")))
                (for [(p peers)] (suggest-node! 'neighbourhood (car p) (cadr p) #f))
                (log-dht/client-debug "Asked ~a(~a) for ~a, got ~a"
                                      (and maybe-id (bytes->hex-string maybe-id))
                                      peer
                                      (bytes->hex-string target-id)
                                      (format-nodes/peers peers))
                (possible-nodes (set-union (possible-nodes)
                                           (set-subtract (list->set peers) (bad-nodes))))
                (define new-best-nodes (K-closest (set->list (possible-nodes)) target-id))
                (if (equal? (best-nodes) new-best-nodes)
                    (state 'stabilizing)
                    (begin (state 'running)
                           (best-nodes new-best-nodes)))]))))

  (define (log-state askable-nodes)
    (log-dht/client-debug "query ~a in-flight ~a askable ~a asked ~a state ~a"
                          (bytes->hex-string target-id)
                          (query-count)
                          (if askable-nodes (set-count askable-nodes) "?")
                          (set-count (asked-nodes))
                          (state)))

  (begin/dataflow
    (when (eq? (state) 'stabilizing)
      (log-state #f)
      (when (zero? (query-count))
        (log-dht/client-info "Query ~a ~a finished." method (bytes->hex-string target-id))
        (state 'final))))

  (on-start
   (log-dht/client-info "New query: ~a ~a" method (bytes->hex-string target-id))
   (possible-nodes (list->set (or roots-list (K-closest (query-all-nodes) target-id))))
   (state 'running)
   (for [(np (possible-nodes))]
     (ask-node np)))
  (on-stop
   (log-dht/client-info "Query ~a ~a released." method (bytes->hex-string target-id)))

  (begin/dataflow
    (when (eq? (state) 'running)
      (define askable-nodes (set-subtract (possible-nodes) (asked-nodes)))
      (log-state askable-nodes)
      (define available-parallelism (- 8 (query-count)))
      (when (positive? available-parallelism)
        (if (and (set-empty? askable-nodes) (zero? (query-count)))
            (begin (log-dht/client-warning "query for ~a didn't stabilize, but has no askable-nodes"
                                           (bytes->hex-string target-id))
                   (state 'final))
            (for [(node/peer (K-closest #:K available-parallelism
                                        (set->list askable-nodes) target-id))]
              (ask-node node/peer)))))))

(spawn #:name 'locate-node-server
       (stop-when-reloaded)

       (during (local-node $local-id)
         (during/spawn (locate-node $target-id $roots-list)
           #:name (list 'locate-node (bytes->hex-string target-id))
           (stop-when-reloaded)
           (recursive-resolver local-id
                               target-id
                               roots-list
                               #"find_node"
                               (hash #"id" local-id #"target" target-id)
                               (lambda (maybe-id peer results) (void))
                               (lambda (best-nodes final?)
                                 (closest-nodes-to target-id best-nodes final?))))))

(define (do-locate-node id [roots #f])
  (react/suspend (k)
    (assert (locate-node id roots))
    (stop-when (asserted (closest-nodes-to id $nps #t))
      (k nps))))

(spawn #:name 'locate-participants-server
       (stop-when-reloaded)

       (message-struct locate-participants:discovered-record (r))

       (during (local-node $local-id)
         (during/spawn (locate-participants $resource-id)
           #:name (list 'locate-participants (bytes->hex-string resource-id))
           (stop-when-reloaded)
           (field [record-holders (list)])
           (on (message (locate-participants:discovered-record $r))
               (assert! r))
           (recursive-resolver local-id
                               resource-id
                               #f
                               #"get_peers"
                               (hash #"id" local-id #"info_hash" resource-id)
                               (lambda (id peer results)
                                 (define token (hash-ref results #"token" #f))
                                 (define ips/ports (hash-ref results #"values" #f))
                                 (define has-records? (and (list? ips/ports)
                                                           (andmap bytes? ips/ports)))
                                 (when (bytes? token)
                                   (record-holders (cons (record-holder id peer token has-records?)
                                                         (record-holders))))
                                 (when has-records?
                                   (define peers
                                     (filter values (for/list [(ip/port ips/ports)]
                                                      (extract-ip/port ip/port))))
                                   (for [(p peers)]
                                     (match-define (udp-remote-address host port) p)
                                     (send! (locate-participants:discovered-record
                                             (participant-record resource-id host port))))))
                               (lambda (_best-nodes final?)
                                 (participants-in resource-id
                                                  (K-closest (record-holders) resource-id
                                                             #:key record-holder-id)
                                                  final?))))))
