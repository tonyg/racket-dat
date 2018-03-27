#lang syndicate

(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/timestate)

(require "protocol.rkt")

(define-logger dht/client)

(define (iterative-resolver local-name
                            target-name
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

  (define (ask-node node/addr)
    (match-define (list maybe-name addr) node/addr)
    (asked-nodes (set-add (asked-nodes) node/addr))
    (query-count (+ (query-count) 1))
    (react (on-stop (query-count (- (query-count) 1)))
           (stop-when
               (asserted
                (krpc-transaction local-name addr (list method addr target-name)
                                  method args $results))
             (match results
               [(or 'timeout 'error)
                (possible-nodes (set-remove (possible-nodes) node/addr))
                (bad-nodes (set-add (bad-nodes) node/addr))]
               [(? hash?)
                (handle-response! maybe-name addr results)
                (define addrs (extract-addrs (hash-ref results #"nodes" #"")))
                (for [(p addrs)] (suggest-node! 'neighbourhood (car p) (cadr p) #f))
                (log-dht/client-debug "Asked ~a(~a) for ~a, got ~a"
                                      (~name maybe-name)
                                      addr
                                      (~name target-name)
                                      (format-nodes/addrs addrs))
                (possible-nodes (set-union (possible-nodes)
                                           (set-subtract (list->set addrs) (bad-nodes))))
                (define new-best-nodes (K-closest (set->list (possible-nodes)) target-name))
                (cond [(equal? (best-nodes) new-best-nodes) (state 'stabilizing)]
                      [else (state 'running)
                            (best-nodes new-best-nodes)])]))))

  (define (log-state askable-nodes)
    (log-dht/client-debug "query ~a in-flight ~a askable ~a asked ~a state ~a"
                          (~name target-name)
                          (query-count)
                          (if askable-nodes (set-count askable-nodes) "?")
                          (set-count (asked-nodes))
                          (state)))

  (begin/dataflow
    (when (eq? (state) 'stabilizing)
      (log-state #f)
      (when (zero? (query-count))
        (log-dht/client-info "Query ~a ~a finished." method (~name target-name))
        (state 'final))))

  (on-start
   (log-dht/client-info "New query: ~a ~a" method (~name target-name))
   (possible-nodes (list->set (or roots-list (K-closest (query-all-nodes) target-name))))
   (state 'running)
   (for-each ask-node (set->list (possible-nodes))))
  (on-stop
   (log-dht/client-info "Query ~a ~a released." method (~name target-name)))

  (begin/dataflow
    (when (eq? (state) 'running)
      (define askable-nodes (set-subtract (possible-nodes) (asked-nodes)))
      (log-state askable-nodes)
      (define available-parallelism (- 8 (query-count)))
      (when (positive? available-parallelism)
        (if (and (set-empty? askable-nodes) (zero? (query-count)))
            (begin (log-dht/client-warning "query for ~a didn't stabilize, but has no askable-nodes"
                                           (~name target-name))
                   (state 'final))
            (for-each ask-node (K-closest #:K available-parallelism
                                          (set->list askable-nodes) target-name)))))))

(spawn #:name 'locate-node-server
       (stop-when-reloaded)

       (during (local-node $local-name)
         (during/spawn (locate-node $target-name $roots-list)
           #:name (list 'locate-node (~name target-name))
           (stop-when-reloaded)
           (iterative-resolver
            local-name
            target-name
            roots-list
            #"find_node"
            (hash #"id" local-name #"target" target-name)
            (lambda (maybe-name addr results) (void))
            (lambda (best-nodes final?) (closest-nodes-to target-name best-nodes final?))))))

(spawn #:name 'locate-participants-server
       (stop-when-reloaded)

       (message-struct locate-participants:discovered-record (r))

       (during (local-node $local-name)
         (during/spawn (locate-participants $resource-name)
           #:name (list 'locate-participants (~name resource-name))
           (stop-when-reloaded)
           (on (message (locate-participants:discovered-record $r))
               (assert! r))
           (field [record-holders (list)])
           (define (handle-response! name addr results)
             (define token (hash-ref results #"token" #f))
             (define ips/ports (hash-ref results #"values" #f))
             (define has-records? (and (list? ips/ports) (andmap bytes? ips/ports)))
             (when (bytes? token)
               (record-holders (cons (record-holder name addr token has-records?)
                                     (record-holders))))
             (when has-records?
               (for [(p (filter values (for/list [(ip/port ips/ports)] (extract-ip/port ip/port))))]
                 (match-define (udp-remote-address host port) p)
                 (send! (locate-participants:discovered-record
                         (participant-record resource-name host port))))))
           (iterative-resolver
            local-name
            resource-name
            #f
            #"get_peers"
            (hash #"id" local-name #"info_hash" resource-name)
            handle-response!
            (lambda (_best-nodes final?)
              (define rhs (K-closest (record-holders) resource-name #:key record-holder-name))
              (participants-in resource-name rhs final?))))))

(spawn #:name 'announce-participation-server
       (stop-when-reloaded)

       (during (local-node $local-name)
         (during/spawn (announce-participation $resource-name $maybe-port)
           #:name (list 'announce-participation (~name resource-name))
           (on-start (log-dht/client-info "Added announce ~a ~a" (~name resource-name) maybe-port))
           (on-stop (log-dht/client-info "Removed announce ~a ~a" (~name resource-name) maybe-port))

           (field [refresh-time (current-inexact-milliseconds)])

           (on (asserted (later-than (refresh-time)))
               (refresh-time (next-refresh-time 8 10))
               (define rhs (react/suspend (k)
                             (assert (locate-participants resource-name))
                             (stop-when (asserted (participants-in resource-name $rhs #t))
                               (k rhs))))
               (log-dht/client-debug "Announcing to ~v" rhs)
               (for [(rh rhs)]
                 (match-define (record-holder name location token _has-records?) rh)
                 (react (on-start (do-krpc-transaction local-name
                                                       location
                                                       (list 'announce_peer name resource-name)
                                                       #"announce_peer"
                                                       (hash #"id" local-name
                                                             #"info_hash" resource-name
                                                             #"implied_port" (if maybe-port 0 1)
                                                             #"port" (or maybe-port 0)
                                                             #"token" token)))))))))
