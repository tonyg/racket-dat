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

(spawn #:name 'closest-nodes-monitor
       (stop-when-reloaded)

       (during (local-node $local-id)
         (during/spawn (locate-node $target-id $fill-routing-table?)
           (stop-when-reloaded)

           (field [bad-nodes (set)]
                  [possible-nodes #f]
                  [asked-nodes (set)]
                  [best-nodes (list)]
                  [query-count 0]
                  [final? #f]
                  [reset-count 0])

           (assert (closest-nodes-to target-id (best-nodes) (final?)))

           (stop-when-true (> (reset-count) 3)
             (log-info "Too many retries on lookup ~a. Giving up" (bytes->hex-string target-id)))

           (define (reset!)
             (bad-nodes (set))
             (possible-nodes #f)
             (asked-nodes (set))
             (best-nodes (list))
             (reset-count (+ (reset-count) 1))
             (react (define/query-set nodes (node-coordinates $i $p _) (list i p))
                    (stop-when-true (not (set-empty? (nodes)))
                                    (possible-nodes (nodes)))))

           (on-start
            (log-info "New query: finding ~a" (bytes->hex-string target-id))
            (reset!))

           (define (ask-node node/peer)
             (match-define (list id peer) node/peer)
             (asked-nodes (set-add (asked-nodes) node/peer))
             (query-count (+ (query-count) 1))
             (react (on-stop (query-count (- (query-count) 1)))
                    (stop-when (asserted
                                (memoized
                                 (krpc-transaction local-id id peer
                                                   (list 'find_node id target-id)
                                                   #"find_node"
                                                   (hash #"id" local-id #"target" target-id)
                                                   $results)))
                      (match results
                        [(or 'timeout 'error)
                         (possible-nodes (set-remove (possible-nodes) node/peer))
                         (bad-nodes (set-add (bad-nodes) node/peer))]
                        [(? hash?)
                         (define peers (extract-peers (hash-ref results #"nodes" #"")))
                         (when fill-routing-table?
                           (for [(p peers)] (suggest-node! 'neighbourhood (car p) (cadr p))))
                         (log-info "Asked ~a(~a) for ~a, got ~a"
                                   (bytes->hex-string id)
                                   peer
                                   (bytes->hex-string target-id)
                                   (format-nodes/peers peers))
                         (possible-nodes (set-union (possible-nodes)
                                                    (set-subtract (list->set peers)
                                                                  (bad-nodes))))
                         (define new-best-nodes (K-closest (set->list (possible-nodes)) target-id))
                         (if (equal? (best-nodes) new-best-nodes)
                             (final? #t)
                             (best-nodes new-best-nodes))]))))

           (begin/dataflow
             (when (set? (possible-nodes))
               (define askable-nodes (set-subtract (possible-nodes) (asked-nodes)))
               (log-info "query-count ~a askable-nodes #~a final? ~a"
                         (query-count)
                         (set-count askable-nodes)
                         (final?))
               (define available-parallelism (- 3 (query-count)))
               (when (and (positive? available-parallelism) (not (final?)))
                 (if (and (zero? (query-count)) (set-empty? askable-nodes))
                     (if (null? (best-nodes)) ;; ugh. Try again
                         (reset!)
                         (final? #t))
                     (for [(node/peer (K-closest #:K available-parallelism
                                                 (set->list askable-nodes) target-id))]
                       (ask-node node/peer)))))))))
