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

(spawn #:name 'locate-node-server
       (stop-when-reloaded)

       (during (local-node $local-id)
         (during/spawn (locate-node $target-id $roots)
           #:name (list 'locate-node (bytes->hex-string target-id))
           (stop-when-reloaded)

           (field [bad-nodes (set)]
                  [possible-nodes (and roots (list->set roots))]
                  [asked-nodes (set)]
                  [best-nodes (list)]
                  [query-count 0]
                  [state (if roots 'running 'starting)])

           (assert (closest-nodes-to target-id (best-nodes) (eq? (state) 'final)))

           (on-start
            (log-info "New query: finding ~a" (bytes->hex-string target-id))
            (when (not roots)
              (react (define/query-set nodes (node-coordinates $i $p _) (list i p))
                     (stop-when-true (not (set-empty? (nodes)))
                       (state 'running)
                       (possible-nodes (nodes))))))

           (define (ask-node node/peer)
             (match-define (list maybe-id peer) node/peer)
             (asked-nodes (set-add (asked-nodes) node/peer))
             (query-count (+ (query-count) 1))
             (react (on-stop (query-count (- (query-count) 1)))
                    (stop-when (asserted
                                (memoized
                                 (krpc-transaction local-id
                                                   peer
                                                   (list 'find_node peer target-id)
                                                   #"find_node"
                                                   (hash #"id" local-id #"target" target-id)
                                                   $results)))
                      (match results
                        [(or 'timeout 'error)
                         (possible-nodes (set-remove (possible-nodes) node/peer))
                         (bad-nodes (set-add (bad-nodes) node/peer))]
                        [(? hash?)
                         (define peers (extract-peers (hash-ref results #"nodes" #"")))
                         (for [(p peers)] (suggest-node! 'neighbourhood (car p) (cadr p) #f))
                         (log-info "Asked ~a(~a) for ~a, got ~a"
                                   (and maybe-id (bytes->hex-string maybe-id))
                                   peer
                                   (bytes->hex-string target-id)
                                   (format-nodes/peers peers))
                         (possible-nodes (set-union (possible-nodes)
                                                    (set-subtract (list->set peers)
                                                                  (bad-nodes))))
                         (define new-best-nodes (K-closest (set->list (possible-nodes)) target-id))
                         (if (equal? (best-nodes) new-best-nodes)
                             (state 'stabilizing)
                             (begin (state 'running)
                                    (best-nodes new-best-nodes)))]))))

           (define (log-state askable-nodes)
             (log-info "query ~a in-flight ~a askable ~a asked ~a state ~a"
                       (bytes->hex-string target-id)
                       (query-count)
                       (if askable-nodes (set-count askable-nodes) "?")
                       (set-count (asked-nodes))
                       (state)))

           (begin/dataflow
             (when (eq? (state) 'stabilizing)
               (log-state #f)
               (when (zero? (query-count))
                 (state 'final))))

           (begin/dataflow
             (when (eq? (state) 'running)
               (define askable-nodes (set-subtract (possible-nodes) (asked-nodes)))
               (log-state askable-nodes)
               (define available-parallelism (- 3 (query-count)))
               (when (positive? available-parallelism)
                 (if (set-empty? askable-nodes)
                     (begin (log-info "query for ~a didn't stabilize, but has no askable-nodes"
                                      (bytes->hex-string target-id))
                            (state 'final))
                     (for [(node/peer (K-closest #:K available-parallelism
                                                 (set->list askable-nodes) target-id))]
                       (ask-node node/peer)))))))))
