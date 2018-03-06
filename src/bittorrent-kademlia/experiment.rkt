#lang syndicate

(require racket/set)
(require (only-in file/sha1 bytes->hex-string))
(require bitsyntax)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)
(require/activate syndicate/drivers/udp)

(require "wire.rkt")
(require "protocol.rkt")

(spawn #:name 'experiment
       (stop-when-reloaded)

       (during (local-node $local-id)
         (assert (locate-node local-id))
         (during (closest-nodes-to local-id $ids)
           (log-info "Closest to self: ~a" (map bytes->hex-string ids)))))

(spawn #:name 'memo-table
       (stop-when-reloaded)
       (during (observe (memoized (krpc-transaction $src $tgt $txn $method $args _)))
         (stop-when (asserted (krpc-transaction src tgt txn method args $result))
           (react (assert (memoized (krpc-transaction src tgt txn method args result)))
                  (stop-when-timeout 30000)))))

(spawn #:name 'closest-nodes-monitor
       (stop-when-reloaded)

       (during (local-node $local-id)
         (during/spawn (locate-node $target-id)
           (stop-when-reloaded)
           (define/query-set all-nodes (node-coordinates $i _ _) i)
           (field [sorted-nodes '()])
           (begin/dataflow
             (let ((new-best (take-at-most K (sort (set->list (all-nodes))
                                                   (distance-to-<? target-id)))))
               (unless (equal? (sorted-nodes) new-best) (sorted-nodes new-best))))
           (assert (closest-nodes-to target-id (sorted-nodes)))
           (begin/dataflow
             (log-info "Finding ~a: closest ~a"
                       (bytes->hex-string target-id)
                       (map bytes->hex-string (sorted-nodes)))
             (for [(id (sorted-nodes))]
               (react
                (stop-when (asserted
                            (memoized (krpc-transaction local-id
                                                        id
                                                        (list 'find_node target-id)
                                                        #"find_node"
                                                        (hash #"id" local-id #"target" target-id)
                                                        $results)))
                  (when (hash? results)
                    (for [(p (extract-peers (hash-ref results #"nodes" #"")))]
                      ;; (log-info "Learned about ~a from ~a"
                      ;;           (bytes->hex-string (car p))
                      ;;           (bytes->hex-string id))
                      (send! (discovered-node (car p) (cadr p))))))))))))

(spawn #:name 'node-factory
       (stop-when-reloaded)
       (define/query-set ids (known-node $id) id)
       (on (message (discovered-node $id $peer))
           (when (not (set-member? (ids) id))
             (ids (set-add (ids) id))
             (spawn #:name (list 'node (bytes->hex-string id))
                    #:assertions (known-node id)
                    (stop-when-reloaded)
                    (on-start (log-info "Tracking node ~a at ~a" (bytes->hex-string id) peer))
                    (on-stop (log-info "Terminated node ~a at ~a" (bytes->hex-string id) peer))
                    (node-main id peer)))))

(define (node-main id peer)
  (field [time-last-heard-from 0]
         [timeout-counter 0]
         [ok? #t])

  (assert (known-node id))
  (assert #:when (ok?) (node-coordinates id peer (time-last-heard-from)))

  (on (message (transaction-resolution id 'timeout))
      (timeout-counter (+ (timeout-counter) 1)))

  (on (message (transaction-resolution id 'reply))
      (time-last-heard-from (current-inexact-milliseconds))
      (timeout-counter 0))

  (begin/dataflow
    (when (>= (timeout-counter) 5)
      (ok? #f)))

  (begin/dataflow
    (when (not (ok?))
      (log-info "Node ~a no longer ok" (bytes->hex-string id))
      (react (stop-when-timeout (* 60 1000) (send! (discard-node id)))
             (stop-when-true (ok?) (log-info "Node ~a is ok again" (bytes->hex-string id))))))

  (stop-when (message (discard-node id)))

  (during (local-node $local-id)
    (assert #:when (ok?) (node-bucket (node-id->bucket id local-id) id))

    (during (later-than (+ (time-last-heard-from) (* 15 60 1000)))
      (on-start
       (log-info "Node ~a became questionable" (bytes->hex-string id))
       (let try-pinging ((ping-count 0))
         (log-info "Questionable node ~a, pinging (~a attempts made already)"
                   (bytes->hex-string id)
                   ping-count)
         (react
          (stop-when (asserted (krpc-transaction local-id
                                                 id
                                                 (list 'ping id)
                                                 #"ping"
                                                 (hash #"id" id)
                                                 $results))
            (when (eq? results 'timeout) ;; error or reply are a response!
              (if (< ping-count 1)
                  (try-pinging (+ ping-count 1))
                  (ok? #f)))))))
      (on-stop
       (log-info "Node ~a became good or was terminated" (bytes->hex-string id)))))

  (begin/dataflow (log-info "Node ~a: timestamp ~a" (bytes->hex-string id) (time-last-heard-from)))
  (begin/dataflow (log-info "Timeouts ~a: ~a" (bytes->hex-string id) (timeout-counter))))
