#lang syndicate

(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)

(require "protocol.rkt")

(define-logger dht/table)
(define-logger dht/neighbourhood)

(spawn #:name 'node-factory
       (stop-when-reloaded)
       (define/query-set ids (known-node $id) id)
       (on (message (discovered-node $id $peer $known-alive?))
           (when (not (set-member? (ids) id))
             (ids (set-add (ids) id))
             (spawn #:name (list 'node (~id id))
                    #:assertions (known-node id)
                    (stop-when-reloaded)
                    (on-start (log-dht/table-debug "Tracking node ~a at ~a" (~id id) peer))
                    (on-stop (log-dht/table-debug "Terminated node ~a at ~a" (~id id) peer))
                    (node-main id peer known-alive?)))))

(define (node-main id initial-peer initially-known-alive?)
  (field [time-last-heard-from (current-inexact-milliseconds)]
         [timeout-counter 0]
         [ok? #t]
         [peer initial-peer])

  (define node-root-facet (current-facet-id))

  (assert (known-node id))
  (assert #:when (ok?) (node-coordinates id (peer) (time-last-heard-from)))

  (on (message (discovered-node id $new-peer $known-alive?))
      (when known-alive? ;; suggestion doubles as indication of actual node activity
        (time-last-heard-from (current-inexact-milliseconds))
        (timeout-counter 0))
      (when (not (equal? (peer) new-peer))
        (log-dht/table-debug "Node ~a changed IP/port: from ~a to ~a" (~id id) (peer) new-peer)
        (peer new-peer)))

  (on (message (node-timeout id))
      (timeout-counter (+ (timeout-counter) 1)))

  (begin/dataflow
    (when (>= (timeout-counter) 3)
      (log-dht/table-debug "Too many timeouts for node ~a" (~id id))
      (ok? #f)))

  (begin/dataflow
    (when (not (ok?))
      (log-dht/table-debug "Node ~a no longer ok" (~id id))
      (react (stop-when-timeout (* 60 1000) (stop-facet node-root-facet))
             (stop-when-true (ok?)
               (log-dht/table-debug "Node ~a is ok again" (~id id))))))

  (on (message (discard-node id))
      (log-dht/table-debug "Discarding node ~a" (~id id))
      (ok? #f))

  (during (local-node $local-id)
    (assert #:when (ok?) (node-bucket (node-id->bucket id local-id) id))

    (define (ping-until-fresh-or-bad)
      (define snapshot-time-last-heard-from (time-last-heard-from))
      (log-dht/table-debug "Node ~a became questionable" (~id id))
      (let try-pinging ((ping-count 0))
        (cond
          [(> (time-last-heard-from) snapshot-time-last-heard-from)
           (log-dht/table-debug "Activity from node ~a, no longer questionable" (~id id))]
          [(not (ok?))
           (log-dht/table-debug "Stopped pinging ~a; questionable node now deemed not OK" (~id id))]
          [else
           (log-dht/table-debug "Questionable node ~a, pinging (~a attempts made already)"
                                (~id id) ping-count)
           (match (do-krpc-transaction local-id id (list 'ping id (gensym)) #"ping" (hash #"id" id))
             ['error
              (log-dht/table-debug "Error pinging node ~a" (~id id))
              (ok? #f)]
             ['timeout
              (try-pinging (+ ping-count 1))]
             [result
              (define remote-node-id (hash-ref result #"id" #f))
              (if (and remote-node-id (not (equal? remote-node-id id)))
                  (begin (log-dht/table-debug "Ping reply had id ~a, not ~a"
                                              (~id remote-node-id) (~id id))
                         (ok? #f))
                  (try-pinging (+ ping-count 1)))])])))

    ;; Ping questionable node after 15 minutes of inactivity:
    (on (asserted (later-than (+ (time-last-heard-from) (* 15 60 1000))))
        (ping-until-fresh-or-bad))

    ;; Also, ping on startup after ~30 seconds if we haven't had activity yet:
    (when (not initially-known-alive?)
      (on-start (define snapshot-time-last-heard-from (time-last-heard-from))
                (sleep (+ 25 (random 10)))
                (when (equal? snapshot-time-last-heard-from (time-last-heard-from))
                  (log-dht/table-debug "Starting initial ping for ~a" (~id id))
                  (ping-until-fresh-or-bad))))))

(spawn #:name 'neighbourhood-maintainer
       (stop-when-reloaded)

       (field [refresh-time (+ (current-inexact-milliseconds) (* 2 60 1000))])

       (during (local-node $local-id)
         (on (asserted (later-than (refresh-time)))
             (refresh-time (next-refresh-time 10 15))
             (define closest-nodes/peers (K-closest (query-all-nodes) local-id))
             (log-dht/neighbourhood-info "Refreshing local neighbourhood")
             (for [(np closest-nodes/peers)]
               (find-node/suggest (list 'neighbourhood (car np)) local-id (cadr np) local-id)))))
