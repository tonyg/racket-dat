#lang syndicate

(require racket/set)
(require (only-in file/sha1 bytes->hex-string))
(require bitsyntax)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)
(require/activate syndicate/drivers/udp)

(require "wire.rkt")
(require "protocol.rkt")

(spawn #:name 'memo-table
       (stop-when-reloaded)
       (during (observe (memoized (krpc-transaction $src $tgt $txn $method $args _)))
         (assert (observe (memoized (krpc-transaction src tgt txn method args _))))
         (stop-when-timeout 30000)
         (on-start
          (react
           (stop-when (asserted (krpc-transaction src tgt txn method args $result))
             (react (assert (memoized (krpc-transaction src tgt txn method args result)))))))))

(spawn #:name 'node-factory
       (stop-when-reloaded)
       (define/query-set ids (known-node $id) id)
       (on (message (discovered-node $id $peer $known-alive?))
           (when (not (set-member? (ids) id))
             (ids (set-add (ids) id))
             (spawn #:name (list 'node (bytes->hex-string id))
                    #:assertions (known-node id)
                    (stop-when-reloaded)
                    (on-start (log-info "Tracking node ~a at ~a" (bytes->hex-string id) peer))
                    (on-stop (log-info "Terminated node ~a at ~a" (bytes->hex-string id) peer))
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
        (log-info "Node ~a changed IP/port: from ~a to ~a"
                  (bytes->hex-string id)
                  (peer)
                  new-peer)
        (peer new-peer)))

  (on (message (node-timeout id))
      (timeout-counter (+ (timeout-counter) 1)))

  (begin/dataflow
    (when (>= (timeout-counter) 3)
      (log-info "Too many timeouts for node ~a" (bytes->hex-string id))
      (ok? #f)))

  (begin/dataflow
    (when (not (ok?))
      (log-info "Node ~a no longer ok" (bytes->hex-string id))
      (react (stop-when-timeout (* 60 1000) (stop-facet node-root-facet))
             (stop-when-true (ok?) (log-info "Node ~a is ok again" (bytes->hex-string id))))))

  (on (message (discard-node id))
      (log-info "Discarding node ~a" (bytes->hex-string id))
      (ok? #f))

  (during (local-node $local-id)
    (assert #:when (ok?) (node-bucket (node-id->bucket id local-id) id))

    (define (ping-until-fresh-or-bad)
      (define snapshot-time-last-heard-from (time-last-heard-from))
      (log-info "Node ~a became questionable" (bytes->hex-string id))
      (let try-pinging ((ping-count 0))
        (cond
          [(> (time-last-heard-from) snapshot-time-last-heard-from)
           (log-info "Activity from node ~a, no longer questionable" (bytes->hex-string id))]
          [(not (ok?))
           (log-info "Stopped pinging ~a; questionable node now deemed not OK"
                     (bytes->hex-string id))]
          [else
           (log-info "Questionable node ~a, pinging (~a attempts made already)"
                     (bytes->hex-string id)
                     ping-count)
           ;; Ignore the result, except for checking for errors: we
           ;; really only care whether time-last-heard-from is
           ;; advanced somehow!
           (match (do-krpc-transaction local-id id (list 'ping id (gensym))
                                       #"ping" (hash #"id" id))
             ['error
              (log-info "Error pinging node ~a" (bytes->hex-string id))
              (ok? #f)]
             ['timeout
              (try-pinging (+ ping-count 1))]
             [result
              (define remote-node-id (hash-ref result #"id" #f))
              (if (and remote-node-id (not (equal? remote-node-id id)))
                  (begin (log-info "Ping reply had id ~a, not ~a"
                                   (bytes->hex-string remote-node-id)
                                   (bytes->hex-string id))
                         (ok? #f))
                  (try-pinging (+ ping-count 1)))])])))

    ;; Ping questionable node after 15 minutes of inactivity:
    (on (asserted (later-than (+ (time-last-heard-from) (* 15 60 1000))))
        (ping-until-fresh-or-bad))

    ;; Also, ping on startup after 30 seconds if we haven't had activity yet:
    (when (not initially-known-alive?)
      (on-start (define snapshot-time-last-heard-from (time-last-heard-from))
                (sleep 30)
                (when (equal? snapshot-time-last-heard-from (time-last-heard-from))
                  (log-info "Starting initial ping for ~a" (bytes->hex-string id))
                  (ping-until-fresh-or-bad)))))

  (begin/dataflow (log-info "Node ~a: timestamp ~a" (bytes->hex-string id) (time-last-heard-from)))
  (begin/dataflow (log-info "Timeouts ~a: ~a" (bytes->hex-string id) (timeout-counter))))
