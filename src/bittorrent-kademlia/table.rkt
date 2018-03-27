#lang syndicate

(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)

(require "protocol.rkt")

(define-logger dht/table)
(define-logger dht/neighbourhood)

(spawn #:name 'node-factory
       (stop-when-reloaded)
       (define/query-set names (known-node $name) name)
       (on (message (discovered-node $name $addr $known-alive?))
           (when (not (set-member? (names) name))
             (names (set-add (names) name))
             (spawn #:name (list 'node (~name name))
                    #:assertions (known-node name)
                    (stop-when-reloaded)
                    (on-start (log-dht/table-debug "Tracking node ~a at ~a" (~name name) addr))
                    (on-stop (log-dht/table-debug "Terminated node ~a at ~a" (~name name) addr))
                    (node-main name addr known-alive?)))))

(define (node-main name initial-addr initially-known-alive?)
  (field [time-last-heard-from (current-inexact-milliseconds)]
         [timeout-counter 0]
         [ok? #t]
         [addr initial-addr])

  (define node-root-facet (current-facet-id))

  (assert (known-node name))
  (assert #:when (ok?) (node-coordinates name (addr) (time-last-heard-from)))

  (on (message (discovered-node name $new-addr $known-alive?))
      (when known-alive? ;; suggestion doubles as indication of actual node activity
        (time-last-heard-from (current-inexact-milliseconds))
        (timeout-counter 0))
      (when (not (equal? (addr) new-addr))
        (log-dht/table-debug "Node ~a changed IP/port: from ~a to ~a" (~name name) (addr) new-addr)
        (addr new-addr)))

  (on (message (node-timeout name))
      (timeout-counter (+ (timeout-counter) 1)))

  (begin/dataflow
    (when (>= (timeout-counter) 3)
      (log-dht/table-debug "Too many timeouts for node ~a" (~name name))
      (ok? #f)))

  (begin/dataflow
    (when (not (ok?))
      (log-dht/table-debug "Node ~a no longer ok" (~name name))
      (react (stop-when-timeout (* 60 1000) (stop-facet node-root-facet))
             (stop-when-true (ok?)
               (log-dht/table-debug "Node ~a is ok again" (~name name))))))

  (on (message (discard-node name))
      (log-dht/table-debug "Discarding node ~a" (~name name))
      (ok? #f))

  (during (local-node $local-name)
    (assert #:when (ok?) (node-bucket (node-name->bucket name local-name) name))

    (define (ping-until-fresh-or-bad)
      (define snapshot-time-last-heard-from (time-last-heard-from))
      (log-dht/table-debug "Node ~a became questionable" (~name name))
      (let try-pinging ((ping-count 0))
        (cond
          [(> (time-last-heard-from) snapshot-time-last-heard-from)
           (log-dht/table-debug "Activity from node ~a, no longer questionable" (~name name))]
          [(not (ok?))
           (log-dht/table-debug "Stopped pinging ~a; was questionable, now not OK" (~name name))]
          [else
           (log-dht/table-debug "Questionable node ~a, pinging (~a attempts made already)"
                                (~name name) ping-count)
           (match (do-krpc-transaction local-name name (list 'ping name (gensym))
                                       #"ping" (hash #"id" name))
             ['error
              (log-dht/table-debug "Error pinging node ~a" (~name name))
              (ok? #f)]
             ['timeout (try-pinging (+ ping-count 1))]
             [result
              (define remote-node-name (hash-ref result #"id" #f))
              (if (and remote-node-name (not (equal? remote-node-name name)))
                  (begin (log-dht/table-debug "Ping reply had name ~a, not ~a"
                                              (~name remote-node-name) (~name name))
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
                  (log-dht/table-debug "Starting initial ping for ~a" (~name name))
                  (ping-until-fresh-or-bad))))))

(spawn #:name 'neighbourhood-maintainer
       (stop-when-reloaded)

       (field [refresh-time (+ (current-inexact-milliseconds) (* 2 60 1000))])

       (during (local-node $local-name)
         (on (asserted (later-than (refresh-time)))
             (refresh-time (next-refresh-time 10 15))
             (define closest-nodes/addrs (K-closest (query-all-nodes) local-name))
             (log-dht/neighbourhood-info "Refreshing local neighbourhood")
             (for [(np closest-nodes/addrs)]
               (find-node/suggest (list 'neighbourhood (car np))
                                  local-name
                                  (cadr np)
                                  local-name)))))
