#lang syndicate

(require (only-in racket/random crypto-random-bytes random-ref))
(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)

(require "protocol.rkt")

(define-logger dht/bucket)

(define (random-name-in-bucket local-name bucket)
  (define name (crypto-random-bytes 20))
  (define-values (byte-pos bit-pos) (quotient/remainder (- 160 bucket) 8))
  (for ([i (in-range 0 byte-pos)]) (bytes-set! name i 0))
  (let* ((b (bytes-ref name byte-pos))
         (b (bitwise-and b (arithmetic-shift #xff (- bit-pos))))
         (b (bitwise-ior b (arithmetic-shift #x80 (- bit-pos)))))
    (bytes-set! name byte-pos b))
  (bytes-xor local-name name))

(spawn #:name 'kademlia-buckets
       (stop-when-reloaded)

       (during (local-node $local-name)
         (during/spawn (node-bucket $bucket _)
           #:name (list 'kademlia-bucket bucket)

           (field [refresh-time (next-refresh-time 10 15)]
                  [nodes (set)])

           (during (node-bucket bucket $name)
             (on-start (refresh-time (next-refresh-time 10 15)))
             (during (node-coordinates name $addr $timestamp)
               (on-start (nodes (set-add (nodes) (node-coordinates name addr timestamp))))
               (on-stop (nodes (set-remove (nodes) (node-coordinates name addr timestamp))))))

           (begin/dataflow
             (define good (sort (set->list (nodes)) #:key node-coordinates-timestamp >))
             ;; ^ newest first; we discard the newest one if necessary
             (log-dht/bucket-debug "Bucket ~a: ~a good nodes" bucket (length good))
             (when (> (length good) (if (= bucket 160) (* 2 K) K))
               (send! (discard-node (node-coordinates-name (car good))))))

           (begin/dataflow
             (log-dht/bucket-debug "Bucket ~a: will refresh at ~a" bucket (refresh-time)))

           (on (asserted (later-than (refresh-time)))
               (define target (random-name-in-bucket local-name bucket))
               (define respondent (car (random-ref (query-all-nodes))))
               (log-dht/bucket-debug "Bucket ~a: refreshing target ~a via ~a"
                                     bucket (~name target) (~name respondent))
               (find-node/suggest (list 'refresh bucket target) local-name respondent target)))))
