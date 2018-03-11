#lang syndicate

(require (only-in racket/random crypto-random-bytes random-ref))
(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)

(require "protocol.rkt")

(define-logger dht/bucket)

(define (random-id-in-bucket local-id bucket)
  (define id (crypto-random-bytes 20))
  (define-values (byte-pos bit-pos) (quotient/remainder (- 160 bucket) 8))
  (for ([i (in-range 0 byte-pos)]) (bytes-set! id i 0))
  (let* ((b (bytes-ref id byte-pos))
         (b (bitwise-and b (arithmetic-shift #xff (- bit-pos))))
         (b (bitwise-ior b (arithmetic-shift #x80 (- bit-pos)))))
    (bytes-set! id byte-pos b))
  (bytes-xor local-id id))

(spawn #:name 'kademlia-buckets
       (stop-when-reloaded)

       (during (local-node $local-id)
         (during/spawn (node-bucket $bucket _)
           #:name (list 'kademlia-bucket bucket)

           (field [refresh-time (next-refresh-time 10 15)]
                  [nodes (set)])

           (during (node-bucket bucket $id)
             (on-start (refresh-time (next-refresh-time 10 15)))
             (during (node-coordinates id $peer $timestamp)
               (on-start (nodes (set-add (nodes) (node-coordinates id peer timestamp))))
               (on-stop (nodes (set-remove (nodes) (node-coordinates id peer timestamp))))))

           (begin/dataflow
             (define good (sort (set->list (nodes)) #:key node-coordinates-timestamp >))
             ;; ^ newest first; we discard the newest one if necessary
             (log-dht/bucket-debug "Bucket ~a: ~a good nodes" bucket (length good))
             (when (> (length good) (if (= bucket 160) (* 2 K) K))
               (send! (discard-node (node-coordinates-id (car good))))))

           (begin/dataflow
             (log-dht/bucket-debug "Bucket ~a: will refresh at ~a" bucket (refresh-time)))

           (on (asserted (later-than (refresh-time)))
               (define target (random-id-in-bucket local-id bucket))
               (define respondent (car (random-ref (query-all-nodes))))
               (log-dht/bucket-debug "Bucket ~a: refreshing target ~a via ~a"
                                     bucket (~id target) (~id respondent))
               (find-node/suggest (list 'refresh bucket target) local-id respondent target)))))
