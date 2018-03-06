#lang syndicate

(require (only-in racket/list partition))
(require (only-in racket/random crypto-random-bytes))
(require racket/set)
(require (only-in file/sha1 bytes->hex-string))
(require (only-in srfi/43 vector-binary-search))
(require bitsyntax)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/timestate)
(require/activate syndicate/drivers/udp)

(require "wire.rkt")
(require "protocol.rkt")

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

           (field [refresh-time (current-inexact-milliseconds)]
                  [nodes (set)])

           (during (node-bucket bucket $id)
             (on-start (refresh-time (current-inexact-milliseconds)))
             (during (node-coordinates id $peer $timestamp)
               (on-start (nodes (set-add (nodes) (node-coordinates id peer timestamp))))
               (on-stop (nodes (set-remove (nodes) (node-coordinates id peer timestamp))))))

           (begin/dataflow
             (define good (sort (set->list (nodes)) #:key node-coordinates-timestamp >))
             ;; ^ newest first
             (log-info "Bucket ~a: ~a good nodes" bucket (length good))
             (when (> (length good) K)
               (send! (discard-node (node-coordinates-id (car good))))))

           (on (asserted (later-than (+ (refresh-time) (* 15 60 1000))))
               (define target (random-id-in-bucket local-id bucket))
               (log-info "Bucket ~a: refreshing target ~a" bucket (bytes->hex-string target))
               (react (assert (locate-node target))
                      (stop-when-timeout (* 15 1000)
                        (log-info "Bucket ~a: declaring refresh over." bucket))
                      (during (closest-nodes-to local-id $ids)
                        (log-info "Bucket ~a: results: ~a" bucket (map bytes->hex-string ids))))))))
