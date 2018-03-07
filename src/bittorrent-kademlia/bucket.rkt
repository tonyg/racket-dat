#lang syndicate

(require (only-in racket/list partition))
(require (only-in racket/random crypto-random-bytes random-ref))
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

           (define (next-refresh-time)
             (+ (current-inexact-milliseconds)
                (* 10 60 1000)
                (* (random (* 5 60)) 1000)))

           (field [refresh-time (next-refresh-time)]
                  [nodes (set)])

           (during (node-bucket bucket $id)
             (on-start (refresh-time (next-refresh-time)))
             (during (node-coordinates id $peer $timestamp)
               (on-start (nodes (set-add (nodes) (node-coordinates id peer timestamp))))
               (on-stop (nodes (set-remove (nodes) (node-coordinates id peer timestamp))))))

           (begin/dataflow
             (define good (sort (set->list (nodes)) #:key node-coordinates-timestamp >))
             ;; ^ newest first; we discard the newest one if necessary
             (log-info "Bucket ~a: ~a good nodes" bucket (length good))
             (when (> (length good) K)
               (send! (discard-node (node-coordinates-id (car good))))))

           (on (asserted (later-than (refresh-time)))
               (define target (random-id-in-bucket local-id bucket))
               (define respondent (car (random-ref (query-all-nodes))))
               (log-info "Bucket ~a: refreshing target ~a via ~a"
                         bucket
                         (bytes->hex-string target)
                         (bytes->hex-string respondent))
               (define results
                 (do-krpc-transaction local-id respondent (list 'refresh target)
                                      #"find_node" (hash #"id" local-id #"target" target)))
               (when (hash? results)
                 (for [(p (extract-peers (hash-ref results #"nodes" #"")))]
                   (suggest-node! (list 'refresh-find-node bucket) (car p) (cadr p))))))))
