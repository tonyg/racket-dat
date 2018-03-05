#lang racket/base

(provide (all-defined-out))

(require racket/match)

(require bitsyntax)
(require syndicate)
(require syndicate/drivers/udp)

(assertion-struct local-node (id))

(assertion-struct known-node (id))
(assertion-struct node-coordinates (id peer timestamp))
(assertion-struct node-bucket (id bucket))

(message-struct discovered-node (id peer))
(message-struct discard-node (id))

(assertion-struct fresh-transaction (name number))

(assertion-struct krpc-ready ())
(message-struct krpc-packet (direction peer id type body))
(assertion-struct krpc-transaction (source-id target-id transaction-name method args results))
(message-struct transaction-resolution (node-id resolution))
(assertion-struct memoized (transaction))

(assertion-struct locate-node (id))
(assertion-struct closest-nodes-to (id ids))

(define K 8)

(define (bytes-xor a b)
  (define result (make-bytes (min (bytes-length a) (bytes-length b))))
  (for [(i (bytes-length result))]
    (bytes-set! result i (bitwise-xor (bytes-ref a i) (bytes-ref b i))))
  result)

(define ((distance-to-<? reference) a b)
  (bytes<? (bytes-xor reference a) (bytes-xor reference b)))

(define (node-id->number id)
  (bit-string-case id ([(n :: big-endian bytes 20)] n)))

(define (number->node-id n)
  (bit-string->bytes (bit-string (n :: big-endian bytes 20))))

(define (node-in-range? id lo hi)
  (define n (node-id->number id))
  (and (<= lo n) (< n hi)))

(define (node-id->bucket id local-id)
  (integer-length (node-id->number (bytes-xor id local-id))))

(define (extract-peers nodes)
  (bit-string-case nodes
    ([] '())
    ([(id :: binary bytes 20) a b c d (port :: big-endian bytes 2) (rest :: binary)]
     (cons (list (bit-string->bytes id)
                 (udp-remote-address (format "~a.~a.~a.~a" a b c d) port))
           (extract-peers rest)))))

(define (dotted-quad->numbers host)
  (match host
    [(regexp #px"([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)" (list _ a b c d))
     (list (string->number a) (string->number b) (string->number c) (string->number d))]
    [_ #f]))

(define (format-peers peers)
  (bit-string->bytes
   (apply bit-string-append
          (map (match-lambda
                 [(list id (udp-remote-address host port))
                  (match (dotted-quad->numbers host)
                    [(list a b c d)
                     (bit-string (id :: binary bytes 20) a b c d (port :: big-endian bytes 2))]
                    [#f
                     (log-warning "Cannot format hostname: ~a" host)
                     (bit-string)])])
               peers))))

(define (take-at-most n xs)
  (cond [(zero? n) '()]
        [(null? xs) '()]
        [else (cons (car xs) (take-at-most (- n 1) (cdr xs)))]))
