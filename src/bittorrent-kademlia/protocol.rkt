#lang syndicate

(provide (all-defined-out))

(require racket/match)
(require racket/set)
(require (only-in file/sha1 bytes->hex-string))
(require (only-in racket/string string-split))

(require bitsyntax)
(require syndicate/drivers/udp)

(define-logger dht/protocol)

(assertion-struct local-node (id))

(assertion-struct known-node (id))
(assertion-struct node-coordinates (id peer timestamp))
(assertion-struct node-bucket (bucket id))

(message-struct discovered-node (id peer known-alive?))
(message-struct discard-node (id))

(assertion-struct fresh-transaction (name number))

(assertion-struct krpc-ready ())
(message-struct krpc-packet (direction peer id type body))
(assertion-struct krpc-transaction (source-id target transaction-name method args results))
;; ^ target is either a bytes (length 20) or a udp-remote-address
(message-struct node-timeout (node))
(assertion-struct memoized (transaction))

(assertion-struct valid-tokens (tokens))

;;---------------------------------------------------------------------------
;; Client API

(assertion-struct locate-node (id root-nodes/peers))
(assertion-struct closest-nodes-to (id nodes/peers final?))

(assertion-struct locate-participants (resource-id))
(assertion-struct participants-in (resource-id record-holders final?))
(assertion-struct record-holder (id location token has-records?))
(assertion-struct participant-record (resource-id host port))

;;---------------------------------------------------------------------------

(define K 8)

(define (bytes-xor a b)
  (define result (make-bytes (min (bytes-length a) (bytes-length b))))
  (for [(i (bytes-length result))]
    (bytes-set! result i (bitwise-xor (bytes-ref a i) (bytes-ref b i))))
  result)

(define ((distance-to-<? reference) a b)
  (cond [(not b) #t] ;; a bootstrap node has no known ID, and is "infinitely dispreferred"
        [(not a) #f]
        [else (bytes<? (bytes-xor reference a) (bytes-xor reference b))]))

(define (node-id->number id)
  (bit-string-case id ([(n :: big-endian bytes 20)] n)))

(define (number->node-id n)
  (bit-string->bytes (bit-string (n :: big-endian bytes 20))))

(define (node-in-range? id lo hi)
  (define n (node-id->number id))
  (and (<= lo n) (< n hi)))

(define (node-id->bucket id local-id)
  (integer-length (node-id->number (bytes-xor id local-id))))

(define (extract-ip/port ip/port)
  (bit-string-case ip/port
    ([a b c d (port :: big-endian bytes 2)]
     (udp-remote-address (format "~a.~a.~a.~a" a b c d) port))
    (else
     #f)))

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

(define (format-ip/port peer)
  (match-define (udp-remote-address host port) peer)
  (match (dotted-quad->numbers host)
    [(list a b c d)
     (bit-string->bytes (bit-string a b c d (port :: big-endian bytes 2)))]
    [#f #f]))

(define (format-peers peers)
  (bit-string->bytes
   (apply bit-string-append
          (map (match-lambda
                 [(list id peer)
                  (match (format-ip/port peer)
                    [#f (bit-string)]
                    [loc (bit-string (id :: binary bytes 20) (loc :: binary))])])
               peers))))

(define (take-at-most n xs)
  (cond [(zero? n) '()]
        [(null? xs) '()]
        [else (cons (car xs) (take-at-most (- n 1) (cdr xs)))]))

(define (query-all-nodes)
  (set->list (immediate-query [query-set (node-coordinates $i $p _) (list i p)])))

(define (K-closest nodes/peers target-id #:K [k K] #:key [key car])
  (take-at-most k (sort nodes/peers #:key key (distance-to-<? target-id))))

(define (do-krpc-transaction source-id target transaction-name method args)
  (react/suspend (k)
    (stop-when (asserted
                (memoized
                 (krpc-transaction source-id target transaction-name method args $results)))
      (k results))))

(define (suggest-node! source id peer known-alive?)
  (match-define (udp-remote-address host port) peer)
  (match (map string->number (string-split host "."))
    [(or (list 10 _ _ _)
         (list 172 (? (lambda (b) (and (>= b 16) (< b 32)))) _ _)
         (list 192 168 _ _))
     (log-dht/protocol-debug "Ignoring RFC 1918 network for ~a (~a) via ~a"
                             (bytes->hex-string id) peer source)]
    [_
     (cond
       [(= 20 (bytes-length id))
        (log-dht/protocol-debug "Suggested node ~a (~a) via ~a" (bytes->hex-string id) peer source)
        (send! (discovered-node id peer known-alive?))]
       [else
        (log-dht/protocol-debug "Ignoring suggestion of bogus ID ~a" (bytes->hex-string id))])]))

(define (format-nodes/peers ns)
  (for/list [(n ns)]
    (format "~a(~a)" (and (car n) (bytes->hex-string (car n))) (cdr n))))
