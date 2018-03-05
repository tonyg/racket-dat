#lang racket/base

(provide encode-packet
         decode-packet

         make-query
         make-response
         make-error

         synthesize-krpc-packet
         analyze-krpc-packet)

(require racket/match)
(require bencode-codec)

;;---------------------------------------------------------------------------

(define (encode-packet packet)
  (bencode->bytes (list packet)))

(define (decode-packet buf [source "unknown source"])
  (match (with-handlers [(exn? (lambda (e) #f))]
           (bytes->bencode buf))
    [(cons v '()) v]
    [(cons v _)
     (log-warning "DHT packet from ~a contained multiple values" source)
     v]
    ['()
     (log-warning "DHT packet from ~a contained no values" source)
     #f]
    [#f
     (log-warning "DHT packet from ~a contained corrupt bencoding" source)
     #f]))

;;---------------------------------------------------------------------------
;; KRPC

(define (make-packet id type . kvs)
  (apply hash
         #"t" id
         #"y" (match type
                ['request #"q"]
                ['response #"r"]
                ['error #"e"])
         kvs))

(define (make-query id method args)
  (make-packet id
               'request
               #"q" method
               #"a" args))

(define (make-response id results)
  (make-packet id
               'response
               #"r" results))

(define (make-error id code message)
  (make-packet id
               'error
               #"e" (list code message)))

(define (synthesize-krpc-packet id type body)
  (match type
    ['request (make-query id (car body) (cadr body))]
    ['response (make-response id body)]
    ['error (make-error id (car body) (cadr body))]))

(define (analyze-krpc-packet packet)
  (define type (match (hash-ref packet #"y" #f)
                 [#f #f]
                 [#"q" 'request]
                 [#"r" 'response]
                 [#"e" 'error]))
  (values (hash-ref packet #"t" #f)
          type
          (match type
            [#f #f]
            ['request (list (hash-ref packet #"q" #f)
                            (hash-ref packet #"a" hash))]
            ['response (hash-ref packet #"r" hash)]
            ['error (hash-ref packet #"e" (lambda () (list #f #f)))])))
