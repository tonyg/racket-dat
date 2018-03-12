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

(define-logger dht/wire)

;;---------------------------------------------------------------------------

(define (encode-packet packet)
  (bencode->bytes (list packet)))

(define (decode-packet buf [source "unknown source"])
  (define v (match (with-handlers [(exn? (lambda (e) #f))] (bytes->bencode buf))
              [(cons v '()) v]
              [_ #f]))
  (when (not v)
    (log-dht/wire-warning "Packet from ~a corrupt or invalid" source)
    (log-dht/wire-debug "Packet from ~a corrupt or invalid: ~a" source buf))
  v)

;;---------------------------------------------------------------------------
;; KRPC

(define (make-packet id type . kvs) (apply hash #"t" id #"y" type kvs))
(define (make-query id method args)  (make-packet id #"q" #"q" method #"a" args))
(define (make-response id results)   (make-packet id #"r" #"r" results))
(define (make-error id code message) (make-packet id #"e" #"e" (list code message)))

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
            ['request (list (hash-ref packet #"q" #f) (hash-ref packet #"a" hash))]
            ['response (hash-ref packet #"r" hash)]
            ['error (hash-ref packet #"e" (lambda () (list #f #f)))])))
