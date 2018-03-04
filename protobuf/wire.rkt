#lang racket/base
;; Just enough Protocol Buffers to get by
;; https://developers.google.com/protocol-buffers/docs/encoding

(provide varint
         ->zig-zag
         zig-zag->
         zig-zag
         (struct-out protobuf-field)
         field)

(require racket/match)
(require bitsyntax)

(module+ test (require rackunit))
(module+ test (require (only-in file/sha1 hex-string->bytes)))

(define (varint-> input ks kf)
  (bit-string-case input
    ([(= 0 :: bits 1) (i :: bits 7) (rest :: binary)]
     (ks i rest))
    ([(= 1 :: bits 1) (i :: bits 7) (j :: (varint)) (rest :: binary)]
     (ks (+ i (arithmetic-shift j 7)) rest))
    (else (kf))))

(define (->varint i)
  (if (< i 128)
      (bytes i)
      (bit-string [1 :: bits 1]
                  [(bitwise-and i 127) :: bits 7]
                  [(arithmetic-shift i -7) :: (varint)])))

(define-syntax varint
  (syntax-rules ()
    [(_ #t input ks kf) (varint-> input ks kf)]
    [(_ #f i) (->varint i)]))

(module+ test
  (check-equal? (bytes 0) (bit-string->bytes (bit-string [0 :: (varint)])))
  (check-equal? (bytes 1) (bit-string->bytes (bit-string [1 :: (varint)])))
  (check-equal? (bytes #xac #x02) (bit-string->bytes (bit-string [300 :: (varint)])))

  (define (varint-> bs)
    (bit-string-case bs
      ([(i :: (varint)) (rest :: binary)] (list i (bit-string->bytes rest)))
      (else (error 'varint-> "Not a varint: ~v" bs))))

  (check-equal? (list 0 #"") (varint-> (bytes 0)))
  (check-equal? (list 1 #"") (varint-> (bytes 1)))
  (check-equal? (list 300 #"") (varint-> (bytes #xac #x02)))
  (check-equal? (list 1234 #"abcd") (varint-> (bytes-append (bytes 210 9) #"abcd"))))

(define (->zig-zag i)
  (if (negative? i)
      (+ (arithmetic-shift (- -1 i) 1) 1)
      (arithmetic-shift i 1)))

(define (zig-zag-> i)
  (if (zero? (bitwise-and i 1))
      (arithmetic-shift i -1)
      (- -1 (arithmetic-shift (- i 1) -1))))

(define-syntax zig-zag
  (syntax-rules ()
    [(_ #t input ks kf) (varint-> input (lambda (i r) (ks (zig-zag-> i) r)) kf)]
    [(_ #f i) (->varint (->zig-zag i))]))

(module+ test
  (check-equal? 0 (->zig-zag 0))
  (check-equal? 1 (->zig-zag -1))
  (check-equal? 2 (->zig-zag 1))
  (check-equal? 3 (->zig-zag -2))
  (check-equal? 4294967294 (->zig-zag 2147483647))
  (check-equal? 4294967295 (->zig-zag -2147483648))

  (check-equal? 0 (zig-zag-> 0))
  (check-equal? -1 (zig-zag-> 1))
  (check-equal? 1 (zig-zag-> 2))
  (check-equal? -2 (zig-zag-> 3))
  (check-equal? 2147483647 (zig-zag-> 4294967294))
  (check-equal? -2147483648 (zig-zag-> 4294967295))

  (check-equal? (bytes 3) (bit-string [-2 :: (zig-zag)]))
  (check-equal? -2 (bit-string-case (bytes 3) ([(i :: (zig-zag))] i))))

(define (encode-message-key field-number wire-type)
  (bitwise-ior (arithmetic-shift field-number 3)
               (match wire-type
                 ['varint 0]
                 ['bits64 1]
                 ['length-delimited 2]
                 ['start-group 3]
                 ['end-group 4]
                 ['bits32 5])))

(define (decode-message-key n)
  (values (arithmetic-shift n -3)
          (match (bitwise-and n 7)
            [0 'varint]
            [1 'bits64]
            [2 'length-delimited]
            [3 'start-group]
            [4 'end-group]
            [5 'bits32]
            [_ #f])))

(struct protobuf-field (number wire-type value) #:prefab)

(define (field-> input ks kf)
  (bit-string-case input
    ([(n :: (varint)) (rest :: binary)]
     (define-values (number wire-type) (decode-message-key n))
     (match wire-type
       ['varint (bit-string-case rest
                  ([(v :: (varint)) (rest :: binary)]
                   (ks (protobuf-field number wire-type v) rest))
                  (else (kf)))]
       ['bits64 (bit-string-case rest
                  ([(v :: binary bytes 8) (rest :: binary)]
                   (ks (protobuf-field number wire-type v) rest))
                  (else (kf)))]
       ['length-delimited (bit-string-case rest
                            ([(len :: (varint)) (v :: binary bytes len) (rest :: binary)]
                             (ks (protobuf-field number wire-type v) rest))
                            (else (kf)))]
       ['start-group (error 'field-> "start-group unsupported")]
       ['end-group (error 'field-> "end-group unsupported")]
       ['bits32 (bit-string-case rest
                  ([(v :: binary bytes 4) (rest :: binary)]
                   (ks (protobuf-field number wire-type v) rest))
                  (else (kf)))]
       [#f (kf)]))
    (else (kf))))

(define (->field f)
  (match-define (protobuf-field number wire-type v) f)
  (bit-string-append
   (bit-string [(encode-message-key number wire-type) :: (varint)])
   (match wire-type
     ['varint (bit-string [v :: (varint)])]
     ['bits64 (bit-string [v :: binary bytes 8])]
     ['length-delimited (bit-string [(bit-string-byte-count v) :: (varint)] [v :: binary])]
     ['start-group (error '->field "start-group unsupported")]
     ['end-group (error '->field "end-group unsupported")]
     ['bits32 (bit-string [v :: binary bytes 4])])))

(define-syntax field
  (syntax-rules ()
    [(_ #t input ks kf) (field-> input ks kf)]
    [(_ #f f) (->field f)]))

(module+ test
  (define (encode-field f) (bit-string->bytes (bit-string [f :: (field)])))
  (define (decode-field bs)
    (bit-string-case bs
      ([(f :: (field))]
       (match f
         [(protobuf-field n w v)
          (protobuf-field n w (bit-string->bytes v))]))))

  (check-equal? (hex-string->bytes "120774657374696e67")
                (encode-field (protobuf-field 2 'length-delimited #"testing")))
  (check-equal? (protobuf-field 2 'length-delimited #"testing")
                (decode-field (hex-string->bytes "120774657374696e67"))))
