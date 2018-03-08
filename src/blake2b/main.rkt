#lang racket/base
;; "The BLAKE2 Cryptographic Hash and Message Authentication Code (MAC)"
;; blake2b implementation based on that in the appendix to RFC 7693.

(provide (struct-out blake2b-context)
         blake2b-init
         blake2b-update!
         blake2b-finish!

         blake2b-hash
         blake2b160
         blake2b256
         blake2b384
         blake2b512)

(require racket/match)
(require (only-in racket/vector vector-copy))

(module+ test (require rackunit))

;; 64-bit arithmetic and vector utilities

(define mask64 #xffffffffffffffff)

(define-syntax-rule (+/64 arg ...)
  (bitwise-and mask64 (+ arg ...)))

(define-syntax-rule (^/64 arg ...)
  ;; Optimization: do not clamp to mask64 here, since we know at every
  ;; use site that the arguments are in range and that the result will
  ;; be clamped properly soon.
  (bitwise-xor arg ...))

(define (>>> n by)
  (bitwise-and mask64 (bitwise-ior (arithmetic-shift n (- by))
                                   (arithmetic-shift n (- 64 by)))))

(define-syntax-rule (@ v x) (vector-ref v x))
(define-syntax-rule (@= v x y) (vector-set! v x y))

;; 2.1.  Parameters

(define R1 32)
(define R2 24)
(define R3 16)
(define R4 63)

;; 2.6.  Initialization Vector

(define IV
  (let ()
    (local-require "exact-sqrt.rkt")
    (for/vector [(n '(2 3 5 7 11 13 17 19))]
      (bitwise-and mask64
                   (truncate (* (expt 2 64)
                                (exact-sqrt/precision n (expt 2 -64))))))))

(module+ test
  (check-equal? (vector #x6A09E667F3BCC908
                        #xBB67AE8584CAA73B
                        #x3C6EF372FE94F82B
                        #xA54FF53A5F1D36F1
                        #x510E527FADE682D1
                        #x9B05688C2B3E6C1F
                        #x1F83D9ABFB41BD6B
                        #x5BE0CD19137E2179)
                IV))

;; 2.7.  Message Schedule SIGMA

(define SIGMA
  (vector
   (vector  0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15)
   (vector 14 10  4  8  9 15 13  6  1 12  0  2 11  7  5  3)
   (vector 11  8 12  0  5  2 15 13 10 14  3  6  7  1  9  4)
   (vector  7  9  3  1 13 12 11 14  2  6  5 10  4  0 15  8)
   (vector  9  0  5  7  2  4 10 15 14  1 11 12  6  8  3 13)
   (vector  2 12  6 10  0 11  8  3  4 13  7  5 15 14  1  9)
   (vector 12  5  1 15 14 13  4 10  0  7  6  3  9  2  8 11)
   (vector 13 11  7 14 12  1  3  9  5  0 15  4  8  6  2 10)
   (vector  6 15 14  9 11  3  0  8 12  2 13  7  1  4 10  5)
   (vector 10  2  8  4  7  6  1  5 15 11  9 14  3 12 13  0)
   (vector  0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15)
   (vector 14 10  4  8  9 15 13  6  1 12  0  2 11  7  5  3)))

;; 3.1.  Mixing Function G

(define-syntax-rule (G v a b c d x y)
  (begin
    (@= v a (+/64 (@ v a) (@ v b) x))
    (@= v d (>>> (^/64 (@ v d) (@ v a)) R1))
    (@= v c (+/64 (@ v c) (@ v d)))
    (@= v b (>>> (^/64 (@ v b) (@ v c)) R2))
    (@= v a (+/64 (@ v a) (@ v b) y))
    (@= v d (>>> (^/64 (@ v d) (@ v a)) R3))
    (@= v c (+/64 (@ v c) (@ v d)))
    (@= v b (>>> (^/64 (@ v b) (@ v c)) R4))))

;; 3.2.  Compression Function F

(define (F h m t f)
  (define v (make-vector 16 0))
  (for [(i (in-range 0 8))] (@= v i (@ h i)))
  (for [(i (in-range 8 16))] (@= v i (@ IV (- i 8))))

  (@= v 12 (^/64 (@ v 12) (bitwise-and mask64 t)))
  (@= v 13 (^/64 (@ v 13) (bitwise-and mask64 (arithmetic-shift t -64))))

  (when f ;; last block flag?
    (@= v 14 (bitwise-xor (@ v 14) mask64)))

  (for [(i (in-range 12))] ;; rounds
    (define s (@ SIGMA i))

    ;; (printf "round=~a F=~a T=~a V=" i f t)
    ;; (for [(n v)] (printf "~a " (number->string n 16)))
    ;; (newline)

    (G v 0 4  8 12 (@ m (@ s  0)) (@ m (@ s  1)))
    (G v 1 5  9 13 (@ m (@ s  2)) (@ m (@ s  3)))
    (G v 2 6 10 14 (@ m (@ s  4)) (@ m (@ s  5)))
    (G v 3 7 11 15 (@ m (@ s  6)) (@ m (@ s  7)))

    (G v 0 5 10 15 (@ m (@ s  8)) (@ m (@ s  9)))
    (G v 1 6 11 12 (@ m (@ s 10)) (@ m (@ s 11)))
    (G v 2 7  8 13 (@ m (@ s 12)) (@ m (@ s 13)))
    (G v 3 4  9 14 (@ m (@ s 14)) (@ m (@ s 15))))

  (for [(i (in-range 8))]
    (@= h i (^/64 (@ h i) (@ v i) (@ v (+ i 8))))))

;; 3.3.  Padding Data and Computing a BLAKE2 Digest

(struct blake2b-context (h buf [fill #:mutable] [t #:mutable] digest-byte-count))

(define (blake2b-init digest-byte-count key)
  (define key-length (bytes-length key))
  (when (or (<= digest-byte-count 0) (> digest-byte-count 64))
    (error 'blake2b-init "digest-byte-count must be >0 and <=64: ~v" digest-byte-count))
  (when (> key-length 64)
    (error 'blake2b-init "key must not be longer than 64 bytes; got ~v bytes" key-length))
  (define h (vector-copy IV))
  (@= h 0 (^/64 (@ h 0)
                #x01010000 ;; NB. hex, not binary
                (arithmetic-shift key-length 8)
                digest-byte-count))
  (define buf (make-bytes 128 0))
  (define ctx
    (blake2b-context h
                     buf
                     0
                     0
                     digest-byte-count))
  (when (positive? key-length)
    (blake2b-update! ctx key)
    (blake2b-update! ctx (make-bytes (- 128 key-length) 0)))
  ctx)

(define (bytes-block->words-block block offset)
  (for/vector [(i (in-range offset (+ offset 128) 8))]
    (integer-bytes->integer block #f #f i (+ i 8))))

(define (words-block->bytes-block h digest-byte-count)
  (define data (make-bytes 64))
  (for [(i (in-range 0 64 8))
        (j (in-range 0 (vector-length h)))]
    (integer->integer-bytes (@ h j) 8 #f #f data i))
  (subbytes data 0 digest-byte-count))

(define (blake2b-update! ctx data)
  (match-define (blake2b-context h buf fill t _) ctx)
  (define data-length (bytes-length data))
  (let loop ((i 0))
    (define remaining-data (- data-length i))
    (if (zero? remaining-data)
        (begin (set-blake2b-context-fill! ctx fill)
               (set-blake2b-context-t! ctx t)
               ctx)
        (let ((fill-this-round (min (- 128 fill) remaining-data)))
          (if (= fill-this-round 128)
              (begin
                (set! t (+ t 128))
                (F h (bytes-block->words-block data i) t #f))
              (begin
                (bytes-copy! buf fill data i (+ i fill-this-round))
                (set! fill (+ fill fill-this-round))
                (set! t (+ t fill-this-round))
                (when (= fill 128)
                  (F h (bytes-block->words-block buf 0) t #f)
                  (set! fill 0))))
          (loop (+ i fill-this-round))))))

(define (blake2b-finish! ctx)
  (match-define (blake2b-context h buf fill t digest-byte-count) ctx)
  (for [(i (in-range fill 128))]
    (bytes-set! buf i 0))
  (set! fill #f)
  (F h (bytes-block->words-block buf 0) t #t)
  (words-block->bytes-block h digest-byte-count))

(define (blake2b-hash digest-byte-count data #:key [key #""])
  (define ctx (blake2b-init digest-byte-count key))
  (blake2b-update! ctx data)
  (blake2b-finish! ctx))

(define (blake2b160 data #:key [key #""]) (blake2b-hash 20 data #:key key))
(define (blake2b256 data #:key [key #""]) (blake2b-hash 32 data #:key key))
(define (blake2b384 data #:key [key #""]) (blake2b-hash 48 data #:key key))
(define (blake2b512 data #:key [key #""]) (blake2b-hash 64 data #:key key))

(module+ test
  (require (only-in file/sha1 hex-string->bytes bytes->hex-string))

  ;; From example in appendix A of RFC 7693

  (check-equal? (hex-string->bytes
                 (string-append "BA80A53F981C4D0D6A2797B69F12F6E9"
                                "4C212F14685AC4B74B12BB6FDBFFA2D1"
                                "7D87C5392AAB792DC252D5DE4533CC95"
                                "18D38AA8DBF1925AB92386EDD4009923"))
                (blake2b512 #"abc"))

  ;; From https://en.wikipedia.org/wiki/BLAKE_(hash_function)#BLAKE2_hashes

  (check-equal? (hex-string->bytes
                 (string-append
                  "786A02F742015903C6C6FD852552D272912F4740E15847618A86E217F71F5419"
                  "D25E1031AFEE585313896444934EB04B903A685B1448B755D56F701AFE9BE2CE"))
                (blake2b512 #""))

  (check-equal? (hex-string->bytes
                 (string-append
                  "A8ADD4BDDDFD93E4877D2746E62817B116364A1FA7BC148D95090BC7333B3673"
                  "F82401CF7AA2E4CB1ECD90296E3F14CB5413F8ED77BE73045B13914CDCD6A918"))
                (blake2b512 #"The quick brown fox jumps over the lazy dog"))

  (check-equal? (hex-string->bytes "3345524abf6bbe1809449224b5972c41790b6cf2")
                (blake2b160 #""))

  (check-equal? (hex-string->bytes
                 "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8")
                (blake2b256 #""))

  (check-equal? "abd696bacfad22f35e7e21976f0d6b5033a0409ee97a741ad437c3d2ce55280d"
                (bytes->hex-string
                 (blake2b256
                  #"hypercore"
                  #:key (hex-string->bytes
                         "0961807e4d9bc4dbee2075a0fa78db499ae8a6bc2d613e17c35a7e49721d52e4")))))