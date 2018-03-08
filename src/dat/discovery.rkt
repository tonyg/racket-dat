#lang racket/base

(provide discovery-key)

(require blake2b)

(module+ test (require rackunit))

(define (discovery-key dat-public-key)
  (blake2b256 #"hypercore" #:key dat-public-key))

(module+ test
  (require (only-in file/sha1 hex-string->bytes bytes->hex-string))
  (check-equal? "abd696bacfad22f35e7e21976f0d6b5033a0409ee97a741ad437c3d2ce55280d"
                (bytes->hex-string
                 (discovery-key
                  (hex-string->bytes
                   "0961807e4d9bc4dbee2075a0fa78db499ae8a6bc2d613e17c35a7e49721d52e4")))))
