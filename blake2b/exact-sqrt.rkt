#lang racket/base
;; Approximate square roots to specified precision.
;; Babylonian method https://en.wikipedia.org/wiki/Methods_of_computing_square_roots

(provide exact-sqrt/precision)

(module+ test (require rackunit))

(define (exact-sqrt/precision S precision)
  (let loop ((old-guess S))
    (define new-guess (/ (+ old-guess (/ S old-guess)) 2))
    (if (< (abs (- new-guess old-guess)) precision)
        new-guess
        (loop new-guess))))

(module+ test
  (define (check-correct S precision)
    (define x (exact-sqrt/precision S precision))
    (check-true (< (- S (* x x)) (* precision precision))
                (format "x^2 = ~a, S = ~a, precision = ~a"
                        (exact->inexact (* x x))
                        S
                        precision)))

  (for* [(S (list 4 2 3 5 100))
         (precision (list 0.1 0.01 0.001 0.000001))]
    (check-correct S precision)))
