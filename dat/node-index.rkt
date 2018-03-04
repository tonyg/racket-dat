#lang racket/base
;; Merkle trees - binary in-order interval numbering ("bin numbering")
;; After https://github.com/mafintosh/flat-tree/blob/master/index.js

(provide make-node-index
         node-index-depth
         node-index-offset
         node-index-parent
         node-index-sibling
         node-index-children
         node-index-roots
         node-index-span)

(module+ test (require rackunit))

;; Nodes in a tree are numbered in a particular pattern:
;;
;;                                                      15     ...
;;                              07                             ...
;;                  03                      11                 ...
;;            01          05          09          13           ...
;;         00    02    04    06    08    10    12    14    16  ...
;;
;; depth    0  1  0  2  0  1  0  3  0  1  0  2  0  1  0  4  0  ...
;; offset   0  0  1  0  2  1  3  0  4  2  5  1  6  3  7  0  8  ...
;; parent   1  3  1  7  5  3  5 15  9 11  9  7 13 11 13 31 17  ...
;; sibling  2  5  0 11  6  1  4 23 10 13  8  3 14  9 12 47 18  ...
;;
;; Chunks of files always live in the even-numbered nodes, at the
;; leaves.
;;
;; Content of an even-numbered node = list of hashes of data chunks.
;; Content of an odd-numbered node = list of hashes of child nodes.
;;
;; Because an odd-numbered node refers to the hash of nodes *ahead* of
;; it in the stream, it may not be possible to compute its contents
;; until the contents of its future child are completely determined.
;; For this reason, a tree snapshot is described by ONE OR MORE root
;; nodes. See "Replication Example" on p6 of the Dat paper.

(define (make-node-index depth offset)
  (- (* (+ (* 2 offset) 1)
        (arithmetic-shift 1 depth))
     1))

(define (node-index-depth index)
  (let loop ((depth 0) (index (+ index 1)))
    (if (bitwise-bit-set? index 0)
        depth
        (loop (+ depth 1) (arithmetic-shift index -1)))))

(module+ test
  (for [(i (in-naturals)) (d (in-list '(0 1 0 2 0 1 0 3 0 1 0 2 0 1 0 4 0)))]
    (check-equal? d (node-index-depth i))))

(define (node-index-offset index #:depth [depth (node-index-depth index)])
  (/ (- (/ (+ index 1) (arithmetic-shift 1 depth)) 1) 2))

(module+ test
  (for [(i (in-naturals)) (o (in-list '(0 0 1 0 2 1 3 0 4 2 5 1 6 3 7 0 8)))]
    (check-equal? o (node-index-offset i) (format "offset ~a != ~a" i o))))

(define (node-index-parent index #:depth [depth (node-index-depth index)])
  (make-node-index (+ depth 1)
                   (arithmetic-shift (node-index-offset index #:depth depth) -1)))

(module+ test
  (for [(i (in-naturals)) (p (in-list '(1 3 1 7 5 3 5 15 9 11 9 7 13 11 13 31)))]
    (check-equal? p (node-index-parent i) (format "parent ~a != ~a" i p))))

(define (node-index-sibling index #:depth [depth (node-index-depth index)])
  (define o (node-index-offset index #:depth depth))
  (make-node-index depth (if (even? o) (+ o 1) (- o 1))))

(module+ test
  (for [(i (in-naturals)) (s (in-list '(2 5 0 11 6 1 4 23 10 13 8 3 14 9 12 47)))]
    (check-equal? s (node-index-sibling i) (format "sibling ~a != ~a" i s))))

(define (node-index-children index #:depth [depth (node-index-depth index)])
  (if (zero? depth)
      (values #f #f)
      (let ((o (* (node-index-offset index #:depth depth) 2)))
        (values (make-node-index (- depth 1) o)
                (make-node-index (- depth 1) (+ o 1))))))

(module+ test
  (for [(i (in-range 0 16 2))]
    (define-values (L R) (node-index-children i))
    (check-false L)
    (check-false R))
  (for [(i (in-range 1 16 2))]
    (define-values (L R) (node-index-children i))
    (check-equal? i (node-index-parent L))
    (check-equal? i (node-index-parent R))
    (check-equal? R (node-index-sibling L))
    (check-equal? L (node-index-sibling R))))

(define (power-of-two-<= target)
  (let loop ((curr 1))
    (define next (* curr 2))
    (if (> next target) curr (loop next))))

(define (node-index-roots index)
  (when (odd? index)
    (error 'node-index-roots "Can only compute roots for leaf index"))
  (let loop ((acc '())
             (index (arithmetic-shift index -1))
             (offset 0))
    (if (zero? index)
        (reverse acc)
        (let ((factor (power-of-two-<= index)))
          (loop (cons (+ offset factor -1) acc)
                (- index factor)
                (+ offset (* 2 factor)))))))

(module+ test
  (check-equal? '() (node-index-roots 0))
  (check-equal? '(0) (node-index-roots 2))
  (check-equal? '(1) (node-index-roots 4))
  (check-equal? '(1 4) (node-index-roots 6))
  (check-equal? '(3) (node-index-roots 8))
  (check-equal? '(3 8) (node-index-roots 10))
  (check-equal? '(7) (node-index-roots 16))
  (check-equal? '(7 16) (node-index-roots 18))
  (check-equal? '(7 17) (node-index-roots 20)))

(define (node-index-span index #:depth [depth (node-index-depth index)])
  (define o (node-index-offset index #:depth depth))
  (define w (arithmetic-shift 1 (+ depth 1)))
  (define left (* o w))
  (values left (+ left w -1)))

(module+ test
  (define (span i)
    (define-values (L R) (node-index-span i))
    (list L R))
  (check-equal? '(0 1) (span 0))
  (check-equal? '(0 3) (span 1))
  (check-equal? '(2 3) (span 2))
  (check-equal? '(0 7) (span 3))
  (check-equal? '(4 5) (span 4))
  (check-equal? '(4 7) (span 5))
  (check-equal? '(6 7) (span 6))
  (check-equal? '(0 15) (span 7)))
