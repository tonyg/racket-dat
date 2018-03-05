#lang racket

(module+ main
  (require "../src/blake2b/main.rkt")

  (define data
    (call-with-input-file "/dev/urandom"
      (lambda (p)
        (read-bytes (* 2 1024 1024) p))))

  (let ((byte-count (bytes-length data)))
    (let-values (((_results cputime realtime gctime) (time-apply blake2b512 (list data))))
      (printf "~a kilobytes per second\n" (/ (/ byte-count 1024) (/ cputime 1000.0))))))
