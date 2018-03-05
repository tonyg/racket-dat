#lang racket/base
;; A Dat link is a 32-byte Ed25519 public signing key.

(provide (struct-out dat-link)
         string->dat-link
         dat-link->string)

(require racket/match)
(require (only-in file/sha1 hex-string->bytes bytes->hex-string))

(module+ test (require rackunit))

;;---------------------------------------------------------------------------

(struct dat-link (public-key-bytes) #:prefab)

(define (string->dat-link s)
  (match s
    [(regexp #px"(?i:[0-9a-f]{64})" (list kh)) (dat-link (hex-string->bytes kh))]
    [(regexp #px"(?i:dat://([0-9a-f]{64}))" (list _ kh)) (dat-link (hex-string->bytes kh))]
    [(regexp #px"(?i:https?://datproject.org/([0-9a-f]{64}))" (list _ kh))
     (dat-link (hex-string->bytes kh))]
    [_ #f]))

(define (dat-link->string d)
  (format "dat://~a" (bytes->hex-string (dat-link-public-key-bytes d))))

(module+ test
  (define H "de1042928b74e9f96cf3f3e290c16cb4eba9c696e9a1e15c7f4d0514ddce1154")
  (define B (hex-string->bytes H))
  (define D (dat-link B))
  (check-equal? D (string->dat-link H))
  (check-equal? D (string->dat-link (string-append "dat://"H)))
  (check-equal? D (string->dat-link (string-append "DAT://"H)))
  (check-equal? D (string->dat-link (string-append "http://dat-project.org/"H)))
  (check-equal? D (string->dat-link (string-append "https://dat-project.org/"H)))
  (check-equal? D (string->dat-link (string-append "Https://Dat-Project.Org/"H)))
  (check-equal? D (string->dat-link (string-append "HTTPS://DAT-PROJECT.ORG/"(string-upcase H))))
  (check-equal? #f (string->dat-link "Hello, world!"))
  (check-equal? #f (string->dat-link "de1042928b74e9f96cf3f3e290c16cb4eba9c696e9a1e15c7f4d0514ddce115z")))
