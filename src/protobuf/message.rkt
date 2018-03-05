#lang racket/base
;; Just enough Protocol Buffers to get by
;; https://developers.google.com/protocol-buffers/docs/encoding

(provide (struct-out field-type)
         (struct-out message-type)
         gen:protobuf-message
         register-message-type!
         lookup-message-type
         message-type-field-sequence
         message->
         ->message
         message
         field->
         ->field
         default-for-field-type
         define-protobuf-message)

(require racket/generic)
(require racket/match)
(require (prefix-in wire: "wire.rkt"))
(require bitsyntax)

(require (for-syntax racket))
(require (for-syntax (only-in racket/syntax format-id)))

(module+ test (require rackunit))

(struct field-type (name index slot-index accessor type-id repetition default) #:prefab)
(struct message-type (name constructor field-types) #:prefab)

(define-generics protobuf-message
  (protobuf-message-type-descriptor protobuf-message))

(define *registry* (make-hash))

(define (register-message-type! mt)
  (when (hash-has-key? *registry* (message-type-name mt))
    (error 'register-message-type! "Duplicate message type: ~a" (message-type-name mt)))
  (hash-set! *registry* (message-type-name mt) mt))

(define (lookup-message-type type-name)
  (hash-ref *registry*
            type-name
            (lambda () (error 'lookup-message-type
                              "No such protobuf message type: ~a"
                              type-name))))

(define (message-type-field-sequence mt)
  (sort (hash-values (message-type-field-types mt)) < #:key field-type-slot-index))

(define (message-> mt input ks kf)
  (define field-sequence (message-type-field-sequence mt))
  (define accumulator (list->vector (map field-type-default field-sequence)))
  (let loop ((input input))
    (bit-string-case input
      ([(f :: (wire:field)) (rest :: binary)]
       (match f
         [(wire:protobuf-field index wire-type body)
          (match (hash-ref (message-type-field-types mt) index #f)
            [#f (loop rest)]
            [(field-type _name _index slot-index _accessor type-id repetition _default)
             ;; TODO: merging of repeated fields. Replacement for simple types,
             ;; otherwise fieldwise merge.
             (field-> type-id body
                      (lambda (v)
                        (match repetition
                          [(or 'required 'optional)
                           (vector-set! accumulator slot-index v)]
                          ['repeated
                           (vector-set! accumulator slot-index
                                        (cons v (vector-ref accumulator slot-index)))])
                        (loop rest))
                      kf)])]))
      ([]
       (for [(f field-sequence)]
         (when (eq? (field-type-repetition f) 'repeated)
           (define i (field-type-slot-index f))
           (vector-set! accumulator i (reverse (vector-ref accumulator i)))))
       (ks (apply (message-type-constructor mt) (vector->list accumulator)) #"")))))

(define (->message m)
  (define mt (protobuf-message-type-descriptor m))
  (for/fold [(acc #"")]
            [(f (message-type-field-sequence mt))]
    (match-define (field-type name index _slot-index accessor type-id repetition default) f)
    (define v (accessor m))
    (cond
      [(and (or (eq? repetition 'optional)
                (eq? repetition 'repeated))
            (equal? default v))
       acc]
      [(eq? repetition 'repeated)
       (for/fold [(acc acc)]
                 [(v v)]
         (bit-string-append acc
                            (bit-string [(->field mt name index type-id v) :: (wire:field)])))]
      [else
       (bit-string-append acc
                          (bit-string [(->field mt name index type-id v) :: (wire:field)]))])))

(define-syntax message
  (syntax-rules ()
    [(_ #t input ks kf mt-name)
     (message-> (lookup-message-type 'mt-name) input ks kf)]
    [(_ #f m)
     (->message m)]))

(define (field-> type-id body ks kf)
  (match type-id
    ['bytes (ks (bit-string->bytes body))]
    ['string (ks (bytes->string/utf-8 (bit-string->bytes body)))]
    ['bool (ks (positive? body))]
    [(or 'uint64 'uint32) (ks body)]
    [(or 'sint64 'sint32) (ks (wire:zig-zag-> body))]
    ['int64 (ks (integer-bytes->integer (integer->integer-bytes body 8 #f) #t))]
    ['int32 (ks (integer-bytes->integer (integer->integer-bytes body 4 #f) #t))]
    [other (message-> (lookup-message-type other) body (lambda (v _rest) (ks v)) kf)]))

(define (->field mt name index type-id v)
  (define-values (wire-type encoded)
    (match type-id
      ['bytes (values 'length-delimited v)]
      ['string (values 'length-delimited (string->bytes/utf-8 v))]
      ['bool (values 'varint (if v 1 0))]
      [(or 'uint64 'uint32) (values 'varint v)]
      [(or 'sint64 'sint32) (values 'varint (wire:->zig-zag v))]
      ['int64 (values 'varint (integer-bytes->integer (integer->integer-bytes v 8 #t) #f))]
      ['int32 (values 'varint (integer-bytes->integer (integer->integer-bytes v 4 #t) #f))]
      [other
       (when (not (eq? (message-type-name (protobuf-message-type-descriptor v)) other))
         (error '->field
                "Expected message of type ~a for field ~a of ~a; got ~v"
                other
                name
                (message-type-name mt)
                v))
       (values 'length-delimited (->message v))]))
  (wire:protobuf-field index wire-type encoded))

(define (default-for-field-type type-id)
  (match type-id
    ['bytes #""]
    ['string ""]
    ['bool #f]
    [(or 'uint64 'uint32 'sint64 'sint32 'int64 'int32) 0]
    [other #f]))

(define-syntax (define-protobuf-message stx)
  (syntax-case stx ()
    [(_ message-type-name fieldspecs ...)
     (let ()
       (define fields
         (let loop ((fields-rev '()) (slot-index 0) (stx #'(fieldspecs ...)))
           (syntax-case stx (=)
             [(repetition type-id-stx name = index more ...)
              (let ()
                (define-values (value-stx more-stx)
                  (syntax-case #'(repetition more ...) (default =)
                    [(_ [default = value] more ...) (values #'value #'(more ...))]
                    [(repeated _ ...) (values #''() #'(more ...))]
                    [_ (values #'(default-for-field-type 'type-id-stx) #'(more ...))]))
                (define accessor-stx
                  (format-id #'name "~a-~a" #'message-type-name #'name))
                (loop (cons (list #'name
                                  #'index
                                  #`(field-type 'name
                                                index
                                                #,slot-index
                                                #,accessor-stx
                                                'type-id-stx
                                                'repetition
                                                #,value-stx))
                            fields-rev)
                      (+ slot-index 1)
                      more-stx))]
             [()
              (reverse fields-rev)])))
       (define mtdesc-name-stx (format-id #'message-type-name "mt:~a" #'message-type-name))
       #`(begin
           (define (#,mtdesc-name-stx)
             (message-type 'message-type-name
                           message-type-name
                           (make-immutable-hash
                            (list #,@(map (lambda (f) #`(cons #,(cadr f) #,(caddr f)))
                                          fields)))))
           (struct message-type-name (#,@(map car fields)) #:transparent
             #:methods gen:protobuf-message
             [(define (protobuf-message-type-descriptor m) (#,mtdesc-name-stx))])
           (register-message-type! (#,mtdesc-name-stx))))]))

(module+ test
  (define-protobuf-message PBTestData
    required uint64 index = 1
    optional bytes value = 2
    repeated PBTestNode nodes = 3
    optional bytes signature = 4)

  (define-protobuf-message PBTestNode
    required uint64 index = 1
    required bytes hash = 2
    required uint64 size = 3)

  (check-equal?
   (bytes-append #"\x08\xe7\x07" ;; field 1, varint, 999
                 #"\x12\x05value" ;; field 2, length-delimited
                 #"\x1a\x0a\x08\x42\x12\x04\x68\x61\x73\x68\x18\x37" ;; field 3, length-delim
                 #"\x1a\x0a\x08\x42\x12\x04\x68\x61\x73\x68\x18\x37" ;; field 3, length-delim
                 #"\x1a\x0a\x08\x42\x12\x04\x68\x61\x73\x68\x18\x37" ;; field 3, length-delim
                 #"\x22\x09signature" ;; field 4, length-delimited
                 )
   (bit-string->bytes
    (bit-string
     [(PBTestData 999
                  #"value"
                  (list (PBTestNode 66 #"hash" 55)
                        (PBTestNode 66 #"hash" 55)
                        (PBTestNode 66 #"hash" 55))
                  #"signature") :: (message)])))

  (check-equal?
   (PBTestData 999
               #"value"
               (list (PBTestNode 66 #"hash" 55)
                     (PBTestNode 66 #"hash" 55)
                     (PBTestNode 66 #"hash" 55))
               #"signature")
   (bit-string-case
       (bytes-append #"\x08\xe7\x07" ;; field 1, varint, 999
                     #"\x12\x05value" ;; field 2, length-delimited
                     #"\x1a\x0a\x08\x42\x12\x04\x68\x61\x73\x68\x18\x37" ;; field 3, length-delim
                     #"\x1a\x0a\x08\x42\x12\x04\x68\x61\x73\x68\x18\x37" ;; field 3, length-delim
                     #"\x1a\x0a\x08\x42\x12\x04\x68\x61\x73\x68\x18\x37" ;; field 3, length-delim
                     #"\x22\x09signature" ;; field 4, length-delimited
                     )
     ([(x :: (message PBTestData))] x))))
