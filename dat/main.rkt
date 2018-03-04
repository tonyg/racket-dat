#lang racket/base

(require racket/match)

(require bitsyntax)
(require racl)
(require (only-in file/sha1 hex-string->bytes))
(require protobuf)

(module+ test (require rackunit))

(require "link.rkt")

;;---------------------------------------------------------------------------
;; Abstract source discovery.
;;
;; Operations:
;;  - join(key, [port]) - look for (and optionally advertise) key.
;;  - leave(key, [port]) - remove subscription to (and optionally advertisement for) key.
;;  - foundpeer(key, ip, port) - callback describing a lookup result.
;;
;; The official implementation includes
;;  - DNS as a source
;;  - mDNS as a source
;;  - Kademlia mainline DHT as a source
;;
;; plus an HTTPS link can be used to point directly to a source.

;;---------------------------------------------------------------------------
;; Wire protocol
;;
;; Uses Protocol Buffers.

;; A dat-message is encoded as:
;;   varint - length of rest of message
;;    - varint - header = channel << 4 | type
;;    - body
;; where channel is 0 for metadata and 1 for content,
;; and type is a 4-bit number as follows.
(struct dat-message (channel type body) #:prefab)

(define CH_METADATA 0)
(define CH_CONTENT 1)

(define-syntax-rule (define-dat-message TYPE number protobuf-pieces ...)
  (begin (define TYPE number)
         (define-protobuf-message protobuf-pieces ...)))

(define-dat-message TYPE_FEED 0 Feed
  required bytes discoveryKey = 1
  optional bytes nonce = 2)

(define-dat-message TYPE_HANDSHAKE 1 Handshake
  optional bytes id = 1
  optional bool live = 2
  optional bytes userData = 3
  repeated string extensions = 4)

(define-dat-message TYPE_INFO 2 Info
  optional bool uploading = 1
  optional bool downloading = 2)

(define-dat-message TYPE_HAVE 3 Have
  required uint64 start = 1
  optional uint64 length = 2 [default = 1]
  optional bytes bitfield = 3)

;; Bitfields are encoded as a byte-vector that is a sequence of binary
;; items. Each item is either:
;;  - a run-length compressed sequence of bits,
;;    varint(byte-length-of-sequence << 2 | bit << 1 | 1), or
;;  - an uncompressed sequence of bits,
;;    varint(byte-length-of-sequence << 1 | 0) ++ bitfield-bytes

(define-dat-message TYPE_UNHAVE 4 Unhave
  required uint64 start = 1
  optional uint64 length = 2 [default = 1])

(define-dat-message TYPE_WANT 5 Want
  required uint64 start = 1
  optional uint64 length = 2 ;; default is "infinity" or "feed.length" if not a live feed
  )

(define-dat-message TYPE_UNWANT 6 Unwant
  required uint64 start = 1
  optional uint64 length = 2 ;; default is "infinity" or "feed.length" if not a live feed
  )

(define-dat-message TYPE_REQUEST 7 Request
  required uint64 index = 1
  optional uint64 bytes = 2
  optional bool hash = 3
  optional uint64 nodes = 4 ;; complicated encoding; low bit is "signature needed"
  )

;; Q. How exactly is `nodes` used?

(define-dat-message TYPE_CANCEL 8 Cancel
  required uint64 index = 1
  optional uint64 bytes = 2
  optional bool hash = 3)

;; From TYPE_CANCEL we deduce that equivalence over TYPE_REQUEST is on
;; (index,bytes,hash) triples.

(define-dat-message TYPE_DATA 9 Data
  required uint64 index = 1
  optional bytes value = 2
  repeated Node nodes = 3
  optional bytes signature = 4)

(define-protobuf-message Node
  required uint64 index = 1
  required bytes hash = 2
  required uint64 size = 3)

;;---------------------------------------------------------------------------
;; Phases.
;;
;;  - Discovery: find sources.
;;  - Contact: Dat is transport neutral but includes TCP, HTTP and UTP in the reference impl
;;  - Hypercore protocol: "message-based replication" over a stateless channel to exch data
