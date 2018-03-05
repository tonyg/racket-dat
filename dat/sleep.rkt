#lang racket/base
;; SLEEP - Syncable Ledger of Exact Events Protocol ("SLEEP v2")
;; Appendix to <https://datproject.org/paper> (locally <../doc/dat-paper.pdf>)

(require racket/match)
(require (only-in racket/file file->bytes))

(require bitsyntax)
(require protobuf)

(require "link.rkt")
(require "node-index.rkt")

(module+ test (require rackunit))

;;---------------------------------------------------------------------------
;; A Repository (a "Dat") includes two Registers: `content` and
;; `metadata`. The metadata Register indexes the content.
;;
;; A Register is a "binary append-only stream whose contents are
;; cryptographically hashed and signed".
;;
;; A Register includes "an optional `data` file that stores all chunks
;; of data" across all versions of a store.
;;
;; Files in a Register (SLEEP v2):
;;  - key
;;  - signatures
;;  - bitfield
;;  - true
;;  - data (mandatory for metadata Registers, optional for content Registers)
;;
;; The `signatures`, `bitfield` and `tree` files are structured as a
;; 32 byte header followed by an arbitrary number of fixed-size
;; entries. The 32 byte header is structured as follows:
;;  - 4 byte magic number (big-endian uint32), varies with file.
;;  - 1 byte version number (current version is 0)
;;  - 2 bytes, unsigned big-endian short: size of each fixed-length entry
;;  - 1 byte, length prefix for "body" field
;;  - "body" field, max 24 bytes: string describing key or hash
;;    algorithm, length equal to preceding byte.
;;  - should be padded to 32 bytes with zeroes after that string.
;;
;; The "body" field can be "Ed25519" or "BLAKE2b" in the current
;; implementation.
;;
;; The `key` file stores the public (!) key that is "described by the
;; `signatures` file". The `data` file stores raw chunk data, and
;; `tree` indexes and hashes `data`.
;;
;; `key` file:
;;   - public key used to verify signatures in `signatures`.
;;   - a single chunk written to disk.
;;   - format described in the `signatures` header.
;;   - always "Ed25519" at in the current implementation.
;;
;; `tree` file:
;;   - serialized Merkle tree based on `data`.
;;   - entry size 40 bytes
;;   - "BLAKE2b" "body" field in the header.
;;   - 4 byte header magic string: 0x05025702
;;   - each entry:
;;      - 32 bytes, BLAKE2b hash
;;      - 8 bytes, unsigned 64-bit big-endian integer: "children leaf byte length",
;;        i.e. the total length of data in all leaf nodes reachable from here.
;;   - even entries are leaves; odd are intermediate nodes.
;;   - node hash calculation:
;;      - LEAF nodes:
;;          BLAKE2b([0:byte, entrylength:uint64be, entrydata:entrylength])
;;      - PARENT nodes (i.e. intermediate but non-root nodes):
;;          BLAKE2b([1:byte, (leftchildlen+rightchildlen):uint64be,
;;                   leftchildhash:32bytes, rightchildhash:32bytes])
;;        where leftchildlen and rightchildlen are the "children leaf
;;        byte length" from those child nodes, respectively, and
;;        leftchildhash and rightchildhash are the BLAKE2b hashes from
;;        those child nodes, respectively.
;;      - ROOT nodes don't live here, they go in `signatures`.
;;
;; `data` file: raw data. Indexed by `tree`.
;;
;; `signatures` file:
;;   - entry size 64 bytes
;;   - "Ed25519" "body" field in the header.
;;   - 4 byte header magic string: 0x05025701
;;   - each entry:
;;      - Ed25519_sign(BLAKE2b([2:byte] ++ [root.hash:32byte,
;;                                          root.treeindex:uint64be,
;;                                          root.childbytelength:uint64be
;;                                          FOR root IN roots]))
;;
;; `bitfield` field:
;;   - indexes `tree` and `data` to figure out which bits are held
;;     locally and which might need downloading from elsewhere.
;;   - not essential: can be regenerated from `tree` and `data`
;;   - entry size 3328 bytes (!) = 2048 + 1024 + 256
;;   - "" (empty string) "body" field in the header.
;;   - 4 byte header magic string: 0x05025700
;;   - each entry:
;;      - 1024 byte "data" bitfield, 1 bit per "data entry" you have
;;      - 2048 byte "tree" bitfield, 1 bit per "tree entry" you have
;;      - 256 byte "bitfield index", a compressed overview of the others
;;        (see p5 of the SLEEP v2 spec tacked on to the Dat paper)
;;
;; `metadata.data`:
;;   - contains snapshots of the (metadata of the) filesystem represented in this repository
;;   - append-only log of entries
;;   - format: protobufs.
;;      - one "Header" message, followed by any number of "Node"
;;        messages (NB different to the "Node" used in the wire
;;        protocol).

(struct repository (metadata content) #:transparent)
(struct register (tree data-input data-output key) #:transparent)
(struct sleep-file (input-port output-port magic entry-size algorithm) #:transparent)

(define BITFIELD_MAGIC #x05025700)
(define SIGNATURES_MAGIC #x05025701)
(define TREE_MAGIC #x05025702)

(struct tree-node (hash length) #:prefab)

(define (open-sleep-file input-port output-port)
  (file-position input-port 0)
  (define header (read-bytes 32 input-port))
  (bit-string-case header
    ([(magic :: big-endian bytes 4)
      (= 0)
      (entry-size :: big-endian bytes 2)
      algorithm-length
      (algorithm :: binary bytes algorithm-length)
      (_ :: bytes (- 24 algorithm-length))]
     (sleep-file input-port
                 output-port
                 magic
                 entry-size
                 (bytes->string/utf-8 (bit-string->bytes algorithm))))))

(define (sleep-file-read-raw-entry sf n)
  (define entry-size (sleep-file-entry-size sf))
  (define port (sleep-file-input-port sf))
  (file-position port (+ 32 (* n entry-size)))
  (read-bytes entry-size port))

(define (sleep-file-write-raw-entry! sf e)
  (define entry-size (sleep-file-entry-size sf))
  (define port (sleep-file-output-port sf))
  (file-position port eof)
  (define p (file-position port))
  (write-bytes e port 0 entry-size)
  (/ (- p 32) entry-size))

(define (sleep-file-entry-count sf)
  (define port (sleep-file-input-port sf))
  (file-position port eof)
  (/ (- (file-position port) 32) (sleep-file-entry-size sf)))

(define (check-magic! who sf magic)
  (when (not (= (sleep-file-magic sf) magic))
    (error who "Unexpected magic number in ~a: wanted ~a, got ~a" who magic (sleep-file-magic sf))))

(define (read-tree-node sf n)
  (check-magic! 'tree-sleep-file-entry sf TREE_MAGIC)
  (match (sleep-file-read-raw-entry sf n)
    [(? eof-object?) #f]
    [bs
     (bit-string-case bs
       ([(hash :: binary bytes 32) (length :: big-endian bytes 8)]
        (tree-node hash length)))]))

(define (read-tree-node-length sf n)
  (define tn (read-tree-node sf n))
  (and tn (tree-node-length tn)))

(define (chunk-location sf chunk-index)
  (define n (* chunk-index 2))
  (define chunk-length (read-tree-node-length sf n))
  (if (not chunk-length)
      (values #f #f)
      (values (foldl + 0 (map (lambda (i) (read-tree-node-length sf i))
                              (node-index-roots n)))
              chunk-length)))

(define (open-register path)
  (define (p suffix) (string-append (path->string path) suffix))
  (define (i suffix) (open-input-file (p suffix)))
  (define (o suffix) (open-output-file (p suffix) #:exists 'append))
  (and (and (file-exists? (p ".tree"))
            (file-exists? (p ".key")))
       (register (open-sleep-file (i ".tree") (o ".tree"))
                 (and (file-exists? (p ".data")) (i ".data"))
                 (and (file-exists? (p ".data")) (o ".data"))
                 (file->bytes (p ".key")))))

(define (read-register-chunk r chunk-index)
  (define port (register-data-input r))
  (and port
       (let-values (((offset length) (chunk-location (register-tree r) chunk-index)))
         (and offset
              (begin (file-position port offset)
                     (read-bytes length port))))))

(define (open-repository path)
  (define dat-dir (build-path path ".dat"))
  (and (directory-exists? dat-dir)
       (let ((metadata (open-register (build-path dat-dir "metadata")))
             (content (open-register (build-path dat-dir "content"))))
         (and metadata
              content
              (let ((r (repository metadata content)))
                (bit-string-case (read-register-chunk metadata 0)
                  ([(header :: (message MetadataHeader))]
                   (and (equal? "hyperdrive" (MetadataHeader-type header))
                        r))
                  (else #f)))))))

(define (repository-link r)
  (dat-link (register-key (repository-metadata r))))

(define-protobuf-message MetadataHeader
  required string type = 1
  optional bytes content = 2)

(define-protobuf-message MetadataNode
  required string path = 1
  optional MetadataStat value = 2
  optional bytes trie = 3 ;; custom encoding, see below
  repeated MetadataWriter writers = 4
  optional uint64 writersSequence = 5)

;; The "trie" field holds ...

(define-protobuf-message MetadataWriter
  required bytes publicKey = 1
  optional string permission = 2)

(define-protobuf-message MetadataStat
  required uint32 mode = 1
  optional uint32 uid = 2
  optional uint32 gid = 3
  optional uint64 size = 4
  optional uint64 blocks = 5
  optional uint64 offset = 6
  optional uint64 byteOffset = 7
  optional uint64 mtime = 8
  optional uint64 ctime = 9)

