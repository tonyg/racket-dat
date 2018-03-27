#lang syndicate

(provide (all-defined-out))

(require racket/set)
(require (only-in file/sha1 bytes->hex-string))
(require (only-in racket/string string-split))

(require bitsyntax)
(require (only-in nat-traversal private-ip-address?))
(require syndicate/drivers/udp)

(define-logger dht/protocol)

;; A NodeName is a Bytes of length 20, sometimes reinterpreted as a
;; big-endian representation of an unsigned number in [0, 2^160).
;;
;; NodeNames identify both nodes in the DHT and resources in the DHT;
;; where possible, I will write ResourceName for the latter usage.

;; An Addr is a (udp-remote-address IPv4String Nat), a description of a
;; UDP endpoint in terms of its IP address and port number. The "host
;; name" part must be in dotted-quad format, because the DHT uses
;; numeric IP addresses consistently, while Racket likes hostnames and
;; doesn't seem to offer a good way to work with the underlying
;; addresses.

;; A Timestamp is a Nat, a count of milliseconds since the system
;; epoch (usually the unix epoch).

;; (local-node NodeName), describes the name of the running local node.
(assertion-struct local-node (name))

;; (known-node NodeName), used to deduplicate node actors, ensuring that
;; no more than one for a single NodeName is running at once.
(assertion-struct known-node (name))

;; (node-coordinates NodeName Addr Timestamp), describes a known and
;; hypothesised-active UDP endpoint for a given NodeName, along with
;; information on when we last heard from this endpoint.
(assertion-struct node-coordinates (name addr timestamp))

;; (node-bucket Nat NodeName), summary of the results of
;; `node-name->bucket` on the `name`. The `bucket` is in [160,0]. An
;; asserted `node-bucket` indicates a hypothesised-active node within
;; the named bucket.
(assertion-struct node-bucket (bucket name))

;; (discovered-node NodeName Addr Boolean), event describing detection
;; of a node and its UDP endpoint. If `known-alive?`, represents
;; certain knowledge of its liveness, by way of the local node
;; receiving a response from the discovered remote node; if not
;; `known-alive?`, then we should operate on the assumption the
;; name-addr mapping is reasonable, but check it soon by pinging the
;; node.
(message-struct discovered-node (name addr known-alive?))

;; (discard-node NodeName), command to discard assumed-good but
;; surplus-to-requirement node mappings in the routing table.
(message-struct discard-node (name))

;; (fresh-transaction Any Nat), an allocated transaction `number` for
;; the (unique) `name`, valid for use so long as interest in this
;; `fresh-transaction` is maintained.
(assertion-struct fresh-transaction (name number))

;; (krpc-packet (U 'inbound 'outbound) Addr Nat 'request (list Bytes BencodeHash))
;; (krpc-packet (U 'inbound 'outbound) Addr Nat 'response BencodeHash)
;; (krpc-packet (U 'inbound 'outbound) Addr Nat 'error (list Nat Bytes))
;;
;; Describe portions of a KRPC interaction. The `id` is a transaction
;; ID; use `fresh-transaction` to allocate these.
;;
(message-struct krpc-packet (direction addr id type body))

;; (krpc-transaction NodeName (U NodeName Addr) Any Bytes BencodeHash BencodeHash)
;; A single KRPC remote procedure call.
(assertion-struct krpc-transaction (source-name target transaction-name method args results))

;; (node-timeout NodeName), sent when a KRPC transaction sent to a
;; NodeName (rather than an Addr) times out.
(message-struct node-timeout (node))

;; (valid-tokens (Listof Bytes)), the currently active get_peers
;; tokens, newest-first.
(assertion-struct valid-tokens (tokens))

;; (received-announcement ParticipantRecord), describes a valid peer announcement.
(message-struct received-announcement (participant))

;;---------------------------------------------------------------------------
;; Client API

;; (locate-node NodeName (Option (Listof (List (Option NodeName) Addr)))),
;; triggers a `find_node` iterative resolution. Results manifest as a
;; `closest-nodes-to` assertion.
(assertion-struct locate-node (name root-nodes/addrs))

;; (closest-nodes-to NodeName (Listof (List (Option NodeName) Addr)) Boolean),
;; partial/ongoing or final results from a `locate-node` request.
(assertion-struct closest-nodes-to (name nodes/addrs final?))

;; (locate-participants ResourceName), triggers a `get_peers` iterative
;; resolution. Results manifest as `participants-in` and
;; `participant-record` assertions.
(assertion-struct locate-participants (resource-name))

;; (participants-in ResourceName (Listof RecordHolder) Boolean),
;; partial/ongoing or final results from a `locate-participants`
;; request.
(assertion-struct participants-in (resource-name record-holders final?))

;; (record-holder ResourceName Addr Bytes Boolean), a single respondent
;; to a `locate-participants` request.
(assertion-struct record-holder (name location token has-records?))

;; (announce-participation ResourceName (Option Nat)), causes the local
;; node to inject a mapping from the given resource name to the local IP
;; address into other nodes in the DHT. The mapping will point back
;; either to the given port (if `port` non-`#f`) or to the DHT port we
;; are using (if `#f`).
(assertion-struct announce-participation (resource-name port))

;; (participant-record ResourceName IPv4String Nat), describes either a
;; discovered answer to an ongoing `get_peers`/`participants-in`
;; query, or a locally-asserted mapping resulting from some peer node
;; sending us an `announce_peer` message.
(assertion-struct participant-record (resource-name host port))

;;---------------------------------------------------------------------------

(define K 8)

(define (bytes-xor a b)
  (define result (make-bytes (min (bytes-length a) (bytes-length b))))
  (for [(i (bytes-length result))]
    (bytes-set! result i (bitwise-xor (bytes-ref a i) (bytes-ref b i))))
  result)

(define ((distance-to-<? reference) a b)
  (cond [(not b) #t] ;; a bootstrap node has no known name, and is "infinitely dispreferred"
        [(not a) #f]
        [else (bytes<? (bytes-xor reference a) (bytes-xor reference b))]))

(define (node-name->number name)
  (bit-string-case name ([(n :: big-endian bytes 20)] n)))

(define (node-name->bucket name local-name)
  (integer-length (node-name->number (bytes-xor name local-name))))

(define (extract-ip/port ip/port)
  (bit-string-case ip/port
    ([a b c d (port :: big-endian bytes 2)]
     (udp-remote-address (format "~a.~a.~a.~a" a b c d) port))
    (else #f)))

(define (extract-addrs nodes)
  (bit-string-case nodes
    ([] '())
    ([(name :: binary bytes 20) a b c d (port :: big-endian bytes 2) (rest :: binary)]
     (cons (list (bit-string->bytes name)
                 (udp-remote-address (format "~a.~a.~a.~a" a b c d) port))
           (extract-addrs rest)))))

(define (dotted-quad->numbers host)
  (define quad (map string->number (string-split host ".")))
  (and (= 4 (length quad)) (andmap byte? quad) quad))

(define (format-ip/port addr)
  (match-define (udp-remote-address host port) addr)
  (match (dotted-quad->numbers host)
    [(list a b c d) (bit-string->bytes (bit-string a b c d (port :: big-endian bytes 2)))]
    [#f #f]))

(define (format-addrs nps)
  (bit-string->bytes
   (for/fold [(acc #"")] [(np nps)]
     (match-define (list name addr) np)
     (match (format-ip/port addr)
       [#f acc]
       [loc (bit-string (acc :: binary) (name :: binary bytes 20) (loc :: binary))]))))

(define (take-at-most n xs) (for/list [(x (in-list xs)) (i (in-range n))] x))

(define (query-all-nodes)
  (set->list (immediate-query [query-set (node-coordinates $i $p _) (list i p)])))

(define (K-closest nodes/addrs target-name #:K [k K] #:key [key car])
  (take-at-most k (sort nodes/addrs #:key key (distance-to-<? target-name))))

(define (do-krpc-transaction source-name target transaction-name method args)
  (react/suspend (k)
    (stop-when (asserted (krpc-transaction source-name target transaction-name
                                           method args $results))
      (k results))))

(define (suggest-node! source name addr known-alive?)
  (match-define (udp-remote-address host port) addr)
  (when (and (not (private-ip-address? host)) (= 20 (bytes-length name)))
    (send! (discovered-node name addr known-alive?))))

(define (format-nodes/addrs ns)
  (for/list [(n ns)] (format "~a(~a)" (~name (car n)) (cdr n))))

(define (next-refresh-time lo-mins hi-mins)
  (+ (current-inexact-milliseconds)
     (* 1000 (+ (* lo-mins 60) (random (* (- hi-mins lo-mins) 60))))))

(define (find-node/suggest txn-name&source local-name respondent target)
  (define results (do-krpc-transaction local-name respondent txn-name&source
                                       #"find_node" (hash #"id" local-name #"target" target)))
  (when (hash? results)
    (for [(p (extract-addrs (hash-ref results #"nodes" #"")))]
      (suggest-node! txn-name&source (car p) (cadr p) #f)))
  results)

(define (~name node-name) (and (bytes? node-name) (bytes->hex-string node-name)))
