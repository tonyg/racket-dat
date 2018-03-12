#lang syndicate

(provide (all-defined-out))

(require racket/set)
(require (only-in file/sha1 bytes->hex-string))
(require (only-in racket/string string-split))

(require bitsyntax)
(require (only-in nat-traversal private-ip-address?))
(require syndicate/drivers/udp)

(define-logger dht/protocol)

;; A NodeID is a Bytes of length 20, sometimes reinterpreted as a
;; big-endian representation of an unsigned number in [0, 2^160).
;;
;; NodeIDs identify both nodes in the DHT and resources in the DHT;
;; where possible, I will write ResourceID for the latter usage.

;; A Peer is a (udp-remote-address IPv4String Nat), a description of a
;; UDP endpoint in terms of its IP address and port number. The "host
;; name" part must be in dotted-quad format, because the DHT uses
;; numeric IP addresses consistently, while Racket likes hostnames and
;; doesn't seem to offer a good way to work with the underlying
;; addresses.

;; A Timestamp is a Nat, a count of milliseconds since the system
;; epoch (usually the unix epoch).

;; (local-node NodeID), describes the name of the running local node.
(assertion-struct local-node (id))

;; (known-node NodeID), used to deduplicate node actors, ensuring that
;; no more than one for a single NodeID is running at once.
(assertion-struct known-node (id))

;; (node-coordinates NodeID Peer Timestamp), describes a known and
;; hypothesised-active UDP endpoint for a given NodeID, along with
;; information on when we last heard from this endpoint.
(assertion-struct node-coordinates (id peer timestamp))

;; (node-bucket Nat NodeID), summary of the results of
;; `node-id->bucket` on the `id`. The `bucket` is in [160,0]. An
;; asserted `node-bucket` indicates a hypothesised-active node within
;; the named bucket.
(assertion-struct node-bucket (bucket id))

;; (discovered-node NodeID Peer Boolean), event describing detection
;; of a node and its UDP endpoint. If `known-alive?`, represents
;; certain knowledge of its liveness, by way of the local node
;; receiving a response from the discovered remote node; if not
;; `known-alive?`, then we should operate on the assumption the
;; id-peer mapping is reasonable, but check it soon by pinging the
;; node.
(message-struct discovered-node (id peer known-alive?))

;; (discard-node NodeID), command to discard assumed-good but
;; surplus-to-requirement node mappings in the routing table.
(message-struct discard-node (id))

;; (fresh-transaction Any Nat), an allocated transaction `number` for
;; the (unique) `name`, valid for use so long as interest in this
;; `fresh-transaction` is maintained.
(assertion-struct fresh-transaction (name number))

;; (krpc-packet (U 'inbound 'outbound) Peer Nat 'request (list Bytes BencodeHash))
;; (krpc-packet (U 'inbound 'outbound) Peer Nat 'response BencodeHash)
;; (krpc-packet (U 'inbound 'outbound) Peer Nat 'error (list Nat Bytes))
;;
;; Describe portions of a KRPC interaction. The `id` is a transaction
;; ID; use `fresh-transaction` to allocate these.
;;
(message-struct krpc-packet (direction peer id type body))

;; (krpc-transaction NodeID (U NodeID Peer) Any Bytes BencodeHash BencodeHash)
;; A single KRPC remote procedure call.
(assertion-struct krpc-transaction (source-id target transaction-name method args results))

;; (node-timeout NodeID), sent when a KRPC transaction sent to a
;; NodeID (rather than a Peer) times out.
(message-struct node-timeout (node))

;; (valid-tokens (Listof Bytes)), the currently active get_peers
;; tokens, newest-first.
(assertion-struct valid-tokens (tokens))

;; (received-announcement ParticipantRecord), describes a valid peer announcement.
(message-struct received-announcement (participant))

;;---------------------------------------------------------------------------
;; Client API

;; (locate-node NodeID (Option (Listof (List (Option NodeID) Peer)))),
;; triggers a `find_node` iterative resolution. Results manifest as a
;; `closest-nodes-to` assertion.
(assertion-struct locate-node (id root-nodes/peers))

;; (closest-nodes-to NodeID (Listof (List (Option NodeID) Peer)) Boolean),
;; partial/ongoing or final results from a `locate-node` request.
(assertion-struct closest-nodes-to (id nodes/peers final?))

;; (locate-participants ResourceID), triggers a `get_peers` iterative
;; resolution. Results manifest as `participants-in` and
;; `participant-record` assertions.
(assertion-struct locate-participants (resource-id))

;; (participants-in ResourceID (Listof RecordHolder) Boolean),
;; partial/ongoing or final results from a `locate-participants`
;; request.
(assertion-struct participants-in (resource-id record-holders final?))

;; (record-holder ResourceID Peer Bytes Boolean), a single respondent
;; to a `locate-participants` request.
(assertion-struct record-holder (id location token has-records?))

;; (announce-participation ResourceID (Option Nat)), causes the local
;; node to inject a mapping from the given resource ID to the local IP
;; address into other nodes in the DHT. The mapping will point back
;; either to the given port (if `port` non-`#f`) or to the DHT port we
;; are using (if `#f`).
(assertion-struct announce-participation (resource-id port))

;; (participant-record ResourceID IPv4String Nat), describes either a
;; discovered answer to an ongoing `get_peers`/`participants-in`
;; query, or a locally-asserted mapping resulting from some peer node
;; sending us an `announce_peer` message.
(assertion-struct participant-record (resource-id host port))

;;---------------------------------------------------------------------------

(define K 8)

(define (bytes-xor a b)
  (define result (make-bytes (min (bytes-length a) (bytes-length b))))
  (for [(i (bytes-length result))]
    (bytes-set! result i (bitwise-xor (bytes-ref a i) (bytes-ref b i))))
  result)

(define ((distance-to-<? reference) a b)
  (cond [(not b) #t] ;; a bootstrap node has no known ID, and is "infinitely dispreferred"
        [(not a) #f]
        [else (bytes<? (bytes-xor reference a) (bytes-xor reference b))]))

(define (node-id->number id) (bit-string-case id ([(n :: big-endian bytes 20)] n)))
(define (node-id->bucket id local-id) (integer-length (node-id->number (bytes-xor id local-id))))

(define (extract-ip/port ip/port)
  (bit-string-case ip/port
    ([a b c d (port :: big-endian bytes 2)]
     (udp-remote-address (format "~a.~a.~a.~a" a b c d) port))
    (else #f)))

(define (extract-peers nodes)
  (bit-string-case nodes
    ([] '())
    ([(id :: binary bytes 20) a b c d (port :: big-endian bytes 2) (rest :: binary)]
     (cons (list (bit-string->bytes id)
                 (udp-remote-address (format "~a.~a.~a.~a" a b c d) port))
           (extract-peers rest)))))

(define (dotted-quad->numbers host)
  (define quad (map string->number (string-split host ".")))
  (and (= 4 (length quad)) (andmap byte? quad) quad))

(define (format-ip/port peer)
  (match-define (udp-remote-address host port) peer)
  (match (dotted-quad->numbers host)
    [(list a b c d) (bit-string->bytes (bit-string a b c d (port :: big-endian bytes 2)))]
    [#f #f]))

(define (format-peers nps)
  (bit-string->bytes
   (for/fold [(acc #"")] [(np nps)]
     (match-define (list id peer) np)
     (match (format-ip/port peer)
       [#f acc]
       [loc (bit-string (acc :: binary) (id :: binary bytes 20) (loc :: binary))]))))

(define (take-at-most n xs) (for/list [(x (in-list xs)) (i (in-range n))] x))

(define (query-all-nodes)
  (set->list (immediate-query [query-set (node-coordinates $i $p _) (list i p)])))

(define (K-closest nodes/peers target-id #:K [k K] #:key [key car])
  (take-at-most k (sort nodes/peers #:key key (distance-to-<? target-id))))

(define (do-krpc-transaction source-id target transaction-name method args)
  (react/suspend (k)
    (stop-when (asserted (krpc-transaction source-id target transaction-name method args $results))
      (k results))))

(define (suggest-node! source id peer known-alive?)
  (match-define (udp-remote-address host port) peer)
  (when (and (not (private-ip-address? host)) (= 20 (bytes-length id)))
    (send! (discovered-node id peer known-alive?))))

(define (format-nodes/peers ns)
  (for/list [(n ns)] (format "~a(~a)" (~id (car n)) (cdr n))))

(define (next-refresh-time lo-mins hi-mins)
  (+ (current-inexact-milliseconds)
     (* 1000 (+ (* lo-mins 60) (random (* (- hi-mins lo-mins) 60))))))

(define (find-node/suggest txn-name&source local-id respondent target)
  (define results (do-krpc-transaction local-id respondent txn-name&source
                                       #"find_node" (hash #"id" local-id #"target" target)))
  (when (hash? results)
    (for [(p (extract-peers (hash-ref results #"nodes" #"")))]
      (suggest-node! txn-name&source (car p) (cadr p) #f)))
  results)

(define (~id node-id) (and (bytes? node-id) (bytes->hex-string node-id)))
