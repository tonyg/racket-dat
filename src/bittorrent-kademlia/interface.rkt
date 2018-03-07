#lang syndicate

(require racket/string)
(require (only-in racket/random crypto-random-bytes))
(require (only-in file/sha1 bytes->hex-string hex-string->bytes))
(require (only-in racket/list group-by))
(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/timestate)
(require syndicate/protocol/advertise)

(require "wire.rkt")
(require "protocol.rkt")

(message-struct ui-watch (id))
(message-struct ui-unwatch (id))

(spawn #:name 'kademlia-user-interface
       (stop-when-reloaded)

       (define PORT 5463)
       (define endpoint (udp-listener PORT))

       (define (reply peer fmt . args)
         (send! (udp-packet endpoint peer (string->bytes/utf-8 (apply format fmt args)))))

       (during (local-node $local-id)
         (on (message (udp-packet $peer endpoint $body))
             (spawn*
              (match (string-trim (bytes->string/utf-8 body))
                ["list"
                 (printf "Local ID is ~a\n" (bytes->hex-string local-id))
                 (define all-entries (set->list (immediate-query
                                                 [query-set (node-coordinates $i $p $t)
                                                            (list i p t)])))
                 (define grouped
                   (sort (group-by (lambda (e) (node-id->bucket (car e) local-id)) all-entries)
                         #:key (lambda (g) (node-id->bucket (car (car g)) local-id))
                         >))
                 (for [(group grouped)]
                   (define bucket (node-id->bucket (car (car group)) local-id))
                   (printf "Bucket ~a:\n" bucket)
                   (for [(entry (sort group #:key car (distance-to-<? local-id)))]
                     (match-define (list i p t) entry)
                     (printf "  node ~a: ~a (~a)\n" (bytes->hex-string i) p t)))
                 (flush-output)
                 (reply peer "Done. Check stdout\n")]
                [(regexp #px"^watch (.*)$" (list _ id-str))
                 (define id (hex-string->bytes id-str))
                 (if (= (bytes-length id) 20)
                     (spawn #:name (ui-watch id)
                            (assert (locate-node id #f))
                            (stop-when (message (ui-unwatch id))
                              (reply peer "~a: done\n"
                                     (bytes->hex-string id)))
                            (on (asserted (closest-nodes-to id $ns $final?))
                                (reply peer "~a ~a: ~a\n"
                                       (if final? "final" "partial")
                                       (bytes->hex-string id)
                                       (format-nodes/peers ns))))
                     (reply peer "Bad id: ~a\n" id))]
                [(regexp #px"^unwatch (.*)$" (list _ id-str))
                 (send! (ui-unwatch (hex-string->bytes id-str)))
                 (reply peer "ok\n")]
                [line
                 (reply peer "Unhandled: ~a\n" line)])))))
