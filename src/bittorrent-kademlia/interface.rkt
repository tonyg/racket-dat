#lang syndicate

(require racket/string)
(require (only-in racket/random crypto-random-bytes))
(require (only-in file/sha1 bytes->hex-string hex-string->bytes))
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

       (on (message (udp-packet $peer endpoint $body))
           (spawn*
            (match (string-trim (bytes->string/utf-8 body))
              [(regexp #px"watch (.*)" (list _ id-str))
               (define id (hex-string->bytes id-str))
               (if (= (bytes-length id) 20)
                   (spawn #:name (ui-watch id)
                          (assert (locate-node id))
                          (stop-when (message (ui-unwatch id))
                            (reply peer "~a: done\n"
                                   (bytes->hex-string id)))
                          (on (asserted (closest-nodes-to id $ids))
                              (reply peer "~a: ~a\n"
                                     (bytes->hex-string id)
                                     (map bytes->hex-string ids))))
                   (reply peer "Bad id: ~a\n" id))]
              [(regexp #px"unwatch (.*)" (list _ id-str))
               (send! (ui-unwatch (hex-string->bytes id-str)))
               (reply peer "ok\n")]
              [line
               (reply peer "Unhandled: ~a\n" line)]))))
