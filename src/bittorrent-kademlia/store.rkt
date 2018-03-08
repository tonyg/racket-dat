#lang syndicate

(require racket/set)
(require (only-in racket/random crypto-random-bytes))
(require (only-in file/sha1 bytes->hex-string hex-string->bytes))

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/timestate)

(require "wire.rkt")
(require "protocol.rkt")

(define-logger dht/store)

(spawn #:name 'store-manager
       (stop-when-reloaded)

       (on (message (received-announcement $r))
           (when (not (immediate-query [query-value #f r #t]))
             (spawn #:name (list 'announced-record r)
                    (stop-when-reloaded)
                    (assert r)
                    (field [deadline (next-refresh-time 31 32)])
                    (on (message (received-announcement $r))
                        (log-dht/store-debug "Refreshed: ~v" r)
                        (deadline (next-refresh-time 31 32)))
                    (stop-when (asserted (later-than (deadline))))
                    (on-start (log-dht/store-debug "Stored: ~v" r))
                    (on-stop (log-dht/store-debug "Expired: ~v" r))))))
