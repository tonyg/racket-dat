#lang syndicate

(require racket/string)
(require (only-in file/sha1 hex-string->bytes))
(require (only-in racket/list group-by))
(require (only-in racket/port with-output-to-string))
(require racket/set)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)

(require "protocol.rkt")
(require "../dat/discovery.rkt")

(message-struct ui-watch (name))
(message-struct ui-unwatch (name))

(spawn #:name 'kademlia-user-interface
       (stop-when-reloaded)

       (define PORT 5463)
       (define endpoint (udp-listener PORT))

       (define ((reply addr) fmt . args)
         (send! (udp-packet endpoint addr (string->bytes/utf-8 (apply format fmt args)))))

       (during (local-node $local-name)
         (on (message (udp-packet $addr endpoint $body))
             (spawn* #:name (list 'udp-interface-handler body)
                     (handle-command (reply addr) local-name body)))))

(define (track-addrs reply name)
  (react (assert (locate-participants name))
         (stop-when (message (ui-unwatch name)) (reply "addrs ~a : done\n" (~name name)))

         (field [rs (set)])
         (on (asserted (participant-record name $host $port))
             (rs (set-add (rs) (list host port)))
             (reply "~a participant discovered: ~a:~a\n" (~name name) host port))

         (on (asserted (participants-in name $npts $final?))
             (define summary
               (with-output-to-string
                 (lambda ()
                   (printf "~a participants ~a:\nnodes:\n"
                           (if final? "final" "partial")
                           (~name name))
                   (for [(npt npts)]
                     (match-define (record-holder n p token rs?) npt)
                     (printf "  ~a ~v ~a ~v\n" (~name n) rs? p token))
                   (when final?
                     (printf "total of ~a participants discovered\n" (set-count (rs)))))))
             (reply "~a" summary))))

(define (handle-command reply local-name body)
  (match (string-trim (bytes->string/utf-8 body))

    ["list"
     (define all-entries
       (set->list (immediate-query [query-set (node-coordinates $i $p $t) (list i p t)])))
     (printf "Local name is ~a; ~a nodes in table\n" (~name local-name) (length all-entries))
     (define grouped
       (sort (group-by (lambda (e) (node-name->bucket (car e) local-name)) all-entries) >
             #:key (lambda (g) (node-name->bucket (car (car g)) local-name))))
     (for [(group grouped)]
       (define bucket (node-name->bucket (car (car group)) local-name))
       (printf "Bucket ~a:\n" bucket)
       (for [(entry (sort group #:key car (distance-to-<? local-name)))]
         (match-define (list i p t) entry)
         (printf "  node ~a: ~a (~a)\n" (~name i) p t)))
     (flush-output)
     (reply "Done. Check stdout\n")]

    ["store"
     (define recs
       (set->list (immediate-query [query-set (participant-record $ih $h $p) (list ih h p)])))
     (printf "~a stored records in total.\n" (length recs))
     (for [(r (sort recs #:key car bytes<?))]
       (match-define (list info_hash host port) r)
       (printf "Stored record resource ~a host ~a port ~a\n" (~name info_hash) host port))
     (flush-output)
     (reply "Done. Check stdout\n")]

    [(regexp #px"^watch (.*)$" (list _ name-str))
     (define name (hex-string->bytes name-str))
     (react (assert (locate-node name #f))
            (stop-when (message (ui-unwatch name)) (reply "~a : done\n" (~name name)))
            (on (asserted (closest-nodes-to name $ns $final?))
                (define summary
                  (with-output-to-string
                    (lambda ()
                      (printf "~a ~a:\n" (if final? "final" "partial") (~name name))
                      (for [(n ns)] (printf "  ~a ~a\n" (~name (car n)) (cadr n))))))
                (reply "~a" summary)))]

    [(regexp #px"^addrs (.*)$" (list _ name-str))
     (track-addrs reply (hex-string->bytes name-str))]

    [(regexp #px"^dat (.*)$" (list _ name-str))
     (track-addrs reply (subbytes (discovery-key (hex-string->bytes name-str)) 0 20))]

    [(regexp #px"^un([^ ]*) (.*)$" (list _ _ name-str))
     (send! (ui-unwatch (hex-string->bytes name-str)))
     (send! (ui-unwatch (subbytes (discovery-key (hex-string->bytes name-str)) 0 20)))
     (reply "ok\n")]

    [(regexp #px"^ann (.*) (.*)$" (list _ name-str port-str))
     (define name (hex-string->bytes name-str))
     (define port (string->number port-str))
     (react (assert (announce-participation name (if (zero? port) #f port)))
            (stop-when (message (ui-unwatch name)))
            (on-start (reply "~a : announcement started\n" (~name name)))
            (on-stop (reply "~a : announcement stopped\n" (~name name))))]

    ["tokens" (reply "~a\n" (map ~name (immediate-query [query-value '() (valid-tokens $ts) ts])))]

    [line (reply "Unhandled: ~a\n" line)]))
