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

(message-struct ui-watch (id))
(message-struct ui-unwatch (id))

(spawn #:name 'kademlia-user-interface
       (stop-when-reloaded)

       (define PORT 5463)
       (define endpoint (udp-listener PORT))

       (define ((reply peer) fmt . args)
         (send! (udp-packet endpoint peer (string->bytes/utf-8 (apply format fmt args)))))

       (during (local-node $local-id)
         (on (message (udp-packet $peer endpoint $body))
             (spawn* #:name (list 'udp-interface-handler body)
                     (handle-command (reply peer) local-id peer body)))))

(define (track-peers reply id)
  (react (assert (locate-participants id))
         (stop-when (message (ui-unwatch id)) (reply "peers ~a : done\n" (~id id)))

         (field [rs (set)])
         (on (asserted (participant-record id $host $port))
             (rs (set-add (rs) (list host port)))
             (reply "~a participant discovered: ~a:~a\n" (~id id) host port))

         (on (asserted (participants-in id $npts $final?))
             (define summary
               (with-output-to-string
                 (lambda ()
                   (printf "~a participants ~a:\nnodes:\n" (if final? "final" "partial") (~id id))
                   (for [(npt npts)]
                     (match-define (record-holder n p token rs?) npt)
                     (printf "  ~a ~v ~a ~v\n" (~id n) rs? p token))
                   (when final?
                     (printf "total of ~a participants discovered\n" (set-count (rs)))))))
             (reply "~a" summary))))

(define (handle-command reply local-id peer body)
  (match (string-trim (bytes->string/utf-8 body))

    ["list"
     (define all-entries
       (set->list (immediate-query [query-set (node-coordinates $i $p $t) (list i p t)])))
     (printf "Local ID is ~a; ~a nodes in table\n" (~id local-id) (length all-entries))
     (define grouped (sort (group-by (lambda (e) (node-id->bucket (car e) local-id)) all-entries) >
                           #:key (lambda (g) (node-id->bucket (car (car g)) local-id))))
     (for [(group grouped)]
       (define bucket (node-id->bucket (car (car group)) local-id))
       (printf "Bucket ~a:\n" bucket)
       (for [(entry (sort group #:key car (distance-to-<? local-id)))]
         (match-define (list i p t) entry)
         (printf "  node ~a: ~a (~a)\n" (~id i) p t)))
     (flush-output)
     (reply "Done. Check stdout\n")]

    ["store"
     (define recs
       (set->list (immediate-query [query-set (participant-record $ih $h $p) (list ih h p)])))
     (printf "~a stored records in total.\n" (length recs))
     (for [(r (sort recs #:key car bytes<?))]
       (match-define (list info_hash host port) r)
       (printf "Stored record resource ~a host ~a port ~a\n" (~id info_hash) host port))
     (flush-output)
     (reply "Done. Check stdout\n")]

    [(regexp #px"^watch (.*)$" (list _ id-str))
     (define id (hex-string->bytes id-str))
     (react (assert (locate-node id #f))
            (stop-when (message (ui-unwatch id)) (reply "~a : done\n" (~id id)))
            (on (asserted (closest-nodes-to id $ns $final?))
                (define summary
                  (with-output-to-string
                    (lambda ()
                      (printf "~a ~a:\n" (if final? "final" "partial") (~id id))
                      (for [(n ns)] (printf "  ~a ~a\n" (~id (car n)) (cadr n))))))
                (reply "~a" summary)))]

    [(regexp #px"^peers (.*)$" (list _ id-str))
     (track-peers reply (hex-string->bytes id-str))]

    [(regexp #px"^dat (.*)$" (list _ id-str))
     (track-peers reply (subbytes (discovery-key (hex-string->bytes id-str)) 0 20))]

    [(regexp #px"^un([^ ]*) (.*)$" (list _ _ id-str))
     (send! (ui-unwatch (hex-string->bytes id-str)))
     (send! (ui-unwatch (subbytes (discovery-key (hex-string->bytes id-str)) 0 20)))
     (reply "ok\n")]

    [(regexp #px"^ann (.*) (.*)$" (list _ id-str port-str))
     (define id (hex-string->bytes id-str))
     (define port (string->number port-str))
     (react (assert (announce-participation id (if (zero? port) #f port)))
            (stop-when (message (ui-unwatch id)))
            (on-start (reply "~a : announcement started\n" (~id id)))
            (on-stop (reply "~a : announcement stopped\n" (~id id))))]

    ["tokens" (reply "~a\n" (map ~id (immediate-query [query-value '() (valid-tokens $ts) ts])))]

    [line (reply "Unhandled: ~a\n" line)]))
