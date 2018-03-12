#lang syndicate

(require racket/match)
(require bencode-codec)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/nat-traversal)
(require syndicate/protocol/advertise)

(require "protocol.rkt")

(define-logger dht/transport)

(spawn #:name 'kademlia-krpc-udp-transport
       (stop-when-reloaded)

       (define PORT 42769)
       (define endpoint (udp-listener PORT))

       (during (nat-mapping 'udp #f PORT $assignment)
         (log-dht/transport-info "NAT port mapping: ~v" assignment))

       (on (message (udp-packet $peer endpoint $body))
           (match (with-handlers [(exn? (lambda (e) #f))] (bytes->bencode body))
             [(cons p '())
              (log-dht/transport-debug "RECEIVING ~a ~v" peer p)
              (define id (hash-ref p #"t" #f))
              (define-values (type body)
                (match (hash-ref p #"y" #f)
                  [#f (values #f #f)]
                  [#"q" (values 'request  (list (hash-ref p #"q" #f) (hash-ref p #"a" hash)))]
                  [#"r" (values 'response (hash-ref p #"r" hash))]
                  [#"e" (values 'error    (hash-ref p #"e" (lambda () (list #f #f))))]))
              (when type (send! (krpc-packet 'inbound peer id type body)))]
             [_ (log-dht/transport-warning "Packet from ~a corrupt or invalid" peer)
                (log-dht/transport-info "Packet from ~a corrupt or invalid: ~v" peer body)]))

       (on (message (krpc-packet 'outbound $peer $id $type $body))
           (define p
             (match type
               ['request  (hash #"t" id #"y" #"q" #"q" (car body) #"a" (cadr body))]
               ['response (hash #"t" id #"y" #"r" #"r" body)]
               ['error    (hash #"t" id #"y" #"e" #"e" (list (car body) (cadr body)))]))
           (log-dht/transport-debug "SENDING ~a ~v" peer p)
           (send! (udp-packet endpoint peer (bencode->bytes (list p)))))

       (on (asserted (advertise (udp-packet _ endpoint _)))
           (log-dht/transport-info "Socket is ready.")))
