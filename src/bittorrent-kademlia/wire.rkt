#lang syndicate

(require racket/match)
(require bencode-codec)

(require/activate syndicate/reload)
(require/activate syndicate/drivers/udp)
(require/activate syndicate/drivers/nat-traversal)
(require syndicate/protocol/advertise)

(require "protocol.rkt")

(define-logger dht/transport)

(define (synthesize-krpc-packet id type body)
  (match type
    ['request  (hash #"t" id #"y" #"q" #"q" (car body) #"a" (cadr body))]
    ['response (hash #"t" id #"y" #"r" #"r" body)]
    ['error    (hash #"t" id #"y" #"e" #"e" (list (car body) (cadr body)))]))

(define (analyze-krpc-packet packet)
  (define id (hash-ref packet #"t" #f))
  (match (hash-ref packet #"y" #f)
    [#f (values id #f #f)]
    [#"q" (values id 'request  (list (hash-ref packet #"q" #f) (hash-ref packet #"a" hash)))]
    [#"r" (values id 'response (hash-ref packet #"r" hash))]
    [#"e" (values id 'error    (hash-ref packet #"e" (lambda () (list #f #f))))]))

(spawn #:name 'kademlia-krpc-udp-transport
       (stop-when-reloaded)

       (define PORT 42769)
       (define endpoint (udp-listener PORT))

       (during (nat-mapping 'udp #f PORT $assignment)
         (log-dht/transport-info "NAT port mapping: ~v" assignment))

       (on (message (udp-packet $peer endpoint $body))
           (define p (match (with-handlers [(exn? (lambda (e) #f))] (bytes->bencode body))
                       [(cons v '()) v]
                       [_ #f]))
           (cond [p (log-dht/transport-debug "RECEIVING ~a ~v" peer p)
                    (define-values (id type body) (analyze-krpc-packet p))
                    (send! (krpc-packet 'inbound peer id type body))]
                 [else (log-dht/transport-warning "Packet from ~a corrupt or invalid" peer)
                       (log-dht/transport-info "Packet from ~a corrupt or invalid: ~v" peer body)]))

       (on (message (krpc-packet 'outbound $peer $id $type $body))
           (define p (synthesize-krpc-packet id type body))
           (log-dht/transport-debug "SENDING ~a ~v" peer p)
           (send! (udp-packet endpoint peer (bencode->bytes (list p)))))

       (on (asserted (advertise (udp-packet _ endpoint _)))
           (log-dht/transport-info "Socket is ready.")))
