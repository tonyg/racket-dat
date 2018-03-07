#lang syndicate

(require/activate syndicate/reload)

(spawn-reloader "krpc.rkt")
(spawn-reloader "node.rkt")
(spawn-reloader "bucket.rkt")
(spawn-reloader "client.rkt")
(spawn-reloader "interface.rkt")
