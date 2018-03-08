#lang syndicate

(require/activate syndicate/reload)

(spawn-reloader "krpc.rkt")
(spawn-reloader "table.rkt")
(spawn-reloader "bucket.rkt")
(spawn-reloader "client.rkt")
(spawn-reloader "store.rkt")
(spawn-reloader "interface.rkt")
