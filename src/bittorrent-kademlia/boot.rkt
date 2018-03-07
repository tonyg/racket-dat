#lang syndicate

(require/activate syndicate/reload)

(spawn-reloader "krpc.rkt")
(spawn-reloader "experiment.rkt")
(spawn-reloader "bucket.rkt")
(spawn-reloader "bootstrap.rkt")
(spawn-reloader "client.rkt")
(spawn-reloader "interface.rkt")
