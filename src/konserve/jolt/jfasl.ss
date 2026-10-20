;; x1 jfasl: Jolt values <-> bytes through Chez's own fasl (fasl-write in Scheme,
;; fasl-read in C). Jolt's collections are nongenerative Chez records, so their
;; rtds are omitted (omit-rtds? #t): both ends run the same runtime build.
;; Objects whose identity matters travel as EXTERNALS (Chez's external-pred):
;;   keyword-t -> (ns . name), re-interned on read (keyword equality is identity)
;;   jolt-nil  -> 'n, the one nil
;;   jrdesc    -> #(d tag), the live type descriptor for the tag
;;   a deftype instance whose tag has a handler -> #(h i data), restored by handler i
;; A lazy seq is forced when fasl-write first meets it, so its realized cells travel.
;; Layout: [fasl descriptors vector][fasl body]; fasl-compressed is off.

;; handlers by deftype tag: tag -> #(index dump restore); index -> restore
(define x1-handler-tags (make-hashtable string-hash string=?))
(define x1-restores (vector))
(define (x1-register-handler! tag dump restore)
  (let ((i (vector-length x1-restores)))
    (set! x1-restores (list->vector (append (vector->list x1-restores) (list restore))))
    (hashtable-set! x1-handler-tags tag (vector i dump))))

(define (x1-encode v)
  (let ((ext '()))
    (define (pred x)
      (cond ((keyword-t? x) (set! ext (cons (cons (keyword-t-ns x) (keyword-t-name x)) ext)) #t)
            ((jolt-nil-t? x) (set! ext (cons 'n ext)) #t)
            ((jrdesc? x) (set! ext (cons (vector 'd (jrdesc-tag x)) ext)) #t)
            ((jolt-lazyseq? x) (jolt-seq x) #f)
            ((and (jrec? x) (hashtable-ref x1-handler-tags (jrdesc-tag (jrec-desc x)) #f))
             => (lambda (h) (set! ext (cons (vector 'h (vector-ref h 0) ((vector-ref h 1) x)) ext)) #t))
            (else #f)))
    (let-values (((bp bget) (open-bytevector-output-port)))
      (parameterize ((fasl-compressed #f)) (fasl-write v bp pred #t))
      (let ((body (bget)))
        (let-values (((p get) (open-bytevector-output-port)))
          (parameterize ((fasl-compressed #f))
            (fasl-write (list->vector (reverse ext)) p))
          (put-bytevector p body)
          (get))))))

(define (x1-restore d)
  (cond ((eq? d 'n) jolt-nil)
        ((pair? d) (keyword (car d) (cdr d)))
        ((eq? (vector-ref d 0) 'd)
         (or (hashtable-ref chez-tag-desc (vector-ref d 1) #f)
             (error 'x1-decode "no live type for tag" (vector-ref d 1))))
        (else ((vector-ref x1-restores (vector-ref d 1)) (vector-ref d 2)))))

(define (x1-decode bv)
  (let* ((p (open-bytevector-input-port bv))
         (descs (fasl-read p)))
    (fasl-read p 'load (vector-map x1-restore descs))))

;; byte-array (jolt-array of kind byte, backed by a bytevector) <-> bytevector, no copy
(define (x1-bv->ba bv) (make-jolt-array bv 'byte))
(define (x1-ba->bv a) (jolt-array-vec a))
