;;; ob-clutch-test.el --- ERT tests for Org-Babel clutch backend -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; URL: https://github.com/LuciusChen/ob-clutch

;;; Commentary:

;; Unit tests for Org-Babel clutch backend.

;;; Code:

(require 'cl-lib)
(require 'ert)
(require 'ob-clutch)
(require 'org)

(defvar clutch-connection-alist)

(ert-deftest ob-clutch-test-normalize-backend-aliases ()
  "Resolve inline aliases through the same backend rules as Clutch."
  (dolist (case '((mysql mysql) (mariadb mysql) (postgresql pg)
                  ("POSTGRES" pg) (POSTGRESQL pg) ("SQLite" sqlite)
                  (oracle oracle)))
    (pcase-let ((`(,backend ,expected) case))
      (should (eq (plist-get
                   (ob-clutch--resolve-connection
                    `((:backend . ,backend) (:user . "u")
                      (:database . ":memory:")) nil)
                   :backend)
                  expected)))))

(ert-deftest ob-clutch-test-resolve-connection-unknown-name-errors ()
  "Test unknown :connection raises explicit user error."
  (let ((clutch-connection-alist nil))
    (should-error
     (ob-clutch--resolve-connection '((:connection . "missing")) 'mysql)
     :type 'user-error)))

(ert-deftest ob-clutch-test-resolve-connection-injects-pass-entry ()
  "Test :connection defaults :pass-entry to connection name."
  (let ((clutch-connection-alist
         '(("dev" . (:backend mysql :host "127.0.0.1" :port 3306 :user "root")))))
    (let ((conn-params
           (ob-clutch--resolve-connection '((:connection . "dev")) 'mysql)))
      (should (eq (plist-get conn-params :backend) 'mysql))
      (should (equal (plist-get conn-params :pass-entry) "dev")))))

(ert-deftest ob-clutch-test-resolve-connection-saved-entry-defaults-to-mysql ()
  "Saved backends override language defaults without mutating the entry."
  (let ((clutch-connection-alist
         '(("dev" . (:host "127.0.0.1" :port 3306 :user "root")))))
    (dolist (default '(nil mysql pg sqlite))
      (let ((conn-params
             (ob-clutch--resolve-connection '((:connection . "dev")) default)))
        (should (eq (plist-get conn-params :backend) (or default 'mysql)))
        (should (equal (plist-get conn-params :host) "127.0.0.1"))))
    (should-not (plist-member (cdar clutch-connection-alist) :backend)))
  (let* ((clutch-connection-alist
          '(("dev" . (:backend postgresql :password "explicit"))))
         (before (copy-tree clutch-connection-alist)))
    (should (eq (plist-get
                 (ob-clutch--resolve-connection '((:connection . "dev")) 'mysql)
                 :backend) 'pg))
    (should (equal clutch-connection-alist before))))

(ert-deftest ob-clutch-test-saved-profile-execution-keeps-password-source ()
  "Babel execution preserves profile secrets and explicit password sources."
  (dolist (case '((nil "profile-secret")
                  ((:password "explicit-secret") "explicit-secret")
                  ((:pass-entry "separate-secret") nil)))
    (pcase-let* ((`(,overrides ,expected-password) case)
                 (clutch-connection-alist
                  (list (cons "dev" (append overrides
                                            '(:backend pg :profile-entry "pg/dev")))))
                 (before (copy-tree clutch-connection-alist))
                 (ob-clutch--connection-cache (make-hash-table :test 'equal))
                 (connected nil))
      (cl-letf (((symbol-function 'auth-source-pass-entries)
                 (lambda () '("pg/dev")))
                ((symbol-function 'auth-source-pass-parse-entry)
                 (lambda (entry)
                   (should (equal entry "pg/dev"))
                   '(("secret" . "profile-secret") ("user" . "reader")
                     ("host" . "db.example") ("port" . "5432"))))
                ((symbol-function 'clutch-open-connection)
                 (lambda (params) (setq connected params) 'fake-conn))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn sql)
                   (should (eq conn 'fake-conn))
                   (should (equal sql "select 17"))
                   (make-clutch-db-result :columns '((:name "n")) :rows '((17))))))
        (should (equal (org-babel-execute:clutch
                        "select 17" '((:connection . "dev") (:max-rows . "nil")))
                       '(("n") hline (17))))
        (should (equal (plist-get connected :password) expected-password))
        (should (equal (plist-get connected :pass-entry)
                       (plist-get overrides :pass-entry)))
        (should (equal (plist-get connected :user) "reader"))
        (should (equal clutch-connection-alist before))))))

(ert-deftest ob-clutch-test-resolve-connection-inline-default-port ()
  "Test inline mysql connection defaults to port 3306."
  (let ((conn-params
         (ob-clutch--resolve-connection
          '((:host . "127.0.0.1") (:user . "u")) 'mysql)))
    (should (eq (plist-get conn-params :backend) 'mysql))
    (should (= (plist-get conn-params :port) 3306))))

(ert-deftest ob-clutch-test-resolve-connection-inline-pass-entry ()
  "Test inline params preserve :pass-entry for Clutch core resolution."
  (let ((conn-params
         (ob-clutch--resolve-connection
          '((:host . "127.0.0.1")
            (:user . "u")
            (:pass-entry . "db/dev"))
          'mysql)))
    (should (eq (plist-get conn-params :backend) 'mysql))
    (should (equal (plist-get conn-params :host) "127.0.0.1"))
    (should (equal (plist-get conn-params :user) "u"))
    (should (= (plist-get conn-params :port) 3306))
    (should (equal (plist-get conn-params :pass-entry) "db/dev"))))

(ert-deftest ob-clutch-test-resolve-connection-preserves-inline-jdbc-sid ()
  "Inline JDBC params should preserve driver-specific keys such as :sid."
  (let ((conn-params
         (ob-clutch--resolve-connection
          '((:backend . oracle)
            (:host . "db")
            (:port . "1521")
            (:user . "scott")
            (:sid . "ORCL"))
          'oracle)))
    (should (eq (plist-get conn-params :backend) 'oracle))
    (should (equal (plist-get conn-params :host) "db"))
    (should (= (plist-get conn-params :port) 1521))
    (should (equal (plist-get conn-params :sid) "ORCL"))))

(ert-deftest ob-clutch-test-resolve-connection-preserves-transport-params ()
  "Saved and inline connection params should keep Clutch transport keys."
  (let ((clutch-connection-alist
         '(("remote" . (:backend pg :host "127.0.0.1" :port 5432
                        :user "postgres" :database "app"
                        :tramp "/ssh:arch:/work/")))))
    (let ((saved (ob-clutch--resolve-connection '((:connection . "remote")) 'pg))
          (inline (ob-clutch--resolve-connection
                   '((:host . "db.internal")
                     (:port . "5432")
                     (:user . "postgres")
                     (:database . "app")
                     (:ssh-host . "bastion"))
                   'pg)))
      (should (equal (plist-get saved :tramp) "/ssh:arch:/work/"))
      (should (equal (plist-get inline :ssh-host) "bastion")))))

(ert-deftest ob-clutch-test-resolve-connection-inline-sqlite-requires-database ()
  "Test sqlite inline params require :database."
  (should-error
   (ob-clutch--resolve-connection '() 'sqlite)
   :type 'user-error))

(ert-deftest ob-clutch-test-connect-caches-live-connection ()
  "Test `ob-clutch--connect' reuses live cached connections."
  (let ((clutch-connection-alist nil)
        (ob-clutch--connection-cache (make-hash-table :test 'equal))
        (created 0))
    (cl-letf (((symbol-function 'clutch-prepare-connection-params)
               (lambda (params _source-default-directory) params))
              ((symbol-function 'clutch-open-connection)
               (lambda (_params)
                 (cl-incf created)
                 (list :conn-id created)))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t)))
      (let* ((params '((:host . "127.0.0.1") (:user . "u")))
             (c1 (ob-clutch--connect params 'mysql))
             (c2 (ob-clutch--connect params 'mysql)))
        (should (equal c1 c2))
        (should (= created 1))))))

(ert-deftest ob-clutch-test-generic-execute-accepts-connection-without-backend ()
  "Generic clutch Babel blocks should accept :connection without inline :backend."
  (let ((clutch-connection-alist
         '(("demo" . (:backend mysql :host "127.0.0.1" :port 3306
                      :user "root" :database "demo"))))
        connected
        queried)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-prepare-connection-params)
               (lambda (params _source-default-directory) params))
              ((symbol-function 'clutch-open-connection)
               (lambda (params)
                 (setq connected params)
                 (should (eq (plist-get params :backend) 'mysql))
                 (should (equal (plist-get params :database) "demo"))
                 'fake-conn))
              ((symbol-function 'org-babel-expand-body:generic)
               (lambda (body _params) body))
              ((symbol-function 'clutch-db-query)
               (lambda (conn sql)
                 (setq queried (list conn sql))
                 (make-clutch-db-result :columns nil :rows nil :affected-rows 1))))
      (should (equal (org-babel-execute:clutch
                      "select 1"
                      '((:connection . "demo") (:max-rows . "nil")))
                     "Affected rows: 1"))
      (should (eq (plist-get connected :backend) 'mysql))
      (should (equal (plist-get connected :database) "demo"))
      (should (equal queried '(fake-conn "select 1"))))))

(ert-deftest ob-clutch-test-generic-execute-accepts-saved-connection-without-entry-backend ()
  "Generic clutch Babel blocks should default saved connections without :backend to mysql."
  (let ((clutch-connection-alist
         '(("demo" . (:host "127.0.0.1" :port 3306
                      :user "root" :database "demo"))))
        connected)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-prepare-connection-params)
               (lambda (params _source-default-directory) params))
              ((symbol-function 'clutch-open-connection)
               (lambda (params)
                 (setq connected params)
                 'fake-conn))
              ((symbol-function 'org-babel-expand-body:generic)
               (lambda (body _params) body))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result :columns nil :rows nil :affected-rows 1))))
      (should (equal (org-babel-execute:clutch
                      "select 1"
                      '((:connection . "demo")))
                     "Affected rows: 1"))
      (should (eq (plist-get connected :backend) 'mysql))
      (should (equal (plist-get connected :database) "demo")))))

(ert-deftest ob-clutch-test-generic-execute-errors-when-backend-and-connection-missing ()
  "Generic clutch Babel blocks should error clearly when both backend and connection are absent."
  (should-error
   (org-babel-execute:clutch "select 1" nil)
   :type 'user-error))

(defmacro ob-clutch-test--with-rows (rows sent &rest body)
  "Run BODY with a fake connection whose queries return ROWS.
Each SQL string sent to the connection is pushed onto the variable SENT."
  (declare (indent 2))
  `(let ((ob-clutch--connection-cache (make-hash-table :test 'equal)))
     (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_conn) nil))
               ((symbol-function 'clutch-prepare-connection-params)
                (lambda (params _dir) params))
               ((symbol-function 'clutch-open-connection)
                (lambda (_params) 'fake-conn))
               ((symbol-function 'org-babel-expand-body:generic)
                (lambda (body _params) body))
               ((symbol-function 'clutch-db-query)
                (lambda (_conn sql)
                  (push sql ,sent)
                  (make-clutch-db-result :columns '((:name "id"))
                                         :rows ,rows))))
       ,@body)))

(defun ob-clutch-test--run (sql &rest params)
  "Run SQL as a mysql block with the extra header PARAMS."
  (org-babel-execute:mysql
   sql (append '((:host . "127.0.0.1") (:user . "root")) params)))

(ert-deftest ob-clutch-test-max-rows-asks-for-one-row-more ()
  "A limited select fetches one row past the limit instead of every row."
  (let (sent)
    (ob-clutch-test--with-rows '((1) (2)) sent
      (ob-clutch-test--run "select id from demo" '(:max-rows . "2"))
      (should (equal sent '("select id from demo LIMIT 3 OFFSET 0"))))))

(ert-deftest ob-clutch-test-max-rows-marks-a-cut-result ()
  "A result with more rows than the limit ends in a visible note."
  (let (sent)
    (ob-clutch-test--with-rows '((1) (2) (3)) sent
      (should (equal (ob-clutch-test--run "select id from demo"
                                          '(:max-rows . "2"))
                     '(("id") hline (1) (2) hline
                       ("First 2 rows shown, more follow")))))))

(ert-deftest ob-clutch-test-max-rows-leaves-a-complete-result-alone ()
  "A result that fits the limit has no note."
  (let (sent)
    (ob-clutch-test--with-rows '((1) (2)) sent
      (should (equal (ob-clutch-test--run "select id from demo"
                                          '(:max-rows . "2"))
                     '(("id") hline (1) (2)))))))

(ert-deftest ob-clutch-test-max-rows-nil-sends-the-sql-as-written ()
  "`:max-rows nil' removes the limit, the query rewrite and the note."
  (let (sent)
    (ob-clutch-test--with-rows '((1) (2) (3)) sent
      (should (equal (ob-clutch-test--run "select id from demo"
                                          '(:max-rows . "nil"))
                     '(("id") hline (1) (2) (3))))
      (should (equal sent '("select id from demo"))))))

(ert-deftest ob-clutch-test-max-rows-keeps-statements-that-are-not-selects ()
  "Writes and a query's own row limit are sent unchanged.
A limit of the query's own still cuts the rows Clutch returns."
  (let (sent)
    (ob-clutch-test--with-rows '((1) (2) (3)) sent
      (ob-clutch-test--run "insert into demo values (1)" '(:max-rows . "2"))
      (should (equal (ob-clutch-test--run "select id from demo limit 9"
                                          '(:max-rows . "2"))
                     '(("id") hline (1) (2) hline
                       ("First 2 rows shown, more follow"))))
      (should (equal (reverse sent)
                     '("insert into demo values (1)"
                       "select id from demo limit 9"))))))

(ert-deftest ob-clutch-test-max-rows-defaults-to-a-finite-limit ()
  "Without a header or an option, a large result is cut at 1000 rows."
  (let (sent)
    (ob-clutch-test--with-rows (mapcar #'list (number-sequence 1 1001)) sent
      (let ((table (ob-clutch-test--run "select id from demo")))
        (should (= (length table) (+ 2 1000 2)))
        (should (equal (car (last table))
                       '("First 1000 rows shown, more follow")))))))

(ert-deftest ob-clutch-test-obsolete-max-rows-option-sets-the-default ()
  "The obsolete option name still sets the default limit."
  (let (sent)
    (ob-clutch-test--with-rows '((1) (2)) sent
      (with-suppressed-warnings ((obsolete org-babel-clutch-max-rows))
        (let ((org-babel-clutch-max-rows 1))
          (should (equal (ob-clutch-test--run "select id from demo")
                         '(("id") hline (1) hline
                           ("First 1 rows shown, more follow")))))))))

(defmacro ob-clutch-test--with-async (started &rest body)
  "Run BODY with a fake backend that starts statements and waits.
Each statement started on it is pushed, as (CONN SQL CALLBACK), onto the
variable STARTED; calling CALLBACK finishes it.  Interrupts are pushed
onto `ob-clutch-test--interrupts'."
  (declare (indent 1))
  `(let ((ob-clutch--connection-cache (make-hash-table :test 'equal))
         (ob-clutch--jobs nil)
         (ob-clutch-test--interrupts nil))
     (cl-letf (((symbol-function 'clutch-db-live-p) (lambda (_conn) nil))
               ((symbol-function 'clutch-prepare-connection-params)
                (lambda (params _dir) params))
               ((symbol-function 'clutch-open-connection)
                (lambda (params) (intern (format "conn-%s" (plist-get params :port)))))
               ((symbol-function 'clutch-db-query-async)
                (lambda (conn sql callback)
                  (push (list conn sql callback) ,started)
                  t))
               ((symbol-function 'clutch-db-interrupt-query)
                (lambda (conn) (push conn ob-clutch-test--interrupts) t)))
       ,@body)))

(defvar ob-clutch-test--interrupts nil
  "Connections `clutch-db-interrupt-query' was called on.")

(defun ob-clutch-test--ok (rows)
  "Return a `clutch-db-result' with one column `id' and ROWS."
  (make-clutch-db-result :columns '((:name "id")) :rows rows))

(defun ob-clutch-test--block (sql &optional port extra)
  "Return Org text for an async mysql block running SQL on PORT.
EXTRA is more header arguments."
  (format "#+begin_src mysql :host 127.0.0.1 :port %s :user root :async yes :max-rows nil %s\n%s\n#+end_src\n"
          (or port 1) (or extra "") sql))

(defmacro ob-clutch-test--in-org (text &rest body)
  "Run BODY in an Org buffer holding TEXT, point at its first block."
  (declare (indent 1))
  `(with-temp-buffer
     (org-mode)
     (let ((org-confirm-babel-evaluate nil))
       (insert ,text)
       (goto-char (point-min))
       ,@body)))

(defun ob-clutch-test--goto-block (n)
  "Move point to the start of the Nth source block."
  (goto-char (point-min))
  (re-search-forward "^ *#\\+begin_src" nil nil n)
  (beginning-of-line))

(defun ob-clutch-test--results ()
  "Return the buffer text after the first #+RESULTS: line, trimmed."
  (save-excursion
    (goto-char (point-min))
    (if (re-search-forward "^#\\+RESULTS:\n" nil t)
        (string-trim (buffer-substring-no-properties (point) (point-max)))
      "")))

(ert-deftest ob-clutch-test-async-block-shows-placeholder-then-result ()
  "An async block returns at once and gets its table when the statement ends."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select id from t")
        (org-babel-execute-src-block)
        (should (= (length started) 1))
        (should (string-match-p "| Running ob-clutch-[0-9]+ |"
                                (ob-clutch-test--results)))
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1) (2))) nil)
        (should (equal (ob-clutch-test--results)
                       "| id |\n|----|\n|  1 |\n|  2 |"))
        (should-not ob-clutch--jobs)))))

(ert-deftest ob-clutch-test-async-block-reports-the-error ()
  "A statement that fails puts the error in the block's result."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select nope")
        (org-babel-execute-src-block)
        (funcall (nth 2 (car started)) nil '(error "Unknown column"))
        (should (string-match-p "Error: Unknown column"
                                (ob-clutch-test--results)))
        (should-not ob-clutch--jobs)))))

(ert-deftest ob-clutch-test-async-blocks-on-one-connection-run-in-order ()
  "A second block waits for the first on the same connection."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (concat (ob-clutch-test--block "select 1")
                                      "\n"
                                      (ob-clutch-test--block "select 2"))
        (org-babel-execute-src-block)
        (ob-clutch-test--goto-block 2)
        (org-babel-execute-src-block)
        (should (equal (mapcar #'cadr started) '("select 1")))
        (should (string-match-p "Queued" (buffer-string)))
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
        (should (equal (mapcar #'cadr (reverse started))
                       '("select 1" "select 2")))
        (should (string-match-p "Running" (buffer-string)))
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((2))) nil)
        (should-not ob-clutch--jobs)
        (should-not (string-match-p "Queued\\|Running" (buffer-string)))))))

(ert-deftest ob-clutch-test-async-blocks-on-two-connections-run-together ()
  "Blocks on different connections do not wait for each other."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (concat (ob-clutch-test--block "select 1" 1)
                                      "\n"
                                      (ob-clutch-test--block "select 2" 2))
        (org-babel-execute-src-block)
        (ob-clutch-test--goto-block 2)
        (org-babel-execute-src-block)
        (should (= (length started) 2))))))

(ert-deftest ob-clutch-test-async-block-without-backend-support-runs-at-once ()
  "A backend that cannot start without blocking returns the result directly."
  (let (started)
    (ob-clutch-test--with-async started
      (cl-letf (((symbol-function 'clutch-db-query-async)
                 (lambda (&rest _) nil))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql) (ob-clutch-test--ok '((7))))))
        (ob-clutch-test--in-org (ob-clutch-test--block "select 7")
          (org-babel-execute-src-block)
          (should (equal (ob-clutch-test--results) "| id |\n|----|\n|  7 |"))
          (should-not ob-clutch--jobs))))))

(ert-deftest ob-clutch-test-async-result-is-dropped-after-the-block-reran ()
  "A late result does not overwrite the result of a newer run."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select 1")
        (org-babel-execute-src-block)
        (let ((old (car started)))
          (org-babel-execute-src-block)
          (funcall (nth 2 old) (ob-clutch-test--ok '((1))) nil)
          (should (string-match-p "Running ob-clutch-[0-9]+" (ob-clutch-test--results)))
          (should-not (string-match-p "| 1 |" (ob-clutch-test--results)))
          (funcall (nth 2 (car started)) (ob-clutch-test--ok '((2))) nil)
          (should (equal (ob-clutch-test--results)
                         "| id |\n|----|\n|  2 |")))))))

(ert-deftest ob-clutch-test-async-result-leaves-an-edited-result-alone ()
  "A result the user replaced while the statement ran is not overwritten."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select 1")
        (org-babel-execute-src-block)
        (goto-char (point-min))
        (re-search-forward "^| Running .* |$")
        (replace-match "| mine |")
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
        (should (equal (ob-clutch-test--results) "| mine |"))))))

(ert-deftest ob-clutch-test-async-failed-dispatch-does-not-stall-the-queue ()
  "A queued statement that cannot start shows its error; the next one starts."
  (let (started (calls 0))
    (ob-clutch-test--with-async started
      (cl-letf (((symbol-function 'clutch-db-query-async)
                 (lambda (conn sql callback)
                   (when (= (cl-incf calls) 2)
                     (signal 'error '("Connection closed")))
                   (push (list conn sql callback) started)
                   t)))
        (ob-clutch-test--in-org (concat (ob-clutch-test--block "select 1") "\n"
                                        (ob-clutch-test--block "select 2") "\n"
                                        (ob-clutch-test--block "select 3"))
          (dolist (n '(1 2 3))
            (ob-clutch-test--goto-block n)
            (org-babel-execute-src-block))
          (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
          (should (string-match-p "Error: Connection closed" (buffer-string)))
          (should (equal (mapcar #'cadr started) '("select 3" "select 1")))
          (funcall (nth 2 (car started)) (ob-clutch-test--ok '((3))) nil)
          (should-not ob-clutch--jobs))))))

(ert-deftest ob-clutch-test-async-append-replaces-only-the-placeholder ()
  "`:results append' keeps earlier results, also wider ones, and leaves no placeholder."
  (dolist (mode '("append" "prepend"))
    (let (started)
      (ob-clutch-test--with-async started
        (ob-clutch-test--in-org
            (ob-clutch-test--block "select 1" nil (format ":results %s table" mode))
          (org-babel-execute-src-block)
          (funcall (nth 2 (car started))
                   (make-clutch-db-result :columns '((:name "id") (:name "name"))
                                          :rows '((1 "a")))
                   nil)
          (org-babel-execute-src-block)
          (funcall (nth 2 (car started)) (ob-clutch-test--ok '((2))) nil)
          (should-not (string-match-p "Running\\|Queued" (buffer-string)))
          (should (string-match-p "| *1 *|" (buffer-string)))
          (should (string-match-p "| *2 *|" (buffer-string))))))))

(ert-deftest ob-clutch-test-async-result-keeps-a-note-added-to-the-placeholder ()
  "A note added to the placeholder row while the statement runs survives."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select 1")
        (org-babel-execute-src-block)
        (re-search-forward "^| Running [^ ]+ |")
        (backward-char 1)
        (insert " my-note |")
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
        (should (string-match-p "my-note" (buffer-string)))
        (should-not (string-match-p "| 1 |" (buffer-string)))))))

(ert-deftest ob-clutch-test-cancel-targets-the-latest-run-of-a-block ()
  "After a rerun, cancelling drops the run the block shows."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select 1")
        (org-babel-execute-src-block)
        (org-babel-execute-src-block)
        (should (string-match-p "Queued" (buffer-string)))
        (ob-clutch-cancel-block-at-point)
        (should (string-match-p "Cancelled" (buffer-string)))
        (should-not ob-clutch-test--interrupts)
        (should (= (length ob-clutch--jobs) 1))))))

(ert-deftest ob-clutch-test-async-block-in-a-list-item-gets-its-result ()
  "A block indented inside a list item has its placeholder replaced too."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org
          "- item\n  #+begin_src mysql :host 127.0.0.1 :port 1 :user root :async yes :max-rows nil\n  select 1\n  #+end_src\n"
        (ob-clutch-test--goto-block 1)
        (org-babel-execute-src-block)
        (should (string-match-p "^ +| Running ob-clutch-[0-9]+ |" (buffer-string)))
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
        (should-not (string-match-p "Running" (buffer-string)))
        (should (string-match-p "^ +| *id *|" (buffer-string)))
        (should-not ob-clutch--jobs)))))

(ert-deftest ob-clutch-test-async-failure-to-show-does-not-stall-the-queue ()
  "A result that cannot be shown, as in a read-only buffer, still starts the next job."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (concat (ob-clutch-test--block "select 1") "\n"
                                      (ob-clutch-test--block "select 2"))
        (ob-clutch-test--goto-block 1)
        (org-babel-execute-src-block)
        (ob-clutch-test--goto-block 2)
        (org-babel-execute-src-block)
        (setq buffer-read-only t)
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
        (should (equal (mapcar #'cadr started) '("select 2" "select 1")))
        (setq buffer-read-only nil)
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((2))) nil)
        (should (string-match-p "| *2 *|" (buffer-string)))
        (should-not ob-clutch--jobs)))))

(ert-deftest ob-clutch-test-async-result-for-a-killed-buffer-is-ignored ()
  "Finishing a statement whose buffer is gone does nothing and moves on."
  (let (started)
    (ob-clutch-test--with-async started
      (with-temp-buffer
        (org-mode)
        (let ((org-confirm-babel-evaluate nil))
          (insert (ob-clutch-test--block "select 1"))
          (goto-char (point-min))
          (org-babel-execute-src-block)))
      (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
      (should-not ob-clutch--jobs))))

(ert-deftest ob-clutch-test-cancel-drops-a-waiting-block ()
  "Cancelling a block that waits for the connection never runs it."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (concat (ob-clutch-test--block "select 1")
                                      "\n"
                                      (ob-clutch-test--block "select 2"))
        (org-babel-execute-src-block)
        (ob-clutch-test--goto-block 2)
        (org-babel-execute-src-block)
        (ob-clutch-cancel-block-at-point)
        (should (string-match-p "Cancelled" (buffer-string)))
        (funcall (nth 2 (car started)) (ob-clutch-test--ok '((1))) nil)
        (should (equal (mapcar #'cadr started) '("select 1")))
        (should-not ob-clutch--jobs)))))

(ert-deftest ob-clutch-test-cancel-interrupts-a-running-block ()
  "Cancelling the running block interrupts its connection."
  (let (started)
    (ob-clutch-test--with-async started
      (ob-clutch-test--in-org (ob-clutch-test--block "select sleep(9)")
        (org-babel-execute-src-block)
        (ob-clutch-cancel-block-at-point)
        (should (equal ob-clutch-test--interrupts '(conn-1)))
        (funcall (nth 2 (car started)) nil '(error "Query interrupted"))
        (should (string-match-p "Error: Query interrupted"
                                (ob-clutch-test--results)))))))

(ert-deftest ob-clutch-test-cancel-needs-an-async-run ()
  "Cancelling a block that has no pending run is a user error."
  (ob-clutch-test--with-async started
    (ob-clutch-test--in-org (ob-clutch-test--block "select 1")
      (should-error (ob-clutch-cancel-block-at-point) :type 'user-error))))

(ert-deftest ob-clutch-test-block-without-async-still-blocks ()
  "Without `:async yes' the block runs through `clutch-db-query'."
  (let (started)
    (ob-clutch-test--with-async started
      (cl-letf (((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql) (ob-clutch-test--ok '((5))))))
        (ob-clutch-test--in-org
            "#+begin_src mysql :host 127.0.0.1 :user root :max-rows nil\nselect 5\n#+end_src\n"
          (org-babel-execute-src-block)
          (should-not started)
          (should (equal (ob-clutch-test--results) "| id |\n|----|\n|  5 |")))))))

(ert-deftest ob-clutch-test-inline-jdbc-driver-class-reaches-connect ()
  "Babel's public executor passes the explicit JDBC driver to Clutch."
  (require 'clutch-db-jdbc)
  (let ((ob-clutch--connection-cache (make-hash-table :test 'equal))
        captured)
    (cl-letf (((symbol-function 'clutch-prepare-connection-params)
               (lambda (params _directory) params))
              ((symbol-function 'clutch-open-connection)
               (lambda (params) (setq captured params) 'audit-conn))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "answer")) :rows '((42))))))
      (should (equal (org-babel-execute:clutch
                      "SELECT 42" '((:backend . "jdbc")
                                    (:url . "jdbc:h2:mem:audit")
                                    (:driver-class . "org.h2.Driver")
                                    (:user . "sa")))
                     '(("answer") hline (42))))
      (should (equal (plist-get captured :driver-class) "org.h2.Driver")))))

(ert-deftest ob-clutch-test-sqlite-cache-uses-resolved-file ()
  "Real Babel blocks separate relative databases and reuse the same file."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((root (make-temp-file "ob-clutch-sqlite-" t))
        (ob-clutch--connection-cache (make-hash-table :test 'equal))
        (params '((:database . "app.db"))))
    (unwind-protect
        (progn
          (dolist (name '("a" "b"))
            (let ((directory (expand-file-name name root)))
              (make-directory directory)
              (let ((db (sqlite-open (expand-file-name "app.db" directory))))
                (unwind-protect
                    (progn
                      (sqlite-execute db "CREATE TABLE marker(value TEXT)")
                      (sqlite-execute db "INSERT INTO marker VALUES (?)" (list name)))
                  (sqlite-close db)))))
          (dolist (name '("a" "b" "a"))
            (let ((default-directory
                   (file-name-as-directory (expand-file-name name root))))
              (should (equal (org-babel-execute:sqlite "SELECT value FROM marker" params)
                             (list '("value") 'hline (list name))))))
          (dolist (name '("a" "b"))
            (let ((default-directory
                   (file-name-as-directory (expand-file-name name root))))
              (let ((conn (ob-clutch--connect params 'sqlite)))
                (dolist (file (list "./app.db" (expand-file-name "app.db")))
                  (should (eq conn (ob-clutch--connect
                                    (list (cons :database file)) 'sqlite)))))))
          (should (= (hash-table-count ob-clutch--connection-cache) 2)))
      (ob-clutch--disconnect-all)
      (delete-directory root t))))

;;; ob-clutch-test.el ends here
