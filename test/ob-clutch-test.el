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
                        "select 17" '((:connection . "dev")))
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
                      '((:connection . "demo")))
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

(ert-deftest ob-clutch-test-max-rows-truncates-table-results ()
  "Table results should respect the :max-rows header."
  (let ((ob-clutch--connection-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-prepare-connection-params)
               (lambda (params _source-default-directory) params))
              ((symbol-function 'clutch-open-connection)
               (lambda (_params)
                 'fake-conn))
              ((symbol-function 'org-babel-expand-body:generic)
               (lambda (body _params) body))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id"))
                  :rows '((1) (2) (3))
                  :affected-rows nil))))
      (should (equal (org-babel-execute:mysql
                      "select id from demo"
                      '((:host . "127.0.0.1")
                        (:user . "root")
                        (:max-rows . "2")))
                     '(("id") hline (1) (2)))))))

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
