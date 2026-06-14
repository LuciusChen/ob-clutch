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
  "Test backend alias normalization."
  (should (eq (ob-clutch--normalize-backend 'mysql) 'mysql))
  (should (eq (ob-clutch--normalize-backend 'mariadb) 'mysql))
  (should (eq (ob-clutch--normalize-backend 'postgresql) 'pg))
  (should (eq (ob-clutch--normalize-backend "POSTGRES") 'pg))
  (should (eq (ob-clutch--normalize-backend 'sqlite) 'sqlite))
  (should (eq (ob-clutch--normalize-backend 'mongodb) 'mongodb))
  (should (eq (ob-clutch--normalize-backend 'mongo) 'mongodb))
  (should (eq (ob-clutch--normalize-backend "Redis") 'redis)))

(ert-deftest ob-clutch-test-language-shims-load ()
  "MongoDB and Redis language shims should load the shared implementation."
  (should (require 'ob-mongodb nil t))
  (should (require 'ob-redis nil t))
  (should (fboundp 'org-babel-execute:mongodb))
  (should (fboundp 'org-babel-execute:redis)))

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
  "Saved connections without :backend should follow clutch's mysql default."
  (let ((clutch-connection-alist
         '(("dev" . (:host "127.0.0.1" :port 3306 :user "root")))))
    (let ((conn-params
          (ob-clutch--resolve-connection '((:connection . "dev")) nil)))
      (should (eq (plist-get conn-params :backend) 'mysql))
      (should (equal (plist-get conn-params :host) "127.0.0.1")))))

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

(ert-deftest ob-clutch-test-resolve-connection-mongodb-inline-no-auth ()
  "Inline MongoDB params should not require SQL-style user auth."
  (let ((conn-params
         (ob-clutch--resolve-connection
          '((:backend . mongodb)
            (:host . "127.0.0.1")
            (:port . "27017")
            (:database . "app")
            (:surface . "sql-interface")
            (:auth-source . "admin"))
          nil)))
    (should (eq (plist-get conn-params :backend) 'mongodb))
    (should (equal (plist-get conn-params :host) "127.0.0.1"))
    (should (= (plist-get conn-params :port) 27017))
    (should (equal (plist-get conn-params :database) "app"))
    (should (equal (plist-get conn-params :surface) "sql-interface"))
    (should (equal (plist-get conn-params :auth-source) "admin"))
    (should-not (plist-member conn-params :user))))

(ert-deftest ob-clutch-test-resolve-connection-redis-inline-no-auth ()
  "Inline Redis params should not require SQL-style user auth."
  (let ((conn-params
         (ob-clutch--resolve-connection
          '((:host . "192.168.1.224")
            (:port . "6379")
            (:database . "0"))
          'redis)))
    (should (eq (plist-get conn-params :backend) 'redis))
    (should (equal (plist-get conn-params :host) "192.168.1.224"))
    (should (= (plist-get conn-params :port) 6379))
    (should (equal (plist-get conn-params :database) "0"))
    (should-not (plist-member conn-params :user))))

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

(ert-deftest ob-clutch-test-max-rows-uses-custom-default ()
  "Table results should respect `ob-clutch-max-rows' when no header is set."
  (let ((ob-clutch--connection-cache (make-hash-table :test 'equal))
        (ob-clutch-max-rows 1))
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
                  :rows '((1) (2))
                  :affected-rows nil))))
      (should (equal (org-babel-execute:mysql
                      "select id from demo"
                      '((:host . "127.0.0.1")
                        (:user . "root")))
                     '(("id") hline (1)))))))

(ert-deftest ob-clutch-test-mongodb-and-redis-executors-use-default-backends ()
  "Language-specific executors should supply MongoDB and Redis defaults."
  (let ((ob-clutch--connection-cache (make-hash-table :test 'equal))
        opened
        queries)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-prepare-connection-params)
               (lambda (params _source-default-directory) params))
              ((symbol-function 'clutch-open-connection)
               (lambda (params)
                 (push params opened)
                 (list :backend (plist-get params :backend))))
              ((symbol-function 'org-babel-expand-body:generic)
               (lambda (body _params) body))
              ((symbol-function 'clutch-db-query)
               (lambda (conn command)
                 (push (list (plist-get conn :backend) command) queries)
                 (make-clutch-db-result :columns nil :rows nil :affected-rows 1))))
      (should (equal (org-babel-execute:mongodb
                      "db.users.find({active: true})"
                      '((:database . "app")))
                     "Affected rows: 1"))
      (should (equal (org-babel-execute:redis "PING" nil)
                     "Affected rows: 1"))
      (setq opened (nreverse opened)
            queries (nreverse queries))
      (should (eq (plist-get (car opened) :backend) 'mongodb))
      (should (equal (plist-get (car opened) :database) "app"))
      (should-not (plist-member (car opened) :user))
      (should (eq (plist-get (cadr opened) :backend) 'redis))
      (should-not (plist-member (cadr opened) :user))
      (should (equal queries
                     '((mongodb "db.users.find({active: true})")
                       (redis "PING")))))))
;;; ob-clutch-test.el ends here
