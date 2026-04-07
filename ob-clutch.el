;;; ob-clutch.el --- Org-Babel integration for clutch database client -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1") (clutch "0.1"))
;; Keywords: languages, data
;; URL: https://github.com/LuciusChen/ob-clutch
;; SPDX-License-Identifier: GPL-3.0-or-later

;; This file is part of ob-clutch.

;; ob-clutch is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; ob-clutch is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with ob-clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Org-Babel backend for MySQL/PostgreSQL/SQLite/JDBC databases via clutch-db.
;;
;; Supported block types:
;;   #+begin_src mysql
;;   #+begin_src postgresql
;;   #+begin_src sqlite
;;
;; Optional generic block (supports all backends including JDBC):
;;   #+begin_src clutch :backend pg
;;   #+begin_src clutch :backend oracle
;;   #+begin_src clutch :backend sqlserver
;;
;; Header arguments:
;;   :connection                name from `clutch-connection-alist'
;;   :backend                   mysql|pg|postgresql|sqlite|oracle|sqlserver|...
;;   :host :port :user :password :database
;;
;; JDBC backends (oracle, sqlserver, db2, snowflake, redshift) require
;; clutch-db-jdbc.el to be loaded and clutch-jdbc-agent.jar to be available.
;; Use :connection to reference a pre-configured clutch-connection-alist entry,
;; or supply :host/:port/:user/:database inline.  Port defaults to the JDBC
;; driver default when omitted.

;;; Code:

(require 'ob)
(require 'auth-source)
(require 'cl-lib)
(require 'clutch-db)

(declare-function auth-source-pass-entries "auth-source-pass" ())
(declare-function auth-source-pass-parse-entry "auth-source-pass" (entry))

(defvar clutch-connection-alist)

(defgroup org-babel-clutch nil
  "Org-Babel integration for clutch database backends."
  :group 'org-babel
  :prefix "org-babel-clutch-")

(defcustom org-babel-clutch-max-rows nil
  "Default maximum number of data rows returned by `ob-clutch'.
Nil means unlimited.  A source block can override this with the
`:max-rows' header argument."
  :type '(choice (const :tag "Unlimited" nil)
                 (natnum :tag "Rows"))
  :group 'org-babel-clutch)

(defvar org-babel-default-header-args:clutch '((:results . "table"))
  "Default header arguments for clutch source blocks.")

(defvar org-babel-default-header-args:mysql '((:results . "table"))
  "Default header arguments for mysql source blocks.")

(defvar org-babel-default-header-args:postgresql '((:results . "table"))
  "Default header arguments for postgresql source blocks.")

(defvar org-babel-default-header-args:sqlite '((:results . "table"))
  "Default header arguments for sqlite source blocks.")

(defconst org-babel-header-args:clutch
  '((connection . :any)
    (backend . :any)
    (host . :any)
    (port . :any)
    (user . :any)
    (password . :any)
    (database . :any)
    (pass-entry . :any)
    (url . :any)
    (sid . :any)
    (schema . :any)
    (catalog . :any)
    (tls . :any)
    (ssl-mode . :any)
    (sslmode . :any)
    (manual-commit . :any)
    (connect-timeout . :any)
    (read-idle-timeout . :any)
    (query-timeout . :any)
    (rpc-timeout . :any)
    (max-rows . :any))
  "Clutch-specific header arguments for source blocks.")

(defvar ob-clutch--connection-cache (make-hash-table :test 'equal)
  "Cache of live DB connections keyed by backend+connection parameters.")

(defconst ob-clutch--meta-keys
  '(:backend :sql-product :pass-entry)
  "Connection plist keys not passed to backend connect functions.")

(defconst ob-clutch--jdbc-backends
  '(oracle oracle-8 oracle-11 sqlserver db2 snowflake redshift clickhouse)
  "Backend symbols handled by `clutch-db-jdbc'.")

(defconst ob-clutch--mysql-inline-param-keys
  '(:host :port :user :password :database :tls :ssl-mode
    :connect-timeout :read-idle-timeout)
  "Supported inline mysql connection keys.")

(defconst ob-clutch--pg-inline-param-keys
  '(:host :port :user :password :database :schema :tls :sslmode
    :connect-timeout :read-idle-timeout :query-timeout)
  "Supported inline PostgreSQL connection keys.")

(defconst ob-clutch--sqlite-inline-param-keys
  '(:database)
  "Supported inline SQLite connection keys.")

(defconst ob-clutch--jdbc-inline-param-keys
  '(:host :port :user :password :database :url :sid :schema :catalog
    :manual-commit :connect-timeout :read-idle-timeout :query-timeout
    :rpc-timeout)
  "Supported inline JDBC connection keys.")

(defconst ob-clutch--numeric-header-keys
  '(:port :connect-timeout :read-idle-timeout :query-timeout :rpc-timeout)
  "Header arguments that should be coerced to natural numbers.")

(defconst ob-clutch--boolean-header-keys
  '(:tls :manual-commit)
  "Header arguments that should be coerced to booleans.")

(defun ob-clutch--pass-secret-by-suffix (suffix)
  "Return pass secret from the first entry whose path ends with SUFFIX.
Matches, for example, `dev-mysql' against `mysql/dev-mysql'.  Returns
nil when no matching entry is found or auth-source-pass is absent."
  (when (and (fboundp 'auth-source-pass-entries)
             (fboundp 'auth-source-pass-parse-entry))
    (let* ((re (format "\\(^\\|/\\)%s$" (regexp-quote suffix)))
           (entry (cl-find-if (lambda (candidate)
                                (string-match-p re candidate))
                              (auth-source-pass-entries))))
      (when entry
        (cdr (assq 'secret (auth-source-pass-parse-entry entry)))))))

(defun ob-clutch--parse-natnum (value key)
  "Return VALUE parsed as a natural number for header KEY."
  (cond
   ((numberp value) value)
   ((and (stringp value)
         (string-match-p "\\`[0-9]+\\'" value))
    (string-to-number value))
   (t
    (user-error "Invalid %s value: %S" key value))))

(defun ob-clutch--parse-boolean (value key)
  "Return VALUE normalized to a boolean for header KEY."
  (cond
   ((memq value '(t yes)) t)
   ((null value) nil)
   ((stringp value)
    (pcase (downcase value)
      ((or "yes" "true" "t") t)
      ((or "no" "false" "nil") nil)
      (_ (user-error "Invalid %s value: %S" key value))))
   (t
    (user-error "Invalid %s value: %S" key value))))

(defun ob-clutch--coerce-header-value (key value)
  "Return Babel header VALUE normalized for connection KEY."
  (cond
   ((memq key ob-clutch--numeric-header-keys)
    (ob-clutch--parse-natnum value key))
   ((memq key ob-clutch--boolean-header-keys)
    (ob-clutch--parse-boolean value key))
   (t value)))

(defun ob-clutch--inline-param-keys (backend)
  "Return supported inline connection keys for BACKEND."
  (pcase backend
    ('mysql ob-clutch--mysql-inline-param-keys)
    ('pg ob-clutch--pg-inline-param-keys)
    ('sqlite ob-clutch--sqlite-inline-param-keys)
    (_ ob-clutch--jdbc-inline-param-keys)))

(defun ob-clutch--params-alist-to-plist (params keys)
  "Return Babel PARAMS as a plist containing only KEYS."
  (let (out)
    (dolist (key keys out)
      (when-let* ((entry (assq key params)))
        (setq out
              (append out
                      (list key
                            (ob-clutch--coerce-header-value key (cdr entry)))))))))

(defun ob-clutch--max-rows (params)
  "Return the effective :max-rows limit for Babel PARAMS."
  (let ((value (cdr (assq :max-rows params))))
    (cond
     ((null value) org-babel-clutch-max-rows)
     ((and (stringp value) (string= (downcase value) "nil")) nil)
     (t (ob-clutch--parse-natnum value :max-rows)))))

(defun ob-clutch--truncate-rows (rows params)
  "Return ROWS truncated according to Babel PARAMS.
When truncation happens, emit a message describing the number of hidden rows."
  (if-let* ((max-rows (ob-clutch--max-rows params)))
      (if (> (length rows) max-rows)
          (progn
            (message "ob-clutch truncated result from %d to %d rows"
                     (length rows) max-rows)
            (cl-subseq rows 0 max-rows))
        rows)
    rows))

(defun ob-clutch--resolve-password (params)
  "Resolve password for PARAMS via :password, pass, then auth-source."
  (let ((pw (plist-get params :password))
        (entry (plist-get params :pass-entry)))
    (cond
     ((and (stringp pw) (> (length pw) 0)) pw)
     (t
      (or (and entry (ob-clutch--pass-secret-by-suffix entry))
          (when-let* ((found (car (auth-source-search
                                   :host (plist-get params :host)
                                   :user (plist-get params :user)
                                   :port (plist-get params :port)
                                   :max 1)))
                      (secret (plist-get found :secret)))
            (if (functionp secret) (funcall secret) secret)))))))

(defun ob-clutch--inject-entry-name (params name)
  "Return PARAMS with :pass-entry defaulting to NAME when needed."
  (if (or (plist-get params :password) (plist-get params :pass-entry))
      params
    (append params (list :pass-entry name))))

(defun ob-clutch--normalize-backend (backend)
  "Normalize BACKEND string or symbol for `clutch-db-connect'.
Pure Elisp backends are canonicalized (mysql/pg/sqlite).
Any other symbol is passed through as-is; `clutch-db-connect' will signal
an error if the backend is truly unknown.  This allows JDBC driver symbols
such as oracle, sqlserver, db2, snowflake, and redshift to work without
ob-clutch needing to know about clutch-db-jdbc."
  (let ((sym (if (stringp backend)
                 (intern (downcase backend))
               backend)))
    (pcase sym
      ((or 'mysql 'mariadb) 'mysql)
      ((or 'pg 'postgres 'postgresql) 'pg)
      ('sqlite 'sqlite)
      (_ sym))))

(defun ob-clutch--plist-without-meta (plist)
  "Return copy of PLIST excluding keys in `ob-clutch--meta-keys'."
  (let (out)
    (while plist
      (let ((k (pop plist))
            (v (pop plist)))
        (unless (memq k ob-clutch--meta-keys)
          (setq out (append out (list k v))))))
    out))

(defun ob-clutch--inline-params (params backend)
  "Build inline connection params from Babel PARAMS for BACKEND."
  (let ((conn-params (ob-clutch--params-alist-to-plist
                      params (ob-clutch--inline-param-keys backend))))
    (pcase backend
      ('sqlite
       (unless (plist-get conn-params :database)
         (user-error "Missing :database for sqlite block"))
       conn-params)
      (_
       (unless (plist-get conn-params :user)
         (user-error "Missing :user (or use :connection)"))
       (unless (or (plist-get conn-params :url)
                   (plist-get conn-params :host))
         (pcase backend
           ('mysql
            (setq conn-params (plist-put conn-params :host "127.0.0.1")))
           ('pg
            (setq conn-params (plist-put conn-params :host "127.0.0.1")))))
       (unless (plist-get conn-params :port)
         (pcase backend
           ('mysql
            (setq conn-params (plist-put conn-params :port 3306)))
           ('pg
            (setq conn-params (plist-put conn-params :port 5432)))))
       conn-params))))

(defun ob-clutch--maybe-inject-password (backend conn-params source-params)
  "Return CONN-PARAMS with password injected unless BACKEND is sqlite.
SOURCE-PARAMS is the plist used for password resolution."
  (if (eq backend 'sqlite)
      conn-params
    (if-let* ((pw (ob-clutch--resolve-password source-params)))
        (plist-put conn-params :password pw)
      conn-params)))

(defun ob-clutch--guard-jdbc-pass-entry (backend source-params conn-params)
  "Fail early when JDBC BACKEND has an unresolved explicit :pass-entry.
Return CONN-PARAMS unchanged otherwise when SOURCE-PARAMS already
resolved a password."
  (when (and (memq backend ob-clutch--jdbc-backends)
             (plist-get source-params :pass-entry)
             (null (plist-get conn-params :password)))
    (user-error
     (concat "No password resolved for JDBC Org-Babel block %s (:pass-entry %s). "
             "Enable auth-source-pass/auth-source, or set :password explicitly")
     backend
     (plist-get source-params :pass-entry)))
  conn-params)

(defun ob-clutch--resolve-connection (params default-backend)
  "Return (BACKEND . CONN-PARAMS) from Babel PARAMS.
DEFAULT-BACKEND is used by language-specific executors."
    (if-let* ((conn-name (cdr (assq :connection params))))
      (let* ((entry (or (assoc conn-name clutch-connection-alist)
                        (user-error "Unknown connection: %s" conn-name)))
             (plist (copy-sequence (cdr entry)))
             (plist (ob-clutch--inject-entry-name plist conn-name))
             (backend (ob-clutch--normalize-backend
                       (or (plist-get plist :backend) default-backend 'mysql)))
             (conn-params (ob-clutch--plist-without-meta plist)))
        (cons backend
              (ob-clutch--guard-jdbc-pass-entry
               backend plist
               (ob-clutch--maybe-inject-password backend conn-params plist))))
    (let* ((backend-sym (or (cdr (assq :backend params)) default-backend))
           (backend (or (and backend-sym
                             (ob-clutch--normalize-backend backend-sym))
                        (user-error
                         "Missing :backend for clutch block (or use :connection to reference a saved connection)")))
           (conn-params (ob-clutch--inline-params params backend))
           (source-params (copy-sequence conn-params))
           (source-params (if-let* ((pass-entry (cdr (assq :pass-entry params))))
                              (plist-put source-params :pass-entry pass-entry)
                            source-params)))
      (cons backend
            (ob-clutch--guard-jdbc-pass-entry
             backend source-params
             (ob-clutch--maybe-inject-password backend conn-params source-params))))))

(defun ob-clutch--connect (params default-backend)
  "Get or create a cached `clutch-db' connection for PARAMS using DEFAULT-BACKEND."
  (pcase-let* ((`(,backend . ,conn-params)
                (ob-clutch--resolve-connection params default-backend))
               (key (format "%S:%S" backend conn-params))
               (cached (gethash key ob-clutch--connection-cache)))
    (if (and cached (clutch-db-live-p cached))
        cached
      (let ((conn (clutch-db-connect backend conn-params)))
        (puthash key conn ob-clutch--connection-cache)
        conn))))

(defun ob-clutch--format-value (val)
  "Format VAL for Org-Babel table output."
  (cond
   ((null val) "NULL")
   ((numberp val) val)           ; raw number for Org column alignment
   ((stringp val) val)
   ((listp val) (or (clutch-db-format-temporal val) (format "%S" val)))
   (t (format "%S" val))))

(defun ob-clutch--execute (body params default-backend)
  "Execute BODY with Babel PARAMS using DEFAULT-BACKEND."
  (let* ((conn (ob-clutch--connect params default-backend))
         (sql (org-babel-expand-body:generic body params))
         (result (clutch-db-query conn sql))
         (columns (clutch-db-result-columns result))
         (rows (ob-clutch--truncate-rows (clutch-db-result-rows result) params)))
    (if columns
        (let ((col-names (mapcar (lambda (c)
                                   (let ((name (plist-get c :name)))
                                     (if (stringp name) name (format "%s" name))))
                                 columns))
              (data (mapcar (lambda (row)
                              (mapcar #'ob-clutch--format-value row))
                            rows)))
          (cons col-names (cons 'hline data)))
      (format "Affected rows: %s"
              (or (clutch-db-result-affected-rows result) 0)))))

(defun org-babel-execute:clutch (body params)
  "Execute a generic clutch BODY with Babel PARAMS."
  (ob-clutch--execute body params
                      (cdr (assq :backend params))))

(defun org-babel-execute:mysql (body params)
  "Execute a MySQL BODY with Babel PARAMS."
  (ob-clutch--execute body params 'mysql))

(defun org-babel-execute:postgresql (body params)
  "Execute a PostgreSQL BODY with Babel PARAMS."
  (ob-clutch--execute body params 'pg))

(defun org-babel-execute:sqlite (body params)
  "Execute a SQLite BODY with Babel PARAMS."
  (ob-clutch--execute body params 'sqlite))

;;;; Cleanup on exit

(defun ob-clutch--disconnect-all ()
  "Disconnect all cached ob-clutch connections and clear the cache."
  (maphash (lambda (_key conn)
             (condition-case nil
                 (clutch-db-disconnect conn)
               (error nil)))
           ob-clutch--connection-cache)
  (clrhash ob-clutch--connection-cache))

(add-hook 'kill-emacs-hook #'ob-clutch--disconnect-all)

(provide 'ob-clutch)
;;; ob-clutch.el ends here
