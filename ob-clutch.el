;;; ob-clutch.el --- Org-Babel integration for clutch database client -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1") (clutch "0.5.2"))
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

;; Org-Babel backend for MySQL/PostgreSQL/SQLite/JDBC databases via clutch.
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
;;   :ssh-host :tramp
;;   :max-rows                  most data rows returned (default 1000)
;;   :async                     yes to run without blocking Emacs
;;
;; JDBC backends (oracle, sqlserver, db2, snowflake, redshift) require
;; clutch-db-jdbc.el to be loaded and clutch-jdbc-agent.jar to be available.
;; Use :connection to reference a pre-configured clutch-connection-alist entry,
;; or supply :host/:port/:user/:database inline.  Port defaults to the JDBC
;; driver default when omitted.

;;; Code:

(require 'ob)
(require 'cl-lib)
(require 'clutch)
(require 'clutch-backend)

(defgroup ob-clutch nil
  "Org-Babel integration for clutch database backends."
  :group 'org-babel
  :prefix "ob-clutch-")

(define-obsolete-variable-alias 'org-babel-clutch-max-rows
  'ob-clutch-max-rows "0.2.0")

(defcustom ob-clutch-max-rows 1000
  "Default maximum number of data rows returned by `ob-clutch'.
Nil means unlimited.  A source block can override this with the
`:max-rows' header argument.  Each row costs a fraction of a
millisecond to insert into the Org buffer, so a few tens of thousands
of rows already make Emacs unresponsive for seconds."
  :type '(choice (const :tag "Unlimited" nil)
                 (natnum :tag "Rows"))
  :group 'ob-clutch)

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
    (ssh-host . :any)
    (tramp . :any)
    (tramp-default-directory . :any)
    (url . :any)
    (driver-class . :any)
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
    (max-rows . :any)
    (async . :any))
  "Clutch-specific header arguments for source blocks.")

(defvar ob-clutch--connection-cache (make-hash-table :test 'equal)
  "Cache of live DB connections keyed by backend+connection parameters.")

(defconst ob-clutch--transport-inline-param-keys
  '(:ssh-host :tramp :tramp-default-directory)
  "Transport keys accepted by inline Org-Babel connection params.")

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
  '(:host :port :user :password :database :url :driver-class :sid :schema :catalog
          :manual-commit :connect-timeout :read-idle-timeout :query-timeout
          :rpc-timeout)
  "Supported inline JDBC connection keys.")

(defconst ob-clutch--numeric-header-keys
  '(:port :connect-timeout :read-idle-timeout :query-timeout :rpc-timeout)
  "Header arguments that should be coerced to natural numbers.")

(defconst ob-clutch--boolean-header-keys
  '(:tls :manual-commit)
  "Header arguments that should be coerced to booleans.")

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
  (append
   (pcase backend
     ('mysql ob-clutch--mysql-inline-param-keys)
     ('pg ob-clutch--pg-inline-param-keys)
     ('sqlite ob-clutch--sqlite-inline-param-keys)
     (_ ob-clutch--jdbc-inline-param-keys))
   ob-clutch--transport-inline-param-keys))

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
     ((null value) ob-clutch-max-rows)
     ((and (stringp value) (string= (downcase value) "nil")) nil)
     (t (ob-clutch--parse-natnum value :max-rows)))))

(defun ob-clutch--limited-sql (conn sql max-rows)
  "Return SQL for CONN limited to one row more than MAX-ROWS.
The extra row shows that the result was cut.  SQL that does not select,
or has a row limit of its own, and a nil MAX-ROWS leave SQL unchanged."
  (if (and max-rows (clutch-db-sql-pageable-query-p sql))
      (clutch-db-build-paged-sql conn sql 0 (1+ max-rows))
    sql))

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

(defun ob-clutch--resolve-connection (params default-backend)
  "Return a Clutch connection plist from Babel PARAMS.
DEFAULT-BACKEND is used by language-specific executors."
  (if-let* ((conn-name (cdr (assq :connection params))))
      (let* ((plist (or (clutch-saved-connection-params conn-name)
                        (user-error "Unknown connection: %s" conn-name)))
             (backend (clutch-backend-normalize
                       (or (plist-get plist :backend) default-backend 'mysql))))
        (plist-put plist :backend backend))
    (let* ((backend-sym (or (cdr (assq :backend params)) default-backend))
           (backend (or (and backend-sym
                             (clutch-backend-normalize backend-sym))
                        (user-error
                         "Missing :backend for clutch block (or use :connection to reference a saved connection)")))
           (conn-params (ob-clutch--inline-params params backend)))
      (when-let* ((pass-entry (cdr (assq :pass-entry params))))
        (setq conn-params (plist-put conn-params :pass-entry pass-entry)))
      (plist-put conn-params :backend backend))))

(defun ob-clutch--connect (params default-backend)
  "Get or create a cached Clutch connection for PARAMS using DEFAULT-BACKEND."
  (let* ((conn-params (ob-clutch--resolve-connection params default-backend))
         (sqlite-p (eq (plist-get conn-params :backend) 'sqlite))
         (conn-params (if sqlite-p
                          (clutch-prepare-connection-params
                           conn-params default-directory)
                        conn-params))
         (source-origin (file-remote-p default-directory))
         (key (format "%S:%S" conn-params source-origin))
         (cached (gethash key ob-clutch--connection-cache)))
    (if (and cached (clutch-db-live-p cached))
        cached
      (let* ((prepared-params
              (if sqlite-p conn-params
                (clutch-prepare-connection-params conn-params default-directory)))
             (conn (clutch-open-connection prepared-params)))
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

(defun ob-clutch--result-table (result max-rows)
  "Return the Org value for the `clutch-db-result' RESULT cut to MAX-ROWS."
  (let* ((columns (clutch-db-result-columns result))
         (rows (clutch-db-result-rows result))
         (cut (and max-rows (> (length rows) max-rows))))
    (if columns
        (let ((col-names (mapcar (lambda (c)
                                   (let ((name (plist-get c :name)))
                                     (if (stringp name) name (format "%s" name))))
                                 columns))
              (data (mapcar (lambda (row)
                              (mapcar #'ob-clutch--format-value row))
                            (if cut (cl-subseq rows 0 max-rows) rows))))
          (append (list col-names 'hline) data
                  (when cut
                    (list 'hline
                          (list (format "First %d rows shown, more follow"
                                        max-rows))))))
      (format "Affected rows: %s"
              (or (clutch-db-result-affected-rows result) 0)))))

;;;; Asynchronous blocks

(cl-defstruct (ob-clutch--job (:constructor ob-clutch--make-job))
  "A block executed with `:async yes' that has not finished."
  conn sql max-rows
  marker                                ; start of the source block
  id                                    ; text that tells its result apart
  result-params
  (state 'queued))                      ; `queued' or `running'

(defvar ob-clutch--jobs nil
  "Unfinished jobs, oldest first.
A connection runs its jobs one at a time, since it refuses other work
while a statement runs.")

(defvar ob-clutch--job-counter 0
  "Number of jobs created, which numbers the next one.")

(defun ob-clutch--with-placeholder (job fn)
  "Call FN with the bounds of JOB's placeholder row, in its block's buffer.
Do nothing when the buffer is gone or the result no longer is that row,
alone unless `:results' appends or prepends: an edited result, or a newer
run of the block, is left as it is."
  (when-let* ((buffer (marker-buffer (ob-clutch--job-marker job)))
              ((buffer-live-p buffer)))
    (with-current-buffer buffer
      (save-excursion
        (goto-char (ob-clutch--job-marker job))
        (when-let* ((start (org-babel-where-is-src-block-result))
                    (end (save-excursion (goto-char start) (org-babel-result-end)))
                    (body (save-excursion
                            (goto-char start)
                            (when (looking-at "[ \t]*#\\+RESULTS.*\n")
                              (goto-char (match-end 0)))
                            (point)))
                    (row (format "| *\\(?:Queued\\|Running\\) %s\\(?: *|\\)+"
                                 (regexp-quote (ob-clutch--job-id job))))
                    (bounds (save-excursion
                              (goto-char start)
                              (and (re-search-forward (concat "^" row "$") end t)
                                   (cons (match-beginning 0) (match-end 0)))))
                    ((or (cl-intersection '("append" "prepend")
                                          (ob-clutch--job-result-params job)
                                          :test #'equal)
                         (string-match-p
                          (concat "\\`" row "\\'")
                          (string-trim
                           (buffer-substring-no-properties body end))))))
          (funcall fn (car bounds) (cdr bounds)))))))

(defun ob-clutch--job-status (job)
  "Show in JOB's placeholder whether the job is running or queued."
  (ob-clutch--with-placeholder
   job (lambda (beg end)
         (goto-char beg)
         (delete-region beg end)
         (insert (format "| %s %s |"
                         (if (eq (ob-clutch--job-state job) 'running)
                             "Running" "Queued")
                         (ob-clutch--job-id job))))))

(defun ob-clutch--job-show (job value)
  "Show VALUE as the result of JOB's block, in place of its placeholder."
  (ob-clutch--with-placeholder
   job (lambda (beg end)
         (delete-region beg (min (point-max) (1+ end)))
         (goto-char (ob-clutch--job-marker job))
         (org-babel-insert-result value (ob-clutch--job-result-params job)
                                  (org-babel-get-src-block-info 'light)))))

(defun ob-clutch--job-start (job)
  "Start JOB's statement without blocking Emacs and return non-nil.
Return nil, starting nothing, when its backend cannot do that."
  (setf (ob-clutch--job-state job) 'running)
  (clutch-db-query-async
   (ob-clutch--job-conn job) (ob-clutch--job-sql job)
   (lambda (result error) (ob-clutch--job-finish job result error))))

(defun ob-clutch--start-next (conn)
  "Start the oldest job waiting for CONN, if any.
A job that fails to start shows the error and the job after it starts."
  (when-let* ((next (cl-find conn ob-clutch--jobs :key #'ob-clutch--job-conn)))
    (setf (ob-clutch--job-state next) 'running)
    (ob-clutch--job-status next)
    (condition-case err
        (ob-clutch--job-start next)
      (error (ob-clutch--job-finish next nil err)))))

(defun ob-clutch--job-finish (job result error)
  "Show RESULT, or ERROR when it is non-nil, for JOB and start the next job."
  (setq ob-clutch--jobs (delq job ob-clutch--jobs))
  (unwind-protect
      (ob-clutch--job-show
       job (if error
               (format "Error: %s" (error-message-string error))
             (ob-clutch--result-table result (ob-clutch--job-max-rows job))))
    (ob-clutch--start-next (ob-clutch--job-conn job))))

(defun ob-clutch--run-async (conn sql max-rows params)
  "Run SQL on CONN without blocking Emacs and return its placeholder result.
The block gets the real result when the statement finishes.  A statement
for a connection that an earlier block still uses waits for it, since the
connection refuses other work meanwhile.  A backend that cannot run SQL
without blocking returns the result at once.  MAX-ROWS limits the rows
and PARAMS are the block's header arguments."
  (let* ((job (ob-clutch--make-job
               :conn conn :sql sql :max-rows max-rows
               :marker (save-excursion
                         (goto-char (org-babel-where-is-src-block-head))
                         (point-marker))
               :id (format "ob-clutch-%d" (cl-incf ob-clutch--job-counter))
               :result-params (cdr (assq :result-params params))))
         (waiting (cl-find conn ob-clutch--jobs :key #'ob-clutch--job-conn)))
    (if (or waiting (ob-clutch--job-start job))
        (progn
          (setq ob-clutch--jobs (append ob-clutch--jobs (list job)))
          (list (list (format "%s %s" (if waiting "Queued" "Running")
                              (ob-clutch--job-id job)))))
      (ob-clutch--result-table (clutch-db-query conn sql) max-rows))))

(defun ob-clutch-cancel-block-at-point ()
  "Cancel the asynchronous run of the source block at point.
A run that waits for the connection is dropped.  A running statement is
interrupted and its block shows the outcome when the server answers."
  (interactive)
  (let* ((head (or (org-babel-where-is-src-block-head)
                   (user-error "Not in a source block")))
         (job (or (cl-find-if
                   (lambda (job)
                     (let ((marker (ob-clutch--job-marker job)))
                       (and (eq (marker-buffer marker) (current-buffer))
                            (= marker head))))
                   ob-clutch--jobs :from-end t)
                  (user-error "No asynchronous run of this block"))))
    (if (eq (ob-clutch--job-state job) 'queued)
        (progn (setq ob-clutch--jobs (delq job ob-clutch--jobs))
               (ob-clutch--job-show job "Cancelled"))
      (unless (clutch-db-interrupt-query (ob-clutch--job-conn job))
        (user-error "This connection cannot interrupt a statement")))))

(defun ob-clutch--execute (body params default-backend)
  "Execute BODY with Babel PARAMS using DEFAULT-BACKEND."
  (let* ((conn (ob-clutch--connect params default-backend))
         (max-rows (ob-clutch--max-rows params))
         (sql (ob-clutch--limited-sql
               conn (org-babel-expand-body:generic body params) max-rows)))
    (if (ob-clutch--parse-boolean (cdr (assq :async params)) :async)
        (ob-clutch--run-async conn sql max-rows params)
      (ob-clutch--result-table (clutch-db-query conn sql) max-rows))))

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
