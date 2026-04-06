;;; ob-mysql.el --- Org-Babel MySQL compatibility shim -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
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

;; Org-Babel MySQL support via clutch.
;; Loads `ob-clutch', which defines `org-babel-execute:mysql'.

;;; Code:

(require 'ob-clutch)

(provide 'ob-mysql)
;;; ob-mysql.el ends here
