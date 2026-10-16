# Changelog

Notable user-visible changes are recorded here.

## Unreleased

### Added

- `:async yes` runs a block without blocking Emacs. The block shows `Running ob-clutch-N` and gets its table or error when the statement ends; blocks on one connection queue and run in order, since a connection refuses other work while a statement runs. `M-x ob-clutch-cancel-block-at-point` drops a queued block or interrupts a running one. Backends that cannot run SQL without blocking Emacs run the block at once. Requires Clutch 0.5.2.

### Changed

- A block returns at most 1000 data rows by default (`ob-clutch-max-rows`, was unlimited), because inserting a large table into the Org buffer freezes Emacs. A `SELECT` without a row limit of its own now asks the server for one row more than the limit instead of fetching every row and cutting the result afterwards. A cut result ends with a line below a rule, `First N rows shown, more follow`, instead of only a message in the echo area; `:max-rows nil` still sends the SQL as written and returns every row.
- The customization group `org-babel-clutch` is now `ob-clutch`, and the option `org-babel-clutch-max-rows` is now `ob-clutch-max-rows`, following the package's prefix. The old option name keeps working as an obsolete alias, so existing settings carry over.
- ob-clutch now declares the Emacs and Clutch versions it already needed: Emacs 29.1, which Clutch itself requires, and Clutch 0.5.1, whose connection preparation resolves relative SQLite `:database` filenames against the block's directory.
