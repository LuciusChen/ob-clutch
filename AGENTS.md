# ob-clutch Development Guide

- This is an Org-Babel bridge for the clutch database client.
- Target Emacs 28.1+.
- Depends on the external `clutch` package.
- Internal helpers use the `ob-clutch-` prefix.
- Org-Babel entry points use the standard `org-babel-` prefix.
- `ob-mysql.el`, `ob-postgresql.el`, and `ob-sqlite.el` are thin shims that only `(require 'ob-clutch)`.
- Byte-compiling `ob-clutch.el` must produce zero warnings.
- All public functions must have docstrings.
- `checkdoc` compliance is required.
- Run tests with:

```bash
emacs -batch -L . -L /path/to/clutch -l ert -l ob-clutch \
  -l test/ob-clutch-test.el \
  --eval '(ert-run-tests-batch-and-exit)'
```
