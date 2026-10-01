# ob-clutch Development Guide

- This is an Org-Babel bridge for the clutch database client.
- Target Emacs 29.1+, Clutch's own baseline.
- Depends on the external `clutch` package. Keep `(clutch "X.Y")` in `Package-Requires` at the oldest Clutch release that provides everything ob-clutch uses, and raise it in the change that needs a newer one.
- Internal helpers use the `ob-clutch--` prefix.
- Org-Babel entry points use the standard `org-babel-` prefix.
- `ob-mysql.el`, `ob-postgresql.el`, and `ob-sqlite.el` are thin shims that only `(require 'ob-clutch)`.
- Do not stack adapter helpers around clutch rules. If `ob-clutch` needs password resolution, transport handling, connection preparation, or backend-specific behavior that clutch already owns, call clutch's public API instead of duplicating the logic here.
- Treat one-use helper ladders as structure debt. Inline trivial wrappers, prefer direct plist construction or table-driven mappings, and only extract a helper when it centralizes a real Org-Babel rule or is shared by multiple call paths.
- Byte-compiling `ob-clutch.el` must produce zero warnings.
- All public functions must have docstrings.
- `checkdoc` compliance is required.
- Run tests with:

```bash
emacs -batch -L . -L /path/to/clutch -l ert -l ob-clutch \
  -l test/ob-clutch-test.el \
  --eval '(ert-run-tests-batch-and-exit)'
```

## Releases

- Record user-visible changes in `CHANGELOG.md` under `Unreleased` in the same change; pure test or internal cleanup needs no entry.
- Only a release changes `;; Version:`, moves the `Unreleased` entries under the new version, and is tagged `vX.Y.Z`.
- ob-clutch has not been released: it is not on MELPA and no version has been tagged.
