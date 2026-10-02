# Changelog

Notable user-visible changes are recorded here.

## Unreleased

### Changed

- The customization group `org-babel-clutch` is now `ob-clutch`, and the option `org-babel-clutch-max-rows` is now `ob-clutch-max-rows`, following the package's prefix. The old option name keeps working as an obsolete alias, so existing settings carry over.
- ob-clutch now declares the Emacs and Clutch versions it already needed: Emacs 29.1, which Clutch itself requires, and Clutch 0.5.1, whose connection preparation resolves relative SQLite `:database` filenames against the block's directory.
