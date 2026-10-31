# Contributing

Thanks for helping. This repo is the code companion to the book
*Redis: The Complete Guide from Zero to Production*, so it has one rule that
most projects don't: **the code should keep matching what the book prints.**

## Welcome

- Fixes for scripts that fail on a newer Redis, redis-py, or node-redis release
- Fixes for scripts that fail on a particular OS (macOS, Linux, WSL, Docker)
- Clearer comments or error messages
- Typos in the README or comments

## Probably not a fit

- New examples or new topics. The repo follows the book's parts; new material
  belongs in a future edition. Feel free to open an issue suggesting it.
- Rewrites into a different style, framework, or language
- Version bumps with no failing test behind them. Versions are pinned to the
  ones the book was tested on, and the weekly test run shows when an upgrade
  is needed.

## How to make a change

1. Fork the repo and create a branch: `git checkout -b fix-part4-lua`.
2. Make the change.
3. Run the whole suite against a throwaway Redis 8:

   ```bash
   pip install -r requirements.txt
   npm install --prefix node
   bash tests/run_all.sh        # expect: passed: 14  failed: 0
   ```

   The runner needs `redis-server` and `redis-cli` on your PATH and a free
   port 6379. See the README for setup on each OS.
4. Open a pull request and fill in the checklist. The same tests run
   automatically on GitHub.

## Style

- Python: standard library style, short functions, comments that explain
  *why*. Every script cleans up only its own keys (see `python/common.py`).
- Shell: `bash`, `set -u`, every server on its own port and temp folder, and
  everything stopped at the end.
- Keep output human-readable. Readers compare it with the book line by line.

## Code of conduct

By taking part, you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
