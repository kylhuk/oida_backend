# Code Review Gate

Review priorities:

1. Correctness and production regressions.
2. Security issues, especially auth, secrets, and exposed ports.
3. Missing tests or weak verification evidence.
4. Operational gaps in compose, backup/restore, monitoring, and docs.
5. Stale documentation that contradicts runtime behavior.

The review gate must run after `./scripts/verify.sh` and before merging.
