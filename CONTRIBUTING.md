# Branching policy: feature --> dev --> prod

1. Cut a feature branch from `dev`: `git checkout -b feature/<short-name> dev`
2. Commit work in small, reviewable chunks.
3. Open a PR `feature/<short-name>` --> `dev`. Requires 1 review + green CI.
4. Merge with `--no-ff` so the feature is visible as a single unit in history.
5. Once `dev` is stable and ready to ship, open a PR `dev` --> `prod`.
6. Merge `dev` --> `prod` with `--no-ff` and tag the merge commit
   (`git tag -a vX.Y.Z -m "..."`).
7. Deploy `prod` to the production Airflow environment (DAG sync picks up
   `dags/` automatically).

Never commit straight to `dev` or `prod`. Hotfixes get their own
`hotfix/<name>` branch off `prod`, merged back into both `prod` and `dev`.
