# Branch Protection and Deploy Policy

## Branch Protection (repository setting)

- Target branch: `master`
- Required status check candidate: `ep-gate`
- Require branches to be up to date before merging: **Yes**

This document is a source-of-truth policy note for reviewers/operators.
Actual branch protection must be enforced in GitHub repository settings.

## Deploy Policy

- Deploy workflow: `.github/workflows/deploy.yml`
- Scope: control-plane only
- Worker fleet rollout: not allowed in this workflow
- DB migrations: not allowed in this workflow
- Environment approval: required through GitHub environment `control-plane-production`

## Current Mode

Current deploy mode is **B (protected scaffold)**.

Blocked prerequisites for mode A (real auto deploy):
- missing deployment target
- missing self-hosted runner labels
- missing deployment secrets
- missing environment protection/approval wiring in GitHub settings
