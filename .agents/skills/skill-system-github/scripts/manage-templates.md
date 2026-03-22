# Manage Templates

Use this procedure for issue template operations: `bootstrap`, `update`.

## Preconditions

- `gh` CLI is installed and authenticated.
- You have repository write permissions.
- File edits must use Read/Write tools for `.github/ISSUE_TEMPLATE/*` and must not use `gh` for file writes.

## 1) Resolve target repository

Use `repo` input when provided (`owner/repo`).

If `repo` is not provided:
1. Read git remote: `git remote get-url origin`
2. Normalize to `owner/repo` (supports SSH and HTTPS remotes)
3. If still unresolved, stop and return `status=error`

## 2) Validate authentication and local workspace

```bash
gh auth status
```

Ensure you are in the target repository workspace before writing files.

## 3) Bootstrap templates (`action=bootstrap`)

Create these files using file tools:
- `.github/ISSUE_TEMPLATE/bug_report.yml`
- `.github/ISSUE_TEMPLATE/feature_request.yml`
- `.github/ISSUE_TEMPLATE/config.yml`

Default `bug_report.yml` content:

```yaml
name: Bug Report
description: Report a reproducible problem
title: "[Bug]: "
labels: ["bug"]
body:
  - type: markdown
    attributes:
      value: "Thanks for filing a bug report."
  - type: textarea
    id: summary
    attributes:
      label: Summary
      description: What happened?
    validations:
      required: true
  - type: textarea
    id: steps
    attributes:
      label: Reproduction
      description: Steps to reproduce
    validations:
      required: true
```

Default `feature_request.yml` content:

```yaml
name: Feature Request
description: Propose an improvement
title: "[Feature]: "
labels: ["enhancement"]
body:
  - type: markdown
    attributes:
      value: "Thanks for sharing your idea."
  - type: textarea
    id: problem
    attributes:
      label: Problem
      description: What problem are you trying to solve?
    validations:
      required: true
  - type: textarea
    id: proposal
    attributes:
      label: Proposal
      description: What should change?
    validations:
      required: true
```

Default `config.yml` content:

```yaml
blank_issues_enabled: false
contact_links:
  - name: Questions and support
    url: https://github.com/<owner>/<repo>/discussions
    about: Ask and answer questions in discussions
```

## 4) Update templates (`action=update`)

Read existing target template first, then write updated YAML content to one of:
- `.github/ISSUE_TEMPLATE/bug_report.yml`
- `.github/ISSUE_TEMPLATE/feature_request.yml`
- `.github/ISSUE_TEMPLATE/config.yml`

Do not use `gh` commands to edit or upload template files.

## Error handling

- `gh not authenticated`: run `gh auth login` and retry.
- `permission denied`: return `error` stating missing repository write permission.
- `rate limited`: return `error` with retry guidance and suggested backoff.
- `resource not found`: verify repository path and template file path, then return `error`.

## Return result

Return structured output:

- `status`: `bootstrapped` | `updated` | `error`
- `url`: repository URL when available, otherwise empty string
- `error`: short reason when `status=error`, otherwise empty string
