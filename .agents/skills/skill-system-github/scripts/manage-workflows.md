# Manage Workflows

Use this procedure for workflow operations: `list`, `check`, `create`, `update`.

## Preconditions

- `gh` CLI is installed and authenticated.
- You have repository actions/workflow permissions.
- File edits for `.github/workflows/*` must use file tools.

## 1) Resolve target repository

Use `repo` input when provided (`owner/repo`).

If `repo` is not provided:
1. Read git remote: `git remote get-url origin`
2. Normalize to `owner/repo` (supports SSH and HTTPS remotes)
3. If still unresolved, stop and return `status=error`

## 2) Validate authentication and inputs

```bash
gh auth status
```

- For `check`: require `workflow_name`.
- For `create`, `update`: require `workflow_file` and YAML `content`.

## 3) List workflows (`action=list`)

```bash
gh workflow list --repo <owner/repo>
```

## 4) Check recent workflow runs (`action=check`)

```bash
gh run list --repo <owner/repo> --workflow "<name>" --limit 5
```

## 5) Create or update workflow file (`action=create|update`)

Write YAML content to `.github/workflows/<workflow_file>` using file tools.

Before any push that includes workflow files, run `safety-check`.

If safety-check returns `git-push`, proceed with normal commit and push.
If safety-check returns `api-fallback`, skip push and apply `gh api` content update path.

If `git push` fails for workflow file changes due to token scope, warn that `workflow` scope is likely missing and suggest `gh api` fallback from `safety-check`.

## Error handling

- `gh not authenticated`: run `gh auth login` and retry.
- `permission denied`: return `error` stating missing actions/workflow permission.
- `rate limited`: return `error` with retry guidance and suggested backoff.
- `resource not found`: verify `owner/repo`, workflow name, and workflow path, then return `error`.

## Return result

Return structured output:

- `status`: `listed` | `checked` | `created` | `updated` | `error`
- `url`: workflow or run URL when available, otherwise empty string
- `error`: short reason when `status=error`, otherwise empty string
