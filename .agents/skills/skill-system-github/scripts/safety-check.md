# Safety Check

Use this procedure before writing or pushing `.github/workflows/*` changes.

## Preconditions

- `gh` CLI is installed and authenticated.
- You have repository access.
- You are about to create or update workflow files.

## 1) Resolve target repository

Use `repo` input when provided (`owner/repo`).

If `repo` is not provided:
1. Read git remote: `git remote get-url origin`
2. Normalize to `owner/repo` (supports SSH and HTTPS remotes)
3. If still unresolved, stop and return `status=error`

## 2) Check authentication and token scopes

```bash
gh auth status
```

Parse auth output for OAuth/PAT scopes and detect whether `workflow` scope is present.

## 3) Decision tree for workflow writes

- If `workflow` scope exists: choose `git push` path.
- If `workflow` scope is missing: choose `gh api` fallback and warn user to update PAT scopes.

Warning text should explicitly mention missing `workflow` scope.

## 4) API fallback procedure (when `workflow` scope is missing)

Base64 encode workflow content and write through GitHub Contents API:

```bash
gh api repos/<owner>/<repo>/contents/.github/workflows/<file> \
  --method PUT \
  -f message="<commit message>" \
  -f content="<base64-content>" \
  -f branch="<branch>"
```

When updating an existing workflow file, first get the current blob SHA and include it in the PUT request:

```bash
gh api repos/<owner>/<repo>/contents/.github/workflows/<file>
gh api repos/<owner>/<repo>/contents/.github/workflows/<file> \
  --method PUT \
  -f message="<commit message>" \
  -f content="<base64-content>" \
  -f sha="<current-sha>" \
  -f branch="<branch>"
```

## Error handling

- `gh not authenticated`: run `gh auth login` and retry.
- `permission denied`: return `error` stating missing repository contents/workflow permissions.
- `rate limited`: return `error` with retry guidance and suggested backoff.
- `resource not found`: verify `owner/repo`, branch, and workflow file path, then return `error`.

## Return result

Return structured output:

- `status`: `ok` | `warn` | `error`
- `url`: repository URL when available, otherwise empty string
- `error`: short reason when `status=error`, otherwise empty string
