# Manage Issues

Use this procedure for issue operations: `create`, `list`, `comment`, `close`, `reopen`.

## Preconditions

- `gh` CLI is installed and authenticated.
- You have repository issue permissions.
- Required inputs for selected action are present.

## 1) Resolve target repository

Use `repo` input when provided (`owner/repo`).

If `repo` is not provided:
1. Read git remote: `git remote get-url origin`
2. Normalize to `owner/repo` (supports SSH and HTTPS remotes)
3. If still unresolved, stop and return `status=error`

## 2) Validate authentication and action inputs

```bash
gh auth status
```

- For `create`: require `title` and `body`.
- For `list`: default `state=open`, `limit=20`.
- For `comment`, `close`, `reopen`: require `number`.

## 3) Create issue (`action=create`) with duplicate detection

Search open issues before creating a new one:

```bash
gh issue list --repo <owner/repo> --state open --limit 50 --search "<title keywords> in:title"
```

- Matching rule: treat as duplicate when normalized titles are identical or one is a strict substring of the other.
- If duplicate found, return duplicate URL with `status=linked`.
- If no duplicate, create issue:

```bash
gh issue create --repo <owner/repo> --title "<title>" --body "<body>"
```

Optionally resolve created issue URL and number:

```bash
gh issue view --repo <owner/repo> "<title or url>" --json number,url
```

## 4) List issues (`action=list`)

```bash
gh issue list --repo <owner/repo> --state <state> --limit <n> --label "<label>"
```

## 5) Comment on issue (`action=comment`)

```bash
gh issue comment --repo <owner/repo> <number> --body "<comment>"
```

## 6) Close issue (`action=close`)

```bash
gh issue close --repo <owner/repo> <number>
```

## 7) Reopen issue (`action=reopen`)

```bash
gh issue reopen --repo <owner/repo> <number>
```

## Error handling

- `gh not authenticated`: run `gh auth login` and retry.
- `permission denied`: return `error` stating missing issue write/read permission.
- `rate limited`: return `error` with retry guidance and suggested backoff.
- `resource not found`: verify `owner/repo` and issue number, then return `error`.

## Return result

Return structured output:

- `status`: `created` | `linked` | `listed` | `commented` | `closed` | `reopened` | `error`
- `url`: issue URL when available, otherwise empty string
- `error`: short reason when `status=error`, otherwise empty string
