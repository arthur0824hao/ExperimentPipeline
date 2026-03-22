# Manage Labels

Use this procedure for label operations: `create`, `update`, `delete`, `apply`.

## Preconditions

- `gh` CLI is installed and authenticated.
- You have repository label and issue permissions.
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

- For `create`: require `name`; default `description` to empty and `color` to `ededed`.
- For `update`: require `name`; optional `new_name`, `description`, `color`.
- For `delete`: require `name` and explicit `confirm_delete=true`.
- For `apply`: require `number` and `labels` (comma-separated).

## 3) Create label (`action=create`)

```bash
gh label create "<name>" --repo <owner/repo> --description "<desc>" --color "<hex>"
```

## 4) Update label (`action=update`)

```bash
gh label edit "<name>" --repo <owner/repo> --new-name "<new>" --description "<desc>" --color "<hex>"
```

## 5) Delete label (`action=delete`)

Safety gate:
- Require explicit confirmation before delete.
- If confirmation is missing, stop and return `status=error`.

```bash
gh label delete "<name>" --repo <owner/repo> --yes
```

## 6) Apply labels to issue (`action=apply`)

```bash
gh issue edit --repo <owner/repo> <number> --add-label "<label1>,<label2>"
```

## Error handling

- `gh not authenticated`: run `gh auth login` and retry.
- `permission denied`: return `error` stating missing label or issue permissions.
- `rate limited`: return `error` with retry guidance and suggested backoff.
- `resource not found`: verify `owner/repo`, label name, and issue number, then return `error`.

## Return result

Return structured output:

- `status`: `created` | `updated` | `deleted` | `applied` | `error`
- `url`: repository or issue URL when available, otherwise empty string
- `error`: short reason when `status=error`, otherwise empty string
