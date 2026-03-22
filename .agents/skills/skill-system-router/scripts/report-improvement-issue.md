# Report Improvement Issue

Use this procedure when you discover a concrete, repeatable improvement opportunity while routing skills.

## Preconditions

- `gh` CLI is installed and authenticated.
- You have enough context to describe the problem clearly.
- You have checked for duplicates in open issues.

## 1) Resolve target repository

Use `repo` input when provided (`owner/repo`).

If `repo` is not provided:
1. Read git remote: `git remote get-url origin`
2. Normalize to `owner/repo` (supports SSH and HTTPS remotes)
3. If still unresolved, stop and report that repo inference failed

## 2) Check duplicate issues

Search open issues before creating a new one:

```bash
gh issue list --repo <owner/repo> --state open --limit 50 --search "<title keywords> in:title"
```

- Matching rule: treat an issue as duplicate when normalized titles are identical or one is a strict substring of the other.
- If a matching issue exists, return that issue URL and mark status as `linked`.
- If no match exists, continue to creation.

## 3) Build issue content

Use this body template:

```markdown
## Summary
<summary>

## Impact
<impact or "Not provided.">

## Reproduction / Evidence
<repro_steps or concrete observations>

## Suggested Improvement
<suggested_fix or "Not provided.">

## Context
- Skill: skill-system-router
- Operation: report-improvement-issue
```

Rules:
- Keep claims factual and verifiable.
- Do not include secrets, credentials, or private user data.
- Do not paste full private transcripts; summarize minimally.

## 4) Create issue

```bash
gh issue create --repo <owner/repo> --title "<title>" --body "<rendered body>"
```

Then fetch the created issue number:

```bash
gh issue view --repo <owner/repo> "<title or url>" --json number,url
```

## 5) Return result

Return structured output to the caller:

- `status`: `created` or `linked`
- `issue_url`: full GitHub issue URL
- `issue_number`: integer issue number

Failure shape (when preconditions or repo resolution fail):

- `status`: `error`
- `error`: short reason string
- `issue_url`: empty string
- `issue_number`: `-1`
