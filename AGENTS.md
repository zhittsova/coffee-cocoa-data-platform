# Working agreements

Read design/README.md, design/architecture.md, design/data-contracts.md and
the relevant design/features/ file before making changes. Maintainers with
local docs/ and specs/ also read docs/working-agreements.md,
specs/decisions.md and specs/scope-v2.md. The project is in planning;
proposals are not accepted decisions.

- Keep dbt at the center of the transformation layer.
- Make the pipeline run on this local machine before adding deployment work.
- Use one change, one descriptive conventional-type branch, and one PR.
- Sign every commit and verify the signature. Never bypass signing on failure.
- Use unscoped conventional commit messages without parentheses. Keep messages
  to one line by default; a separate body, if needed, is at most one line.
- Add no AI trailers, AI co-author attribution, generated-by text, or branding.
- Keep PR bodies very brief and humanized: one outcome sentence and, if useful,
  one short validation line. Follow the github-pr-flow skill.
- User reviews/merges the first two PR-bearing sessions, currently S00B/S01.
  After their verified merges, review and squash-merge subsequent scoped PRs
  autonomously after relevant checks, without asking again. Local maintainers
  track the transition in specs/delivery-status.md and follow
  specs/execution-protocol.md when those files are present.
- Keep local main fast-forward-only.
- Do not commit directly to main without an explicit bootstrap exception.
- Manage Python, environments, dependencies, and Python tools with uv.
- Follow the user's git-branch-naming, github-pr-flow, and humanizer skills.
- Write direct, specific prose. Keep public documentation concise. design/ is the
  curated public-design path; local maintainers follow specs/publication-plan.md
  when that file is present.
- Define normal features in issues before implementation; one feature PR includes
  code, tests and durable spec updates. Separate material design decisions when useful.
- Keep AGENTS.md and CLAUDE.md tracked as public assistant guidance. docs/,
  specs/, requirements/, .agents/, .claude/ and .codex/ remain local until a
  scoped publication change; do not suppress them in the tracked .gitignore.
- Check both the staged paths and staged contents before committing or pushing.
- Never force-add excluded files or include them in container build contexts.
- Do not deploy cloud resources during the local development phase.

The root instructions apply to the entire repository. Local exclusions live in
.git/info/exclude and do not propagate to another clone.
