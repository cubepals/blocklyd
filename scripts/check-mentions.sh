#!/bin/sh
# SPDX-FileCopyrightText: 2026 The Cubepals Authors
#
# SPDX-License-Identifier: FSL-1.1-ALv2

# Checks that a commit message or pull request names nobody by accident. GitHub turns `@name` into
# a mention and notifies whoever owns that name, and commits don't render markdown, so even a
# backticked Minecraft selector like `@s` pings a real account. Pull requests are squash-merged
# and the body becomes the commit's, so the title and body are held to the same rule.
#
#   scripts/check-mentions.sh FILE   the commit message in FILE (lefthook's commit-msg hook)
#   scripts/check-mentions.sh --pr   the pull request's title and body, from PR_TITLE and PR_BODY (CI)
#
# Lines starting with `#` (git's comments) are dropped. Email addresses and anything inside `<...>`
# (trailers like Co-authored-by) aren't mentions. Commits by a bot pass as they are. Cubepals'
# scripts/check-mentions.ts is the same check; this one keeps to sh, sed and grep. It reads bytes,
# so any character outside ASCII before an at sign counts as punctuation, not a letter: a curly
# quote before `@s` is caught, and a rare accented letter before one is refused too.
set -eu

case ${1:-} in
  '')
    echo 'usage: check-mentions.sh FILE | --pr' >&2
    exit 2 ;;
  --pr)
    what='Pull request'
    text=$(printf '%s\n%s\n' "${PR_TITLE:-}" "${PR_BODY:-}") ;;
  *)
    case $(git var GIT_AUTHOR_IDENT 2>/dev/null || true) in
      *'[bot]'*) exit 0 ;;
    esac
    what='Commit message'
    text=$(cat "$1") ;;
esac

# `@name` or `@org/team` where GitHub would link it: not after a letter, digit, `_`, `.`, `%`, `+`,
# `-`, `@`, `]` or a backslash. Each line gets a leading space so a mention can open it, and the
# character before the at sign is cut off again. Brackets are stripped until none is left, so a
# bracket left by one pass can't hide a mention from the next.
found=$(printf '%s\n' "$text" | LC_ALL=C sed -e '/^#/d' -e ':strip' -e 's/<[^<>]*>/ /' -e 't strip' -e 's/^/ /' |
  LC_ALL=C grep -oE '[^]\[:alnum:]_.%+@-]@[A-Za-z0-9][A-Za-z0-9-]{0,38}(/[A-Za-z0-9_-]+)?' |
  LC_ALL=C sed 's/^.//' | awk '!seen[$0]++ { out = out (n++ ? ", " : "") $0 } END { printf "%s", out }')

if [ -n "$found" ]; then
  printf '%s mentions %s.\n' "$what" "$found" >&2
  echo 'GitHub would notify whoever owns that name, even inside backticks in a commit. Write it without the at sign, e.g. "the s selector".' >&2
  exit 1
fi
