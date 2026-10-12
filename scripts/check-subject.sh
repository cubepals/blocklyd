#!/bin/sh
# Checks a commit subject: at most 50 characters, not empty, no trailing period. GitHub cuts
# longer ones off in its file list, and main's subjects are pull request titles.
#
#   scripts/check-subject.sh FILE   the commit message in FILE (lefthook's commit-msg hook)
#   scripts/check-subject.sh        the subject on stdin (CI, with the pull request's title)
#
# Merge commits, `Revert "…"`, `fixup!`/`squash!`/`amend!` and commits by bots pass as they are.
set -eu

max=50

# The first line that isn't blank or a comment: git hands the hook the message before cleanup.
subject=$(sed -e '/^#/d' -e '/^[[:space:]]*$/d' -e 's/[[:space:]]*$//' "${1:--}" | sed -n 1p)

case $(git var GIT_AUTHOR_IDENT 2>/dev/null || true) in
  *'[bot]'*) exit 0 ;;
esac
case $subject in
  "Merge branch '"* | "Merge remote-tracking branch '"* | "Merge tag '"* | "Merge commit '"* | \
    'Merge pull request #'* | 'Revert "'* | 'fixup! '* | 'squash! '* | 'amend! '*)
    exit 0 ;;
esac

if [ -z "$subject" ]; then
  echo 'The subject is empty: say the change in a line of 50 characters or fewer.' >&2
  exit 1
fi
# Characters, not bytes: UTF-8 continuation bytes don't count.
length=$(printf '%s' "$subject" | LC_ALL=C tr -d '\200-\277' | wc -c | tr -d ' ')
if [ "$length" -gt "$max" ]; then
  printf 'The subject is %s characters, over %s: say the change, put the detail in the body.\n  %s\n' \
    "$length" "$max" "$subject" >&2
  exit 1
fi
case $subject in
  *.)
    printf 'The subject ends with a period; drop it.\n  %s\n' "$subject" >&2
    exit 1 ;;
esac
