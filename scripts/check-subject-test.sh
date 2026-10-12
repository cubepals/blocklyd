#!/bin/sh
# Tests for check-subject.sh: the limit, what breaks it, and what passes as it is.
set -eu

check=$(dirname "$0")/check-subject.sh
failed=0
# Not a bot, unless a case says so.
GIT_AUTHOR_NAME='A Person'
GIT_AUTHOR_EMAIL=person@example.com
export GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL

# pass|fail MESSAGE: check-subject.sh accepts or refuses MESSAGE, given on stdin.
expect() {
  if printf '%s\n' "$2" | "$check" 2>/dev/null; then got=pass; else got=fail; fi
  if [ "$got" != "$1" ]; then
    printf 'expected %s, got %s:\n%s\n\n' "$1" "$got" "$2" >&2
    failed=1
  fi
}

fifty=$(printf '%050d' 0 | tr 0 a)

expect pass 'Keep each node record in its own file'
expect pass "$fifty"
expect fail "${fifty}a"
expect pass "$(printf '%049d' 0 | tr 0 a)…"
expect fail "${fifty}…"
expect pass "$fifty   "
expect pass "Move: $(printf '%044d' 0 | tr 0 a)"
expect fail "Move: $(printf '%045d' 0 | tr 0 a)"
expect fail ''
expect fail '   '
expect fail 'Say what the hook checks.'
expect pass 'Say what the hook checks…'

# As the hook sees it: comments and blank lines before the subject, a body after it.
expect pass "$(printf '\n# A comment\n%s\n\nA body line that runs well past fifty characters, as bodies do.' "$fifty")"
expect fail "$(printf '# A comment\n%sa\n' "$fifty")"
expect fail '# Only a comment'

long="Take the subject's whole limit and then go well past it"
expect pass "Merge branch 'main' into claude/$long"
expect pass "Merge remote-tracking branch 'origin/main' into $long"
expect pass "Merge pull request #12 from cubepals/$long"
expect pass "Revert \"$long\""
expect pass "fixup! $long"
expect pass "squash! $long"
expect pass "amend! $long"
expect fail "Merge $long"
expect fail "Revert $long"

GIT_AUTHOR_NAME='dependabot[bot]' expect pass "Bump the cargo group with 3 updates across 2 directories"
GIT_AUTHOR_NAME='dependabot[bot]' expect pass 'Bump a crate.'

# A file, as lefthook passes it.
file=$(mktemp)
printf '%s\n' "$fifty" > "$file"
"$check" "$file" || { echo 'expected pass for a file' >&2; failed=1; }
printf '%sa\n' "$fifty" > "$file"
if "$check" "$file" 2>/dev/null; then echo 'expected fail for a file' >&2; failed=1; fi
rm -f "$file"

[ "$failed" = 0 ] && echo 'check-subject.sh: all cases pass'
exit "$failed"
