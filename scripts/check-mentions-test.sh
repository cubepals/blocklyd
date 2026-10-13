#!/bin/sh
# SPDX-FileCopyrightText: 2026 The Cubepals Authors
#
# SPDX-License-Identifier: FSL-1.1-ALv2

# Tests for check-mentions.sh: what GitHub would link, what it wouldn't, and the script as the
# hook and CI run it.
# Backticks in single quotes are the text under test, not a command to expand.
# shellcheck disable=SC2016
set -eu

check=$(dirname "$0")/check-mentions.sh
failed=0
# Not a bot, unless a case says so.
GIT_AUTHOR_NAME='A Person'
GIT_AUTHOR_EMAIL=person@example.com
export GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL

# expect MENTIONS TEXT: check-mentions.sh, given TEXT as a pull request body, reports exactly
# MENTIONS ("@a, @b"), or passes when MENTIONS is empty.
expect() {
  if out=$(PR_TITLE='' PR_BODY=$2 "$check" --pr 2>&1); then
    got=''
  else
    got=$(printf '%s\n' "$out" | sed -n 's/^Pull request mentions \(.*\)\.$/\1/p')
    [ -n "$got" ] || got="(failed without a list: $out)"
  fi
  if [ "$got" != "$1" ]; then
    printf 'expected [%s], got [%s]:\n%s\n\n' "$1" "$got" "$2" >&2
    failed=1
  fi
}

# A selector even inside backticks, since commits don't render markdown.
expect '@s' 'One `execute as <player> at @s unless block` check'
expect '@p' '@p joined'
expect '@octocat, @cubepals/devs' 'ask (@octocat) or @cubepals/devs'
expect '@blockly/db' 'import from `@blockly/db`'
expect '@s' 'Runs `execute at @s` twice: @s, then @s again'
expect '@b' 'a[@b'
expect '@s' '“@s”'
expect '@s' "$(printf 'A title\r\n@s\r\n')"

# Addresses, trailers and git comments.
expect '' 'Write to hello@cubepals.com'
expect '' 'Co-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>'
expect '' '# Please enter the commit message @ignored'
expect '' 'a_@b a.@b a%@b a+@b a-@b a]@b a\@b'
expect '@a' '@@a and @a@b'
# A bracket a first pass leaves behind still hides what is inside it.
expect '' '<a <b> @c>'

# Text without any.
expect '' 'Keep take them back off magma, cactus and snow'
expect '' 'costs 5 @ 3 each'

# A commit message in a file, as lefthook passes it.
dir=$(mktemp -d)
printf 'Check the block under the player\n\nRuns `execute at @s` each time.\n' > "$dir/bad"
printf 'Check the block under the player\n\nRuns the s selector each time.\n' > "$dir/good"
if "$check" "$dir/bad" 2> "$dir/err"; then
  echo 'expected a file mentioning @s to fail' >&2
  failed=1
elif ! grep -q '@s' "$dir/err"; then
  echo 'expected the refusal to name @s' >&2
  failed=1
fi
"$check" "$dir/good" || { echo 'expected a file without mentions to pass' >&2; failed=1; }
GIT_AUTHOR_NAME='dependabot[bot]' "$check" "$dir/bad" ||
  { echo "expected a bot's commit to pass" >&2; failed=1; }
rm -f "$dir/bad" "$dir/good" "$dir/err"
rmdir "$dir"

# A pull request's title and body, from the environment.
PR_TITLE='Fix spawn' PR_BODY='thanks @someone' "$check" --pr 2>/dev/null &&
  { echo 'expected a body mentioning @someone to fail' >&2; failed=1; }
PR_TITLE='Thank @someone' PR_BODY='' "$check" --pr 2>/dev/null &&
  { echo 'expected a title mentioning @someone to fail' >&2; failed=1; }
PR_TITLE='Fix spawn' PR_BODY='mail hello@cubepals.com' "$check" --pr ||
  { echo 'expected a body with only an address to pass' >&2; failed=1; }

# Without an argument it says how to run it.
code=0
"$check" 2>/dev/null || code=$?
[ "$code" = 2 ] || { echo "expected exit 2 without an argument, got $code" >&2; failed=1; }

[ "$failed" = 0 ] && echo 'check-mentions.sh: all cases pass'
exit "$failed"
