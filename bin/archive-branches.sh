#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
set -x

auth="-u kubuxu:$GH_TOKEN"
org=ipfs
repo=go-ipfs
api_repo="repos/$org/$repo"

gh_api_next() {
	links=$(grep '^Link:' | sed -e 's/Link: //' -e 's/, /\n/g')
	echo "$links" | grep '; rel="next"' >/dev/null || return
	link=$(echo "$links" | grep '; rel="next"' | sed -e 's/^<//' -e 's/>.*//')

	curl $auth -sD >(gh_api_next) $link
}

gh_api() {
	curl $auth -sD >(gh_api_next) "https://api.github.com/$1" | jq -s '[.[] | .[]]'
}

pr_branches() {
	gh_api "$api_repo/pulls" |  jq -r '.[].head.label | select(test("^ipfs:"))' \
		| sed 's/^ipfs://'
}

active_branches() {


}

pr_branches
