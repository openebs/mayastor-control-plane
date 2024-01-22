#!/usr/bin/env bash

# For a walk through of what this does, how it works,
# see: https://stackoverflow.com/a/68673744/1448212

# git show-branch supports 29 branches; reserve 1 for current branch
GIT_SHOW_BRANCH_MAX=28
CURRENT_BRANCH=${CURRENT_BRANCH:-$(git branch --show-current)}

##
# Given Params:
#   EXCEPT : $1
#   VALUES : $2..N
#
# Return all values except EXCEPT, in order.
#
function valuesExcept() {
    local except=$1 ; shift
    for value in "$@"; do
        if [[ "$value" != "$except" ]]; then
            echo $value
        fi
    done
}


##
# Given Params:
#   BASE_BRANCH : $1           : base branch; default is current branch
#   BRANCHES    : [ $2 .. $N ] : list of unique branch names (no duplicates);
#                                perhaps possible parents.
#                                Default is all branches except base branch.
#
# For the most recent commit in the commit history for BASE_BRANCH that is
# also in the commit history of at least one branch in BRANCHES: output all
# BRANCHES that share that commit in their commit history.
#
function nearestCommonBranches() {
    local BASE_BRANCH
    if [[ -z "${1+x}" || "$1" == '.' ]]; then
        BASE_BRANCH="$CURRENT_BRANCH"
    else
        BASE_BRANCH="$1"
    fi

    shift
    local -a CANDIDATES
    if [[ -z "${1+x}" ]]; then
        CANDIDATES=( $(git rev-parse --symbolic --branches) )
    else
        CANDIDATES=("$@")
    fi
    local BRANCHES=( $(valuesExcept "$BASE_BRANCH" "${CANDIDATES[@]}") )

    local BRANCH_COUNT=${#BRANCHES[@]}
    if (( $BRANCH_COUNT > $GIT_SHOW_BRANCH_MAX )); then
        echo "Too many branches: limit $GIT_SHOW_BRANCH_MAX" >&2
        exit 1
    fi

    local MAP=( $(git show-branch --topo-order "${BRANCHES[@]}" "$BASE_BRANCH" \
                      | tail -n +$(($BRANCH_COUNT+3)) \
                      | sed "s/ \[.*$//" \
                      | sed "s/ /_/g" \
                      | sed "s/*/+/g" \
                      | egrep '^_*[^_].*[^_]$' \
                      | head -n1 \
                      | sed 's/\(.\)/\1\n/g'
                ) )

    for idx in "${!BRANCHES[@]}"; do
        ## to include "merge", symbolized by '-', use
        ## ALT: if [[ "${MAP[$idx]}" != "_" ]]
        # if [[ "${MAP[$idx]}" == "+" ]]; then
        if [[ "${MAP[$idx]}" != "_" ]]; then
            echo "${BRANCHES[$idx]}"
        fi
    done
}

# Usage: gitr [ baseBranch [branchToConsider]* ]
#   baseBranch: '.' (no quotes needed) corresponds to default current branch
#   branchToConsider* : list of unique branch names (no duplicates);
#                        perhaps possible (bias?) parents.
#                        Default is all branches except base branch.
#nearestCommonBranches "${@}"~

##
# Given:
#   BASE_BRANCH : $1           : first param on every batch
#   BRANCHES    : [ $2 .. $N ] : list of unique branch names (no duplicates);
#                                perhaps possible parents
#                                Default is all branches except base branch.
#
# Output all BRANCHES that share that commit in their commit history.
#
function repeatBatchingUntilStableResults() {
    local BASE_BRANCH="$1"

    shift
    local -a CANDIDATES
    if [[ -z "${1+x}" ]]; then
        CANDIDATES=( $(git rev-parse --symbolic --branches --remotes=origin) )
    else
        CANDIDATES=("$@")
    fi
    local BRANCHES=( $(valuesExcept "$BASE_BRANCH" "${CANDIDATES[@]}") )

    local SIZE=$GIT_SHOW_BRANCH_MAX
    local COUNT=${#BRANCHES[@]}
    local LAST_COUNT=$(( $COUNT + 1 ))

    local NOT_DONE=1
    while (( $NOT_DONE && $COUNT < $LAST_COUNT )); do
        NOT_DONE=$(( $SIZE < $COUNT ))
        LAST_COUNT=$COUNT

        local -a BRANCHES_TO_BATCH=( "${BRANCHES[@]}" )
        local -a AGGREGATE=()
        while (( ${#BRANCHES_TO_BATCH[@]} > 0 )); do
            local -a BATCH=( "${BRANCHES_TO_BATCH[@]:0:$SIZE}" )
            AGGREGATE+=( $(nearestCommonBranches "$BASE_BRANCH" "${BATCH[@]}") )
            BRANCHES_TO_BATCH=( "${BRANCHES_TO_BATCH[@]:$SIZE}" )
        done
        BRANCHES=( "${AGGREGATE[@]}" )
        COUNT=${#BRANCHES[@]}
    done
    if (( ${#BRANCHES[@]} > $SIZE )); then
        echo "Unable to reduce candidate branches below MAX for git-show-branch" >&2
        echo "  Base Branch : $BASE_BRANCH" >&2
        echo "  MAX Branches: $SIZE" >&2
        echo "  Candidates  : ${BRANCHES[@]}" >&2
        exit 1
    fi
    echo "${BRANCHES[@]}"
}

#repeatBatchingUntilStableResults "$@"

##
# Given Params:
#   BASE_BRANCH : $1           : base branch
#   REGEXs      : $2 [ .. $N ] : regex(s)
#
# Output:
#   - git branches matching at least one of the regex params
#   - base branch is excluded from result
#   - order: branches matching the Nth regex will appear before
#            branches matching the (N+1)th regex.
#   - no duplicates in output
#
function expandUniqGitBranches() {
    local -A BSET[$1]=1
    shift

    local ALL_BRANCHES=$(git rev-parse --symbolic --branches --remotes=origin)
    for regex in "$@"; do
        for branch in $ALL_BRANCHES; do
            ## RE: -z ${BSET[$branch]+x ...  ; presumes ENV 'x' is not defined
            if [[ $branch =~ $regex && -z "${BSET[$branch]+x}" ]]; then
                echo "$branch"
                BSET[$branch]=1
            fi
        done
    done
}


##
# Params:
#   BASE_BRANCH: $1    : "." equates to the current branch;
#   REGEXS     : $2..N : regex(es) corresponding to other branhes to include
#
function findBranchesSharingFirstCommonCommit() {
    if [[ -z "$1" ]]; then
        echo "Usage: findBranchesSharingFirstCommonCommit ( . | baseBranch ) [ regex [ ... ] ]" >&2
        exit 1
    fi

    local BASE_BRANCH
    if [[ -z "${1+x}" || "$1" == '.' ]]; then
        BASE_BRANCH="$CURRENT_BRANCH"
        if [ "$CURRENT_BRANCH" = "" ]; then
          return
        fi
    else
        BASE_BRANCH="$1"
    fi

    shift
    local REGEXS
    if [[ -z "$1" ]]; then
        REGEXS=(".*")
    else
        REGEXS=("$@")
    fi

    local BRANCHES=( $(expandUniqGitBranches "$BASE_BRANCH" "${REGEXS[@]}") )

## nearestCommonBranches  can also be used here, if batching not used.
    repeatBatchingUntilStableResults "$BASE_BRANCH" "${BRANCHES[@]}"
}

##
# usage: gitr [ <basebranch> [ <regex> ]* ]
#
# where:
#    baseBranch: . (dot) equates to default; with no parameters
#                  default to current branch
#    regex     : regexes correspond to other branches to include
#
# findBranchesSharingFirstCommonCommit "$@"
