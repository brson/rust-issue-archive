#!/bin/bash
# Fetch rust-lang/rust issues and PRs created before 2016-01-01

set -euo pipefail

REPO="rust-lang/rust"
ITEMS_DIR="items"
START_ISSUE="${1:-1}"
MAX_ISSUE="${2:-30664}"
CUTOFF_DATE="2016-01-01T00:00:00Z"
RATE_LIMIT_BUFFER=100

# Counters for progress
fetched=0
skipped_exists=0
skipped_date=0
skipped_404=0
errors=0

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Check rate limit and sleep if needed.
check_rate_limit() {
    local remaining="$1"
    local reset="$2"

    if [[ -n "$remaining" && "$remaining" -lt "$RATE_LIMIT_BUFFER" ]]; then
        local now
        now=$(date +%s)
        local sleep_time=$((reset - now + 5))
        if [[ "$sleep_time" -gt 0 ]]; then
            log "Rate limit low ($remaining remaining). Sleeping ${sleep_time}s until reset..."
            sleep "$sleep_time"
        fi
    fi
}

# Fetch with rate limit handling. Returns response body, sets global RATE_* vars.
fetch_api() {
    local endpoint="$1"
    local response
    local http_code
    local attempt=0
    local max_attempts=5

    while [[ $attempt -lt $max_attempts ]]; do
        # Fetch with headers
        response=$(gh api "$endpoint" --include 2>&1) || {
            local exit_code=$?
            # Check if rate limited (gh api returns non-zero on 403/429)
            if echo "$response" | grep -q "rate limit\|API rate limit"; then
                log "Rate limited. Checking reset time..."
                local reset_time
                reset_time=$(gh api rate_limit --jq '.resources.core.reset')
                local now
                now=$(date +%s)
                local sleep_time=$((reset_time - now + 10))
                if [[ "$sleep_time" -gt 0 ]]; then
                    log "Sleeping ${sleep_time}s for rate limit reset..."
                    sleep "$sleep_time"
                fi
                ((attempt++))
                continue
            fi
            # 404 is expected for deleted issues
            if echo "$response" | grep -q "404\|Not Found"; then
                echo "404"
                return 0
            fi
            log "API error (attempt $((attempt+1))): $response"
            ((attempt++))
            sleep $((2 ** attempt))
            continue
        }

        # Parse headers and body
        local headers body
        headers=$(echo "$response" | sed -n '1,/^\r*$/p')
        body=$(echo "$response" | sed '1,/^\r*$/d')

        # Extract rate limit info
        RATE_REMAINING=$(echo "$headers" | grep -i "x-ratelimit-remaining:" | awk '{print $2}' | tr -d '\r')
        RATE_RESET=$(echo "$headers" | grep -i "x-ratelimit-reset:" | awk '{print $2}' | tr -d '\r')

        # Check for rate limit in response
        http_code=$(echo "$headers" | head -1 | awk '{print $2}')
        if [[ "$http_code" == "403" || "$http_code" == "429" ]]; then
            local now
            now=$(date +%s)
            local sleep_time=$((RATE_RESET - now + 10))
            if [[ "$sleep_time" -gt 0 ]]; then
                log "Rate limited (HTTP $http_code). Sleeping ${sleep_time}s..."
                sleep "$sleep_time"
            fi
            ((attempt++))
            continue
        fi

        if [[ "$http_code" == "404" ]]; then
            echo "404"
            return 0
        fi

        if [[ "$http_code" != "200" ]]; then
            log "Unexpected HTTP $http_code (attempt $((attempt+1)))"
            ((attempt++))
            sleep $((2 ** attempt))
            continue
        fi

        # Proactively check rate limit
        check_rate_limit "$RATE_REMAINING" "$RATE_RESET"

        echo "$body"
        return 0
    done

    log "Failed after $max_attempts attempts for $endpoint"
    return 1
}

# Main loop
log "Starting fetch from issue $START_ISSUE to $MAX_ISSUE"
log "Cutoff date: $CUTOFF_DATE"

for ((i=START_ISSUE; i<=MAX_ISSUE; i++)); do
    padded=$(printf "%05d" "$i")
    item_file="$ITEMS_DIR/$padded.json"
    comments_file="$ITEMS_DIR/$padded-comments.json"

    # Skip if already fetched
    if [[ -f "$item_file" ]]; then
        ((skipped_exists++)) || true
        continue
    fi

    # Fetch issue/PR with retry on parse errors
    parse_attempts=0
    max_parse_attempts=3
    created_at=""

    while [[ $parse_attempts -lt $max_parse_attempts ]]; do
        response=$(fetch_api "repos/$REPO/issues/$i") || {
            ((errors++)) || true
            log "ERROR: Failed to fetch #$i"
            break
        }

        if [[ "$response" == "404" ]]; then
            ((skipped_404++)) || true
            break
        fi

        # Try to parse created_at
        if created_at=$(echo "$response" | jq -re '.created_at' 2>/dev/null); then
            break  # Success
        fi

        ((parse_attempts++)) || true
        if [[ $parse_attempts -lt $max_parse_attempts ]]; then
            log "WARNING #$i: jq parse failed, retry $parse_attempts/$max_parse_attempts in 5s..."
            sleep 5
        fi
    done

    # Skip if we couldn't get created_at
    if [[ -z "$created_at" ]]; then
        if [[ "$response" != "404" ]]; then
            log "ERROR #$i: jq failed parsing after $max_parse_attempts attempts, skipping"
            ((errors++)) || true
        fi
        continue
    fi
    if [[ "$created_at" > "$CUTOFF_DATE" || "$created_at" == "$CUTOFF_DATE" ]]; then
        ((skipped_date++)) || true
        continue
    fi

    # Determine type (issue or PR)
    has_pr=$(echo "$response" | jq 'has("pull_request")' 2>&1) || {
        log "ERROR #$i: jq failed checking pull_request, skipping"
        ((errors++)) || true
        continue
    }
    if [[ "$has_pr" == "true" ]]; then
        item_type="pr"
    else
        item_type="issue"
    fi

    # Add metadata and save
    if ! echo "$response" | jq --arg type "$item_type" '. + {"_meta": {"type": $type}}' > "$item_file"; then
        log "ERROR #$i: jq failed adding metadata, skipping"
        rm -f "$item_file"
        ((errors++)) || true
        continue
    fi
    ((fetched++)) || true

    # Fetch comments if any (with pagination)
    comment_count=$(echo "$response" | jq -r '.comments' 2>/dev/null) || comment_count=0
    if [[ "$comment_count" =~ ^[0-9]+$ && "$comment_count" -gt 0 ]]; then
        all_comments="[]"
        page=1
        while true; do
            comments_response=$(fetch_api "repos/$REPO/issues/$i/comments?per_page=100&page=$page") || {
                log "WARNING: Failed to fetch comments page $page for #$i"
                break
            }
            if [[ "$comments_response" == "404" ]]; then
                break
            fi
            if ! page_count=$(echo "$comments_response" | jq -e 'length'); then
                log "WARNING #$i: jq failed parsing comments length"
                break
            fi
            if [[ "$page_count" -eq 0 ]]; then
                break
            fi
            if ! all_comments=$(echo "$all_comments" "$comments_response" | jq -s 'add'); then
                log "WARNING #$i: jq failed merging comments"
                break
            fi
            if [[ "$page_count" -lt 100 ]]; then
                break
            fi
            ((page++)) || true
        done
        if [[ "$all_comments" != "[]" ]]; then
            echo "$all_comments" > "$comments_file"
        fi
    fi

    # Fetch timeline events (cross-references, commits, label changes, etc.)
    timeline_file="$ITEMS_DIR/$padded-timeline.json"
    if [[ ! -f "$timeline_file" ]]; then
        all_timeline="[]"
        page=1
        while true; do
            timeline_response=$(fetch_api "repos/$REPO/issues/$i/timeline?per_page=100&page=$page") || {
                log "WARNING: Failed to fetch timeline page $page for #$i"
                break
            }
            if [[ "$timeline_response" == "404" ]]; then
                break
            fi
            if ! page_count=$(echo "$timeline_response" | jq -e 'length'); then
                log "WARNING #$i: jq failed parsing timeline length"
                break
            fi
            if [[ "$page_count" -eq 0 ]]; then
                break
            fi
            if ! all_timeline=$(echo "$all_timeline" "$timeline_response" | jq -s 'add'); then
                log "WARNING #$i: jq failed merging timeline"
                break
            fi
            if [[ "$page_count" -lt 100 ]]; then
                break
            fi
            ((page++)) || true
        done
        if [[ "$all_timeline" != "[]" ]]; then
            echo "$all_timeline" > "$timeline_file"
        fi
    fi

    # Progress report
    if (( i % 100 == 0 )); then
        log "Progress: $i/$MAX_ISSUE | fetched=$fetched exists=$skipped_exists date=$skipped_date 404=$skipped_404 errors=$errors"
    fi
done

log "Done!"
log "Fetched: $fetched"
log "Skipped (exists): $skipped_exists"
log "Skipped (date): $skipped_date"
log "Skipped (404): $skipped_404"
log "Errors: $errors"
