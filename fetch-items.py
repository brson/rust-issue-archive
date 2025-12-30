#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx"]
# ///
"""Fetch rust-lang/rust issues and PRs from GitHub API."""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

REPO = "rust-lang/rust"
ITEMS_DIR = Path("items")
CUTOFF_DATE = "2016-01-01T00:00:00Z"
RATE_LIMIT_BUFFER = 100
MAX_RETRIES = 5
BASE_BACKOFF = 2.0
JITTER = 0.25


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def padded(n: int) -> str:
    return f"{n:06d}"


def backoff_sleep(attempt: int) -> None:
    """Exponential backoff with jitter."""
    base = BASE_BACKOFF ** attempt
    jitter = base * JITTER * (2 * random.random() - 1)
    sleep_time = base + jitter
    log(f"  Backing off {sleep_time:.1f}s...")
    time.sleep(sleep_time)


class GitHubClient:
    def __init__(self) -> None:
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if not token:
            # Try to get token from gh CLI
            import subprocess
            try:
                result = subprocess.run(
                    ["gh", "auth", "token"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                token = result.stdout.strip()
            except (subprocess.CalledProcessError, FileNotFoundError):
                log("WARNING: No GitHub token found. Rate limits will be very low.")
                token = None

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        self.client = httpx.Client(
            base_url="https://api.github.com",
            headers=headers,
            timeout=30.0,
            follow_redirects=True,
        )
        self.rate_remaining: int | None = None
        self.rate_reset: int | None = None

    def close(self) -> None:
        self.client.close()

    def _update_rate_limit(self, response: httpx.Response) -> None:
        if "x-ratelimit-remaining" in response.headers:
            self.rate_remaining = int(response.headers["x-ratelimit-remaining"])
        if "x-ratelimit-reset" in response.headers:
            self.rate_reset = int(response.headers["x-ratelimit-reset"])

    def _check_rate_limit(self) -> None:
        if self.rate_remaining is not None and self.rate_remaining < RATE_LIMIT_BUFFER:
            if self.rate_reset:
                now = int(time.time())
                sleep_time = self.rate_reset - now + 5
                if sleep_time > 0:
                    log(f"Rate limit low ({self.rate_remaining} remaining). Sleeping {sleep_time}s...")
                    time.sleep(sleep_time)

    def _handle_rate_limit_response(self, response: httpx.Response) -> None:
        if self.rate_reset:
            now = int(time.time())
            sleep_time = self.rate_reset - now + 10
            if sleep_time > 0:
                log(f"Rate limited (HTTP {response.status_code}). Sleeping {sleep_time}s...")
                time.sleep(sleep_time)

    def fetch(self, endpoint: str) -> dict | list | None:
        """Fetch endpoint with retry logic. Returns None for 404."""
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.get(endpoint)
                self._update_rate_limit(response)

                if response.status_code == 404:
                    return None

                if response.status_code in (403, 429):
                    self._handle_rate_limit_response(response)
                    continue

                if response.status_code != 200:
                    log(f"  HTTP {response.status_code} (attempt {attempt + 1}/{MAX_RETRIES})")
                    backoff_sleep(attempt)
                    continue

                self._check_rate_limit()
                return response.json()

            except httpx.TimeoutException:
                log(f"  Timeout (attempt {attempt + 1}/{MAX_RETRIES})")
                backoff_sleep(attempt)
            except httpx.RequestError as e:
                log(f"  Request error: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
                backoff_sleep(attempt)
            except json.JSONDecodeError as e:
                log(f"  JSON parse error: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
                backoff_sleep(attempt)

        raise Exception(f"Failed after {MAX_RETRIES} attempts: {endpoint}")

    def fetch_paginated(self, endpoint: str) -> list:
        """Fetch all pages of a paginated endpoint."""
        all_items: list = []
        page = 1

        while True:
            sep = "&" if "?" in endpoint else "?"
            data = self.fetch(f"{endpoint}{sep}per_page=100&page={page}")

            if data is None:  # 404
                break

            if not isinstance(data, list):
                raise Exception(f"Expected list, got {type(data)}")

            if len(data) == 0:
                break

            all_items.extend(data)

            if len(data) < 100:
                break

            page += 1

        return all_items


def write_failed(path: Path, error: str, component: str) -> None:
    data = {
        "error": error,
        "component": component,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    path.write_text(json.dumps(data) + "\n")


def extract_xrefs(timeline: list) -> list:
    """Extract cross-references and commit references from timeline."""
    xrefs = []
    for event in timeline:
        if not isinstance(event, dict):
            continue

        event_type = event.get("event")
        actor = event.get("actor")
        actor_login = actor.get("login") if isinstance(actor, dict) else None

        if event_type == "cross-referenced":
            source = event.get("source") or {}
            issue = source.get("issue") or {}
            if issue.get("number"):
                xrefs.append({
                    "event": "cross-referenced",
                    "from": issue["number"],
                    "type": source.get("type", "issue"),
                    "actor": actor_login,
                    "date": event.get("created_at"),
                })

        elif event_type == "referenced":
            commit_id = event.get("commit_id")
            if commit_id:
                xrefs.append({
                    "event": "referenced",
                    "commit": commit_id,
                    "actor": actor_login,
                    "date": event.get("created_at"),
                })

    return xrefs


def discover_latest(client: GitHubClient) -> int:
    """Find the latest issue/PR number."""
    log("Discovering latest issue/PR number...")
    data = client.fetch(f"/repos/{REPO}/issues?state=all&sort=created&direction=desc&per_page=1")
    if not data or len(data) == 0:
        raise Exception("Could not discover latest issue number")
    number = data[0]["number"]
    log(f"Latest: #{number}")
    return number


def process_item(
    client: GitHubClient,
    num: int,
    do_main: bool,
    do_comments: bool,
    do_timeline: bool,
    do_xrefs: bool,
) -> dict:
    """Process a single item. Returns stats dict."""
    stats = {"fetched": 0, "skip_404": 0, "skip_date": 0, "skip_exists": 0, "failed": 0}
    prefix = padded(num)

    path_404 = ITEMS_DIR / f"{prefix}.404"
    path_skip = ITEMS_DIR / f"{prefix}.skip"
    path_main = ITEMS_DIR / f"{prefix}-main.json"
    path_main_failed = ITEMS_DIR / f"{prefix}-main.failed"
    path_comments = ITEMS_DIR / f"{prefix}-comments.json"
    path_comments_failed = ITEMS_DIR / f"{prefix}-comments.failed"
    path_timeline = ITEMS_DIR / f"{prefix}-timeline.json"
    path_timeline_failed = ITEMS_DIR / f"{prefix}-timeline.failed"
    path_xrefs = ITEMS_DIR / f"{prefix}-xrefs.json"
    path_xrefs_failed = ITEMS_DIR / f"{prefix}-xrefs.failed"

    status_parts = []

    # Check global skip states
    if path_404.exists():
        stats["skip_404"] = 1
        return stats
    if path_skip.exists():
        stats["skip_date"] = 1
        return stats

    # Main
    if do_main:
        if path_main.exists():
            status_parts.append("main=EXISTS")
            stats["skip_exists"] += 1
        else:
            try:
                data = client.fetch(f"/repos/{REPO}/issues/{num}")

                if data is None:
                    path_404.write_text("")
                    log(f"#{prefix} 404")
                    stats["skip_404"] = 1
                    return stats

                created_at = data.get("created_at", "")
                if created_at >= CUTOFF_DATE:
                    path_skip.write_text("")
                    log(f"#{prefix} skip (created {created_at[:10]})")
                    stats["skip_date"] = 1
                    return stats

                item_type = "pr" if "pull_request" in data else "issue"
                data["_meta"] = {"type": item_type}

                path_main.write_text(json.dumps(data, indent=2) + "\n")
                path_main_failed.unlink(missing_ok=True)
                status_parts.append(f"main=OK ({item_type})")
                stats["fetched"] += 1

            except Exception as e:
                write_failed(path_main_failed, str(e), "main")
                status_parts.append(f"main=FAIL")
                stats["failed"] += 1
                log(f"#{prefix} {' '.join(status_parts)}")
                return stats

    # Need main data for comments/timeline/xrefs; read it if we didn't just fetch
    if (do_comments or do_timeline or do_xrefs) and not do_main:
        if not path_main.exists():
            # Can't fetch comments/timeline/xrefs without main
            return stats

    # Comments
    if do_comments:
        if path_comments.exists():
            status_parts.append("comments=EXISTS")
            stats["skip_exists"] += 1
        else:
            try:
                comments = client.fetch_paginated(f"/repos/{REPO}/issues/{num}/comments")
                path_comments.write_text(json.dumps(comments, indent=2) + "\n")
                path_comments_failed.unlink(missing_ok=True)
                status_parts.append(f"comments=OK ({len(comments)})")
                stats["fetched"] += 1
            except Exception as e:
                write_failed(path_comments_failed, str(e), "comments")
                status_parts.append("comments=FAIL")
                stats["failed"] += 1

    # Timeline
    if do_timeline:
        if path_timeline.exists():
            status_parts.append("timeline=EXISTS")
            stats["skip_exists"] += 1
        else:
            try:
                timeline = client.fetch_paginated(f"/repos/{REPO}/issues/{num}/timeline")
                path_timeline.write_text(json.dumps(timeline, indent=2) + "\n")
                path_timeline_failed.unlink(missing_ok=True)
                status_parts.append(f"timeline=OK ({len(timeline)})")
                stats["fetched"] += 1
            except Exception as e:
                write_failed(path_timeline_failed, str(e), "timeline")
                status_parts.append("timeline=FAIL")
                stats["failed"] += 1

    # Xrefs (extracted from timeline)
    if do_xrefs:
        if path_xrefs.exists():
            status_parts.append("xrefs=EXISTS")
            stats["skip_exists"] += 1
        else:
            try:
                timeline = client.fetch_paginated(f"/repos/{REPO}/issues/{num}/timeline")
                xrefs = extract_xrefs(timeline)
                path_xrefs.write_text(json.dumps(xrefs, indent=2) + "\n")
                path_xrefs_failed.unlink(missing_ok=True)
                status_parts.append(f"xrefs=OK ({len(xrefs)})")
                stats["fetched"] += 1
            except Exception as e:
                write_failed(path_xrefs_failed, str(e), "xrefs")
                status_parts.append("xrefs=FAIL")
                stats["failed"] += 1

    if status_parts:
        log(f"#{prefix} {' '.join(status_parts)}")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch rust-lang/rust issues and PRs")
    parser.add_argument("--start", type=int, help="Start issue number (required unless --discover)")
    parser.add_argument("--end", type=int, help="End issue number (required unless --discover)")
    parser.add_argument("--main", dest="main", action="store_true", default=True)
    parser.add_argument("--no-main", dest="main", action="store_false")
    parser.add_argument("--comments", dest="comments", action="store_true", default=True)
    parser.add_argument("--no-comments", dest="comments", action="store_false")
    parser.add_argument("--timeline", dest="timeline", action="store_true", default=False)
    parser.add_argument("--xrefs", dest="xrefs", action="store_true", default=False)
    parser.add_argument("--discover", action="store_true", help="Print latest issue number and exit")
    args = parser.parse_args()

    ITEMS_DIR.mkdir(exist_ok=True)
    client = GitHubClient()

    try:
        if args.discover:
            latest = discover_latest(client)
            print(latest)
            return

        if args.start is None or args.end is None:
            parser.error("--start and --end are required")

        log(f"Fetching #{args.start} to #{args.end}")
        log(f"Components: main={args.main} comments={args.comments} timeline={args.timeline} xrefs={args.xrefs}")
        log(f"Cutoff date: {CUTOFF_DATE}")

        totals = {"fetched": 0, "skip_404": 0, "skip_date": 0, "skip_exists": 0, "failed": 0}

        for num in range(args.start, args.end + 1):
            stats = process_item(client, num, args.main, args.comments, args.timeline, args.xrefs)
            for k, v in stats.items():
                totals[k] += v

            if num % 100 == 0:
                log(f"Progress: {num}/{args.end} | fetched={totals['fetched']} "
                    f"exists={totals['skip_exists']} date={totals['skip_date']} "
                    f"404={totals['skip_404']} failed={totals['failed']}")

        log("Done!")
        log(f"Fetched: {totals['fetched']}")
        log(f"Skipped (exists): {totals['skip_exists']}")
        log(f"Skipped (date): {totals['skip_date']}")
        log(f"Skipped (404): {totals['skip_404']}")
        log(f"Failed: {totals['failed']}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
