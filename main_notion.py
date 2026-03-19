import os
import re
import sys

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
from notion_client import Client as NotionClient

# --- Configuration ---
GITHUB_TRENDING_URL = "https://github.com/trending?since=daily"
LA_TZ = ZoneInfo("America/Los_Angeles")

TAG_RULES = {
    "llm-infra": [
        "llm", "inference", "serving", "tensorrt", "vllm", "sglang", "triton", "cuda",
        "kv cache", "flashattention", "pagedattention", "prefill", "speculative",
        "quantization", "fp8", "fp4"
    ],
    "data-tools": [
        "etl", "elt", "pipeline", "warehouse", "lakehouse", "duckdb", "dbt", "spark",
        "ray", "airflow", "dagster", "dataframe", "sql", "postgres", "clickhouse"
    ],
    "agents": [
        "agent", "agents", "tool", "tools", "workflow", "orchestration", "planner",
        "tool-calling", "mcp", "function calling"
    ],
    "skills": [
        "prompt", "eval", "evaluation", "benchmark", "guardrail", "observability",
        "tracing", "monitoring", "alignment"
    ],
    "rag": [
        "rag", "retrieval", "vector", "embedding", "rerank", "bm25"
    ],
    "observability": [
        "observability", "tracing", "telemetry", "monitoring", "logs", "metrics"
    ],
    "eval": [
        "eval", "evaluation", "benchmark", "leaderboard", "harness"
    ],
}

BLACKLIST_TERMS = [
    "awesome", "roadmap", "cheatsheet", "resume", "wallpaper", "interview",
    "leetcode", "system design", "curated list"
]

NOTION_DEFAULTS = {
    "Eval Metric": "🧐 Interesting",
    "Action": "Bookmark",
}

# --- Helpers ---
def la_today_yyyy_mm_dd() -> str:
    return datetime.now(LA_TZ).date().isoformat()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def text_norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def match_tags(text: str) -> list[str]:
    t = text_norm(text)
    matched = []
    for tag, rules in TAG_RULES.items():
        for r in rules:
            if r in t:
                matched.append(tag)
                break
    
    # De-dup while maintaining order
    seen = set()
    return [x for x in matched if not (x in seen or seen.add(x))]

def is_blacklisted(text: str) -> bool:
    t = text_norm(text)
    return any(b in t for b in BLACKLIST_TERMS)

def fetch_trending() -> list[dict]:
    resp = requests.get(GITHUB_TRENDING_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    for art in soup.select("article.Box-row"):
        h2 = art.select_one("h2.h3 a")
        if not h2: 
            continue
        repo_path = h2.get("href", "").strip("/")
        repo_full = repo_path.replace(" ", "")
        repo_url = f"https://github.com/{repo_full}"
        desc_el = art.select_one("p.col-9")
        desc = (desc_el.get_text(" ", strip=True) if desc_el else "").strip()
        lang_el = art.select_one('span[itemprop="programmingLanguage"]')
        language = (lang_el.get_text(strip=True) if lang_el else "").strip()
        items.append({
            "repo_full": repo_full,
            "repo_url": repo_url,
            "description": desc,
            "language": language,
        })
    return items

def filter_and_rank(repos: list[dict], limit: int = 5) -> list[dict]:
    scored = []
    for r in repos:
        blob = f"{r['repo_full']} {r.get('description','')} {r.get('language','')}"
        if is_blacklisted(blob): 
            continue
        
        tags = match_tags(blob)
        lang = text_norm(r.get("language", ""))
        is_python = (lang == "python")
        
        if not tags and not is_python: 
            continue

        # Score: Python + count of matched tags
        score = (3 if is_python else 0) + len(tags)
        r["tags"] = tags # Inject tags into the dict
        scored.append((score, r))
        
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item[1] for item in scored[:limit]]

# --- Notion Operations ---
def notion_client() -> NotionClient:
    token = os.environ.get("NOTION_TOKEN")
    if not token: 
        raise RuntimeError("Missing env NOTION_TOKEN")
    return NotionClient(auth=token)

def notion_db_id() -> str:
    db_id = os.environ.get("NOTION_DB_ID_GITHUB_TRENDING")
    if not db_id: 
        raise RuntimeError("Missing env NOTION_DB_ID_GITHUB_TRENDING")
    return db_id

def query_existing_urls_for_today(notion: NotionClient, db_id: str, today: str) -> set[str]:
    urls = set()
    cursor = None
    while True:
        payload = {
            "database_id": db_id,
            "page_size": 100,
            "filter": {"property": "Date", "date": {"equals": today}}
        }
        if cursor: 
            payload["start_cursor"] = cursor
        res = notion.databases.query(database_id=db_id, filter={...})
        for row in res.get("results", []):
            u = row.get("properties", {}).get("URL", {}).get("url")
            if u: 
                urls.add(u)
        if not res.get("has_more"): 
            break
        cursor = res.get("next_cursor")
    return urls

def create_notion_page(notion: NotionClient, db_id: str, today: str, repo: dict):
    primary_tag = repo["tags"][0] if repo.get("tags") else "python"
    title = f"[GitHub][{primary_tag}] {repo['repo_full']}"

    lang = repo.get("language") or "Other"
    allowed_langs = ["Python", "Jupyter Notebook", "TypeScript", "JavaScript", "Go", "Rust", "C++"]
    if lang not in allowed_langs: 
        lang = "Other"

    props = {
        "Title": {"title": [{"text": {"content": title}}]},
        "URL": {"url": repo["repo_url"]},
        "Date": {"date": {"start": today}},
        "Language": {"select": {"name": lang}},
        "Tags": {"multi_select": [{"name": t} for t in repo.get("tags", [])]},
        "Core Insight": {"rich_text": [{"text": {"content": repo.get("description", "")[:180]}}]},
        "Action": {"select": {"name": NOTION_DEFAULTS["Action"]}},
        "Eval Metric": {"select": {"name": NOTION_DEFAULTS["Eval Metric"]}},
    }

    notion.pages.create(
        parent={"database_id": db_id},
        properties=props,
        children=[{
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"text": {"content": f"Source: {repo['repo_url']}"}}]}
        }]
    )

def main():
    today = la_today_yyyy_mm_dd()
    print(f"[info] Start: {now_utc_iso()} | LA Today: {today}")
    
    repos = fetch_trending()
    selected = filter_and_rank(repos, limit=5)
    
    notion = notion_client()
    db_id = notion_db_id()
    existing_urls = query_existing_urls_for_today(notion, db_id, today)
    
    created = 0
    for r in selected:
        if r["repo_url"] in existing_urls:
            print(f"[skip] Already exists: {r['repo_full']}")
            continue
        create_notion_page(notion, db_id, today, r)
        print(f"[ok] Created: {r['repo_full']}")
        created += 1
    print(f"[done] Created: {created} | Skipped: {len(selected) - created}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(1)
